import asyncio
import json
import os
import time
from typing import Any

import aiohttp
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from flowcore_story.adapters.factory import get_adapter
from flowcore_story.config import config as app_config
from flowcore_story.config.config import PROXIES_FILE, PROXIES_FOLDER
from flowcore_story.config.proxy_provider import load_proxies, shuffle_proxies
from flowcore_story.core.config_loader import apply_env_overrides
from flowcore_story.core.crawl_planner import CategoryCrawlPlan
from flowcore_story.utils.io_utils import create_proxy_template_if_not_exists
from flowcore_story.utils.kafka_ssl import build_ssl_context
from flowcore_story.utils.logger import logger
from flowcore_story.utils.progress_emitter import emit_progress_event
from flowcore_story.workers.crawler_missing_chapter import loop_once_multi_sites
from flowcore_story.workers.crawler_single_missing_chapter import crawl_single_story_worker
from flowcore_story.workers.retry_failed_chapters import retry_single_chapter

_MAIN_MODULE = None
_MAIN_IMPORT_ERROR: BaseException | None = None


def _get_main_module():
    """Lazily import the main application module to avoid hard dependency during tests."""
    global _MAIN_MODULE, _MAIN_IMPORT_ERROR
    if _MAIN_MODULE is not None:
        return _MAIN_MODULE
    if _MAIN_IMPORT_ERROR is not None:  # pragma: no cover - cached error path
        raise RuntimeError("main module unavailable") from _MAIN_IMPORT_ERROR
    try:
        from flowcore_story.apps import main as app_main
    except Exception as exc:  # pragma: no cover - optional dependency
        _MAIN_IMPORT_ERROR = exc
        raise RuntimeError("main module unavailable") from exc
    _MAIN_MODULE = app_main
    return _MAIN_MODULE


def _require_main_attr(name: str):
    module = _get_main_module()
    try:
        return getattr(module, name)
    except AttributeError as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"main module missing attribute '{name}'") from exc

# ==== Job Dispatcher Mapping ====

async def _no_op(**_: dict) -> None:
    """Ignore non-job messages that may be mirrored on the job topic."""
    return None


def _is_meaningful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    if isinstance(value, (list, tuple, set, dict)) and not value:
        return False
    return True


def _extract_job_id(job: dict[str, Any]) -> str | None:
    for key in ("job_id", "id", "uuid", "task_id"):
        value = job.get(key)
        if not _is_meaningful(value):
            continue
        try:
            return str(value)
        except Exception:
            continue
    return None


def _build_worker_payload(job_type: str, job: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job_type": job_type,
        "job_id": _extract_job_id(job),
        "site_key": job.get("site_key"),
        "source": job.get("source"),
        "priority": job.get("priority"),
        "category_id": job.get("category_id") or job.get("genre_url"),
        "category_name": job.get("category_name")
        or job.get("genre_name")
        or job.get("genre"),
        "story_title": job.get("title") or job.get("story_title"),
        "story_url": job.get("url") or job.get("story_url"),
        "scheduled_for": job.get("scheduled_for"),
        "retry_count": job.get("retry_count"),
        "part_index": job.get("part_index"),
        "total_parts": job.get("total_parts"),
        "plan_size": job.get("total") or job.get("total_stories"),
        "plan_processed": job.get("processed_stories"),
    }

    extra_metadata = job.get("metadata")
    if isinstance(extra_metadata, dict) and extra_metadata:
        condensed_metadata = {
            key: value
            for key, value in extra_metadata.items()
            if _is_meaningful(value)
        }
        if condensed_metadata:
            payload["metadata"] = condensed_metadata

    return {key: value for key, value in payload.items() if _is_meaningful(value)}


def _emit_worker_event(
    action: str,
    job_type: str,
    job: dict[str, Any],
    *,
    started_at: float | None = None,
    error: BaseException | None = None,
    status: str | None = None,
) -> None:
    payload = _build_worker_payload(job_type, job)
    payload["action"] = action
    if status:
        payload["status"] = status
    if started_at is not None and action in {"succeeded", "failed"}:
        payload["duration"] = max(time.time() - started_at, 0.0)
    if error is not None:
        payload["error"] = str(error)
        payload["error_type"] = error.__class__.__name__
    emit_progress_event("worker", payload)


def _skip_disabled_site(job_type: str, job: dict[str, Any]) -> bool:
    site_key = job.get("site_key")
    if not site_key:
        return False
    if site_key in app_config.BASE_URLS:
        return False
    logger.info(
        "[Kafka] Bỏ qua job `%s` cho site `%s` (không có trong ENABLED_SITE_KEYS).",
        job_type,
        site_key,
    )
    _emit_worker_event("skipped", job_type, job, status="site_disabled")
    return True


async def _missing_check_handler(*, force_unskip: bool = False, **_ignored) -> None:
    """Run the missing chapter sweep while ignoring extraneous metadata."""
    await loop_once_multi_sites(force_unskip=force_unskip)


async def _single_story_handler(**job):
    title = job.get("title")
    url = job.get("url")
    site_key = job.get("site_key")
    genre_name = job.get("genre_name")

    if _skip_disabled_site("single_story", job):
        return

    if title:
        run_single_story = _require_main_attr("run_single_story")
        return await run_single_story(title=title, site_key=site_key, genre_name=genre_name)

    if url:
        logger.info(
            "[Kafka] 🔁 single_story fallback: không có title, sử dụng URL để crawl",
        )
        return await crawl_single_story_worker(story_url=url, title=title)

    logger.error("[Kafka] Thiếu cả `title` và `url` trong job `single_story`.")


async def _retry_chapter_handler(**job):
    """Handle retry_chapter with site filtering."""
    # Note: retry_chapter uses 'site' key instead of 'site_key'
    if "site" in job and "site_key" not in job:
        job["site_key"] = job["site"]
    if _skip_disabled_site("retry_chapter", job):
        return
    return await retry_single_chapter(job)


async def _healthcheck_handler(**job):
    """Handle healthcheck with site filtering."""
    site_key = job.get("site_key")
    if not site_key:
        logger.error("[Kafka] Thiếu `site_key` trong job `healthcheck`.")
        return
    if _skip_disabled_site("healthcheck", job):
        return
    return await healthcheck_adapter(site_key=site_key)


async def _check_missing_chapters_handler(**job):
    """Handle check_missing_chapters with site filtering."""
    if _skip_disabled_site("check_missing_chapters", job):
        return
    return await crawl_single_story_worker(
        story_folder_path=job.get("story_folder_path"),
        site_key=job.get("site_key"),
    )


async def escalate_to_sticky_worker(job: dict, reason: str):
    """Republish the job to Kafka with force_sticky=True to trigger the Sticky Worker."""
    job_copy = dict(job)
    job_copy["force_sticky"] = True
    job_copy["escalation_reason"] = reason
    
    logger.warning(
        f"[Kafka] 🚨 Escalating job to Sticky Worker: {job_copy.get('story_title')} "
        f"(Reason: {reason})"
    )
    
    producer = AIOKafkaProducer(bootstrap_servers=app_config.KAFKA_BOOTSTRAP_SERVERS)
    try:
        await producer.start()
        await producer.send_and_wait(
            app_config.KAFKA_TOPIC, 
            json.dumps(job_copy).encode("utf-8")
        )
        logger.info("[Kafka] ✅ Escalation successful.")
    except Exception as e:
        logger.error(f"[Kafka] Failed to escalate job: {e}")
    finally:
        await producer.stop()

async def _crawl_story_handler(**job):
    """Handle crawl_story job from queue dispatcher.

    This processes stories that were queued in PostgreSQL and dispatched via Kafka.
    Expected payload keys:
    - queue_id: ID in story_queue table
    - site_key: Site to crawl from
    - story_url: URL of the story
    - story_title: Title of the story
    - genre_name, genre_url: Category info
    - chapter_count: Expected chapter count (optional)
    """
    
    # 1. Skip if this is a sticky job (Standard workers shouldn't touch these)
    if job.get("force_sticky"):
        return

    from flowcore_story.storage.db_pool import get_db_pool

    queue_id = job.get("queue_id")
    site_key = job.get("site_key")
    story_url = job.get("story_url")
    story_title = job.get("story_title")
    genre_name = job.get("genre_name")

    if not story_url:
        logger.error("[Kafka] Thiếu `story_url` trong job `crawl_story`.")
        return

    if _skip_disabled_site("crawl_story", job):
        # Don't update queue status when skipping - let the correct consumer handle it
        # The xtruyen consumer (in separate group) will receive this job and process it
        return

    logger.info(
        f"[Kafka] 📖 Crawling story from queue: {story_title or story_url[:50]}... "
        f"(queue_id={queue_id}, site={site_key})"
    )

    try:
        # Use crawl_single_story_worker to process the story
        await crawl_single_story_worker(story_url=story_url, title=story_title)

        # Mark as completed in queue
        if queue_id:
            pool = await get_db_pool()
            if pool:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE story_queue SET status = 'completed', updated_at = NOW() WHERE id = $1",
                        queue_id,
                    )
        logger.info(f"[Kafka] ✅ Completed crawl_story: {story_title or story_url[:50]}...")

    except Exception as e:
        error_msg = str(e).lower()
        # Detect Anti-bot related errors to trigger escalation
        is_antibot = any(x in error_msg for x in [
            "403", "forbidden", "cloudflare", "captcha", "timeout", 
            "connection closed", "reset", "anti-bot"
        ])
        
        if is_antibot:
            logger.warning(f"[Kafka] 🛡️ Anti-bot/Network error detected for {story_url}: {e}")
            # Trigger Auto-Escalation to Sticky Worker
            await escalate_to_sticky_worker(job, str(e))
            # We treat this as "handled" by escalation, so we don't mark as failed in DB yet.
            # The sticky worker will retry.
            return

        logger.error(f"[Kafka] ❌ Failed crawl_story {story_url}: {e}")
        # Mark as failed in queue only if NOT escalated
        if queue_id:
            pool = await get_db_pool()
            if pool:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE story_queue SET status = 'failed', last_error = $2, updated_at = NOW() WHERE id = $1",
                        queue_id,
                        str(e)[:500],
                    )
        raise


WORKER_HANDLERS = {
    "single_story": _single_story_handler,
    "missing_check": _missing_check_handler,
    "healthcheck": _healthcheck_handler,
    "retry_chapter": _retry_chapter_handler,
    "check_missing_chapters": _check_missing_chapters_handler,
    "crawl_story": _crawl_story_handler,
    # No-op handler to avoid error logs when plan summaries are mirrored to the main topic
    "plan_summary": _no_op,
    # Refresh a specific category by chunks (scheduled by planner)
    "category_batch_refresh": lambda **job: _category_batch_refresh_handler(**job),
}


async def _category_batch_refresh_handler(**job):
    """Handle a category batch refresh job produced by the planner.

    Expected payload keys:
    - type: "category_batch_refresh"
    - site_key, category_id, category_name, category_url
    - stories: List[story dicts]
    - metadata/raw_genre: optional passthrough
    - part_index/total_parts: optional for logging
    - change_summary: optional
    """
    site_key = job.get("site_key")
    if not site_key:
        logger.error("[Kafka] Thiếu `site_key` trong job `category_batch_refresh`.")
        return

    if _skip_disabled_site("category_batch_refresh", job):
        return

    adapter = get_adapter(site_key)


    name = job.get("category_name") or (job.get("metadata") or {}).get("name") or "Unknown"
    url = job.get("category_url") or (job.get("metadata") or {}).get("url") or ""
    stories = list(job.get("stories") or [])
    metadata = dict(job.get("metadata") or {})
    raw_genre = dict(job.get("raw_genre") or {})

    # Build a minimal CategoryCrawlPlan so we can reuse the existing flow
    category_plan = CategoryCrawlPlan(
        name=name,
        url=url,
        stories=stories,
        planned_story_total=len(stories) if stories else None,
        metadata=metadata,
        raw_genre=raw_genre,
    )

    crawl_state = {}
    process_genre_with_limit = _require_main_attr("process_genre_with_limit")
    async with aiohttp.ClientSession() as session:
        await process_genre_with_limit(
            session,
            category_plan,
            crawl_state,
            adapter,
            site_key,
        )


async def dispatch_job(job: dict):
    apply_env_overrides(job)
    job_type = job.get("type")

    if not job_type:
        logger.warning("[Kafka] Không tìm thấy type trong message.")
        _emit_worker_event("invalid", "unknown", job, status="missing_type")
        return

    _emit_worker_event("received", job_type, job)

    if job_type == "retry_failed_genres":
        site_key = job.get("site_key")
        if not site_key:
            logger.error("[Kafka] Thiếu `site_key` trong job `retry_failed_genres`.")
            _emit_worker_event(
                "invalid",
                job_type,
                job,
                status="missing_site_key",
            )
            return

        if _skip_disabled_site("retry_failed_genres", job):
            return

        WorkerSettings = _require_main_attr("WorkerSettings")
        retry_failed_genres_fn = _require_main_attr("retry_failed_genres")

        settings = WorkerSettings(

            genre_batch_size=app_config.GENRE_BATCH_SIZE,
            genre_async_limit=app_config.GENRE_ASYNC_LIMIT,
            proxies_file=app_config.PROXIES_FILE,
            failed_genres_file=app_config.FAILED_GENRES_FILE,
            retry_genre_round_limit=app_config.RETRY_GENRE_ROUND_LIMIT,
            retry_sleep_seconds=app_config.RETRY_SLEEP_SECONDS,
        )
        started_at = time.time()
        _emit_worker_event("started", job_type, job)
        try:
            await retry_failed_genres_fn(
                get_adapter(site_key), site_key, settings, shuffle_proxies
            )
        except Exception as exc:
            _emit_worker_event(
                "failed",
                job_type,
                job,
                started_at=started_at,
                error=exc,
            )
            raise
        else:
            _emit_worker_event(
                "succeeded",
                job_type,
                job,
                started_at=started_at,
            )
        return

    elif job_type == "full_site":
        site_key = job.get("site_key")
        crawl_mode = job.get("crawl_mode")
        if not site_key:
            logger.error("[Kafka] Thiếu `site_key` trong job `full_site`.")
            _emit_worker_event(
                "invalid",
                job_type,
                job,
                status="missing_site_key",
            )
            return

        if _skip_disabled_site("full_site", job):
            return

        run_single_site = _require_main_attr("run_single_site")
        started_at = time.time()

        _emit_worker_event("started", job_type, job)
        try:
            await run_single_site(site_key=site_key, crawl_mode=crawl_mode)
        except Exception as exc:
            _emit_worker_event(
                "failed",
                job_type,
                job,
                started_at=started_at,
                error=exc,
            )
            raise
        else:
            _emit_worker_event(
                "succeeded",
                job_type,
                job,
                started_at=started_at,
            )
        return

    elif job_type == "all_sites":
        crawl_mode = job.get("crawl_mode")
        run_all_sites = _require_main_attr("run_all_sites")
        started_at = time.time()
        _emit_worker_event("started", job_type, job)
        try:
            await run_all_sites(crawl_mode=crawl_mode)
        except Exception as exc:
            _emit_worker_event(
                "failed",
                job_type,
                job,
                started_at=started_at,
                error=exc,
            )
            raise
        else:
            _emit_worker_event(
                "succeeded",
                job_type,
                job,
                started_at=started_at,
            )
        return

    handler = WORKER_HANDLERS.get(job_type)
    if not handler:
        logger.error(f"[Kafka] Không hỗ trợ job type: {job_type}")
        _emit_worker_event("unsupported", job_type, job)
        return

    logger.info(f"[Kafka] 🔧 Đang xử lý job `{job_type}` với data: {job}")
    started_at = time.time()
    _emit_worker_event("started", job_type, job)
    try:
        await handler(**job)
    except TypeError as te:
        _emit_worker_event(
            "failed",
            job_type,
            job,
            started_at=started_at,
            error=te,
        )
        logger.error(f"[DISPATCH] Lỗi gọi hàm `{handler.__name__}` với kwargs: {te}")
    except Exception as ex:
        _emit_worker_event(
            "failed",
            job_type,
            job,
            started_at=started_at,
            error=ex,
        )
        logger.exception(f"[Kafka] ❌ Lỗi khi xử lý job `{job_type}`: {ex}")
    else:
        _emit_worker_event(
            "succeeded",
            job_type,
            job,
            started_at=started_at,
        )

async def consume():
    try:
        await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
        await load_proxies(PROXIES_FILE)
        if app_config.LOADED_PROXIES:
            logger.info("[Proxy] Kafka dispatcher loaded %d proxies.", len(app_config.LOADED_PROXIES))
        else:
            logger.warning("[Proxy] Kafka dispatcher has no proxies; requests will be direct.")
    except Exception as exc:
        logger.warning("[Proxy] Kafka dispatcher failed to load proxies: %s", exc)

    # Support multiple topics
    env_topics = os.environ.get("KAFKA_TOPICS")
    if env_topics:
        topics = [t.strip() for t in env_topics.split(",") if t.strip()]
    else:
        topics = [app_config.KAFKA_TOPIC]

    logger.info(
        f"[Kafka] 🔌 Kết nối đến Kafka tại {app_config.KAFKA_BOOTSTRAP_SERVERS} | topics={topics}"
    )
    max_retries = app_config.KAFKA_BOOTSTRAP_MAX_RETRIES
    retry_delay = app_config.KAFKA_BOOTSTRAP_RETRY_DELAY
    attempt = 0
    consumer = None

    kafka_auth_config: dict[str, Any] = {}
    # ... (rest of auth config remains the same)
    protocol_raw = getattr(app_config, "KAFKA_SECURITY_PROTOCOL", None)
    protocol = protocol_raw.upper() if protocol_raw else None
    if protocol:
        kafka_auth_config["security_protocol"] = protocol

        if protocol in {"SSL", "SASL_SSL"}:
            ca_file = getattr(app_config, "KAFKA_SSL_CA_FILE", None)
            ssl_context = build_ssl_context(
                ca_file,
                verify=getattr(app_config, "KAFKA_SSL_VERIFY", True),
            )
            if ssl_context:
                kafka_auth_config["ssl_context"] = ssl_context
            else:
                raise ValueError(
                    f"Kafka security protocol is {protocol}, but failed to create SSL context. "
                    "Check KAFKA_SSL_CA_FILE config or disable verification via KAFKA_SSL_VERIFY=0."
                )

        if protocol in {"SASL_SSL", "SASL_PLAINTEXT"}:
            missing = [
                name
                for name, value in (
                    ("KAFKA_SASL_MECHANISM", app_config.KAFKA_SASL_MECHANISM),
                    ("KAFKA_USERNAME", app_config.KAFKA_USERNAME),
                    ("KAFKA_PASSWORD", app_config.KAFKA_PASSWORD),
                )
                if not value
            ]
            if missing:
                missing_vars = ", ".join(missing)
                raise ValueError(
                    f"Kafka security protocol is set to {protocol} but the following environment "
                    f"variables are missing: {missing_vars}. Either provide the credentials or set "
                    "KAFKA_SECURITY_PROTOCOL=PLAINTEXT for the internal broker."
                )

            kafka_auth_config["sasl_mechanism"] = app_config.KAFKA_SASL_MECHANISM
            kafka_auth_config["sasl_plain_username"] = app_config.KAFKA_USERNAME
            kafka_auth_config["sasl_plain_password"] = app_config.KAFKA_PASSWORD

    while True:
        attempt += 1
        consumer = AIOKafkaConsumer(
            *topics,  # Subscribe to all topics
            bootstrap_servers=app_config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id=app_config.KAFKA_GROUP_ID,
            session_timeout_ms=30000,
            max_poll_interval_ms=3600000,  # 60 phút
            **kafka_auth_config
        )
        try:
            await consumer.start()
            if attempt > 1:
                logger.info(f"[Kafka] Kết nối Kafka thành công sau {attempt} lần thử.")
            break
        except KafkaConnectionError as ex:
            logger.warning(f"[Kafka] Không kết nối được Kafka (attempt {attempt}): {ex}")
            await consumer.stop()
            if max_retries and attempt >= max_retries:
                logger.error("[Kafka] Vượt quá số lần retry kết nối Kafka. Thử lại sau.")
                raise
            await asyncio.sleep(retry_delay)
        except Exception:
            await consumer.stop()
            raise

    try:
        logger.info(f"[Kafka] Đang lắng nghe jobs trên topics `{topics}`...")
        async for msg in consumer:
            job = msg.value
            await dispatch_job(job)  # type: ignore
    except Exception as ex:
        logger.exception(f"[Kafka] Lỗi toàn cục trong consumer: {ex}")
    finally:
        if consumer:
            await consumer.stop()
        logger.info("[Kafka] Đã dừng consumer.")



async def healthcheck_adapter(site_key: str):
    adapter = get_adapter(site_key)
    try:
        genres = await adapter.get_genres()
        if not genres or len(genres) == 0:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy genres FAIL hoặc rỗng!")
            return False
        genre = genres[0]
        stories = await adapter.get_stories_in_genre(genre['title'],genre['url'], )
        if not stories or len(stories) == 0:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy stories FAIL hoặc rỗng!")
            return False
        story = stories[0]
        details = await adapter.get_story_details(story['url'], story['title']) #type: ignore
        if not details or "title" not in details:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy details FAIL hoặc thiếu field!")
            return False
        chapters = await adapter.get_chapter_list(story['url'], story['title'], site_key)#type: ignore
        if not chapters or len(chapters) == 0:
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy chapters FAIL hoặc rỗng!")
            return False
        chap = chapters[0]
        content = await adapter.get_chapter_content(chap['url'], chap['title'], site_key)
        if not content or len(content) < 50: #type: ignore
            logger.error(f"[HEALTHCHECK] {site_key}: Lấy nội dung chương FAIL hoặc rỗng!")
            return False
        logger.info(f"[HEALTHCHECK] {site_key}: OK")
        return True
    except Exception as ex:
        logger.error(f"[HEALTHCHECK] {site_key}: Exception: {ex}")
        return False

if __name__ == "__main__":
    asyncio.run(consume())

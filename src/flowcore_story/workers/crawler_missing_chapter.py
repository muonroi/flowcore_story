import asyncio
import datetime
import hashlib
import json
import os
import re
import shutil
import time
import traceback
from dataclasses import dataclass
from typing import Any, cast

from filelock import FileLock, Timeout

from flowcore_story.adapters.factory import get_adapter
from flowcore_story.apps.scraper import initialize_scraper
from flowcore_story.config import config as app_config
from flowcore_story.config.config import (
    BASE_URLS,
    COMPLETED_FOLDER,
    DATA_FOLDER,
    PROXIES_FILE,
    PROXIES_FOLDER,
)
from flowcore_story.config.proxy_provider import load_proxies
from flowcore.utils.async_primitives import LoopBoundSemaphore
from flowcore.utils.batch_utils import smart_delay
from flowcore.utils.cache_utils import cached_get_chapter_list, cached_get_story_details
from flowcore.utils.chapter_utils import (
    SEM,
    count_dead_chapters,
    count_txt_files,
    crawl_missing_chapters_for_story,
    export_chapter_metadata_sync,
    extract_real_chapter_number,
    get_actual_chapters_for_export,
    get_chapter_filename,
    get_missing_chapters,
    get_real_total_chapters,
    mark_dead_chapter,
)
from flowcore.utils.dead_letter_queue import send_to_dlq
from flowcore.utils.domain_rate_limiter import domain_circuit_breaker
from flowcore.utils.domain_utils import get_site_key_from_url, is_url_for_site
from flowcore.utils.io_utils import create_proxy_template_if_not_exists, move_story_to_completed
from flowcore.utils.logger import logger
from flowcore.utils.meta_utils import get_primary_category_name, normalize_categories_field
from flowcore.utils.notifier import send_telegram_notify
from flowcore.utils.progress_emitter import compact_payload, emit_progress_event
from flowcore.utils.state_utils import get_missing_worker_state_file, load_crawl_state, save_crawl_state

auto_fixed_titles: list[str] = []
MAX_CONCURRENT_STORIES = 3
STORY_SEM = LoopBoundSemaphore(MAX_CONCURRENT_STORIES)
MISSING_SUMMARY_LOG = "missing_summary.log"
MAX_SOURCE_TIMEOUT_RETRY = 3
LOADED_PROXIES = getattr(app_config, "LOADED_PROXIES", [])

# Config for special stories queue
SPECIAL_STORIES_TOPIC = os.environ.get("SPECIAL_STORIES_TOPIC", "storyflow.special")
SPECIAL_STORY_MAX_NO_PROGRESS = int(os.environ.get("SPECIAL_STORY_MAX_NO_PROGRESS", "3"))


async def send_to_special_queue(metadata: dict, story_folder: str, site_key: str, reason: str, missing_count: int, total_count: int):
    """Gửi truyện đặc biệt vào queue riêng để xử lý thủ công hoặc bởi worker chuyên dụng."""
    from flowcore.utils.kafka_producer import get_kafka_producer

    try:
        producer = await get_kafka_producer()
        if not producer:
            logger.error("[SPECIAL] Không thể gửi vào queue - producer không khả dụng")
            return False

        payload = {
            "type": "special_story",
            "title": metadata.get("title"),
            "url": metadata.get("url"),
            "story_folder": os.path.basename(story_folder),
            "site_key": site_key,
            "reason": reason,
            "missing_count": missing_count,
            "total_count": total_count,
            "completion_percent": round((total_count - missing_count) / total_count * 100, 1) if total_count > 0 else 0,
            "timestamp": time.time(),
        }

        await producer.send_and_wait(SPECIAL_STORIES_TOPIC, payload)
        logger.info(
            f"[SPECIAL] Đã gửi '{metadata.get('title')}' vào queue đặc biệt: {reason} "
            f"({total_count - missing_count}/{total_count} chương = {payload['completion_percent']}%)"
        )

        # Gửi notification qua Telegram
        await send_telegram_notify(
            f"📚 *Truyện đặc biệt cần xử lý*\n\n"
            f"📖 *{metadata.get('title')}*\n"
            f"🔗 Site: {site_key}\n"
            f"❌ Lý do: {reason}\n"
            f"📊 Tiến độ: {total_count - missing_count}/{total_count} chương ({payload['completion_percent']}%)\n"
            f"📁 Folder: `{os.path.basename(story_folder)}`"
        )

        return True
    except Exception as exc:
        logger.exception(f"[SPECIAL] Lỗi khi gửi '{metadata.get('title')}' vào queue: {exc}")
        return False


async def verify_chapter_missing_on_site(adapter, chapter_url: str, site_key: str) -> bool:
    """Kiểm tra xem chapter có thực sự missing trên site hay không (404, deleted, etc)."""
    try:
        # Fetch chapter để kiểm tra
        from flowcore.utils.chapter_utils import fetch_with_retry

        result = await fetch_with_retry(
            adapter,
            chapter_url,
            site_key,
            max_retries=2,
            timeout=15,
        )

        if result is None:
            return True  # Chapter thực sự missing

        # Kiểm tra content có valid không
        content = result.get("content", "") if isinstance(result, dict) else ""
        if not content or len(content.strip()) < 100:
            return True  # Content quá ngắn, coi như missing

        return False  # Chapter tồn tại và có content

    except Exception as exc:
        logger.warning(f"[VERIFY] Không thể verify chapter {chapter_url}: {exc}")
        return False  # Không chắc chắn, không đánh dấu missing

_MISSING_TERMINAL_ACTIONS = {"succeeded", "failed", "completed"}


async def move_completed_story_immediately(
    story_folder: str,
    metadata: dict,
    crawl_state: dict,
    state_file: str,
    site_key: str,
) -> bool:
    """Move story to completed folder immediately after it has all chapters.

    Returns True if move was successful, False otherwise.
    """
    try:
        chapter_count = count_txt_files(story_folder)
        dead_count = count_dead_chapters(story_folder)
        real_total = metadata.get("total_chapters_on_site", 0)

        if real_total <= 0:
            logger.warning(f"[IMMEDIATE MOVE] '{metadata.get('title')}' has invalid total: {real_total}")
            return False

        if chapter_count + dead_count < real_total:
            logger.info(
                f"[IMMEDIATE MOVE] '{metadata.get('title')}' not ready: {chapter_count}+{dead_count}/{real_total}"
            )
            return False

        genre_name = get_primary_category_name(metadata)

        logger.info(
            f"[IMMEDIATE MOVE] Moving '{metadata.get('title')}' to completed/{genre_name} "
            f"({chapter_count}+{dead_count}/{real_total})"
        )

        move_success = await move_story_to_completed(story_folder, genre_name)

        if move_success:
            story_url = metadata.get("url")
            if story_url:
                completed_urls = set(crawl_state.get("globally_completed_story_urls", []))
                completed_urls.add(story_url)
                crawl_state["globally_completed_story_urls"] = sorted(completed_urls)
                await save_crawl_state(crawl_state, state_file, site_key=site_key)

            logger.info(
                f"[IMMEDIATE MOVE] Successfully moved '{metadata.get('title')}' to completed/{genre_name}"
            )
            return True
        else:
            logger.warning(f"[IMMEDIATE MOVE] Failed to move '{metadata.get('title')}'")
            return False

    except Exception as exc:
        logger.exception(f"[IMMEDIATE MOVE] Error moving '{metadata.get('title')}': {exc}")
        return False


def _parse_shard_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "[MISSING][SHARD] Invalid value '%s' for %s; using default %s",
            raw,
            name,
            default,
        )
        return default
    return value


WORKER_SHARD_TOTAL = max(_parse_shard_env("MISSING_WORKER_SHARD_TOTAL", 1), 1)
_raw_shard_index = _parse_shard_env("MISSING_WORKER_SHARD_INDEX", 0)
WORKER_SHARD_INDEX = _raw_shard_index % WORKER_SHARD_TOTAL if WORKER_SHARD_TOTAL else 0

if _raw_shard_index != WORKER_SHARD_INDEX:
    logger.warning(
        "[MISSING][SHARD] Normalized shard index from %s to %s (total=%s)",
        _raw_shard_index,
        WORKER_SHARD_INDEX,
        WORKER_SHARD_TOTAL,
    )

WORKER_INSTANCE_ID = (
    os.getenv("MISSING_WORKER_ID")
    or os.getenv("WORKER_INSTANCE_ID")
    or f"missing-worker-{WORKER_SHARD_INDEX}"
)


def _emit_missing_event(
    action: str,
    scope: str,
    *,
    site_key: str | None = None,
    started_at: float | None = None,
    status: str | None = None,
    error: BaseException | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "action": action,
        "scope": scope,
        "site_key": site_key,
        "status": status,
        "timestamp": time.time(),
    }

    if started_at is not None and action in _MISSING_TERMINAL_ACTIONS:
        payload["duration"] = max(time.time() - started_at, 0.0)

    if extra:
        payload.update(extra)

    if error is not None:
        payload["error"] = str(error)
        payload["error_type"] = error.__class__.__name__

    emit_progress_event("missing_worker", compact_payload(payload))


@dataclass(slots=True)
class MissingStoryContext:
    story_folder: str
    metadata: dict[str, Any]
    metadata_path: str
    source_list: list[dict[str, Any]]
    primary_source: dict[str, Any] | None
    latest_total: int


def _iter_story_folders() -> list[str]:
    return sorted([
        os.path.join(DATA_FOLDER, cast(str, folder_name))
        for folder_name in os.listdir(DATA_FOLDER)
        if os.path.isdir(os.path.join(DATA_FOLDER, cast(str, folder_name)))
    ])


def _flush_autofix_titles() -> None:
    if not auto_fixed_titles:
        return

    msg = "[AUTO-FIX] Đã tự động tạo metadata cho các truyện: " + ", ".join(auto_fixed_titles[:10])
    if len(auto_fixed_titles) > 10:
        msg += f" ... (và {len(auto_fixed_titles) - 10} truyện nữa)"
    logger.debug(msg)
    auto_fixed_titles.clear()


async def _prepare_missing_story_context(
    story_folder: str,
    site_key: str,
    adapter,
    force_unskip: bool = False,
) -> MissingStoryContext | None:
    metadata_path = os.path.join(story_folder, "metadata.json")

    # === Pre-check: Avoid cross-site pollution ===
    # Nếu metadata đã tồn tại, kiểm tra xem truyện này có thuộc về site hiện tại không.
    # Nếu không, bỏ qua ngay lập tức để tránh việc "fix" nhầm hoặc đoán URL sai.
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, encoding="utf-8") as f:
                pre_meta = json.load(f)
            
            # Check explicit site_key
            pre_site_key = pre_meta.get("site_key")
            if pre_site_key and pre_site_key != site_key:
                # Special case: allow if site_key matches one of the sources
                has_valid_source = False
                for src in pre_meta.get("sources", []):
                    if isinstance(src, dict) and src.get("site_key") == site_key:
                        has_valid_source = True
                        break
                
                if not has_valid_source:
                    # logger.debug(
                    #     f"[SKIP-CROSS] Truyện '{pre_meta.get('title')}' thuộc '{pre_site_key}', "
                    #     f"không phải '{site_key}'. Bỏ qua."
                    # )
                    return None

            # Check URL domain
            pre_url = pre_meta.get("url")
            if pre_url and not is_url_for_site(pre_url, site_key):
                 # Nếu URL chính không khớp site hiện tại, và không có source nào khớp
                has_valid_source = False
                for src in pre_meta.get("sources", []):
                    if isinstance(src, dict) and src.get("site_key") == site_key:
                        has_valid_source = True
                        break
                if not has_valid_source:
                    return None

        except Exception:
            # Nếu file lỗi, để logic phía sau xử lý (có thể xóa và tạo lại)
            pass

    completed_root = os.path.abspath(COMPLETED_FOLDER)
    data_root = os.path.abspath(DATA_FOLDER)
    if (
        os.path.dirname(story_folder) == completed_root
        and completed_root != data_root
    ):
        return None

    metadata: dict[str, Any] | None = None
    need_autofix = False

    if not os.path.exists(metadata_path):
        guessed_url = f"{BASE_URLS.get(site_key, '').rstrip('/')}/{os.path.basename(story_folder)}"
        logger.info(f"[AUTO-FIX] Không có metadata.json, đang lấy metadata chi tiết từ {guessed_url}")
        details = await cached_get_story_details(
            adapter, guessed_url, os.path.basename(story_folder).replace("-", " ")
        )
        logger.info("... sau await get_story_details ...")
        metadata = autofix_metadata(story_folder, site_key)
        if details:
            for k, v in details.items():
                if v is not None and v != "" and metadata.get(k) != v:
                    logger.info(f"[UPDATE] {metadata['title']}: Trường '{k}' được cập nhật.")
                    metadata[k] = v
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(
                f"[AUTO-FIX] Đã tạo metadata đầy đủ/merge cho '{metadata.get('title')}' ({metadata.get('total_chapters_on_site', 0)} chương)"
            )
            fields_required = ["title", "categories", "total_chapters_on_site", "author", "description", "cover", "sources"]
            missing = [f for f in fields_required if not metadata.get(f)]
            if missing:
                logger.warning(f"[AUTO-FIX] Metadata của '{metadata.get('title')}' vẫn còn thiếu các trường: {missing}")
        else:
            logger.info(
                f"[AUTO-FIX] Tạo metadata tạm cho '{metadata['title']}' ({metadata.get('total_chapters_on_site', 0)} chương)"
            )
        auto_fixed_titles.append(metadata["title"])
    else:
        try:
            with open(metadata_path, encoding="utf-8") as f:
                metadata = json.load(f)
            metadata = normalize_categories_field(metadata, metadata_path)

            fixed_sources = []
            for src in metadata.get("sources", []):
                s_url = src.get("url") if isinstance(src, dict) else src
                s_key = (
                    get_site_key_from_url(s_url)
                    or (src.get("site_key") if isinstance(src, dict) else None)
                    or (src.get("site") if isinstance(src, dict) else None)
                    or metadata.get("site_key")
                )
                if s_url and s_key and is_url_for_site(s_url, s_key):
                    fixed_sources.append(src)
                else:
                    logger.warning(
                        f"[FIX] Source có url {s_url} không đúng domain với key {s_key}, đã loại khỏi sources."
                    )
            metadata["sources"] = fixed_sources

            if not isinstance(metadata.get("sources", []), list):
                need_autofix = True
            fields_required = ["title", "categories", "total_chapters_on_site"]
            if not all(metadata.get(f) for f in fields_required):
                need_autofix = True
        except Exception as ex:
            logger.warning(
                f"[AUTO-FIX] metadata.json lỗi/parsing fail tại {story_folder}, sẽ xoá file và tạo lại! {ex}"
            )
            need_autofix = True

    if need_autofix:
        try:
            os.remove(metadata_path)
        except Exception as ex:
            logger.error(f"Lỗi xóa metadata lỗi: {ex}")
        metadata = autofix_metadata(story_folder, site_key)
        auto_fixed_titles.append(metadata["title"])

    if not metadata:
        return None

    source_list, primary_source = ensure_primary_source(metadata, metadata_path)

    # Try to auto-discover mirror sources by title (e.g., add tangthuvien when missing)
    try:
        from flowcore.utils.mirror_discovery import discover_and_update_mirrors
        _, additions = await discover_and_update_mirrors(
            metadata,
            metadata_path,
            prefer_sites=[key for key in app_config.ENABLED_SITE_KEYS if key],
            max_add=2,
        )
        if additions:
            logger.info(
                f"[MIRROR] Đã tự động bổ sung {len(additions)} nguồn mirror: "
                + ", ".join(f"{s.get('site_key')}" for s in additions if isinstance(s, dict))
            )
            # Re-normalize sources to ensure primary/fallback ordering
            source_list, primary_source = ensure_primary_source(metadata, metadata_path)
    except Exception as ex:
        logger.debug(f"[MIRROR] Bỏ qua auto-discovery mirrors do lỗi: {ex}")

    if metadata.get("site_key") and metadata.get("site_key") != site_key:
        logger.debug(
            f"[SKIP] '{metadata.get('title')}' thuộc nguồn chuẩn {metadata.get('site_key')} – bỏ qua tại worker {site_key}."
        )
        return None

    if force_unskip:
        changed = False
        if metadata.get("skip_crawl"):
            metadata.pop("skip_crawl", None)
            changed = True
        if "meta_retry_count" in metadata:
            metadata.pop("meta_retry_count", None)
            changed = True
        if changed:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[UNSKIP] Tự động unskip: {metadata.get('title')}")

    if metadata.get("skip_crawl", False):
        logger.info(
            f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua (skip_crawl), không crawl lại nữa."
        )
        return None

    if not await fix_metadata_with_retry(
        metadata, metadata_path, story_folder, site_key=site_key, adapter=adapter
    ):
        return None

    latest_total = await refresh_total_chapters_from_web(metadata, metadata_path, adapter)
    if not latest_total:
        latest_total = metadata.get("total_chapters_on_site") or 0

    return MissingStoryContext(
        story_folder=story_folder,
        metadata=metadata,
        metadata_path=metadata_path,
        source_list=source_list,
        primary_source=primary_source,
        latest_total=latest_total,
    )

def calculate_missing_crawl_timeout(num_chapters: int | None = None) -> float:
    """Return a dynamic timeout for crawling missing chapters."""

    base_timeout = max(1, app_config.MISSING_CRAWL_TIMEOUT_SECONDS)
    if not num_chapters or num_chapters <= 0:
        return base_timeout

    http_retry_count = max(int(getattr(app_config, "RETRY_ATTEMPTS", 0)), 0) + 1
    http_budget = app_config.TIMEOUT_REQUEST * http_retry_count

    playwright_timeout = getattr(app_config, "PLAYWRIGHT_REQUEST_TIMEOUT", app_config.TIMEOUT_REQUEST)
    playwright_retry_count = max(int(getattr(app_config, "PLAYWRIGHT_MAX_RETRIES", 0)), 0) + 1
    playwright_budget = playwright_timeout * playwright_retry_count

    per_chapter_budget = max(
        app_config.MISSING_CRAWL_TIMEOUT_PER_CHAPTER,
        http_budget,
        playwright_budget,
    )

    dynamic_timeout = base_timeout + num_chapters * per_chapter_budget

    max_cap = getattr(app_config, "MISSING_CRAWL_TIMEOUT_MAX", 0)
    if max_cap and max_cap > 0:
        if dynamic_timeout <= max_cap:
            return max(base_timeout, dynamic_timeout)
        logger.debug(
            f"[MISSING][TIMEOUT] Computed timeout {dynamic_timeout:.1f}s exceeds configured cap {max_cap:.1f}s; using computed value."
        )

    # Either max cap is disabled or unrealistically low; fall back to the computed value.
    return max(base_timeout, dynamic_timeout)

def get_existing_real_chapter_numbers(story_folder):
    files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
    nums = set()
    for f in files:
        match = re.match(r'(\d{4})_', f)
        if match:
            nums.add(int(match.group(1)))
    return nums

def update_metadata_from_details(metadata: dict, details: dict) -> bool:
    if not details:
        return False
    changed = False
    for k, v in details.items():
        if v is not None and v != "" and metadata.get(k) != v:
            metadata[k] = v
            changed = True
    return changed


async def refresh_total_chapters_from_web(metadata: dict, metadata_path: str, adapter) -> int:
    """Ensure metadata.total_chapters_on_site matches the latest number from the web."""
    if not metadata:
        return 0

    metadata_changed = False

    # Chuẩn hóa danh sách nguồn để tránh lỗi khi lấy total chương thực tế
    try:
        normalized_sources = normalize_source_list(metadata)
    except Exception as ex:
        logger.warning(f"[REFRESH] Lỗi khi chuẩn hóa sources cho '{metadata.get('title')}': {ex}")
        normalized_sources = metadata.get("sources", [])

    if normalized_sources != metadata.get("sources"):
        metadata["sources"] = normalized_sources
        metadata_changed = True

    latest_total = metadata.get("total_chapters_on_site") or 0

    try:
        real_total = await get_real_total_chapters(metadata, adapter)
    except Exception as ex:  # pragma: no cover - network/adapter issues
        logger.warning(
            f"[REFRESH] Không lấy được total chương thực tế cho '{metadata.get('title')}' từ web: {ex}"
        )
        real_total = 0

    if real_total and real_total > 0 and real_total != latest_total:
        logger.info(
            f"[REFRESH] Cập nhật total_chapters_on_site cho '{metadata.get('title')}' từ {latest_total} -> {real_total}"
        )
        metadata["total_chapters_on_site"] = real_total
        latest_total = real_total
        metadata_changed = True
    else:
        latest_total = max(latest_total, real_total)

    if metadata_changed and metadata_path:
        try:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
        except Exception as ex:
            logger.warning(f"[REFRESH] Không thể lưu metadata cho '{metadata.get('title')}': {ex}")

    return latest_total or 0


def check_and_fix_chapter_filename(story_folder: str, ch: dict, real_num: int, idx: int):
    """
    Nếu tên file hiện tại không khớp với tên dự kiến từ title → rename lại cho đúng.
    """
    # Danh sách file trong folder
    existing_files = [f for f in os.listdir(story_folder) if f.endswith(".txt")]

    # Tên dự kiến từ title chương
    expected_name = get_chapter_filename(ch.get("title", ""), real_num)
    expected_path = os.path.join(story_folder, expected_name)

    # Nếu file đích đã tồn tại đúng → OK
    if os.path.exists(expected_path):
        return

    # Tìm file sai tên theo prefix số chương
    prefix = f"{real_num:04d}_"
    for fname in existing_files:
        if fname.startswith(prefix):
            current_path = os.path.join(story_folder, fname)
            # Nếu khác tên → rename
            if current_path != expected_path:
                try:
                    os.rename(current_path, expected_path)
                    logger.info(f"[RENAME] Đã rename file '{fname}' → '{expected_name}'")
                except Exception as e:
                    logger.warning(f"[RENAME ERROR] Không thể rename '{fname}' → '{expected_name}': {e}")
            break


def get_current_category(metadata):
    categories = autofix_category(metadata)
    return categories[0]


async def loop_once_multi_sites(force_unskip: bool = False, **_ignored):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"\n===== [START] Check missing for all sites at {now} =====")
    started_at = time.time()
    _emit_missing_event(
        "started",
        "multi_site",
        extra={"force_unskip": force_unskip, "timestamp": now},
    )

    async def _run_site(site_key: str, url: str):
        site_started_at = time.time()
        _emit_missing_event(
            "started",
            "site_scan",
            site_key=site_key,
            extra={"force_unskip": force_unskip},
        )
        adapter = get_adapter(site_key)
        try:
            await check_and_crawl_missing_all_stories(
                adapter,
                url,
                site_key=site_key,
                force_unskip=force_unskip,
            )
        except Exception as exc:
            _emit_missing_event(
                "failed",
                "site_scan",
                site_key=site_key,
                started_at=site_started_at,
                extra={"force_unskip": force_unskip},
                error=exc,
            )
            raise
        else:
            _emit_missing_event(
                "succeeded",
                "site_scan",
                site_key=site_key,
                started_at=site_started_at,
                extra={"force_unskip": force_unskip},
            )

    tasks = [asyncio.create_task(_run_site(site_key, url)) for site_key, url in BASE_URLS.items()]

    error_count = 0
    try:
        logger.info("Before await gather")
        site_items = list(BASE_URLS.items())
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (site_key, _), result in zip(site_items, results, strict=False):
            if isinstance(result, Exception):
                error_count += 1
                logger.error(f"Task {site_key} bị lỗi: {result}\n{traceback.format_exc()}")
        logger.info("===== [DONE] =====\n")
    except Exception as e:
        _emit_missing_event(
            "failed",
            "multi_site",
            started_at=started_at,
            extra={"force_unskip": force_unskip, "timestamp": now},
            error=e,
        )
        logger.error(f"[ERROR] Lỗi khi kiểm tra/crawl missing: {e}")
    else:
        status = "completed_with_errors" if error_count else "completed"
        _emit_missing_event(
            "succeeded",
            "multi_site",
            started_at=started_at,
            status=status,
            extra={
                "force_unskip": force_unskip,
                "timestamp": now,
                "site_count": len(BASE_URLS),
                "error_count": error_count,
            },
        )
    logger.info("===== [DONE] =====\n")
    await send_telegram_notify(f"✅ DONE: Đã crawl/check missing xong toàn bộ ({now})")
async def crawl_missing_until_complete(
    adapter, site_key, session, chapters_from_web, metadata, current_category, story_folder, crawl_state, state_file, max_retry=3
):
    retry = 0
    while retry < max_retry:
        missing_chapters = get_missing_chapters(
            story_folder, chapters_from_web, site_key, 
            remote_story_title=metadata.get("title"),
            remote_author=metadata.get("author")
        )
        for ch in chapters_from_web:
            title = ch.get('title', '') or ''
            aligned = ch.get('aligned_index')
            if isinstance(aligned, int):
                real_num = aligned
            else:
                real_num = extract_real_chapter_number(title)
                if not isinstance(real_num, int):
                    real_num = ch.get('idx', 0) + 1
            check_and_fix_chapter_filename(story_folder, ch, real_num, ch.get('idx', 0))

        if not missing_chapters:
            logger.info(f"[COMPLETE] Đã đủ tất cả chương cho '{metadata['title']}'")
            chapters_for_export = get_actual_chapters_for_export(story_folder)
            export_chapter_metadata_sync(story_folder, chapters_for_export)
            return True
        logger.info(f"[RETRY] {len(missing_chapters)} chương còn thiếu, bắt đầu crawl lần {retry+1}/{max_retry}")

        # Progressive backoff: Wait before retry with exponential delay
        if retry > 0:
            backoff_delay = min(5 * (2 ** (retry - 1)), 60)  # 5s, 10s, 20s, 40s, max 60s
            logger.info(f"[BACKOFF] Waiting {backoff_delay}s before retry {retry+1}/{max_retry}")
            await asyncio.sleep(backoff_delay)

        # Tính số batch dựa trên số chương còn thiếu, batch size từ config (default 250)
        batch_size = app_config.get("MISSING_CHAPTER_BATCH_SIZE", 250)
        num_batches = max(1, (len(missing_chapters) + batch_size - 1) // batch_size)
        logger.info(f"Crawl {len(missing_chapters)} chương với {num_batches} batch (mỗi batch tối đa {batch_size} chương)")
        await crawl_story_with_limit(
            site_key,
            session,
            missing_chapters,
            metadata,
            current_category,
            story_folder,
            crawl_state,
            num_batches=num_batches,
            state_file=state_file,
            adapter=adapter,
            chapters_all=chapters_from_web,
        )
        # Kiểm tra lại sau khi crawl
        missing_chapters = get_missing_chapters(
            story_folder, chapters_from_web, site_key, 
            remote_story_title=metadata.get("title"),
            remote_author=metadata.get("author")
        )
        if not missing_chapters:
            logger.info(f"[COMPLETE] Đã đủ tất cả chương sau lần crawl {retry+1}")
            return True
        retry += 1
    logger.warning(
        f"[FAILED] Sau {max_retry} lần retry vẫn còn thiếu {len(missing_chapters)} chương cho '{metadata['title']}'"
    )
    chapters_for_export = get_actual_chapters_for_export(story_folder)
    export_chapter_metadata_sync(story_folder, chapters_for_export)
    if retry >= max_retry:
        logger.warning(
            f"[FATAL] Sau {max_retry} lần vẫn còn thiếu chương. Đánh dấu dead_chapters và bỏ qua."
        )
        for ch in missing_chapters:
            chapter_data = {
                "index": ch.get("real_num"),
                "title": ch.get("title"),
                "url": ch.get("url"),
                "reason": "max_retry_reached",
            }
            await mark_dead_chapter(story_folder, chapter_data)

            # Send to Dead Letter Queue for later analysis/retry
            try:
                await send_to_dlq(
                    chapter_url=ch.get("url", ""),
                    story_folder=story_folder,
                    story_title=metadata.get("title", "Unknown"),
                    chapter_index=ch.get("real_num", 0),
                    reason="max_retries_exceeded",
                    retry_count=max_retry,
                    last_error=f"Failed after {max_retry} retry attempts",
                    site_key=site_key,
                    metadata={"chapter_title": ch.get("title")},
                )
            except Exception as dlq_error:
                logger.warning(f"[DLQ] Failed to send chapter to DLQ: {dlq_error}")
        warn_msg = (
            f"[MISSING] '{metadata['title']}' vẫn thiếu {len(missing_chapters)} chương sau khi thử mọi nguồn"
        )
        logger.warning(warn_msg)
        await send_telegram_notify(warn_msg)
        try:
            with open(MISSING_SUMMARY_LOG, "a", encoding="utf-8") as f:
                f.write(f"{metadata.get('title')}\t{story_folder}\t{len(missing_chapters)}\n")
        except Exception:
            pass
        return False
    return False
def autofix_category(metadata):
    """
    Đảm bảo metadata có 'categories' là list[dict] chuẩn.
    Nếu thiếu hoặc sai kiểu thì set lại Unknown.
    """
    categories = metadata.get('categories')
    if not (isinstance(categories, list) and categories and isinstance(categories[0], dict) and 'name' in categories[0]):
        logger.warning(f"[AUTO-FIX] Metadata '{metadata.get('title')}' thiếu hoặc sai categories. Set lại Unknown.")
        metadata['categories'] = [{"name": "Unknown", "url": ""}]
    return metadata['categories']


def normalize_source_list(metadata):
    """Chuẩn hoá danh sách nguồn và gắn cờ nguồn chuẩn."""

    normalized: list[dict] = []
    seen: set[tuple[str, str]] = set()
    primary_key: tuple[str, str] | None = None

    raw_sources = metadata.get("sources", []) or []
    for raw in raw_sources:
        if isinstance(raw, dict):
            url = raw.get("url")
            site_key = (
                raw.get("site_key")
                or raw.get("site")
                or get_site_key_from_url(url)
                or metadata.get("site_key")
            )
            entry = dict(raw)
        elif isinstance(raw, str):
            url = raw
            site_key = get_site_key_from_url(url) or metadata.get("site_key")
            entry = {"url": url}
        else:
            continue

        if not url or not site_key:
            continue
        if not is_url_for_site(url, site_key):
            continue

        key = (url, site_key)
        if key in seen:
            continue

        entry["url"] = url
        entry["site_key"] = site_key
        normalized.append(entry)
        seen.add(key)

        if primary_key is None:
            primary_key = key

    main_url = metadata.get("url")
    main_key = metadata.get("site_key") or get_site_key_from_url(main_url)
    if main_url and main_key and is_url_for_site(main_url, main_key):
        key = (main_url, main_key)
        if key not in seen:
            normalized.append({"url": main_url, "site_key": main_key})
            seen.add(key)
        if primary_key is None:
            primary_key = key

    if not normalized:
        return normalized

    primary_sources: list[dict] = []
    fallback_sources: list[dict] = []
    for entry in normalized:
        entry_copy = dict(entry)
        key = (entry_copy.get("url"), entry_copy.get("site_key"))
        is_primary = primary_key is not None and key == primary_key
        entry_copy["is_primary"] = is_primary
        if is_primary:
            primary_sources.append(entry_copy)
        else:
            fallback_sources.append(entry_copy)

    return primary_sources + fallback_sources


def ensure_primary_source(metadata: dict, metadata_path: str | None = None) -> tuple[list[dict], dict | None]:
    """Normalize sources, persist them, và đảm bảo site_key trỏ về Nguồn Chuẩn."""

    try:
        source_list = normalize_source_list(metadata)
    except Exception as ex:  # pragma: no cover - defensive guard
        logger.warning(
            f"[SOURCE] Lỗi khi chuẩn hoá sources của '{metadata.get('title')}': {ex}"
        )
        source_list = metadata.get("sources", []) or []

    primary_source = next((src for src in source_list if src.get("is_primary")), None)

    changed = False
    if source_list and metadata.get("sources") != source_list:
        metadata["sources"] = source_list
        changed = True

    if primary_source:
        primary_site_key = primary_source.get("site_key")
        if primary_site_key and metadata.get("site_key") != primary_site_key:
            metadata["site_key"] = primary_site_key
            changed = True

    if changed and metadata_path:
        try:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
        except Exception as ex:  # pragma: no cover - IO guard
            logger.warning(
                f"[SOURCE] Không thể lưu metadata sau khi cập nhật nguồn cho '{metadata.get('title')}': {ex}"
            )

    return source_list, primary_source


async def check_and_crawl_missing_all_stories(adapter, home_page_url, site_key, force_unskip=False):
    started_at = time.time()
    _emit_missing_event(
        "started",
        "site_sweep",
        site_key=site_key,
        extra={"force_unskip": force_unskip},
    )
    try:
        status, stories_checked, stories_missing = await _check_and_crawl_missing_all_stories_core(
            adapter,
            home_page_url,
            site_key,
            force_unskip=force_unskip,
        )
    except Exception as exc:
        _emit_missing_event(
            "failed",
            "site_sweep",
            site_key=site_key,
            started_at=started_at,
            extra={"force_unskip": force_unskip},
            error=exc,
        )
        raise
    else:
        extra = {
            "force_unskip": force_unskip,
            "stories_checked": stories_checked,
            "stories_missing": stories_missing,
        }
        if status and status != "completed":
            _emit_missing_event(
                "failed",
                "site_sweep",
                site_key=site_key,
                started_at=started_at,
                status=status,
                extra=extra,
            )
        else:
            _emit_missing_event(
                "succeeded",
                "site_sweep",
                site_key=site_key,
                started_at=started_at,
                extra=extra,
            )


async def _check_and_crawl_missing_all_stories_core(adapter, home_page_url, site_key, force_unskip=False):
    stories_checked = 0
    stories_missing = 0
    status = "completed"
    state_file = get_missing_worker_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)
    adapter = get_adapter(site_key)
    all_genres = await adapter.get_genres()
    genre_name_to_url = {g['name']: g['url'] for g in all_genres if isinstance(g, dict) and 'name' in g and 'url' in g}
    if not all_genres:
        logger.error(f"[{site_key}] Không lấy được danh sách thể loại (all_genres rỗng) từ {home_page_url}!")
        status = "no_genres"
        return status, stories_checked, stories_missing

    genre_complete_checked = set()
    os.makedirs(COMPLETED_FOLDER, exist_ok=True)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    await initialize_scraper(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    story_folders = _iter_story_folders()
    if WORKER_SHARD_TOTAL > 1:
        assigned_folders: list[str] = []
        skipped_folders = 0
        for story_folder in story_folders:
            folder_name = os.path.basename(story_folder)
            digest = int(hashlib.sha256(folder_name.encode("utf-8")).hexdigest(), 16)
            if digest % WORKER_SHARD_TOTAL == WORKER_SHARD_INDEX:
                assigned_folders.append(story_folder)
            else:
                skipped_folders += 1
        logger.info(
            "[MISSING][SHARD] Worker %s handling %d/%d stories (skipped %d) [index=%d total=%d]",
            WORKER_INSTANCE_ID,
            len(assigned_folders),
            len(story_folders),
            skipped_folders,
            WORKER_SHARD_INDEX,
            WORKER_SHARD_TOTAL,
        )
        story_folders = assigned_folders
    else:
        logger.info(
            "[MISSING][SHARD] Worker %s processing all %d stories (single shard)",
            WORKER_INSTANCE_ID,
            len(story_folders),
        )
    story_retry_counts: dict[str, int] = {}

    # ============ 1. Kiểm tra và crawl thiếu theo từng truyện ============
    for story_folder in story_folders:
        lock_path = os.path.join(story_folder, ".missing.lock")
        story_lock = FileLock(lock_path)
        try:
            story_lock.acquire(timeout=0)
        except Timeout:
            logger.info(
                "[MISSING][LOCK] '%s' dang duoc xu ly boi worker khac, bo qua.",
                os.path.basename(story_folder),
            )
            continue
        except Exception as exc:
            logger.warning(
                "[MISSING][LOCK] Khong the khoa '%s': %s",
                os.path.basename(story_folder),
                exc,
            )
            continue
        try:
            _flush_autofix_titles()
            context = await _prepare_missing_story_context(
                story_folder,
                site_key,
                adapter,
                force_unskip=force_unskip,
            )
            if not context:
                continue

            stories_checked += 1

            metadata = context.metadata
            source_list = context.source_list
            primary_source = context.primary_source
            latest_total = context.latest_total

            crawled_files = count_txt_files(story_folder)
            if crawled_files < latest_total: #type:ignore
                stories_missing += 1
                logger.info(
                    f"[MISSING] '{metadata['title']}' thiếu chương ({crawled_files}/{latest_total}) -> Đang kiểm tra/crawl bù theo nguồn chuẩn trước."
                )

                if not source_list:
                    logger.error(f"Không có nguồn nào hợp lệ cho truyện '{metadata['title']}'. Bỏ qua.")
                    continue

                primary_source = primary_source or (
                    next((src for src in source_list if src.get("is_primary")), source_list[0])
                    if source_list
                    else None
                )
                fallback_sources = [src for src in source_list if src is not primary_source]
                ordered_sources = [primary_source] + fallback_sources if primary_source else source_list

                canonical_chapters = None
                canonical_source = None
                retry_story = False
                for source in ordered_sources:
                    url = source.get("url")
                    src_site_key = source.get("site_key")
                    if not src_site_key or not url:
                        continue

                    adapter = get_adapter(src_site_key)
                    try:
                        if source.get("is_primary"):
                            logger.info(
                                f"Đang lấy danh sách chương chuẩn từ Nguồn Chuẩn ({src_site_key})."
                            )
                        else:
                            logger.info(
                                f"Nguồn Chuẩn không khả dụng, thử lấy danh sách chương chuẩn từ nguồn dự phòng: {src_site_key}."
                            )
                        chapters = await cached_get_chapter_list(
                            adapter,
                            url,
                            metadata['title'],
                            src_site_key,
                            total_chapters=metadata.get("total_chapters_on_site"),
                        )
                        if chapters:
                            canonical_chapters = chapters
                            canonical_source = source
                            export_chapter_metadata_sync(story_folder, canonical_chapters)
                            logger.info(
                                f"Lấy được {len(canonical_chapters)} chương chuẩn từ nguồn {src_site_key}."
                            )
                            break
                    except Exception as ex:
                        logger.warning(
                            f"Lỗi khi lấy chương chuẩn từ nguồn {src_site_key}: {ex}. Thử nguồn tiếp theo."
                        )

                if not canonical_chapters:
                    logger.error(
                        f"Không thể lấy danh sách chương từ bất kỳ nguồn nào cho '{metadata['title']}'. Bỏ qua."
                    )
                    continue

                canonical_site_key = canonical_source.get("site_key") if canonical_source else None

                def remaining_missing(
                    _canonical_chapters: list[dict] | None = canonical_chapters,
                    _canonical_site_key: str | None = canonical_site_key,
                    _story_folder: str = story_folder,
                ) -> list[dict]:
                    if not _canonical_chapters or not _canonical_site_key:
                        return []
                    return get_missing_chapters(
                        _story_folder, _canonical_chapters, _canonical_site_key, 
                        remote_story_title=metadata.get("title"),
                        remote_author=metadata.get("author")
                    )

                current_category = get_current_category(metadata)

                # Progress-based retry tracking
                no_progress_count = story_retry_counts.get(story_folder, 0)

                for idx, source in enumerate(ordered_sources, start=1):
                    url = source.get("url")
                    src_site_key = source.get("site_key")
                    if not src_site_key or not url:
                        continue

                    adapter = get_adapter(src_site_key)
                    logger.info(
                        f"[CRAWL SOURCE {idx}/{len(ordered_sources)}] site_key={src_site_key}, url={url}"
                    )

                    try:
                        if (
                            canonical_source
                            and canonical_source.get("url") == url
                            and canonical_source.get("site_key") == src_site_key
                        ):
                            chapters_from_source = canonical_chapters
                        else:
                            chapters_from_source = await cached_get_chapter_list(
                                adapter,
                                url,
                                metadata['title'],
                                src_site_key,
                                total_chapters=metadata.get("total_chapters_on_site"),
                            )
                        if not chapters_from_source:
                            logger.warning(f"Nguồn {src_site_key} không trả về danh sách chương.")
                            continue

                        missing_chapters = get_missing_chapters(
                            story_folder, chapters_from_source, src_site_key,
                            remote_story_title=metadata.get("title"),
                            remote_author=metadata.get("author")
                        )

                        # --- SWITCH PRIMARY SOURCE IMMEDIATELY IF MIRROR IS MUCH BETTER ---
                        remote_count = len(chapters_from_source)
                        local_count = metadata.get("total_chapters_on_site") or 0
                        if remote_count > local_count + 5 and src_site_key != metadata.get("site_key"):
                            logger.warning(
                                f"[SWITCH-UPGRADE] Phát hiện nguồn mirror tốt hơn hẳn cho '{metadata.get('title')}' "
                                f"({local_count} -> {remote_count} chương). Chuyển nguồn chính sang {src_site_key}."
                            )
                            metadata["site_key"] = src_site_key
                            if url:
                                metadata["url"] = url
                            metadata["total_chapters_on_site"] = remote_count
                            
                            # Update sources list to set this mirror as primary
                            new_sources = []
                            for src in metadata.get("sources", []):
                                if not isinstance(src, dict): continue
                                s_key = src.get("site_key") or src.get("site")
                                if s_key == src_site_key:
                                    src["is_primary"] = True
                                else:
                                    src.pop("is_primary", None)
                                new_sources.append(src)
                            metadata["sources"] = new_sources
                            
                            # Save metadata immediately
                            try:
                                with open(context.metadata_path, "w", encoding="utf-8") as f:
                                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                            except Exception as e:
                                logger.error(f"[SWITCH-ERROR] Không thể lưu metadata: {e}")

                        if not missing_chapters:
                            logger.info(
                                f"Không phát hiện chương thiếu nào từ nguồn {src_site_key}."
                            )
                            if not remaining_missing():
                                logger.info(
                                    f"Truyện '{metadata['title']}' đã đủ chương sau khi đối chiếu nguồn {src_site_key}."
                                )
                                # Move ngay lập tức vào completed folder
                                if latest_total and latest_total > 0:
                                    metadata["total_chapters_on_site"] = latest_total
                                await move_completed_story_immediately(
                                    story_folder=story_folder,
                                    metadata=metadata,
                                    crawl_state=crawl_state,
                                    state_file=state_file,
                                    site_key=site_key,
                                )
                                break
                            continue

                        # Track progress trước khi crawl
                        chapters_before = count_txt_files(story_folder)

                        logger.info(
                            f"Bắt đầu crawl {len(missing_chapters)} chương thiếu từ nguồn {src_site_key}."
                        )

                        try:
                            await crawl_story_with_limit(
                                src_site_key,
                                None,
                                missing_chapters,
                                metadata,
                                current_category,
                                story_folder,
                                crawl_state,
                                state_file=state_file,
                                adapter=adapter,
                                chapters_all=chapters_from_source,
                            )
                        except TimeoutError:
                            logger.warning(
                                f"[TIMEOUT] Crawl cho '{metadata.get('title')}' từ nguồn {src_site_key} bị timeout, kiểm tra progress..."
                            )

                        # Kiểm tra progress sau khi crawl (kể cả khi timeout)
                        chapters_after = count_txt_files(story_folder)
                        progress_made = chapters_after - chapters_before

                        if progress_made > 0:
                            logger.info(
                                f"[PROGRESS] '{metadata['title']}': +{progress_made} chương (now: {chapters_after})"
                            )
                            # Reset no-progress counter khi có progress
                            story_retry_counts[story_folder] = 0

                            # Kiểm tra nếu đã đủ chương
                            if not remaining_missing():
                                logger.info(
                                    f"Truyện '{metadata['title']}' đã đủ chương sau khi crawl từ nguồn {src_site_key}."
                                )
                                
                                # Nếu nguồn crawl thành công khác với nguồn chính hiện tại, update metadata
                                # để health-checker và các lần chạy sau dùng nguồn tốt hơn.
                                if src_site_key != metadata.get("site_key"):
                                    try:
                                        logger.info(f"[SWITCH] Chuyển nguồn chính từ {metadata.get('site_key')} sang {src_site_key} cho '{metadata.get('title')}'")
                                        metadata["site_key"] = src_site_key
                                        if url:
                                            metadata["url"] = url
                                        
                                        # Update sources list
                                        new_sources = []
                                        for src in metadata.get("sources", []):
                                            s_key = src.get("site_key") or src.get("site")
                                            if s_key == src_site_key:
                                                src["is_primary"] = True
                                            else:
                                                src.pop("is_primary", None)
                                            new_sources.append(src)
                                        metadata["sources"] = new_sources
                                        
                                        # Save metadata
                                        with open(context.metadata_path, "w", encoding="utf-8") as f:
                                            json.dump(metadata, f, ensure_ascii=False, indent=4)
                                    except Exception as e:
                                        logger.warning(f"[SWITCH ERROR] Không thể cập nhật metadata khi chuyển nguồn: {e}")

                                # Move ngay lập tức vào completed folder
                                if latest_total and latest_total > 0:
                                    metadata["total_chapters_on_site"] = latest_total
                                await move_completed_story_immediately(
                                    story_folder=story_folder,
                                    metadata=metadata,
                                    crawl_state=crawl_state,
                                    state_file=state_file,
                                    site_key=site_key,
                                )
                                break

                            # Nếu còn thiếu, thêm lại vào queue để tiếp tục
                            remaining = len(remaining_missing())
                            if remaining > 0:
                                logger.info(
                                    f"[CONTINUE] '{metadata['title']}' còn {remaining} chương, sẽ tiếp tục sau."
                                )
                                story_folders.append(story_folder)
                                retry_story = True
                                break
                        else:
                            # Không có progress
                            no_progress_count += 1
                            story_retry_counts[story_folder] = no_progress_count
                            logger.warning(
                                f"[NO PROGRESS] '{metadata['title']}' không có progress từ nguồn {src_site_key} "
                                f"(lần {no_progress_count}/{SPECIAL_STORY_MAX_NO_PROGRESS})"
                            )

                            if no_progress_count >= SPECIAL_STORY_MAX_NO_PROGRESS:
                                # Đã thử nhiều lần không có progress -> gửi vào special queue
                                remaining = remaining_missing()
                                total_chapters = latest_total or len(chapters_from_source)

                                logger.warning(
                                    f"[SPECIAL] '{metadata['title']}' không có progress sau {SPECIAL_STORY_MAX_NO_PROGRESS} lần, "
                                    f"gửi vào queue đặc biệt để xử lý."
                                )

                                await send_to_special_queue(
                                    metadata=metadata,
                                    story_folder=story_folder,
                                    site_key=src_site_key,
                                    reason=f"Không có progress sau {SPECIAL_STORY_MAX_NO_PROGRESS} lần retry",
                                    missing_count=len(remaining) if remaining else 0,
                                    total_count=total_chapters,
                                )
                                break

                    except Exception:
                        logger.exception(
                            f"Lỗi không xác định khi xử lý nguồn {src_site_key}"
                        )
                        continue

                if retry_story:
                    continue

            logger.info(f"[NEXT] Kết thúc process cho story: {story_folder}")



        finally:
            try:
                story_lock.release()
            except Exception as exc:
                logger.warning(
                    "[MISSING][LOCK] Khong the giai phong khoa '%s': %s",
                    os.path.basename(story_folder),
                    exc,
                )
    # ============ 3. Quét lại & move, cảnh báo, đồng bộ ============
    notified_titles = set()
    completed_root = os.path.abspath(COMPLETED_FOLDER)
    data_root = os.path.abspath(DATA_FOLDER)
    for story_folder in story_folders:
        # Bỏ qua nếu folder hiện tại đã nằm trong completed (đã move ở vòng trước)
        story_abs_path = os.path.abspath(story_folder)
        try:
            if (
                os.path.commonpath([story_abs_path, completed_root]) == completed_root
                and completed_root != data_root
            ):
                continue
        except ValueError:
            # Nếu commonpath không xác định được (khác drive, ...), bỏ qua việc skip
            pass

        # Nếu truyện đã tồn tại trong completed, vẫn tiếp tục process để ghi đè
        if os.path.exists(COMPLETED_FOLDER):
            genre_folders = [
                os.path.join(COMPLETED_FOLDER, d)
                for d in os.listdir(COMPLETED_FOLDER)
                if os.path.isdir(os.path.join(COMPLETED_FOLDER, d))
            ]
            existing_completed_path = next(
                (
                    os.path.join(gf, os.path.basename(story_folder))
                    for gf in genre_folders
                    if os.path.exists(os.path.join(gf, os.path.basename(story_folder)))
                ),
                None,
            )
            if existing_completed_path:
                logger.info(
                    "[CLEANUP] Truyện đã tồn tại trong completed, sẽ ghi đè bằng dữ liệu mới: %s",
                    os.path.basename(story_folder),
                )

        meta_path = os.path.join(story_folder, "metadata.json")
        with FileLock(meta_path + ".lock", timeout=10):
            if not os.path.exists(meta_path):
                metadata = autofix_metadata(story_folder, site_key)
                auto_fixed_titles.append(metadata["title"])
            else:
                with open(meta_path, encoding="utf-8") as f:
                    metadata = json.load(f)
                metadata = normalize_categories_field(metadata, meta_path)
                # Sửa lại sources nếu cần
                if "sources" in metadata and isinstance(metadata["sources"], list):
                    fixed_sources = []
                    for src in metadata["sources"]:
                        if isinstance(src, dict):
                            fixed_sources.append(src)
                        elif isinstance(src, str):
                            fixed_sources.append({"url": src})
                    if len(fixed_sources) != len(metadata["sources"]):
                        logger.warning(f"[FIX] Đã phát hiện và sửa nguồn 'sources' bị sai type ở {story_folder}")
                        metadata["sources"] = fixed_sources
                        with open(meta_path, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=4)

        real_total = await get_real_total_chapters(metadata, adapter)
        chapter_count = recount_chapters(story_folder)
        dead_count = count_dead_chapters(story_folder)

        # Luôn cập nhật lại metadata cho đúng số chương thực tế từ web
        if metadata.get("total_chapters_on_site") != real_total:
            metadata["total_chapters_on_site"] = real_total
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[RECOUNT] Cập nhật lại metadata: {real_total} chương cho '{os.path.basename(story_folder)}'")

        logger.info(f"[CHECK] {metadata.get('title')} - txt: {chapter_count} / web: {real_total}")

        logger.info(f"[MOVE CHECK] {metadata.get('title')}: chapter_count={chapter_count}, dead_count={dead_count}, real_total={real_total}")
        # Move nếu đủ chương thực tế trên web (bao gồm chương đã đánh dấu fail)
        if chapter_count + dead_count >= real_total and real_total > 0:
            genre_name = get_primary_category_name(metadata)
            move_success = await move_story_to_completed(story_folder, genre_name)

            # Cập nhật globally_completed_story_urls để đồng bộ với main crawler
            if move_success:
                story_url = metadata.get("url")
                if story_url:
                    completed_urls = set(crawl_state.get("globally_completed_story_urls", []))
                    completed_urls.add(story_url)
                    crawl_state["globally_completed_story_urls"] = sorted(completed_urls)
                    await save_crawl_state(crawl_state, state_file, site_key=site_key)
                    logger.info(f"[COMPLETED] Đã đánh dấu '{metadata.get('title')}' hoàn thành và cập nhật state")

            if genre_name not in genre_complete_checked:
                genre_url = genre_name_to_url.get(genre_name)
                if genre_url:
                    await check_genre_complete_and_notify(genre_name, genre_url, site_key)
                genre_complete_checked.add(genre_name)
        else:
            # Cảnh báo thiếu chương (chỉ 1 lần/truyện)
            title = metadata.get('title')
            if title and title not in notified_titles:
                warning_msg = (
                    f"[WARNING] Sau crawl bù, truyện '{title}' vẫn thiếu chương: {chapter_count}+{dead_count}/{real_total}"
                )
                logger.warning(warning_msg)
                await send_telegram_notify(warning_msg)
                notified_titles.add(title)

        # Fix metadata nếu thiếu trường quan trọng (chỉ gọi 1 lần)
        fields_required = ['description', 'author', 'cover', 'categories', 'title', 'total_chapters_on_site']
        meta_ok = all(metadata.get(f) for f in fields_required)
        if not meta_ok:
            logger.info(f"[SKIP] '{story_folder}' thiếu trường quan trọng, sẽ cố gắng lấy lại metadata...")
            details = await cached_get_story_details(
                adapter, metadata.get("url"), metadata.get("title")
            )
            if update_metadata_from_details(metadata, details):#type:ignore
                meta_ok = all(metadata.get(f) for f in fields_required)
                if meta_ok:
                    logger.info(f"[FIXED] Đã bổ sung metadata đủ cho '{metadata.get('title')}'")
                    with open(meta_path, "w", encoding="utf-8") as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=4)
                else:
                    logger.error(f"[ERROR] Không lấy đủ metadata cho '{metadata.get('title')}'! Sẽ bỏ qua move.")
                    continue
            else:
                logger.error(f"[ERROR] Không lấy đủ metadata cho '{metadata.get('title')}'! Sẽ bỏ qua move.")
                continue

    logger.info(f"[TASK END] Task {site_key} đã xong toàn bộ story.")
    return status, stories_checked, stories_missing

def recount_chapters(story_folder):
    """Trả về số file .txt thực tế trong folder truyện."""
    return len([f for f in os.listdir(story_folder) if f.endswith('.txt')])



async def check_genre_complete_and_notify(genre_name, genre_url, site_key):
    adapter = get_adapter(site_key)
    stories_on_web = await  adapter.get_all_stories_from_genre(genre_name, genre_url)
    completed_folder = os.path.join(COMPLETED_FOLDER, genre_name)
    completed_folders = os.listdir(completed_folder)
    completed_titles = []
    for folder in completed_folders:
        meta_path = os.path.join(completed_folder, folder, "metadata.json")
        if os.path.exists(meta_path):
            lock_path = meta_path + ".lock"
            lock = FileLock(lock_path, timeout=30)
            with lock:
                with open(meta_path, encoding="utf-8") as f:
                    meta = json.load(f)
                    completed_titles.append(meta.get("title"))
    missing = [story for story in stories_on_web if story["title"] not in completed_titles]
    if not missing:
        await send_telegram_notify(f"🎉 Đã crawl xong **TẤT CẢ** truyện của thể loại [{genre_name}] trên web!")

async def fix_metadata_with_retry(metadata, metadata_path, story_folder, site_key=None, adapter=None):
    from flowcore_story.apps.scraper import make_request

    def is_url_for_site(url, site_key):
        from flowcore_story.config.config import BASE_URLS
        base = BASE_URLS.get(site_key)
        return base and url and url.startswith(base)

    if metadata.get("skip_crawl", False):
        logger.info(f"[SKIP] Truyện '{metadata.get('title')}' đã bị đánh dấu bỏ qua.")
        return False

    retry_count = metadata.get("meta_retry_count", 0)
    url = metadata.get("url")
    title = metadata.get("title")
    total_chapters = metadata.get("total_chapters_on_site")

    # === Ưu tiên gán lại url từ sources nếu thiếu ===
    if not url and metadata.get("sources"):
        for src in metadata["sources"]:
            s_url = src.get("url") if isinstance(src, dict) else src
            if s_url:
                url = s_url
                metadata["url"] = url
                logger.info(f"[FIX] Gán lại url từ sources cho '{story_folder}': {url}")
                break

    # === Ưu tiên lấy title từ sources nếu thiếu ===
    if not title and metadata.get("sources"):
        for src in metadata["sources"]:
            s_title = src.get("title") if isinstance(src, dict) else None
            if s_title:
                title = s_title
                metadata["title"] = title
                logger.info(f"[FIX] Gán lại title từ sources: {title}")
                break

    # === Gán lại title nếu vẫn thiếu bằng tên thư mục ===
    if not title:
        folder_title = os.path.basename(story_folder).replace("-", " ").title()
        metadata["title"] = title = folder_title
        logger.info(f"[FALLBACK] Gán title từ folder: {title}")

    # === Nếu vẫn thiếu url, đoán từ base_url + slug ===
    # KHÔNG guess URL cho các site yêu cầu story ID (truyencom: cần .ID, xtruyen/tangthuvien: chỉ cần slug)
    from flowcore_story.config.config import SITES_REQUIRING_ID
    if not url and site_key and site_key not in SITES_REQUIRING_ID:
        from flowcore_story.config.config import BASE_URLS
        base_url = BASE_URLS.get(site_key, "").rstrip("/")
        slug = os.path.basename(story_folder)

        # Xây dựng URL dựa trên format của từng site
        if site_key == "xtruyen":
            guessed_url = f"{base_url}/truyen/{slug}/"
        elif site_key == "tangthuvien":
            guessed_url = f"{base_url}/doc-truyen/{slug}"
        else:
            guessed_url = f"{base_url}/{slug}"

        try:
            resp = await make_request(guessed_url, site_key)
            if resp and getattr(resp, "status_code", None) == 200:
                # Validate final URL to avoid soft 404s (redirect to home)
                final_url = str(getattr(resp, "url", "")).rstrip("/")
                expected_url = guessed_url.rstrip("/")
                
                # Check if redirected to base URL (homepage)
                is_homepage = final_url == base_url.rstrip("/")
                
                # Check if slug is still in URL (basic validation)
                has_slug = slug in final_url

                if not is_homepage and (final_url == expected_url or has_slug):
                    url = str(getattr(resp, "url", guessed_url))
                    metadata["url"] = url
                    logger.info(f"[GUESS] Đoán url thành công cho '{story_folder}': {url}")
                else:
                    logger.warning(
                        f"[GUESS INVALID] URL đoán '{guessed_url}' bị redirect về '{final_url}' "
                        f"(Homepage? {is_homepage}). Bỏ qua."
                    )
            else:
                logger.warning(f"[GUESS FAIL] Status code {getattr(resp, 'status_code', 'N/A')} cho {guessed_url}")

        except Exception as e:
            logger.warning(f"[GUESS FAIL] Lỗi khi request guessed url {guessed_url}: {e}")
    elif not url and site_key in SITES_REQUIRING_ID and title:
        # Đối với truyencom: thử search by title để lấy ID
        logger.info(f"[SEARCH] Site '{site_key}' cần story ID, đang search bằng title: '{title}'")
        try:
            # Lấy adapter cho site này
            search_adapter = get_adapter(site_key)

            # Kiểm tra xem adapter có method search_by_title không
            if hasattr(search_adapter, 'search_by_title'):
                search_results = await search_adapter.search_by_title(title)

                if search_results:
                    # Lấy kết quả đầu tiên (best match)
                    best_match = search_results[0]
                    url = best_match.get('url')

                    if url:
                        metadata["url"] = url
                        logger.info(f"[SEARCH SUCCESS] Tìm thấy URL cho '{title}': {url}")

                        # Lưu thêm thông tin slug và ID nếu có
                        if 'slug_with_id' in best_match and best_match['slug_with_id']:
                            metadata["slug"] = best_match['slug_with_id']
                            metadata.setdefault("slug_plain", best_match.get('slug'))
                        elif 'slug' in best_match:
                            metadata["slug"] = best_match['slug']
                        if 'id' in best_match:
                            metadata["story_id"] = best_match['id']
                    else:
                        logger.warning(f"[SEARCH FAIL] Không tìm thấy URL hợp lệ cho '{title}'")
                else:
                    logger.warning(f"[SEARCH FAIL] Không tìm thấy kết quả search cho '{title}' trên {site_key}")
            else:
                logger.warning(f"[SEARCH SKIP] Adapter '{site_key}' không hỗ trợ search_by_title")
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Lỗi khi search title '{title}' trên {site_key}: {e}")
    elif not url and site_key in SITES_REQUIRING_ID:
        logger.warning(f"[SKIP GUESS] Site '{site_key}' yêu cầu story ID trong URL (format: slug.ID), không có title để search.")

    # === Nếu vẫn không có url/title thì skip ===
    if not url or not title:
        metadata["skip_crawl"] = True
        metadata["skip_reason"] = "missing_url_title"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
        logger.warning(f"[SKIP] '{story_folder}' thiếu url/title → bỏ qua.")
        return False

    # === Lấy lại metadata nếu thiếu total_chapters ===
    retry_count = metadata.get("meta_retry_count", 0)
    while retry_count < 3 and (not total_chapters or total_chapters < 1):
        logger.info(f"[META] Đang lấy metadata lần {retry_count+1} từ web...")
        adapter = get_adapter(site_key)  # type:ignore
        details = await cached_get_story_details(adapter, url, title)
        update_metadata_from_details(metadata, details) #type:ignore
        retry_count += 1
        metadata["meta_retry_count"] = retry_count

        if isinstance(details, dict) and details.get("total_chapters_on_site"):
            metadata.update(details)
            metadata["total_chapters_on_site"] = details["total_chapters_on_site"]
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"[META] Đã cập nhật thành công metadata cho '{title}'")
            return True
        else:
            logger.warning(f"[META FAIL] Không lấy được metadata hợp lệ từ {url}")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)

    # === Nếu thất bại sau 3 lần: xoá folder ===
    if not metadata.get("total_chapters_on_site", 0):
        try:
            shutil.rmtree(story_folder)
            logger.info(f"[REMOVE] Xoá '{story_folder}' do không lấy được metadata.")
        except Exception as e:
            logger.error(f"[ERROR] Không thể xoá folder '{story_folder}': {e}")
        return False

    return True



def autofix_metadata(story_folder, site_key=None):
    if not os.path.exists(story_folder):
        logger.warning(f"[AUTO-FIX] Không tồn tại folder để autofix: {story_folder}")
        return {}
    folder_name = os.path.basename(story_folder)
    chapter_count = recount_chapters(story_folder)
    guessed_url = f"{BASE_URLS.get(site_key, '').rstrip('/')}/{folder_name}" if site_key else None
    parent_folder = os.path.basename(os.path.dirname(story_folder))
    genre_guess = parent_folder if parent_folder and parent_folder != os.path.basename(DATA_FOLDER) else None
    meta = {
        "title": folder_name.replace("-", " ").replace("_", " ").title().strip(),
        "url": guessed_url,
        "total_chapters_on_site": chapter_count,
        "description": "",
        "author": "",
        "cover": "",
        "categories": [{"name": genre_guess}] if genre_guess else [],
        "sources": [],
        "skip_crawl": False,
        "site_key": site_key
    }
    meta_path = os.path.join(story_folder, "metadata.json")
    # Chỉ tạo file nếu chưa tồn tại
    if not os.path.exists(meta_path):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info(f"[AUTO-FIX] Đã tạo metadata cho '{meta['title']}' ({chapter_count} chương)")
    return meta


def split_to_batches(items, num_batches):
    k, m = divmod(len(items), num_batches)
    return [items[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(num_batches)]

def get_auto_batch_count(fixed=None, default=10, min_batch=1, max_batch=20, num_items=None):
    if fixed is not None:
        return fixed
    batch = default
    if num_items:
        batch = min(batch, num_items)
    return min(batch, max_batch)

async def crawl_story_with_limit(
    site_key: str,
    session,
    missing_chapters: list,
    metadata: dict,
    current_category: dict,
    story_folder: str,
    crawl_state: dict,
    num_batches: int = 10,
    state_file: str = None,  # type: ignore
    adapter=None,
    chapters_all: list | None = None,
):
    await STORY_SEM.acquire()
    story_started_at = time.time()
    story_title = metadata.get('title', 'Unknown')

    # Emit story started event
    _emit_missing_event(
        "story_started",
        "story",
        site_key=site_key,
        extra={
            "story_title": story_title,
            "story_folder": os.path.basename(story_folder),
            "missing_count": len(missing_chapters),
            "total_chapters": metadata.get('total_chapters_on_site', 0),
        }
    )

    try:
        chapters_pool = chapters_all or missing_chapters
        if not chapters_pool:
            logger.info(f"[SKIP] Không có dữ liệu chương để crawl cho '{story_title}'")
            _emit_missing_event(
                "story_skipped",
                "story",
                site_key=site_key,
                started_at=story_started_at,
                extra={
                    "story_title": story_title,
                    "reason": "no_chapters_pool",
                }
            )
            return

        index_candidates = []
        for ch in missing_chapters or []:
            if not isinstance(ch, dict):
                continue
            idx = ch.get("idx")
            if isinstance(idx, int):
                index_candidates.append(idx)
                continue
            real_index = ch.get("index")
            if isinstance(real_index, int):
                index_candidates.append(real_index - 1)
                continue

            title = ch.get("title") or ""
            if isinstance(ch.get("aligned_index"), int):
                real_num = ch["aligned_index"]
            else:
                real_num = extract_real_chapter_number(title)
            if isinstance(real_num, int):
                index_candidates.append(real_num - 1)

        if not index_candidates:
            index_candidates = list(range(len(chapters_pool)))

        unique_indexes = sorted({idx for idx in index_candidates if isinstance(idx, int) and idx >= 0})
        if not unique_indexes:
            logger.info(f"[SKIP] Không tìm được index hợp lệ để crawl missing cho '{metadata.get('title')}'")
            return

        batch_count = max(1, min(num_batches, len(unique_indexes)))
        batches = split_to_batches(unique_indexes, batch_count)

        for batch_idx, batch in enumerate(batches):
            if not batch:
                continue
            logger.info(
                f"[MISSING][BATCH][{metadata.get('title')}] Batch {batch_idx+1}/{len(batches)} -> {len(batch)} targets (indexes={batch})"
            )
            await crawl_missing_with_limit(
                site_key,
                session,
                chapters_pool,
                metadata,
                current_category,
                story_folder,
                crawl_state,
                target_indexes=set(batch),
                state_file=state_file,
                adapter=adapter,
            )
            await smart_delay()

        # Emit story completed event
        _emit_missing_event(
            "story_completed",
            "story",
            site_key=site_key,
            started_at=story_started_at,
            status="completed",
            extra={
                "story_title": story_title,
                "story_folder": os.path.basename(story_folder),
                "batches_processed": len(batches),
            }
        )

    except Exception as exc:
        _emit_missing_event(
            "story_failed",
            "story",
            site_key=site_key,
            started_at=story_started_at,
            status="failed",
            error=exc,
            extra={
                "story_title": story_title,
            }
        )
        raise
    finally:
        STORY_SEM.release()
    logger.info(f"[MISSING][DONE][{story_title}] crawl_missing_story_with_limit completed")


async def crawl_missing_with_limit(
    site_key: str,
    session,
    chapters_all: list,
    metadata: dict,
    current_category: dict,
    story_folder: str,
    crawl_state: dict,
    num_batches: int = 10,
    state_file: str = None,  # type: ignore
    adapter=None,
    target_indexes: set[int] | None = None,
):
    if not state_file:
        state_file = get_missing_worker_state_file(site_key)
    num_targets = len(target_indexes) if target_indexes else len(chapters_all or [])
    logger.info(f"[MISSING][START][{metadata['title']}] Crawl missing (targets={num_targets})")
    timeout_seconds = calculate_missing_crawl_timeout(num_targets)
    async with SEM, domain_circuit_breaker.limit(site_key):
        result = await asyncio.wait_for(
            crawl_missing_chapters_for_story(
                site_key,
                session,
                chapters_all,
                metadata,
                current_category,
                story_folder,
                crawl_state,
                num_batches,
                state_file=state_file,
                adapter=adapter,
                target_indexes=target_indexes,
            ),
            timeout=timeout_seconds,
        )
    logger.info(
        f"[MISSING][DONE][{metadata['title']}] timeout={timeout_seconds:.0f}s targets={num_targets}"
    )
    return result

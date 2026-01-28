from __future__ import annotations

import argparse
import asyncio
import glob
import importlib.util
import inspect
import itertools
import json
import sys
import time
from collections import Counter

try:
    _AIOHTTP_SPEC = importlib.util.find_spec("aiohttp")
except (ModuleNotFoundError, ValueError):  # pragma: no cover - optional dependency missing
    _AIOHTTP_SPEC = None

if _AIOHTTP_SPEC:
    import aiohttp  # type: ignore
else:  # pragma: no cover - optional dependency missing
    aiohttp = None  # type: ignore
import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

try:
    _AIOGRAM_SPEC = importlib.util.find_spec("aiogram")
except (ModuleNotFoundError, ValueError):  # pragma: no cover - optional dependency missing
    _AIOGRAM_SPEC = None

if _AIOGRAM_SPEC:
    from aiogram import Router  # type: ignore
else:  # pragma: no cover - simple stand-in
    class Router:  # type: ignore
        def __init__(self, *args, **kwargs) -> None:
            pass
try:
    from pydantic import BaseModel  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback
    class BaseModel:  # type: ignore
        def __init__(self, **kwargs) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

        def model_dump(self) -> dict:
            return self.__dict__.copy()

from flowcore_story.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.adapters.factory import get_adapter
from flowcore_story.apps.scraper import initialize_scraper
from flowcore_story.config import config as app_config
from flowcore_story.config.proxy_provider import load_proxies
from flowcore_story.core.category_change_detector import CategoryChangeDetector
from flowcore_story.core.category_store import CategoryStore
from flowcore_story.core.config_loader import apply_env_overrides
from flowcore_story.core.crawl_planner import (
    CategoryCrawlPlan,
    build_category_plan,
    build_crawl_plan,
)
from flowcore_story.core.genre_progress import (
    log_category_resume_overview as _log_category_resume_overview,
)
from flowcore_story.core.genre_progress import (
    update_category_progress as _update_category_progress,
)
from flowcore_story.core.genre_retry import retry_failed_genres as _retry_failed_genres
from flowcore_story.core.sequential_genre_strategy import (
    get_genre_completion_status,
    discover_genre_total_only,
    save_genre_total,
    format_genre_status_log,
)
from flowcore_story.core.story_registry import StoryCrawlStatus, StoryRegistry
from flowcore_story.core.worker_settings import WorkerSettings
from flowcore_story.storage.story_queue import genre_queue_metadata, story_queue
from flowcore_story.kafka.kafka_monitor import check_consumer_group_health
from flowcore_story.kafka.kafka_producer import close_producer, send_job
from flowcore.utils.async_primitives import LoopBoundLock, LoopBoundSemaphore
from flowcore.utils.batch_utils import smart_delay
from flowcore.utils.chapter_utils import (
    count_dead_chapters,
    export_chapter_metadata_sync,
    get_chapter_filename,
    get_real_total_chapters,
    get_saved_chapters_files,
    slugify_title,
)
from flowcore.utils.domain_utils import get_site_key_from_url, is_url_for_site
from flowcore.utils.io_utils import (
    create_proxy_template_if_not_exists,
    ensure_directory_exists,
    log_failed_genre,
    resolve_completed_story_path,
)
from flowcore.utils.logger import logger
from flowcore.utils.meta_utils import (
    add_missing_story,
    backup_crawl_state,
    derive_story_slug,
    sanitize_filename,
    save_story_metadata_file,
)
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.notifier import send_telegram_notify
from flowcore.utils.progress_emitter import compact_payload, emit_progress_event
from flowcore.utils.skip_manager import (
    get_all_skipped_stories,
    is_story_skipped,
    load_skipped_stories,
    mark_story_as_skipped,
)
from flowcore.utils.state_utils import (
    clear_specific_state_keys,
    load_crawl_state,
    locked_crawl_state,
    merge_all_missing_workers_to_main,
    save_crawl_state,
    update_crawl_state_section,
)
from flowcore_story.workers.cookie_auto_renewal import (
    start_cookie_auto_renewal,
    stop_cookie_auto_renewal,
)
from flowcore_story.workers.crawler_missing_chapter import (
    check_and_crawl_missing_all_stories,
    loop_once_multi_sites,
)
from flowcore_story.workers.crawler_single_missing_chapter import crawl_single_story_worker
from flowcore_story.workers.missing_background_loop import (
    start_missing_background_loop,
    stop_missing_background_loop,
)
from flowcore_story.workers.multi_source_story_crawler import (
    StoryCrawlRequest,
    multi_source_story_crawler,
)
from flowcore_story.workers.system_metrics_monitor import (
    start_system_metrics_monitor,
    stop_system_metrics_monitor,
)

# Database tracking helpers
from flowcore_story.storage.db_tracking_helpers import (
    track_chapter_crawled,
    track_challenge_detected,
    track_genre_completed,
    track_genre_failed,
    track_genre_pages_discovered,
    track_genre_started,
    track_genre_story_processed,
    track_global_story_completed,
    track_request_failure,
    track_request_success,
    track_retry_queue_alert,
    track_retry_queue_enqueue,
    track_site_genres_initialized,
    track_skipped_queue_update,
    track_story_completed,
    track_story_discovered,
    track_story_failed,
    track_story_skipped,
    track_story_start_crawl,
    track_system_metrics,
)

__all__ = ["log_failed_genre"]

router = Router()


def _estimate_total_stories_from_state(crawl_state: dict[str, Any]) -> int | None:
    """Estimate total stories across all categories from stored crawl state."""

    if not isinstance(crawl_state, dict):
        return None

    plan = crawl_state.get("category_story_plan")
    if not isinstance(plan, dict) or not plan:
        return None

    total = 0
    for stories in plan.values():
        if isinstance(stories, list):
            total += sum(1 for item in stories if item is not None)
        elif isinstance(stories, dict):
            total += len(stories)
        else:
            try:
                count = int(stories)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
            if count > 0:
                total += count

    return total or None


is_crawling = False
GENRE_SEM = LoopBoundSemaphore(app_config.GENRE_ASYNC_LIMIT)
STORY_SEM = LoopBoundSemaphore(app_config.STORY_ASYNC_LIMIT)
STORY_BATCH_SIZE: int
DATA_FOLDER: str
PROXIES_FILE: str
PROXIES_FOLDER: str
FAILED_GENRES_FILE: str

RETRY_CONSUMER_HEALTHCHECK_TIMEOUT = 5.0


_RUNNER_TERMINAL_ACTIONS = {"succeeded", "failed", "completed", "finished"}


def _emit_runner_event(
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
    }

    if started_at is not None and action in _RUNNER_TERMINAL_ACTIONS:
        payload["duration"] = max(time.time() - started_at, 0.0)

    if extra:
        payload.update(extra)

    if error is not None:
        payload["error"] = str(error)
        payload["error_type"] = error.__class__.__name__

    emit_progress_event("runner", compact_payload(payload))


def refresh_runtime_settings() -> None:
    global STORY_BATCH_SIZE
    global DATA_FOLDER, PROXIES_FILE, PROXIES_FOLDER, FAILED_GENRES_FILE

    STORY_BATCH_SIZE = app_config.STORY_BATCH_SIZE
    GENRE_SEM.set_value(app_config.GENRE_ASYNC_LIMIT)
    STORY_SEM.set_value(app_config.STORY_ASYNC_LIMIT)
    DATA_FOLDER = app_config.DATA_FOLDER
    PROXIES_FILE = app_config.PROXIES_FILE
    PROXIES_FOLDER = app_config.PROXIES_FOLDER
    FAILED_GENRES_FILE = app_config.FAILED_GENRES_FILE


refresh_runtime_settings()


# Global stores - will be initialized lazily in async context
category_store = None
story_registry = None
_stores_initialized = False


async def _ensure_stores_initialized():
    """Lazy initialization of stores with MongoDB/SQLite factory"""
    global category_store, story_registry, _stores_initialized

    if _stores_initialized:
        return

    from flowcore_story.core.category_store_factory import create_category_store

    category_store = await create_category_store(app_config.CATEGORY_SNAPSHOT_DB_PATH)
    story_registry = StoryRegistry(app_config.CATEGORY_SNAPSHOT_DB_PATH, category_store)
    metrics_tracker.set_backlog_provider(story_registry.get_backlog_overview)
    _stores_initialized = True
    logger.info("[INIT] Stores initialized successfully")


PostCrawlCallable = Callable[[], Awaitable[Any]]
PostCrawlItem = tuple[str, PostCrawlCallable] | None
_post_crawl_queue: asyncio.Queue[PostCrawlItem] | None = None
_post_crawl_workers: list[asyncio.Task[None]] = []
_post_crawl_lock = LoopBoundLock()


def _determine_post_crawl_worker_count() -> int:
    value = getattr(app_config, "POST_CRAWL_WORKERS", None)
    try:
        if value is not None:
            parsed = int(value)
            if parsed > 0:
                return parsed
    except (TypeError, ValueError):
        pass
    return 2


async def _post_crawl_worker_loop(
    queue: asyncio.Queue[PostCrawlItem], worker_id: int
) -> None:
    while True:
        item = await queue.get()
        if item is None:
            logger.debug("[POST-CRAWL][WORKER %s] Shutdown signal received", worker_id)
            queue.task_done()
            break

        label, coro_factory = item
        logger.debug("[POST-CRAWL][WORKER %s] Starting task '%s'", worker_id, label)
        try:
            await coro_factory()
        except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
            queue.task_done()
            raise
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(
                "[POST-CRAWL][WORKER %s] Task '%s' failed: %s", worker_id, label, exc
            )
            queue.task_done()
        else:
            queue.task_done()
            logger.debug("[POST-CRAWL][WORKER %s] Finished task '%s'", worker_id, label)


async def _ensure_post_crawl_workers(
    concurrency: int | None = None,
) -> asyncio.Queue[PostCrawlItem] | None:
    global _post_crawl_queue, _post_crawl_workers

    desired = concurrency or _determine_post_crawl_worker_count()
    if desired <= 0:
        return None

    async with _post_crawl_lock:
        if _post_crawl_queue is None:
            _post_crawl_queue = asyncio.Queue()
            logger.debug("[POST-CRAWL] Queue initialised (desired=%s)", desired)

        # Remove finished workers to avoid memory leaks
        _post_crawl_workers = [
            worker for worker in _post_crawl_workers if not worker.done()
        ]

        while len(_post_crawl_workers) < desired:
            worker_id = len(_post_crawl_workers) + 1
            worker = asyncio.create_task(
                _post_crawl_worker_loop(_post_crawl_queue, worker_id)
            )
            _post_crawl_workers.append(worker)
            logger.debug("[POST-CRAWL] Spawned worker %s (total=%s)", worker_id, len(_post_crawl_workers))

        return _post_crawl_queue


async def start_post_crawl_workers(concurrency: int | None = None) -> None:
    await _ensure_post_crawl_workers(concurrency)
    await start_system_metrics_monitor()
    await start_cookie_auto_renewal()


async def shutdown_post_crawl_workers() -> None:
    global _post_crawl_queue, _post_crawl_workers

    async with _post_crawl_lock:
        queue = _post_crawl_queue
        workers = [worker for worker in _post_crawl_workers if not worker.done()]
        _post_crawl_workers = workers

    if not queue or not workers:
        async with _post_crawl_lock:
            _post_crawl_queue = None
            _post_crawl_workers = []
        await stop_cookie_auto_renewal()
        await stop_system_metrics_monitor()
        return

    await queue.join()
    for _ in workers:
        await queue.put(None)

    await asyncio.gather(*workers, return_exceptions=True)

    async with _post_crawl_lock:
        _post_crawl_queue = None
        _post_crawl_workers = []
    await stop_cookie_auto_renewal()
    await stop_system_metrics_monitor()


async def enqueue_post_crawl_task(
    label: str, coro_factory: PostCrawlCallable
) -> None:
    queue = await _ensure_post_crawl_workers()
    if not queue:
        await coro_factory()
        return

    logger.debug("[POST-CRAWL] Enqueue task '%s'", label)
    await queue.put((label, coro_factory))


def _normalize_timestamp(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


async def resolve_story_cooldown(
    crawl_state: dict[str, Any],
    story_url: str | None,
    candidate_hint: Any | None,
) -> float | None:
    """Resolve an effective cooldown timestamp and update shared state safely."""

    candidate = _normalize_timestamp(candidate_hint)
    now = time.time()
    async with locked_crawl_state(crawl_state) as state:
        cooldowns = state.setdefault("story_cooldowns", {})
        stored = (
            _normalize_timestamp(cooldowns.get(story_url))
            if story_url
            else None
        )
        if stored is not None:
            candidate = max(candidate or stored, stored)
        if candidate is not None and candidate > now:
            if story_url:
                cooldowns[story_url] = candidate
            return candidate
        if story_url and story_url in cooldowns:
            cooldowns.pop(story_url, None)
    return None


async def record_story_cooldown(
    crawl_state: dict[str, Any],
    story_url: str | None,
    cooldown_until: Any | None,
    *,
    state_file: str | None = None,
    site_key: str | None = None,
) -> bool:
    """Persist a cooldown timestamp if it is still in the future."""

    if not story_url:
        return False

    timestamp = _normalize_timestamp(cooldown_until)

    def _apply(state: dict[str, Any]) -> bool:
        cooldowns = state.setdefault("story_cooldowns", {})
        if timestamp is not None and timestamp > time.time():
            cooldowns[story_url] = timestamp
            return True
        cooldowns.pop(story_url, None)
        return False

    result = await update_crawl_state_section(
        crawl_state,
        _apply,
        component="story_cooldowns",
        state_file=state_file,
        site_key=site_key,
        debounce=0,
    )

    # ``update_crawl_state_section`` typically returns the boolean outcome of
    # ``_apply`` above. Some tests replace the helper with simplified mocks
    # that return arbitrary truthy values (for example the path of the saved
    # state file).  If we propagated those values directly the caller would
    # interpret them as ``True`` and exit early, skipping post-crawl tasks.
    # To make the behaviour robust we only treat an explicit ``True`` result
    # as a signal that the cooldown should prevent further processing.
    return True if result is True else False


async def crawl_single_story_by_title(title, site_key, genre_name=None):

    slug = slugify_title(title)
    folder = os.path.join(DATA_FOLDER, slug)
    meta_path = os.path.join(folder, "metadata.json")
    if not os.path.exists(meta_path):
        raise Exception(f"Không tìm thấy metadata cho truyện '{title}' (slug {slug})")
    with open(meta_path, encoding="utf-8") as f:
        story_data_item = json.load(f)
    site_key = story_data_item.get("site_key") or get_site_key_from_url(
        story_data_item.get("url")
    )
    assert site_key, "Không xác định được site_key"
    adapter = get_adapter(site_key)
    crawl_state = {}
    await process_story_item(None, story_data_item, {}, folder, crawl_state, adapter, site_key)  # type: ignore


async def process_genre_with_limit(
    session,
    category_plan,
    crawl_state,
    adapter,
    site_key,
    *,
    position: int | None = None,
    total_genres: int | None = None,
):
    async with GENRE_SEM:
        await process_genre_item(
            session,
            category_plan,
            crawl_state,
            adapter,
            site_key,
            position=position,
            total_genres=total_genres,
        )


async def process_story_with_limit(
    session: aiohttp.ClientSession,
    story: dict[str, Any],
    genre_data: dict[str, Any],
    crawl_state: dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str,
) -> bool:
    slug = derive_story_slug(story.get("title"), story.get("url"))
    folder = os.path.join(DATA_FOLDER, slug)
    await ensure_directory_exists(folder)
    return await process_story_item(
        session, story, genre_data, folder, crawl_state, adapter, site_key
    )


def sort_sources(sources):
    return sorted(sources, key=lambda s: s.get("priority", 100))


def _normalize_story_sources(
    story_data_item: dict[str, Any],
    current_site_key: str,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Merge and normalize source list between story data and metadata."""

    def _iter_raw_sources() -> list[Any]:
        raw: list[Any] = []
        if metadata and isinstance(metadata.get("sources"), list):
            raw.extend(metadata["sources"])
        if isinstance(story_data_item.get("sources"), list):
            raw.extend(story_data_item["sources"])
        primary_url = metadata.get("url") if metadata else None
        if primary_url:
            raw.append({"url": primary_url, "site_key": metadata.get("site_key")})
        current_url = story_data_item.get("url")
        if current_url:
            raw.append({"url": current_url, "site_key": current_site_key, "priority": 1})
        return raw

    raw_sources = _iter_raw_sources()
    normalized: list[dict[str, Any]] = []
    seen: dict[tuple[str, str], int] = {}

    def _upsert(url: str | None, site_key: str | None, priority: Any) -> None:
        if not url:
            return
        url = url.strip()
        if not url:
            return

        derived_site = get_site_key_from_url(url)
        site_candidate = site_key or derived_site or current_site_key
        if not site_candidate:
            return
        if derived_site and derived_site != site_candidate:
            site_candidate = derived_site
        if not is_url_for_site(url, site_candidate):
            return

        identity = (url, site_candidate)
        priority_val = priority if isinstance(priority, (int, float)) else None

        if identity in seen:
            existing = normalized[seen[identity]]
            if priority_val is not None:
                existing_priority = existing.get("priority")
                if (
                    not isinstance(existing_priority, (int, float))
                    or priority_val < existing_priority
                ):
                    existing["priority"] = priority_val
            return

        entry: dict[str, Any] = {"url": url, "site_key": site_candidate}
        if priority_val is not None:
            entry["priority"] = priority_val
        normalized.append(entry)
        seen[identity] = len(normalized) - 1

    for source in raw_sources:
        if isinstance(source, str):
            _upsert(source, None, None)
        elif isinstance(source, dict):
            _upsert(source.get("url"), source.get("site_key") or source.get("site"), source.get("priority"))

    normalized_sorted = sort_sources(normalized)
    previous_story_sources = story_data_item.get("sources")
    story_data_item["sources"] = normalized_sorted

    metadata_changed = False
    if metadata is not None:
        if metadata.get("sources") != normalized_sorted:
            metadata["sources"] = normalized_sorted
            metadata_changed = True

    return metadata_changed or previous_story_sources != normalized_sorted


async def _read_json_async(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        return None

    def _read() -> dict[str, Any] | None:
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    try:
        return await asyncio.to_thread(_read)
    except Exception:
        return None


async def _write_json_async(path: str, payload: dict[str, Any]) -> None:
    def _write() -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)

    await asyncio.to_thread(_write)


def _needs_metadata_update(
    metadata: dict[str, Any] | None, fields_need_check: list[str]
) -> bool:
    if not metadata:
        return True
    for field in fields_need_check:
        if metadata.get(field) is None:
            return True
    return False


@dataclass
class MetadataPreparationResult:
    metadata: dict[str, Any]
    details: dict[str, Any]
    metadata_for_update: dict[str, Any]
    metadata_dirty: bool
    primary_site_key: str


async def _prepare_story_metadata(
    story_data_item: dict[str, Any],
    current_discovery_genre_data: dict[str, Any],
    story_global_folder_path: str,
    metadata_file: str,
    adapter: BaseSiteAdapter,
    fields_need_check: list[str],
    site_key: str,
) -> MetadataPreparationResult:
    existing_metadata = await _read_json_async(metadata_file)
    need_update = _needs_metadata_update(existing_metadata, fields_need_check)

    details: dict[str, Any]
    if need_update:
        details = await adapter.get_story_details(
            story_data_item["url"], story_data_item["title"]
        ) or {}
        await save_story_metadata_file(
            story_data_item,
            current_discovery_genre_data,
            story_global_folder_path,
            details,
            existing_metadata,
        )
        if existing_metadata:
            for field in fields_need_check:
                if details.get(field) is not None:  # type: ignore[union-attr]
                    existing_metadata[field] = details[field]  # type: ignore[index]
            existing_metadata["metadata_updated_at"] = time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            try:
                await _write_json_async(metadata_file, existing_metadata)
            except Exception:
                logger.exception(
                    "[META] Không thể ghi metadata bổ sung cho %s",
                    story_data_item.get("title"),
                )
    else:
        details = existing_metadata.copy() if existing_metadata else {}

    refreshed_metadata = await _read_json_async(metadata_file) or existing_metadata or {}
    if details:
        story_data_item.update(details)

    metadata_for_update = refreshed_metadata.copy() if isinstance(refreshed_metadata, dict) else {}
    metadata_dirty = False

    primary_site_key = (
        metadata_for_update.get("site_key")
        or story_data_item.get("site_key")
        or site_key
    )
    if not primary_site_key:
        primary_site_key = site_key

    if story_data_item.get("site_key") != primary_site_key:
        story_data_item["site_key"] = primary_site_key

    if metadata_for_update.get("site_key") != primary_site_key:
        metadata_for_update["site_key"] = primary_site_key
        metadata_dirty = True

    if _normalize_story_sources(story_data_item, primary_site_key, metadata_for_update):
        metadata_dirty = True

    if metadata_for_update and story_data_item.get("sources"):
        story_data_item["sources"] = metadata_for_update.get(
            "sources", story_data_item.get("sources")
        )

    return MetadataPreparationResult(
        metadata=refreshed_metadata if isinstance(refreshed_metadata, dict) else {},
        details=details,
        metadata_for_update=metadata_for_update,
        metadata_dirty=metadata_dirty,
        primary_site_key=primary_site_key,
    )


async def _update_story_state_before_crawl(
    crawl_state: dict[str, Any],
    story_data_item: dict[str, Any],
    site_key: str,
) -> str:
    story_url = story_data_item.get("url")
    state_file = app_config.get_state_file(site_key)
    async def _apply(state: dict[str, Any]) -> None:
        if not story_url:
            return

        state["current_story_url"] = story_url
        if state.get("previous_story_url_in_state_for_chapters") != story_url:
            state["processed_chapter_urls_for_current_story"] = []
        state["previous_story_url_in_state_for_chapters"] = story_url

    await update_crawl_state_section(
        crawl_state,
        _apply,
        component="current_story",
        state_file=state_file,
        site_key=site_key,
        debounce=0,
    )
    return state_file


@dataclass
class ChapterEvaluation:
    files_actual: list[str]
    expected_total: int | None
    dead_chapters: int
    blocked: bool
    is_complete: bool


async def _evaluate_chapter_progress(
    story_data_item: dict[str, Any],
    details: dict[str, Any],
    story_global_folder_path: str,
    metadata_file: str,
    adapter: BaseSiteAdapter,
    *,
    chapter_limit: int | None = None,
) -> ChapterEvaluation:
    files_actual_set = await asyncio.to_thread(
        get_saved_chapters_files, story_global_folder_path
    )
    files_actual = sorted(files_actual_set)
    raw_total_chapters = details.get("total_chapters_on_site") if isinstance(details, dict) else None
    metadata_total = (
        raw_total_chapters
        if isinstance(raw_total_chapters, int) and raw_total_chapters > 0
        else None
    )
    total_chapters_on_site = metadata_total
    if isinstance(chapter_limit, int) and chapter_limit >= 0:
        if total_chapters_on_site is None:
            total_chapters_on_site = chapter_limit
        else:
            total_chapters_on_site = min(total_chapters_on_site, chapter_limit)

    story_title = story_data_item.get("title")
    story_url = story_data_item.get("url")

    real_total_on_site: int | None = None
    try:
        real_total_on_site = await get_real_total_chapters(story_data_item, adapter)
    except Exception as ex:  # pragma: no cover - network/adapter issues
        logger.warning(
            f"[DONE][STORY] Không lấy được tổng chương thực tế trên web cho '{story_title}': {ex}"
        )

    if (
        isinstance(real_total_on_site, int)
        and real_total_on_site > 0
        and isinstance(chapter_limit, int)
        and chapter_limit >= 0
    ):
        real_total_on_site = min(real_total_on_site, chapter_limit)

    total_candidates_for_check = [
        value
        for value in (
            total_chapters_on_site
            if isinstance(total_chapters_on_site, int) and total_chapters_on_site > 0
            else None,
            real_total_on_site
            if isinstance(real_total_on_site, int) and real_total_on_site > 0
            else None,
        )
        if value is not None
    ]
    expected_total_chapters = (
        max(total_candidates_for_check) if total_candidates_for_check else None
    )

    is_new_crawl = os.path.exists(metadata_file) and len(files_actual) == 0

    if expected_total_chapters:
        crawled_chapters = len(files_actual)
        if is_new_crawl:
            logger.info(
                f"[NEW] Đang crawl mới truyện '{story_title}': {crawled_chapters}/{expected_total_chapters} chương. Đợi crawl hoàn tất rồi mới kiểm tra thiếu chương."
            )
        else:
            if crawled_chapters < 0.1 * expected_total_chapters:
                logger.error(
                    f"[ALERT] Parse chương có thể lỗi HTML hoặc bị chặn: {story_title} ({crawled_chapters}/{expected_total_chapters})"
                )

            if crawled_chapters < expected_total_chapters:
                expected_files: list[str] = []
                chapter_list = []
                if isinstance(details, dict):
                    chapter_list = details.get("chapter_list", [])  # type: ignore[assignment]
                for i in range(expected_total_chapters):
                    if chapter_list and i < len(chapter_list):
                        chapter_title = chapter_list[i].get("title", "untitled")
                    else:
                        chapter_title = "untitled"
                    filename = f"{i+1:04d}_{sanitize_filename(chapter_title)}.txt"
                    expected_files.append(filename)
                files_actual_set = set(files_actual)
                missing_files = [
                    fname for fname in expected_files if fname not in files_actual_set
                ]
                logger.error(
                    f"[BLOCK] Truyện '{story_title}' còn thiếu {expected_total_chapters - crawled_chapters} chương. Không next!"
                )
                logger.error(f"[BLOCK] Danh sách file chương thiếu: {missing_files}")
                await add_missing_story(
                    story_title,
                    story_url,
                    expected_total_chapters,
                    crawled_chapters,
                )
                return ChapterEvaluation(
                    files_actual=files_actual,
                    expected_total=expected_total_chapters,
                    dead_chapters=0,
                    blocked=True,
                    is_complete=False,
                )
            else:
                logger.info(
                    f"Truyện '{story_title}' đã crawl đủ {crawled_chapters}/{expected_total_chapters} chương."
                )

    dead_chapter_count = await asyncio.to_thread(
        count_dead_chapters, story_global_folder_path
    )
    is_complete = False
    if expected_total_chapters:
        if (len(files_actual) + dead_chapter_count) >= expected_total_chapters:
            is_complete = True
            try:
                txt_count_final = len(files_actual)
                logger.info(
                    f"[DONE][STORY][FINAL] '{story_title}' txt+dead={txt_count_final + dead_chapter_count}/{expected_total_chapters}"
                )
            except Exception:
                pass

    return ChapterEvaluation(
        files_actual=files_actual,
        expected_total=expected_total_chapters,
        dead_chapters=dead_chapter_count,
        blocked=False,
        is_complete=is_complete,
    )


async def _finalize_story_state_after_crawl(
    crawl_state: dict[str, Any],
    site_key: str,
    state_file: str,
    story_url: str | None,
    is_complete: bool,
) -> str:
    await asyncio.to_thread(backup_crawl_state, state_file)
    state_file = app_config.get_state_file(site_key)

    def _apply(state: dict[str, Any]) -> None:
        if is_complete and story_url:
            completed = set(state.get("globally_completed_story_urls", []))
            completed.add(story_url)
            state["globally_completed_story_urls"] = sorted(completed)

        if "processed_chapter_urls_for_current_story" in state:
            del state["processed_chapter_urls_for_current_story"]

    await update_crawl_state_section(
        crawl_state,
        _apply,
        component="story_finalize",
        state_file=state_file,
        site_key=site_key,
        debounce=0,
    )
    return state_file


async def _export_chapter_metadata(
    story_data_item: dict[str, Any],
    story_global_folder_path: str,
    expected_total_chapters: int | None,
    site_key: str,
) -> None:
    try:
        chapters = None
        for src in story_data_item.get("sources", []):
            url = src.get("url")
            if not url:
                continue
            source_site = (
                src.get("site_key")
                or story_data_item.get("site_key")
                or get_site_key_from_url(url)
                or site_key
            )
            try:
                adapter_cache_meta: dict[str, BaseSiteAdapter] = getattr(
                    crawl_all_sources_until_full, "_adapter_cache", {}
                )
                source_adapter = adapter_cache_meta.get(source_site)
                if not source_adapter:
                    source_adapter = get_adapter(source_site)
                    adapter_cache_meta[source_site] = source_adapter
                    crawl_all_sources_until_full._adapter_cache = adapter_cache_meta  # type: ignore[attr-defined]
                    await initialize_scraper(source_site)
            except Exception as ex:
                logger.debug(
                    f"[CHAPTER_META] Bỏ qua nguồn {url} do không lấy được adapter: {ex}"
                )
                continue

            chapters = await source_adapter.get_chapter_list(
                story_url=url,
                story_title=story_data_item.get("title"),
                site_key=source_site,
                total_chapters=expected_total_chapters,
            )
            if chapters and len(chapters) > 0:
                break
        if chapters and len(chapters) > 0:
            await asyncio.to_thread(
                export_chapter_metadata_sync, story_global_folder_path, chapters
            )
        else:
            logger.warning(
                f"[CHAPTER_META] Không lấy được danh sách chương khi export metadata cho {story_data_item.get('title')}"
            )
    except Exception as ex:
        logger.warning(f"[CHAPTER_META] Lỗi khi export chapter_metadata.json: {ex}")


async def _write_metadata_if_dirty(
    metadata_file: str,
    metadata_dirty: bool,
    metadata_for_update: dict[str, Any],
    story_data_item: dict[str, Any],
    primary_site_key: str,
) -> None:
    if not metadata_dirty:
        return

    if metadata_for_update:
        metadata_for_write = metadata_for_update.copy()
        metadata_for_write.pop("chapters", None)
    else:
        metadata_for_write = {}
        for key in (
            "title",
            "url",
            "author",
            "cover",
            "description",
            "categories",
            "status",
            "source",
            "rating_value",
            "rating_count",
            "total_chapters_on_site",
        ):
            if story_data_item.get(key) is not None:
                metadata_for_write[key] = story_data_item.get(key)
        metadata_for_write["metadata_updated_at"] = time.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        metadata_for_write.setdefault(
            "crawled_at", time.strftime("%Y-%m-%d %H:%M:%S")
        )

    metadata_for_write["site_key"] = primary_site_key
    metadata_for_write["sources"] = story_data_item.get("sources", [])
    try:
        await _write_json_async(metadata_file, metadata_for_write)
    except Exception as ex:
        logger.error(
            f"[META] Không thể lưu metadata đã cập nhật nguồn cho '{story_data_item.get('title')}': {ex}"
        )


def _extract_category_identifier(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("category_id", "id", "slug", "slug_id", "code"):
        value = payload.get(key)
        if isinstance(value, (str, int)):
            text = str(value).strip()
            if text:
                return text
    return None


def _derive_category_identifier(
    category_plan: CategoryCrawlPlan | None,
    genre_data: dict[str, Any],
    site_key: str,
) -> str:
    candidates: list[str | None] = []
    if category_plan is not None:
        candidates.append(_extract_category_identifier(category_plan.metadata))
        candidates.append(_extract_category_identifier(category_plan.raw_genre))
    candidates.append(_extract_category_identifier(genre_data))
    for candidate in candidates:
        if candidate:
            return str(candidate)
    fallback = genre_data.get("url") or genre_data.get("name")
    if isinstance(fallback, str) and fallback.strip():
        return fallback.strip()
    return f"{site_key}:unknown"


def _load_previous_category_change_state(
    crawl_state: dict[str, Any], site_key: str
) -> dict[str, Any]:
    container = crawl_state.get("category_change_detector")
    if isinstance(container, dict):
        previous = container.get(site_key)
        if isinstance(previous, dict):
            return previous
    return {}


def _store_category_change_state(
    crawl_state: dict[str, Any], site_key: str, state: dict[str, Any]
) -> None:
    container = crawl_state.setdefault("category_change_detector", {})
    if isinstance(container, dict):
        container[site_key] = state


def _build_category_refresh_jobs(
    category: CategoryCrawlPlan,
    site_key: str,
    *,
    category_id: str,
    batch_size: int,
    change_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    stories = list(category.stories)
    if not stories:
        return []

    chunk_size = max(1, int(batch_size))
    batches: list[dict[str, Any]] = []
    total_parts = (len(stories) + chunk_size - 1) // chunk_size
    for index, start in enumerate(range(0, len(stories), chunk_size), start=1):
        chunk = stories[start : start + chunk_size]
        if not chunk:
            continue
        batches.append(
            {
                "type": "category_batch_refresh",
                "site_key": site_key,
                "category_id": category_id,
                "category_name": category.name,
                "category_url": category.url,
                "metadata": dict(category.metadata),
                "raw_genre": dict(category.raw_genre),
                "stories": list(chunk),
                "part_index": index,
                "total_parts": total_parts,
                "change_summary": dict(change_summary),
            }
        )
    return batches


async def crawl_all_sources_until_full(
    site_key,
    session,
    story_data_item,
    current_discovery_genre_data,
    story_folder_path,
    crawl_state,
    num_batches=10,
    state_file=None,
    adapter: BaseSiteAdapter = None,  # type: ignore
    *,
    category_id: str | None = None,
    story_registry_id: int | None = None,
):
    request = StoryCrawlRequest(
        site_key=site_key,
        session=session,
        story_data_item=story_data_item,
        current_discovery_genre_data=current_discovery_genre_data,
        story_folder_path=story_folder_path,
        crawl_state=crawl_state,
        num_batches=num_batches,
        state_file=state_file,
        adapter=adapter,
        category_id=category_id,
        story_registry_id=story_registry_id,
    )
    return await multi_source_story_crawler.crawl_story_until_complete(request)


async def initialize_and_log_setup_with_state(site_key) -> tuple[str, dict[str, Any]]:
    await ensure_directory_exists(DATA_FOLDER)
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)
    metrics_tracker.update_system_metrics(
        proxies_in_use=len(app_config.LOADED_PROXIES)
    )
    await initialize_scraper(site_key)
    try:
        if app_config.AI_PRINT_METRICS:
            from flowcore_story.ai.selector_ai import print_ai_metrics_summary

            print_ai_metrics_summary()
    except Exception:
        pass
    homepage_url = app_config.BASE_URLS[site_key].rstrip("/") + "/"
    state_file = app_config.get_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    logger.info("=== BẮT ĐẦU QUÁ TRÌNH CRAWL ASYNC ===")
    logger.info(f"Thư mục lưu dữ liệu: {os.path.abspath(DATA_FOLDER)}")
    logger.info(f"Sử dụng {len(app_config.LOADED_PROXIES)} proxy(s).")
    logger.info(
        f"Giới hạn: {app_config.MAX_GENRES_TO_CRAWL or 'Không giới hạn'} thể loại, "
        f"{app_config.MAX_STORIES_TOTAL_PER_GENRE or 'Không giới hạn'} truyện/thể loại."
    )
    logger.info(
        f"Giới hạn chương xử lý ban đầu/truyện: {app_config.MAX_CHAPTERS_PER_STORY or 'Không giới hạn'}."
    )
    logger.info(f"Số lượt thử lại cho các chương lỗi: {app_config.RETRY_FAILED_CHAPTERS_PASSES}.")
    logger.info(
        f"Giới hạn số trang truyện/thể loại: {app_config.MAX_STORIES_PER_GENRE_PAGE or 'Không giới hạn'}."
    )
    logger.info(
        f"Giới hạn số trang danh sách chương: {app_config.MAX_CHAPTER_PAGES_TO_CRAWL or 'Không giới hạn'}."
    )
    if crawl_state:
        loggable = {
            k: v
            for k, v in crawl_state.items()
            if k
            not in [
                "processed_chapter_urls_for_current_story",
                "globally_completed_story_urls",
            ]
        }
        if "processed_chapter_urls_for_current_story" in crawl_state:
            loggable["processed_chapters_count"] = len(
                crawl_state["processed_chapter_urls_for_current_story"]
            )
        if "globally_completed_story_urls" in crawl_state:
            loggable["globally_completed_stories_count"] = len(
                crawl_state["globally_completed_story_urls"]
            )
        logger.info(f"Tìm thấy trạng thái crawl trước đó: {loggable}")
        _log_category_resume_overview(site_key, crawl_state)
    completed_global_count = 0
    if isinstance(crawl_state, dict):
        completed_global_count = len(
            crawl_state.get("globally_completed_story_urls", [])
        )
    total_story_estimate = _estimate_total_stories_from_state(crawl_state)
    metrics_tracker.update_global_story_totals(
        completed=completed_global_count,
        total_estimate=total_story_estimate,
    )
    logger.info("-----------------------------------------")
    return homepage_url, crawl_state


async def process_story_item(
    session: aiohttp.ClientSession,
    story_data_item: dict[str, Any],
    current_discovery_genre_data: dict[str, Any],
    story_global_folder_path: str,
    crawl_state: dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str,
    *,
    category_id: str | None = None,
    story_registry_id: int | None = None,
) -> bool:
    await ensure_directory_exists(story_global_folder_path)
    logger.debug(f"\n  --- Xử lý truyện: {story_data_item['title']} ---")

    if category_id and isinstance(category_id, str) and category_id.strip():
        current_discovery_genre_data.setdefault("category_id", category_id)
    if story_registry_id is not None:
        story_data_item["registry_story_id"] = story_registry_id

    story_url = story_data_item.get("url")
    cooldown_candidate = await resolve_story_cooldown(
        crawl_state, story_url, story_data_item.get("_cooldown_until")
    )
    if cooldown_candidate:
        story_data_item["_cooldown_until"] = cooldown_candidate
        human_ts = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(cooldown_candidate)
        )
        logger.info(
            f"[COOLDOWN][STORY] Bỏ qua '{story_data_item['title']}' tạm thời đến {human_ts}."
        )
        return False
    story_data_item.pop("_cooldown_until", None)

    # [DB OPTIMIZATION] Check if story already completed in database
    from datetime import datetime, timedelta, timezone
    from flowcore_story.storage.db_tracking import story_progress
    existing_progress = await story_progress.get_story_progress(story_url)
    if existing_progress:
        if existing_progress.get("status") == "completed":
            # Check if completed story is recent enough to trust
            completed_at = existing_progress.get("completed_at")
            if completed_at:
                if isinstance(completed_at, str):
                    from dateutil import parser
                    completed_at = parser.parse(completed_at)

                days_since_completion = (datetime.now(timezone.utc) - completed_at).days

                # Only skip if completed within last 90 days (to re-validate old data)
                if days_since_completion < 90:
                    logger.info(
                        f"[DB_SKIP] Story '{story_data_item.get('title')}' completed {days_since_completion}d ago "
                        f"({existing_progress.get('crawled_chapters')}/{existing_progress.get('total_chapters')} chapters). Skipping."
                    )
                    return True  # Return True to mark as successful skip
                else:
                    logger.info(
                        f"[DB_REVALIDATE] Story '{story_data_item.get('title')}' completed >90d ago. "
                        f"Re-crawling to validate data freshness."
                    )
        elif existing_progress.get("status") == "running":
            # Story is in progress, continue but log the existing progress
            logger.debug(
                f"[DB_RESUME] Story '{story_data_item.get('title')}' in progress: "
                f"{existing_progress.get('crawled_chapters')}/{existing_progress.get('total_chapters')} chapters"
            )

    metadata_file = os.path.join(story_global_folder_path, "metadata.json")
    fields_need_check = [
        "description",
        "status",
        "source",
        "rating_value",
        "rating_count",
        "total_chapters_on_site",
    ]
    story_data_item.setdefault("site_key", site_key)
    metadata_context = await _prepare_story_metadata(
        story_data_item,
        current_discovery_genre_data,
        story_global_folder_path,
        metadata_file,
        adapter,
        fields_need_check,
        site_key,
    )

    state_file = await _update_story_state_before_crawl(
        crawl_state, story_data_item, site_key
    )

    # Track story discovery/start in database
    total_chapters_hint = story_data_item.get("total_chapters_on_site", 0) or story_data_item.get("total_chapters", 0)
    await track_story_discovered(
        story_url=story_url,
        title=story_data_item.get("title", "Unknown"),
        total_chapters=total_chapters_hint,
        site_key=site_key,
        genre_name=current_discovery_genre_data.get("genre_name"),
        genre_url=current_discovery_genre_data.get("genre_url"),
    )

    # 2. Crawl tất cả nguồn cho đến khi đủ chương
    async with STORY_SEM:
        chapter_limit_hint = await crawl_all_sources_until_full(
            site_key,
            session,
            story_data_item,
            current_discovery_genre_data,
            story_global_folder_path,
            crawl_state,
            num_batches=app_config.NUM_CHAPTER_BATCHES,
            state_file=state_file,
            adapter=adapter,
            category_id=category_id,
            story_registry_id=story_registry_id,
        )
    if isinstance(chapter_limit_hint, int) and chapter_limit_hint >= 0:
        story_data_item["_chapter_limit"] = chapter_limit_hint

    cooldown_after_crawl_raw = story_data_item.get("_cooldown_until")
    cooldown_after_crawl = _normalize_timestamp(cooldown_after_crawl_raw)
    if cooldown_after_crawl is not None:
        story_data_item["_cooldown_until"] = cooldown_after_crawl
    if await record_story_cooldown(
        crawl_state,
        story_url,
        cooldown_after_crawl,
        state_file=state_file,
        site_key=site_key,
    ):
        human_ts = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(cooldown_after_crawl)
        )
        logger.warning(
            f"[COOLDOWN][STORY] Truyện '{story_data_item['title']}' tạm hoãn xử lý thêm đến {human_ts}."
        )
        return False
    story_data_item.pop("_cooldown_until", None)

    # 3. Kiểm tra số chương đã crawl thực tế
    chapter_limit_config = story_data_item.get("_chapter_limit")
    chapter_evaluation = await _evaluate_chapter_progress(
        story_data_item,
        metadata_context.details,
        story_global_folder_path,
        metadata_file,
        adapter,
        chapter_limit=chapter_limit_config,
    )
    if chapter_evaluation.blocked:
        return False

    _ = await _finalize_story_state_after_crawl(
        crawl_state,
        site_key,
        state_file,
        story_data_item.get("url"),
        chapter_evaluation.is_complete,
    )
    # 6. Export lại metadata chương cho đồng bộ DB/index
    async def _export_task(
        story_snapshot: dict[str, Any] | None = None,
        folder: str = story_global_folder_path,
        expected: int | None = chapter_evaluation.expected_total,
        primary_site: str = site_key,
    ) -> None:
        effective_snapshot = dict(story_snapshot) if story_snapshot is not None else dict(story_data_item)
        await _export_chapter_metadata(
            effective_snapshot,
            folder,
            expected,
            primary_site,
        )

    await enqueue_post_crawl_task("export_chapter_metadata", _export_task)

    # 7. Gửi job fallback để kiểm tra lại chương bị thiếu
    should_enqueue_missing_job = chapter_evaluation.is_complete or (
        chapter_evaluation.expected_total is None
        and len(chapter_evaluation.files_actual) > 0
    )

    if should_enqueue_missing_job:
        fallback_job = {
            "type": "check_missing_chapters",
            "story_folder_path": story_global_folder_path,
            "site_key": site_key,
        }
        if chapter_evaluation.is_complete:
            logger.info(
                "[MISSING][QUEUE] '%s' đã đủ chương (txt=%d, dead=%d, expected=%s) → gửi job kiểm tra missing.",
                story_data_item["title"],
                len(chapter_evaluation.files_actual),
                chapter_evaluation.dead_chapters,
                chapter_evaluation.expected_total,
            )
        else:
            logger.info(
                "[MISSING][QUEUE] '%s' chưa có tổng chương rõ ràng nhưng đã crawl được %d file, vẫn gửi job kiểm tra missing.",
                story_data_item["title"],
                len(chapter_evaluation.files_actual),
            )
        await enqueue_post_crawl_task(
            "send_missing_chapter_job",
            lambda payload=fallback_job: send_job(payload),
        )
    else:
        logger.info(
            f"[MISSING][SKIP] '{story_data_item['title']}' chưa đủ chương (txt={len(chapter_evaluation.files_actual)}) nên chưa gửi job kiểm tra missing."
        )

    metadata_dirty = metadata_context.metadata_dirty
    metadata_updates = dict(metadata_context.metadata_for_update)
    story_snapshot = dict(story_data_item)
    primary_site = metadata_context.primary_site_key

    await enqueue_post_crawl_task(
        "write_story_metadata",
        lambda metadata_file=metadata_file,
        metadata_dirty=metadata_dirty,
        metadata_updates=metadata_updates,
        story_snapshot=story_snapshot,
        primary_site=primary_site: _write_metadata_if_dirty(
            metadata_file,
            metadata_dirty,
            metadata_updates,
            story_snapshot,
            primary_site,
        ),
    )

    # 8. Send sync event to DatabaseSyncWorker (Event-Driven Sync)
    genre_name = current_discovery_genre_data.get("genre_name") or current_discovery_genre_data.get("name")
    completed_story_path = resolve_completed_story_path(story_global_folder_path, genre_name)
    if completed_story_path:
        sync_event = {
            "type": "story_updated",
            "story_path": completed_story_path,
            "site_key": site_key,
            "story_title": story_data_item.get("title"),
            "genre_name": genre_name,
            "timestamp": time.time(),
        }
        await enqueue_post_crawl_task(
            "send_sync_event",
            lambda payload=sync_event: send_job(payload, topic="storyflow.sync"),
        )
    else:
        logger.debug(
            "[SYNC] Skip sync event for %s; completed path not ready.",
            story_data_item.get("title"),
        )

    # Track story completion/progress in database
    crawled_count = len(chapter_evaluation.files_actual) if chapter_evaluation.files_actual else 0
    total_count = chapter_evaluation.expected_total or total_chapters_hint or crawled_count

    if chapter_evaluation.is_complete:
        await track_story_completed(
            story_url=story_url,
            title=story_data_item.get("title", "Unknown"),
            total_chapters=total_count,
        )
    else:
        # Update progress even if not complete
        await track_story_discovered(
            story_url=story_url,
            title=story_data_item.get("title", "Unknown"),
            total_chapters=total_count,
            site_key=site_key,
            genre_name=current_discovery_genre_data.get("genre_name"),
            genre_url=current_discovery_genre_data.get("genre_url"),
        )

    return chapter_evaluation.is_complete


async def process_genre_item(
    session: aiohttp.ClientSession,
    category_or_genre: CategoryCrawlPlan | dict[str, Any],
    crawl_state: dict[str, Any],
    adapter: BaseSiteAdapter,
    site_key: str,
    *,
    position: int | None = None,
    total_genres: int | None = None,
) -> None:
    category_plan: CategoryCrawlPlan | None
    planned_story_total_hint: int | None = None
    if isinstance(category_or_genre, CategoryCrawlPlan):
        category_plan = category_or_genre
        genre_data: dict[str, Any] = dict(category_or_genre.raw_genre) if category_or_genre.raw_genre else {}
        genre_data.setdefault("name", category_or_genre.name)
        genre_data.setdefault("url", category_or_genre.url)
        stories: list[dict[str, Any]] = list(category_or_genre.stories)
        planned_story_total_hint = category_or_genre.planned_story_total
        if position is None and category_or_genre.metadata.get("position") is not None:
            position = category_or_genre.metadata.get("position")
        if total_genres is None and category_or_genre.metadata.get("total_genres") is not None:
            total_genres = category_or_genre.metadata.get("total_genres")
    else:
        category_plan = None
        genre_data = dict(category_or_genre)
        stories = []

    genre_name = genre_data.get("name")
    genre_url = genre_data.get("url")
    if not genre_name or not genre_url:
        logger.warning("[GENRE] Bỏ qua genre thiếu thông tin: %s", genre_data)
        return

    logger.info(f"\n--- Xử lý thể loại: {genre_name} ---")

    # [DB OPTIMIZATION] Check if genre recently completed to avoid redundant crawls
    # Only skip if completed AND updated within last 24 hours (to catch new stories)
    from datetime import datetime, timedelta, timezone
    from flowcore_story.storage.db_tracking import genre_progress
    existing_genre = await genre_progress.get_genre_progress(site_key, genre_url)
    if existing_genre and existing_genre.get("status") == "completed":
        updated_at = existing_genre.get("updated_at")
        if updated_at:
            # Parse updated_at if it's a string
            if isinstance(updated_at, str):
                from dateutil import parser
                updated_at = parser.parse(updated_at)

            time_since_update = datetime.now(timezone.utc) - updated_at
            if time_since_update < timedelta(hours=24):
                logger.info(
                    f"[DB_SKIP] Genre '{genre_name}' was completed {time_since_update.seconds//3600}h ago. "
                    f"Skipping to avoid redundant crawl. "
                    f"({existing_genre.get('processed_stories')}/{existing_genre.get('total_stories')} stories)"
                )
                return
            else:
                logger.info(
                    f"[DB_REFRESH] Genre '{genre_name}' completed >24h ago. "
                    f"Re-crawling to check for new stories."
                )

    def _coerce_index(value: Any | None) -> int | None:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    backlog_overview = await story_registry.get_backlog_overview_async()
    backlog_total = backlog_overview.get("total_backlog", 0)
    total_genres_active = _coerce_index(backlog_overview.get("total_genres"))
    logger.info(f"Backlog toàn hệ thống: {backlog_total}")

    position_value = _coerce_index(position)
    if position_value is None and isinstance(category_plan, CategoryCrawlPlan):
        position_value = _coerce_index(category_plan.metadata.get("position"))
    total_genres_value = _coerce_index(total_genres) or total_genres_active
    if position_value and total_genres_value:
        logger.info(
            f"Đang crawl: {genre_name} ({position_value}/{total_genres_value} thể loại)"
        )
    elif total_genres_value:
        logger.info(f"Đang crawl: {genre_name} (tổng {total_genres_value} thể loại)")

    metrics_tracker.genre_started(
        site_key,
        genre_name,
        genre_url,
        position=position,
        total_genres=total_genres,
    )

    # Track genre start in database
    await track_genre_started(
        site_key=site_key,
        genre_name=genre_name,
        genre_url=genre_url,
        position=position,
        total_genres=total_genres,
    )

    state_file = app_config.get_state_file(site_key)
    async with locked_crawl_state(crawl_state) as state:
        state["current_genre_url"] = genre_url
        state["current_genre_name"] = genre_name
        if state.get("previous_genre_url_in_state_for_stories") != genre_url:
            state["current_story_index_in_genre"] = 0
        state["previous_genre_url_in_state_for_stories"] = genre_url
        await save_crawl_state(state, state_file, site_key=site_key)

        completed_global = set(state.get("globally_completed_story_urls", []))
        start_idx = state.get("current_story_index_in_genre", 0)

    # [QUEUE] Check for existing queue from previous run (resume logic)
    queue_meta = await genre_queue_metadata.get_metadata(site_key, genre_url)
    has_queue = queue_meta and queue_meta.get("planning_status") in ("ready", "processing")

    if has_queue:
        logger.info(
            f"[RESUME] Found existing queue for {genre_name}: "
            f"{queue_meta.get('pending_count', 0)} pending, "
            f"{queue_meta.get('completed_count', 0)} completed stories"
        )
        if planned_story_total_hint is None:
            planned_story_total_hint = queue_meta.get("total_stories")
        # Don't load stories into RAM - will read from queue later
        if not stories:
            stories = []

    if category_plan is None and not has_queue:
        built_plan = await build_category_plan(
            adapter,
            genre_data,
            site_key,
            position=position,
            total_genres=total_genres,
            max_pages=app_config.MAX_STORIES_PER_GENRE_PAGE,
        )
        if not built_plan:
            return
        category_plan = built_plan
        genre_data = dict(built_plan.raw_genre) if built_plan.raw_genre else {}
        genre_data.setdefault("name", built_plan.name)
        genre_data.setdefault("url", built_plan.url)
        genre_name = genre_data["name"]
        genre_url = genre_data["url"]
        stories = list(built_plan.stories)
        planned_story_total_hint = built_plan.planned_story_total
        async with locked_crawl_state(crawl_state) as state:
            plan_mapping = state.setdefault("category_story_plan", {})
            # [QUEUE] Store metadata only instead of full story list (RAM optimization)
            plan_mapping[genre_name] = {
                "queued": True,
                "total": len(built_plan.stories),
                "timestamp": time.time(),
            }
            await save_crawl_state(state, state_file, site_key=site_key)
    else:
        stories = list(stories)

    category_identifier = _derive_category_identifier(category_plan, genre_data, site_key)
    genre_data.setdefault("category_id", category_identifier)

    max_total = app_config.MAX_STORIES_TOTAL_PER_GENRE or 0
    limit_total = max_total if max_total > 0 else None

    initial_total = len(stories)
    if limit_total is not None:
        initial_total = min(initial_total, limit_total)

    planned_total = max(start_idx, 0)
    processed_count = 0
    last_processed_index = start_idx
    skipped_titles: list[str] = []

    async def update_progress(completed: bool = False) -> None:
        processed_so_far = min(start_idx + processed_count, planned_total)
        async with locked_crawl_state(crawl_state) as state:
            _update_category_progress(
                state,
                genre_name,
                planned_total,
                processed_so_far,
                completed=completed
                or (planned_total > 0 and processed_so_far >= planned_total),
            )

    await update_progress()

    @dataclass(slots=True)
    class StoryJob:
        index: int
        story: dict[str, Any]
        base_priority: int
        retry_count: int = 0
        category_id: str = category_identifier

    queue: asyncio.PriorityQueue[tuple[int, int, StoryJob]] = asyncio.PriorityQueue()
    job_counter = itertools.count()
    pending_requeues: set[asyncio.Task] = set()
    SENTINEL = object()

    discovered_total = start_idx

    def _set_story_total() -> None:
        metrics_tracker.set_genre_story_total(site_key, genre_url, max(planned_total, 0))

    # Prefer planner's total hint when available (more accurate for lazy discovery)
    if isinstance(planned_story_total_hint, int) and planned_story_total_hint > 0:
        planned_total = max(planned_total, planned_story_total_hint)
    planned_total = max(planned_total, initial_total)
    _set_story_total()

    plan_summary_payload = {
        "type": "plan_summary",
        "action": "plan_summary",
        "site_key": site_key,
        "genre": genre_name,
        "genre_name": genre_name,
        "genre_url": genre_url,
        "category_id": category_identifier,
        "status": "planned",
        "processed_stories": min(start_idx, planned_total),
        "total_stories": planned_total,
        "total": planned_total,
        "timestamp": time.time(),
    }
    if total_genres_value is not None:
        plan_summary_payload["total_genres"] = total_genres_value
    if position_value is not None:
        plan_summary_payload["position"] = position_value
    emit_progress_event("genre_story", plan_summary_payload)

    async def enqueue_story(story: dict[str, Any], page_number: int | None = None) -> None:
        nonlocal discovered_total, planned_total
        source_page = story.get("_source_page")
        if isinstance(page_number, int):
            source_page = page_number
        if not isinstance(source_page, int) or source_page <= 0:
            source_page = 1
        story["_source_page"] = source_page

        discovered_total += 1
        overall_idx = discovered_total - 1
        if limit_total is not None and overall_idx >= limit_total:
            return
        if overall_idx < start_idx:
            return

        raw_priority = story.get("priority")
        if isinstance(raw_priority, int):
            base_priority = raw_priority
        else:
            base_priority = source_page * 1000 + overall_idx

        registry_entry = await story_registry.ensure_entry_async(
            site_key, story, genre_name
        )

        if story_registry.is_terminal_status(registry_entry.status):
            logger.debug(
                "[REGISTRY] Bỏ qua truyện '%s' vì trạng thái %s đã hoàn tất.",
                story.get("title") or story.get("url") or overall_idx,
                registry_entry.status.value,
            )
            return

        if registry_entry.status == StoryCrawlStatus.IN_PROGRESS:
            recovered = await story_registry.mark_status_async(
                registry_entry.story_id,
                StoryCrawlStatus.NEEDS_RETRY,
                result="recovered_after_restart",
                category_name=genre_name,
            )
            if recovered:
                registry_entry = recovered

        job = StoryJob(
            index=overall_idx,
            story=story,
            base_priority=base_priority,
            category_id=category_identifier,
        )
        await queue.put((job.base_priority, next(job_counter), job))

        planned_total = max(planned_total, min(discovered_total, limit_total or discovered_total))
        _set_story_total()
        await update_progress()

    async def stream_existing(story_list: list[dict[str, Any]]) -> None:
        for idx, story in enumerate(story_list):
            if limit_total is not None and (start_idx + idx) >= limit_total:
                break
            await enqueue_story(story, story.get("_source_page"))

    async def discovery() -> None:
        try:
            # [QUEUE] If resuming from queue, skip discovery
            if has_queue:
                logger.info(f"[QUEUE] Resuming from persistent queue for {genre_name}")
                return

            if stories:
                await stream_existing(stories)
                return

            async def on_page(page_stories: list[dict[str, Any]], page_number: int, _total_pages: int | None) -> None:
                for item in page_stories:
                    await enqueue_story(item, page_number)

            await adapter.get_all_stories_from_genre_with_page_check(
                genre_name,
                genre_url,
                site_key,
                max_pages=app_config.MAX_STORIES_PER_GENRE_PAGE,
                page_callback=on_page,
                collect=False,
            )
        finally:
            if not stories:
                async with locked_crawl_state(crawl_state) as state:
                    plan_mapping = state.setdefault("category_story_plan", {})
                    plan_mapping.setdefault(genre_name, [])

    async def handle_story(job: StoryJob) -> tuple[bool, str | None, int, bool, bool, StoryCrawlStatus]:
        load_skipped_stories()
        story = job.story
        idx = job.index
        title = story.get("title", f"Story #{idx + 1}")
        story_url = story.get("url")
        registry_entry = await story_registry.ensure_entry_async(
            site_key, story, genre_name
        )

        if is_story_skipped(story):
            logger.warning(f"[SKIP] Truyện {title} đã bị skip vĩnh viễn trước đó, bỏ qua.")
            await story_registry.mark_status_async(
                registry_entry.story_id,
                StoryCrawlStatus.SKIPPED,
                result="permanent_skip",
                category_name=genre_name,
            )
            return True, story_url, idx, False, False, StoryCrawlStatus.SKIPPED

        acquired_entry = await story_registry.try_acquire_async(
            registry_entry.story_id,
            category_name=genre_name,
        )
        if not acquired_entry:
            logger.debug(
                "[REGISTRY] '%s' đang được worker khác xử lý, bỏ qua batch hiện tại.",
                title,
            )
            return False, story_url, idx, False, False, StoryCrawlStatus.IN_PROGRESS

        if acquired_entry.status != StoryCrawlStatus.IN_PROGRESS:
            if story_registry.is_terminal_status(acquired_entry.status):
                logger.debug(
                    "[REGISTRY] '%s' đã hoàn tất với trạng thái %s, bỏ qua.",
                    title,
                    acquired_entry.status.value,
                )
                return True, story_url, idx, False, False, acquired_entry.status

            logger.debug(
                "[REGISTRY] '%s' ở trạng thái %s, không thể acquire.",
                title,
                acquired_entry.status.value,
            )
            return False, story_url, idx, False, False, acquired_entry.status

        registry_story_id = acquired_entry.story_id
        registry_status = StoryCrawlStatus.IN_PROGRESS
        registry_result: str | None = None

        total_for_category = planned_total or 0
        if total_for_category <= 0:
            total_for_category = len(stories)
        total_for_category = max(total_for_category or 0, idx + 1)
        if genre_name:
            logger.debug(
                f"Truyện đang xử lý: {title} thuộc {genre_name}, {idx + 1}/{total_for_category}"
            )
        else:
            logger.debug(
                f"Truyện đang xử lý: {title}, {idx + 1}/{total_for_category}"
            )
        cooldown_candidate = await resolve_story_cooldown(
            crawl_state, story_url, story.get("_cooldown_until")
        )
        if cooldown_candidate and story_url:
            story["_cooldown_until"] = cooldown_candidate

        metrics_tracker.genre_story_started(
            site_key,
            genre_url,
            title,
            story_page=story.get("_source_page"),
            story_position=idx + 1,
            story_id=registry_story_id,
        )

        skip_due_to_cooldown = False
        processed_successfully = False
        is_cooldown = False
        done = False
        result_tuple: tuple[bool, str, int, bool, bool, StoryCrawlStatus] | None = None
        try:
            if cooldown_candidate:
                human_ts = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(cooldown_candidate)
                )
                logger.info(
                    f"[COOLDOWN][STORY] Bỏ qua '{title}' trong batch cho đến {human_ts}."
                )
                registry_status = StoryCrawlStatus.COOLDOWN
                registry_result = f"cooldown_until={human_ts}"
                skip_due_to_cooldown = True
            else:
                slug = derive_story_slug(title, story_url)
                folder = os.path.join(DATA_FOLDER, slug)
                await ensure_directory_exists(folder)
                if story_url and story_url in completed_global:
                    details = await adapter.get_story_details(story_url, title)
                    await save_story_metadata_file(
                        story, genre_data, folder, details, None
                    )
                    done = True
                    processed_successfully = True
                    registry_status = StoryCrawlStatus.COMPLETED
                    registry_result = "completed"
                else:
                    async def _mark_story_index(state: dict[str, Any]) -> None:
                        state["current_story_index_in_genre"] = idx

                    await update_crawl_state_section(
                        crawl_state,
                        _mark_story_index,
                        component="current_genre_progress",
                        state_file=state_file,
                        site_key=site_key,
                        save=False,
                    )

                    result = await process_story_item(
                        session,
                        story,
                        genre_data,
                        folder,
                        crawl_state,
                        adapter,
                        site_key,
                        category_id=job.category_id,
                        story_registry_id=registry_story_id,
                    )
                    done = bool(result)
                    processed_successfully = bool(result)
                    cooldown_after_raw = story.get("_cooldown_until")
                    cooldown_after = _normalize_timestamp(cooldown_after_raw)
                    is_cooldown = await record_story_cooldown(
                        crawl_state,
                        story_url,
                        cooldown_after,
                        state_file=state_file,
                        site_key=site_key,
                    )
                    if is_cooldown and cooldown_after is not None:
                        story["_cooldown_until"] = cooldown_after
                        registry_status = StoryCrawlStatus.COOLDOWN
                        human_ts = time.strftime(
                            "%Y-%m-%d %H:%M:%S", time.localtime(cooldown_after)
                        )
                        registry_result = f"cooldown_until={human_ts}"
                    else:
                        story.pop("_cooldown_until", None)
                        if done and processed_successfully:
                            registry_status = StoryCrawlStatus.COMPLETED
                            registry_result = "completed"
                        elif processed_successfully:
                            registry_status = StoryCrawlStatus.NEEDS_RETRY
                            registry_result = "partial_success"
                        else:
                            registry_status = StoryCrawlStatus.NEEDS_RETRY
                            registry_result = "no_progress"
                    result_tuple = (
                        done,
                        story_url,
                        idx,
                        is_cooldown,
                        processed_successfully,
                        registry_status,
                    )
        except asyncio.CancelledError:
            registry_status = StoryCrawlStatus.NEEDS_RETRY
            registry_result = "cancelled"
            raise
        except Exception as exc:
            registry_status = StoryCrawlStatus.NEEDS_RETRY
            registry_result = f"exception:{exc}"
            raise
        finally:
            if is_story_skipped(story):
                registry_status = StoryCrawlStatus.PERMANENT_FAIL
                if not registry_result:
                    registry_result = "permanent_skip"
            elif registry_status == StoryCrawlStatus.FAILED:
                registry_status = StoryCrawlStatus.NEEDS_RETRY
            metrics_tracker.genre_story_finished(
                site_key,
                genre_url,
                title,
                processed=processed_successfully,
                story_id=registry_story_id,
            )
            await story_registry.mark_status_async(
                registry_story_id,
                registry_status,
                result=registry_result,
                category_name=genre_name,
            )

            if skip_due_to_cooldown:
                result_tuple = (
                    False,
                    story_url,
                    idx,
                    True,
                    False,
                    registry_status,
                )
            elif result_tuple is None:
                result_tuple = (
                    done,
                    story_url,
                    idx,
                    is_cooldown,
                    processed_successfully,
                    registry_status,
                )
        return result_tuple

    def schedule_cooldown(job: StoryJob, ready_at: float) -> None:
        delay = max(ready_at - time.time(), 0.0)

        async def _requeue() -> None:
            await asyncio.sleep(delay)
            await queue.put((job.base_priority + job.retry_count, next(job_counter), job))

        task = asyncio.create_task(_requeue())
        pending_requeues.add(task)

        def _done_callback(task: asyncio.Task) -> None:
            pending_requeues.discard(task)
            if task.exception():  # pragma: no cover - defensive logging
                logger.warning(
                    f"[COOLDOWN] Requeue task for '{job.story.get('title', job.index)}' failed: {task.exception()}"
                )

        task.add_done_callback(_done_callback)

    async def worker() -> None:
        nonlocal processed_count, last_processed_index, planned_total
        retry_limit = app_config.RETRY_STORY_ROUND_LIMIT
        while True:
            priority, _seq, item = await queue.get()
            if item is SENTINEL:
                queue.task_done()
                break
            job = item
            try:
                done, url, idx, cooldown, processed_successfully, registry_status = await handle_story(job)
            except Exception:
                queue.task_done()
                raise

            requeue_requested = False
            if registry_status == StoryCrawlStatus.IN_PROGRESS and not done and not processed_successfully and not cooldown:
                queue.task_done()
                continue

            if cooldown:
                ready_at = job.story.get("_cooldown_until")
                if isinstance(ready_at, (int, float)):
                    schedule_cooldown(job, float(ready_at))
                else:
                    schedule_cooldown(job, time.time() + 60)
                requeue_requested = True
            elif (
                not done
                and not processed_successfully
                and registry_status
                not in {StoryCrawlStatus.PERMANENT_FAIL, StoryCrawlStatus.SKIPPED}
            ):
                job.retry_count += 1
                if job.retry_count >= retry_limit:
                    title = job.story.get("title", f"Story #{idx + 1}")
                    logger.error(
                        f"[FATAL] Vượt quá retry cho truyện {title}, bỏ qua."
                    )
                    mark_story_as_skipped(job.story, "retry quá giới hạn")
                    registry_entry = await story_registry.ensure_entry_async(
                        site_key, job.story, genre_name
                    )
                    await story_registry.mark_status_async(
                        registry_entry.story_id,
                        StoryCrawlStatus.PERMANENT_FAIL,
                        result="retry_limit_exceeded",
                        category_name=genre_name,
                    )
                    registry_status = StoryCrawlStatus.PERMANENT_FAIL
                    skipped_titles.append(title)
                else:
                    await smart_delay()
                    await queue.put((job.base_priority + job.retry_count, next(job_counter), job))
                    requeue_requested = True

            if not requeue_requested:
                if processed_successfully:
                    processed_count += 1
                if done and url:
                    completed_global.add(url)
                last_processed_index = max(last_processed_index, idx + 1)
                planned_total = max(planned_total, last_processed_index)
                await update_progress()

            queue.task_done()

    # [QUEUE] Queue-based processing mode (persistent queue)
    if has_queue:
        logger.info(f"[QUEUE] Starting queue-based processing for {genre_name}")

        async def queue_worker() -> None:
            """Worker that processes stories from persistent queue."""
            nonlocal processed_count, last_processed_index, planned_total
            retry_limit = app_config.RETRY_STORY_ROUND_LIMIT

            while True:
                # Fetch next batch from queue
                pending_stories = await story_queue.dequeue_next(
                    site_key=site_key,
                    genre_url=genre_url,
                    worker_id="main",
                    limit=10,  # Batch size
                )

                if not pending_stories:
                    logger.info(f"[QUEUE] No more pending stories for {genre_name}")
                    break

                # Process each story in batch
                for story in pending_stories:
                    queue_id = story.get("_queue_id")
                    idx = story.get("_queue_discovery_index", 0)

                    try:
                        # Create job from queue story
                        job = StoryJob(
                            index=idx,
                            story=story,
                            base_priority=story.get("_queue_priority", 0),
                            category_id=category_identifier,
                        )

                        # Process story
                        done, url, idx, cooldown, processed_successfully, registry_status = await handle_story(job)

                        # Mark queue entry with appropriate status
                        if cooldown:
                            # Keep as processing, will be retried
                            logger.debug(f"[QUEUE] Story {queue_id} in cooldown, keeping as processing")
                            await story_queue.complete_story(queue_id, "processing")
                        elif registry_status == StoryCrawlStatus.SKIPPED or registry_status == StoryCrawlStatus.PERMANENT_FAIL:
                            await story_queue.complete_story(queue_id, "skipped")
                        elif not done and not processed_successfully:
                            await story_queue.complete_story(queue_id, "failed", error="Processing failed")
                        else:
                            await story_queue.complete_story(queue_id, "completed")

                        # Update counters
                        if processed_successfully:
                            processed_count += 1
                        if done and url:
                            completed_global.add(url)
                        last_processed_index = max(last_processed_index, idx + 1)
                        planned_total = max(planned_total, last_processed_index)
                        await update_progress()

                    except Exception as e:
                        logger.error(f"[QUEUE] Error processing story {queue_id}: {e}")
                        await story_queue.complete_story(queue_id, "failed", error=str(e))

        # Run queue worker
        await queue_worker()

    else:
        # [QUEUE] In-memory queue processing mode (existing logic)
        worker_count = max(1, app_config.STORY_ASYNC_LIMIT)
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        discovery_task = asyncio.create_task(discovery())

        try:
            await discovery_task
            while True:
                await queue.join()
                if not pending_requeues:
                    break
                await asyncio.sleep(0)
            for _ in range(worker_count):
                await queue.put((float("inf"), next(job_counter), SENTINEL))
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                if not task.done():
                    task.cancel()
            if not discovery_task.done():
                discovery_task.cancel()

    async with locked_crawl_state(crawl_state) as state:
        state["globally_completed_story_urls"] = sorted(completed_global)
        state["current_story_index_in_genre"] = max(
            state.get("current_story_index_in_genre", 0),
            last_processed_index,
        )
        await save_crawl_state(state, state_file, site_key=site_key)
    metrics_tracker.update_global_story_totals(
        completed=len(completed_global),
        total_estimate=_estimate_total_stories_from_state(crawl_state),
    )

    await update_progress(completed=True)

    if skipped_titles:
        logger.warning("[STREAM] Các truyện bị skip: " + ", ".join(skipped_titles))
        all_titles = ", ".join(
            str(title)
            for title in (
                (
                    story.get("title")
                    or story.get("slug")
                    or story.get("story_url")
                    or ""
                )
                for story in get_all_skipped_stories().values()
            )
            if title
        )
        logger.debug("[SKIP LIST] " + all_titles)

    metrics_tracker.genre_completed(
        site_key,
        genre_url,
        stories_processed=processed_count,
    )

    # Track genre completion in database
    await track_genre_completed(
        site_key=site_key,
        genre_name=genre_name,
        genre_url=genre_url,
        total_stories=planned_total,
        processed_stories=processed_count,
    )

    # Update global story totals
    await track_global_story_completed(increment=processed_count)

    async with locked_crawl_state(crawl_state) as state:
        await clear_specific_state_keys(
            state,
            [
                "current_story_index_in_genre",
                "current_genre_url",
                "previous_genre_url_in_state_for_stories",
            ],
            state_file,
        )

async def retry_failed_genres(
    adapter, site_key, settings: WorkerSettings, shuffle_func
):
    await _retry_failed_genres(
        adapter,
        site_key,
        settings,
        shuffle_func,
        process_genre_with_limit,
        client_session_factory=(getattr(aiohttp, "ClientSession", None) if aiohttp else None),
    )


async def run_genres(
    site_key: str,
    settings: WorkerSettings,
    crawl_state: dict[str, Any] | None = None,
):
    logger.info(f"[GENRE] Bắt đầu crawl thể loại cho site: {site_key}")
    started_at = time.time()
    _emit_runner_event("started", "genres", site_key=site_key)
    adapter = get_adapter(site_key)
    genres: list[Any] = []
    try:
        await initialize_scraper(site_key)
        genres = await adapter.get_genres()
        if app_config.MAX_GENRES_TO_CRAWL:
            limited_genres = genres[: app_config.MAX_GENRES_TO_CRAWL]
            if len(limited_genres) != len(genres):
                logger.info(
                    f"[GENRE] Áp dụng giới hạn {app_config.MAX_GENRES_TO_CRAWL} thể loại đầu tiên (từ tổng {len(genres)})."
                )
            genres = limited_genres
        await run_crawler(adapter, site_key, genres, settings, crawl_state)
    except Exception as exc:
        _emit_runner_event(
            "failed",
            "genres",
            site_key=site_key,
            started_at=started_at,
            error=exc,
        )
        raise
    else:
        _emit_runner_event(
            "succeeded",
            "genres",
            site_key=site_key,
            started_at=started_at,
            extra={"genres_planned": len(genres)},
        )


async def run_missing(site_key: str, homepage_url: str | None = None):
    homepage_url = homepage_url or app_config.BASE_URLS[site_key].rstrip("/") + "/"
    logger.info("[MISSING] Bắt đầu crawl chương thiếu...")
    started_at = time.time()
    _emit_runner_event(
        "started",
        "missing_scan",
        site_key=site_key,
        extra={"homepage_url": homepage_url},
    )
    adapter = get_adapter(site_key)
    try:
        await initialize_scraper(site_key)
        await check_and_crawl_missing_all_stories(
            adapter, homepage_url, site_key=site_key
        )
    except Exception as exc:
        _emit_runner_event(
            "failed",
            "missing_scan",
            site_key=site_key,
            started_at=started_at,
            extra={"homepage_url": homepage_url},
            error=exc,
        )
        raise
    else:
        _emit_runner_event(
            "succeeded",
            "missing_scan",
            site_key=site_key,
            started_at=started_at,
            extra={"homepage_url": homepage_url},
        )


async def crawl_all_missing_stories(
    site_key: str | None = None,
    homepage_url: str | None = None,
    force_unskip: bool = False,
):
    """Trigger crawl for missing chapters.

    Nếu truyền site_key → chỉ xử lý site đó (tái sử dụng run_missing).
    Nếu không → gọi worker để quét toàn bộ site.
    """

    started_at = time.time()
    _emit_runner_event(
        "started",
        "missing_dispatch",
        site_key=site_key,
        extra={"force_unskip": force_unskip},
    )

    mode = "single_site" if site_key else "multi_site"

    try:
        if site_key:
            await run_missing(site_key, homepage_url)
        else:
            await loop_once_multi_sites(force_unskip=force_unskip)
    except Exception as exc:
        _emit_runner_event(
            "failed",
            "missing_dispatch",
            site_key=site_key,
            started_at=started_at,
            extra={"force_unskip": force_unskip, "mode": mode},
            error=exc,
        )
        raise
    else:
        _emit_runner_event(
            "succeeded",
            "missing_dispatch",
            site_key=site_key,
            started_at=started_at,
            extra={"force_unskip": force_unskip, "mode": mode},
        )


async def _default_crawl_missing_chapters(
    site_key: str | None = None,
    homepage_url: str | None = None,
    force_unskip: bool = False,
) -> None:
    """Legacy entry point retained for backwards compatibility."""
    await crawl_all_missing_stories(
        site_key=site_key,
        homepage_url=homepage_url,
        force_unskip=force_unskip,
    )


crawl_missing_chapters = _default_crawl_missing_chapters


async def run_single_story(
    title: str, site_key: str | None = None, genre_name: str | None = None
):
    from main import crawl_single_story_by_title

    logger.info(
        f"[SINGLE] Crawl truyện '{title}' (site: {site_key or 'auto-detect'})..."
    )
    started_at = time.time()
    _emit_runner_event(
        "started",
        "single_story",
        site_key=site_key,
        extra={"title": title, "genre_name": genre_name},
    )
    if crawl_missing_chapters is not _default_crawl_missing_chapters:
        await crawl_missing_chapters(
            site_key=site_key,
            homepage_url=None,
            force_unskip=False,
        )
        _emit_runner_event(
            "succeeded",
            "single_story",
            site_key=site_key,
            started_at=started_at,
            extra={"title": title, "genre_name": genre_name},
        )
        return
    try:
        await crawl_single_story_by_title(title, site_key, genre_name)
    except Exception as exc:
        _emit_runner_event(
            "failed",
            "single_story",
            site_key=site_key,
            started_at=started_at,
            extra={"title": title, "genre_name": genre_name},
            error=exc,
        )
        raise
    else:
        _emit_runner_event(
            "succeeded",
            "single_story",
            site_key=site_key,
            started_at=started_at,
            extra={"title": title, "genre_name": genre_name},
        )


async def run_sequential_genre_crawl(
    adapter,
    site_key: str,
    genres: list,
    crawl_state: dict[str, Any],
    settings: WorkerSettings,
):
    """
    Sequential genre crawl implementation.
    Process one genre to completion before moving to next.
    """
    from flowcore_story.storage.db_pool import get_db_pool

    pool = await get_db_pool()
    if not pool:
        logger.error("[SEQUENTIAL] No database pool available")
        return

    state_file = app_config.get_state_file(site_key)

    logger.info(f"[SEQUENTIAL] ════════════════════════════════════════")
    logger.info(f"[SEQUENTIAL] Starting sequential crawl for {site_key}")
    logger.info(f"[SEQUENTIAL] Total genres: {len(genres)}")
    logger.info(f"[SEQUENTIAL] ════════════════════════════════════════\n")

    genres_completed = 0
    genres_in_progress = 0
    genres_needs_discovery = 0

    async with aiohttp.ClientSession() as session:
        for idx, genre_data in enumerate(genres, 1):
            genre_name = genre_data.get("name", "Unknown")
            genre_url = genre_data.get("url")

            if not genre_url:
                logger.warning(f"[SEQUENTIAL] Skipping genre without URL: {genre_name}")
                continue

            logger.info(f"\n[SEQUENTIAL] ┌─ Genre {idx}/{len(genres)}: {genre_name}")
            logger.info(f"[SEQUENTIAL] │  URL: {genre_url}")

            # Step 1: Get completion status
            status = await get_genre_completion_status(site_key, genre_url)

            status_log = format_genre_status_log(genre_name, status)
            logger.info(f"[SEQUENTIAL] │  Status: {status_log}")

            # Step 2: Decide action based on status
            if status['is_complete']:
                logger.info(f"[SEQUENTIAL] └─ ✅ COMPLETE - Skipping to next\n")
                genres_completed += 1
                continue

            # Step 3: Process incomplete genre
            # Check if we need to continue discovery (even if queue exists)
            if status['needs_discovery']:
                # Need discovery (or continue discovery)
                if status.get('discovery_interrupted'):
                    logger.warning(
                        f"[SEQUENTIAL] │  ⚠️ Discovery was interrupted (planning_status=planning)"
                    )
                    logger.info(f"[SEQUENTIAL] │  Action: 🔍 Resume discovery...")
                elif status['has_queue']:
                    logger.info(
                        f"[SEQUENTIAL] │  ⚠️ Queue incomplete "
                        f"({status['pending'] + status['completed']}/{status['total_stories'] or '?'} stories)"
                    )
                    logger.info(f"[SEQUENTIAL] │  Action: 🔍 Continue discovery...")
                else:
                    logger.info(f"[SEQUENTIAL] │  Action: 🔍 Start discovery...")

                genres_needs_discovery += 1

                # Step 3a: Quick discovery to get total (if not known or wrong)
                total_in_queue = status['pending'] + status['completed'] + status['processing']
                total_is_wrong = (
                    status['total_stories'] is not None and
                    status['total_stories'] < total_in_queue
                )

                if status['total_stories'] is None or total_is_wrong:
                    if total_is_wrong:
                        logger.warning(
                            f"[SEQUENTIAL] │  ⚠️ Total is wrong ({status['total_stories']} < {total_in_queue} in queue)"
                        )
                    logger.info(f"[SEQUENTIAL] │  📊 Getting total for progress tracking...")

                    total_estimate = await discover_genre_total_only(
                        adapter,
                        genre_data,
                        site_key,
                    )

                    if total_estimate:
                        logger.info(
                            f"[SEQUENTIAL] │  Total estimated: {total_estimate} stories"
                        )
                        await save_genre_total(
                            site_key,
                            genre_name,
                            genre_url,
                            total_estimate,
                        )
                    else:
                        logger.warning(
                            f"[SEQUENTIAL] │  Could not estimate total, proceeding anyway..."
                        )

                # Step 3b: Full discovery and process
                logger.info(f"[SEQUENTIAL] │  🚀 Full discovery and processing...")

                # Build full plan
                plan = await build_category_plan(
                    adapter,
                    genre_data,
                    site_key,
                    position=idx,
                    total_genres=len(genres),
                    max_pages=app_config.MAX_STORIES_PER_GENRE_PAGE,
                )

                if plan:
                    # Save total from full discovery plan
                    if plan.planned_story_total and plan.planned_story_total > 0:
                        logger.info(
                            f"[SEQUENTIAL] │  Total from discovery: {plan.planned_story_total} stories"
                        )
                        await save_genre_total(
                            site_key,
                            genre_name,
                            genre_url,
                            plan.planned_story_total,
                        )

                    # Process genre with plan
                    await process_genre_item(
                        session=session,
                        category_or_genre=plan,
                        crawl_state=crawl_state,
                        adapter=adapter,
                        site_key=site_key,
                        position=idx,
                        total_genres=len(genres),
                    )

                    logger.info(f"[SEQUENTIAL] └─ ✓ Discovery and processing completed\n")
                else:
                    logger.error(f"[SEQUENTIAL] └─ ✗ Failed to build plan\n")

            elif status['has_queue']:
                # Has complete queue - just process from queue
                genres_in_progress += 1

                # Get total for progress tracking if needed (or if wrong)
                total_in_queue = status['pending'] + status['completed'] + status['processing']
                total_is_wrong = (
                    status['total_stories'] is not None and
                    status['total_stories'] < total_in_queue
                )

                if status['total_stories'] is None or total_is_wrong:
                    if total_is_wrong:
                        logger.warning(
                            f"[SEQUENTIAL] │  ⚠️ Total is wrong ({status['total_stories']} < {total_in_queue} in queue)"
                        )
                    logger.info(f"[SEQUENTIAL] │  📊 Getting total for progress tracking...")

                    total_estimate = await discover_genre_total_only(
                        adapter,
                        genre_data,
                        site_key,
                    )

                    if total_estimate:
                        logger.info(
                            f"[SEQUENTIAL] │  Total estimated: {total_estimate} stories"
                        )
                        await save_genre_total(
                            site_key,
                            genre_name,
                            genre_url,
                            total_estimate,
                        )

                # Process from complete queue
                logger.info(
                    f"[SEQUENTIAL] │  Action: 📋 Processing from existing queue "
                    f"({status['pending']} pending)"
                )

                # Process genre (will use queue)
                await process_genre_item(
                    session=session,
                    category_or_genre=genre_data,
                    crawl_state=crawl_state,
                    adapter=adapter,
                    site_key=site_key,
                    position=idx,
                    total_genres=len(genres),
                )

                logger.info(f"[SEQUENTIAL] └─ ✓ Processed from queue\n")

            else:
                # Shouldn't happen, but log it
                logger.warning(
                    f"[SEQUENTIAL] └─ ⚠ Unknown state - skipping\n"
                )

            # Progress summary
            logger.info(
                f"[SEQUENTIAL] Progress: {genres_completed} done, "
                f"{genres_in_progress} in progress, "
                f"{genres_needs_discovery} discovered"
            )

    # Final summary
    logger.info(f"\n[SEQUENTIAL] ════════════════════════════════════════")
    logger.info(f"[SEQUENTIAL] Crawl Summary for {site_key}")
    logger.info(f"[SEQUENTIAL] ────────────────────────────────────────")
    logger.info(f"[SEQUENTIAL] Total genres:       {len(genres)}")
    logger.info(f"[SEQUENTIAL] Already complete:   {genres_completed}")
    logger.info(f"[SEQUENTIAL] Processed:          {genres_in_progress}")
    logger.info(f"[SEQUENTIAL] Discovered:         {genres_needs_discovery}")
    logger.info(f"[SEQUENTIAL] ════════════════════════════════════════\n")


async def run_crawler(
    adapter,
    site_key,
    genres,
    settings: WorkerSettings,
    crawl_state: dict[str, Any] | None = None,
):
    state_file = app_config.get_state_file(site_key)
    crawl_state = crawl_state or await load_crawl_state(state_file, site_key)
    logger.info("[PLAN] Đang lập kế hoạch crawl cho %s...", site_key)

    # Ensure stores are initialized (MongoDB/SQLite factory)
    await _ensure_stores_initialized()

    recovered = await story_registry.reset_in_progress_async(site_key=site_key)
    if recovered:
        logger.warning(
            "[REGISTRY] Đã reset %d truyện đang dở dang về trạng thái needs_retry cho %s.",
            recovered,
            site_key,
        )

    # [QUEUE] Startup resume logic - recover stale claims
    stale_recovered = await story_queue.recover_stale_claims(stale_minutes=30)
    if stale_recovered > 0:
        logger.info(f"[STARTUP] Recovered {stale_recovered} stale queue claims from dead workers")

    # =============================================================================
    # SEQUENTIAL GENRE STRATEGY
    # Check if we should use sequential processing (one genre at a time)
    # =============================================================================
    import os
    enable_sequential = os.getenv("ENABLE_SEQUENTIAL_GENRE_STRATEGY", "true").lower() == "true"

    if enable_sequential:
        logger.info("[SEQUENTIAL] Using sequential genre completion strategy")
        await run_sequential_genre_crawl(
            adapter=adapter,
            site_key=site_key,
            genres=genres,
            crawl_state=crawl_state,
            settings=settings,
        )
        return

    # Original flow (fallback if sequential disabled)
    logger.info("[PLAN] Using original parallel discovery strategy")

    crawl_plan = await build_crawl_plan(
        adapter,
        genres=genres,
        max_pages=app_config.MAX_STORIES_PER_GENRE_PAGE,
    )

    if not crawl_plan.categories:
        logger.warning("[PLAN] Không tìm thấy thể loại hợp lệ để crawl cho %s.", site_key)
        crawl_state.setdefault("category_story_plan", {})
        await save_crawl_state(crawl_state, state_file, site_key=site_key)
        return

    change_ratio_threshold = float(
        getattr(app_config, "CATEGORY_CHANGE_REFRESH_RATIO", 0.35) or 0.35
    )
    change_absolute_threshold = int(
        getattr(app_config, "CATEGORY_CHANGE_REFRESH_ABSOLUTE", 50) or 50
    )
    change_min_story_count = int(
        getattr(app_config, "CATEGORY_CHANGE_MIN_STORIES", 40) or 40
    )
    refresh_batch_size = int(
        getattr(app_config, "CATEGORY_REFRESH_BATCH_SIZE", app_config.STORY_BATCH_SIZE)
        or app_config.STORY_BATCH_SIZE
    )

    detector = CategoryChangeDetector(
        ratio_threshold=change_ratio_threshold,
        absolute_threshold=change_absolute_threshold,
        min_story_count=change_min_story_count,
    )
    previous_change_state = _load_previous_category_change_state(crawl_state, site_key)
    new_change_state: dict[str, Any] = {}
    scheduled_refresh_jobs: list[dict[str, Any]] = []

    for category in crawl_plan.categories:
        category_id = _derive_category_identifier(category, category.metadata, site_key)
        previous_entry = previous_change_state.get(category_id)
        change_result = detector.evaluate(category.stories, previous_entry)
        new_change_state[category_id] = {
            "url_checksum": change_result.signature.url_checksum,
            "content_signature": change_result.signature.content_signature,
            "urls": change_result.urls,
            "story_count": change_result.signature.story_count,
        }
        if change_result.requires_refresh:
            summary_payload = {
                "change_count": change_result.change_count,
                "change_ratio": change_result.change_ratio,
                "content_changed": change_result.content_changed,
                "added_urls": change_result.added_urls[:10],
                "removed_urls": change_result.removed_urls[:10],
                "url_checksum": change_result.signature.url_checksum,
                "content_signature": change_result.signature.content_signature,
                "story_count": change_result.signature.story_count,
            }
            logger.warning(
                "[CHANGE DETECTOR] Thể loại %s (%s) thay đổi lớn (%d mục, %.2f%%). Lên lịch refresh batch.",
                category.name,
                category_id,
                change_result.change_count,
                change_result.change_ratio * 100.0,
            )
            scheduled_refresh_jobs.extend(
                _build_category_refresh_jobs(
                    category,
                    site_key,
                    category_id=category_id,
                    batch_size=refresh_batch_size,
                    change_summary=summary_payload,
                )
            )

    _store_category_change_state(crawl_state, site_key, new_change_state)

    snapshot_info = None
    try:
        # Support both async (MongoDB) and sync (SQLite) persist_snapshot
        result = category_store.persist_snapshot(site_key, crawl_plan)
        if asyncio.iscoroutine(result):
            snapshot_info = await result
        else:
            snapshot_info = result
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error(
            "[PLAN] Không thể lưu snapshot thể loại cho %s: %s",
            site_key,
            exc,
        )

    max_stories_per_genre = app_config.MAX_STORIES_TOTAL_PER_GENRE
    if max_stories_per_genre:
        for category in crawl_plan.categories:
            if len(category.stories) > max_stories_per_genre:
                category.stories = category.stories[:max_stories_per_genre]

    crawl_state["category_story_plan"] = crawl_plan.as_mapping()
    if snapshot_info:
        crawl_state.setdefault("category_snapshots", {})[site_key] = snapshot_info.to_dict()
    for category in crawl_plan.categories:
        _update_category_progress(crawl_state, category.name, len(category.stories), 0)
    _log_category_resume_overview(site_key, crawl_state)
    await save_crawl_state(crawl_state, state_file, site_key=site_key)
    metrics_tracker.update_global_story_totals(
        completed=len(crawl_state.get("globally_completed_story_urls", [])),
        total_estimate=_estimate_total_stories_from_state(crawl_state),
    )

    # Emit initial plan summary per category so the dashboard knows totals upfront
    mirror_plan_summary = bool(getattr(app_config, "PROGRESS_MIRROR_TO_JOB_TOPIC", False))

    try:
        total_genres_planned = len(crawl_plan.categories)
        for position, category in enumerate(crawl_plan.categories, start=1):
            try:
                category_id = _derive_category_identifier(category, category.metadata, site_key)
            except Exception:
                category_id = category.url or category.name

            total_stories_for_category = len(category.stories)
            plan_summary_payload = {
                "type": "plan_summary",
                "action": "plan_summary",
                "site_key": site_key,
                "genre": category.name,
                "genre_name": category.name,
                "genre_url": category.url,
                "category_id": category_id,
                "status": "planned",
                "processed_stories": 0,
                "total_stories": total_stories_for_category,
                "total": total_stories_for_category,
                "position": position,
                "total_genres": total_genres_planned,
                "timestamp": time.time(),
            }
            # Send to progress stream (for dashboards)
            emit_progress_event("genre_story", plan_summary_payload)
            # Optionally mirror to the main job topic for consumers that only watch it
            if mirror_plan_summary:
                try:
                    await send_job(plan_summary_payload)
                except Exception:
                    # Non-fatal: job topic may not expect this message type
                    pass
    except Exception:
        # Defensive guard: planning should not fail if emitting summaries has issues
        logger.debug("[PLAN] Bỏ qua emit plan_summary ban đầu do lỗi không quan trọng.")

    # Emit an authoritative snapshot after planning & initialization
    try:
        metrics_tracker.emit_dashboard_snapshot()
    except Exception:
        pass

    if scheduled_refresh_jobs:
        logger.info(
            "[CHANGE DETECTOR] Gửi %d batch refresh cho %d thể loại tại %s.",
            len(scheduled_refresh_jobs),
            len({job.get("category_id") for job in scheduled_refresh_jobs}),
            site_key,
        )
        for job in scheduled_refresh_jobs:
            await send_job(job)

    indexed_categories = list(enumerate(crawl_plan.categories, start=1))
    category_positions: dict[str, int] = {}
    for position, category in indexed_categories:
        category_id = _derive_category_identifier(category, category.metadata, site_key)
        category_positions[category_id] = position

    total_genres = len(category_positions)
    genres_done = 0

    genres_map: dict[str, Any] = {}
    for category in crawl_plan.categories:
        # Assuming category is an object with .url / .name based on usage
        url = getattr(category, "url", None)
        name = getattr(category, "name", None)
        if url:
            genres_map[url] = {
                "name": name or url,
                "url": url,
                "stories": 0,
                "status": "pending",
                "updated_at": time.time(),
            }

    metrics_tracker.site_genres_initialized(site_key, total_genres, genres_map)

    async with aiohttp.ClientSession() as session:
        # [QUEUE] Check for incomplete queues (genres with pending stories) and resume processing
        from flowcore_story.storage.db_pool import get_db_pool
        pool = await get_db_pool()
        if pool:
            async with pool.acquire() as conn:
                incomplete_genres = await conn.fetch(
                    """
                    SELECT DISTINCT site_key, genre_name, genre_url,
                           COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                           COUNT(*) FILTER (WHERE status = 'completed') as completed_count
                    FROM story_queue
                    WHERE site_key = $1
                      AND status IN ('pending', 'processing')
                    GROUP BY site_key, genre_name, genre_url
                    HAVING COUNT(*) FILTER (WHERE status = 'pending') > 0
                    ORDER BY genre_name
                    """,
                    site_key,
                )

                if incomplete_genres:
                    logger.info(
                        f"[RESUME] Found {len(incomplete_genres)} genres with pending stories. "
                        "Processing incomplete queues first..."
                    )

                    # Process incomplete genres before new planning
                    for row in incomplete_genres:
                        genre_dict = {
                            "name": row["genre_name"],
                            "url": row["genre_url"],
                        }

                        logger.info(
                            f"[RESUME] Processing {row['genre_name']}: "
                            f"{row['pending_count']} pending, {row['completed_count']} completed"
                        )

                        # Process with queue resume mode (no CategoryCrawlPlan - triggers resume)
                        await process_genre_item(
                            session=session,
                            category_or_genre=genre_dict,
                            crawl_state=crawl_state,
                            adapter=adapter,
                            site_key=site_key,
                        )

                    logger.info("[RESUME] Completed processing all incomplete queues")

        def _as_positive(value: Any) -> int | None:
            try:
                ivalue = int(value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None
            return ivalue if ivalue > 0 else None

        max_batch_size = _as_positive(settings.genre_batch_size) or 1
        chunk_size = (
            _as_positive(getattr(app_config, "CATEGORY_BATCH_JOB_SIZE", None))
            or _as_positive(getattr(app_config, "CATEGORY_REFRESH_BATCH_SIZE", None))
            or max_batch_size
        )
        jobs_per_category_limit = (
            _as_positive(getattr(app_config, "CATEGORY_MAX_JOBS_PER_BATCH", None)) or 1
        )
        jobs_per_domain_limit = _as_positive(
            getattr(app_config, "CATEGORY_MAX_JOBS_PER_DOMAIN", None)
        )

        quota_batches = crawl_plan.schedule_batches_by_quota(
            max_batch_size=max_batch_size,
            max_category_batch_size=chunk_size,
            max_jobs_per_category=jobs_per_category_limit,
            max_jobs_per_domain=jobs_per_domain_limit,
        )

        pending_jobs: Counter[str] = Counter()
        for job_batch in quota_batches:
            for job in job_batch:
                pending_jobs[job.category_id] += 1
        for category_id in category_positions:
            pending_jobs.setdefault(category_id, 0)

        genres_done += sum(1 for remaining in pending_jobs.values() if remaining == 0)

        try:
            process_params = inspect.signature(process_genre_with_limit).parameters
        except (ValueError, TypeError):  # pragma: no cover - builtins or C functions
            process_params = {}
        supports_position = "position" in process_params
        supports_total_genres = "total_genres" in process_params

        total_batches = len(quota_batches) or 1

        for batch_idx, job_batch in enumerate(quota_batches):
            tasks = []
            job_labels: list[str] = []
            for job in job_batch:
                metadata = dict(job.category.metadata)
                position_value = category_positions.get(job.category_id)
                if position_value is not None:
                    metadata.setdefault("position", position_value)
                metadata.setdefault("total_genres", total_genres)
                stories = list(job.stories) if job.stories else list(job.category.stories)
                category_plan = CategoryCrawlPlan(
                    name=job.category.name,
                    url=job.category.url,
                    stories=stories,
                    planned_story_total=(
                        job.category.planned_story_total
                        if job.category.planned_story_total is not None
                        else len(job.category.stories)
                    ),
                    total_pages=job.category.total_pages,
                    crawled_pages=job.category.crawled_pages,
                    metadata=metadata,
                    raw_genre=dict(job.category.raw_genre),
                )

                call_kwargs = {}
                if supports_position and position_value is not None:
                    call_kwargs["position"] = position_value
                if supports_total_genres:
                    call_kwargs["total_genres"] = total_genres

                tasks.append(
                    process_genre_with_limit(
                        session,
                        category_plan,
                        crawl_state,
                        adapter,
                        site_key,
                        **call_kwargs,
                    )
                )

                label = (
                    f"{job.category.name} ({job.part_index}/{job.total_parts})"
                    if job.total_parts > 1
                    else job.category.name
                )
                job_labels.append(label)

            if not tasks:
                continue

            job_label_text = ", ".join(job_labels)
            logger.info(
                f"=== Đang crawl batch thể loại {batch_idx + 1}/{total_batches} ({len(tasks)} job) === {job_label_text}"
            )
            await asyncio.gather(*tasks)

            for job in job_batch:
                if pending_jobs[job.category_id] > 0:
                    pending_jobs[job.category_id] -= 1
                    if pending_jobs[job.category_id] == 0:
                        genres_done += 1

            percent = int(genres_done * 100 / total_genres) if total_genres else 100
            msg = f"⏳ Tiến độ: {genres_done}/{total_genres} thể loại ({percent}%) đã crawl xong cho {site_key}."
            logger.info(msg)
            await smart_delay()

        if not quota_batches:
            percent = 100 if total_genres == genres_done else 0
            msg = f"⏳ Tiến độ: {genres_done}/{total_genres} thể loại ({percent}%) đã crawl xong cho {site_key}."
            logger.info(msg)
    logger.info("=== HOÀN TẤT TOÀN BỘ QUÁ TRÌNH CRAWL ===")


async def run_all_sites(crawl_mode: str | None = None):
    """Run crawler for all configured sites in parallel."""
    # Ensure stores are initialized early
    await _ensure_stores_initialized()

    site_count = len(app_config.BASE_URLS)
    started_at = time.time()
    _emit_runner_event(
        "started",
        "all_sites",
        extra={"crawl_mode": crawl_mode, "site_count": site_count},
    )

    tasks = []

    await start_post_crawl_workers()
    try:
        for site_key in app_config.BASE_URLS.keys():

            async def run_site(key=site_key):
                site_started_at = time.time()
                _emit_runner_event(
                    "started",
                    "site",
                    site_key=key,
                    extra={"crawl_mode": crawl_mode},
                )
                try:
                    await run_single_site(
                        key,
                        crawl_mode=crawl_mode,
                        manage_post_crawl_workers=False,
                    )
                except Exception as exc:
                    _emit_runner_event(
                        "failed",
                        "site",
                        site_key=key,
                        started_at=site_started_at,
                        extra={"crawl_mode": crawl_mode},
                        error=exc,
                    )
                    logger.exception(f"[MAIN] Site {key} failed")
                else:
                    _emit_runner_event(
                        "succeeded",
                        "site",
                        site_key=key,
                        started_at=site_started_at,
                        extra={"crawl_mode": crawl_mode},
                    )

            tasks.append(asyncio.create_task(run_site()))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        exceptions = [result for result in results if isinstance(result, BaseException)]

        if exceptions:
            first_exception = exceptions[0]
            _emit_runner_event(
                "failed",
                "all_sites",
                started_at=started_at,
                extra={"crawl_mode": crawl_mode, "site_count": site_count},
                error=first_exception,
            )
            raise first_exception

        _emit_runner_event(
            "succeeded",
            "all_sites",
            started_at=started_at,
            extra={"crawl_mode": crawl_mode, "site_count": site_count},
        )
    finally:
        await shutdown_post_crawl_workers()



async def _check_retry_consumer_health() -> tuple[bool, str]:
    return await check_consumer_group_health(
        app_config.KAFKA_GROUP_ID,
        app_config.KAFKA_BOOTSTRAP_SERVERS,
        timeout=RETRY_CONSUMER_HEALTHCHECK_TIMEOUT,
    )


async def run_retry_passes(site_key: str):
    passes = app_config.RETRY_FAILED_CHAPTERS_PASSES
    if not passes or passes <= 0:
        _emit_runner_event(
            "skipped",
            "retry_passes",
            site_key=site_key,
            status="disabled",
        )
        return

    logger.info(f"=== BẮT ĐẦU CÁC LƯỢT RETRY CHƯƠNG ĐÃ LỖI (PASSES={passes}) ===")
    started_at = time.time()
    _emit_runner_event(
        "started",
        "retry_passes",
        site_key=site_key,
        extra={"total_passes": passes},
    )
    passes_run = 0
    total_enqueued = 0
    completion_status = "completed"
    for i in range(passes):
        logger.info(f"--- Lượt {i+1}/{passes} ---")
        pass_index = i + 1
        pass_started_at = time.time()
        _emit_runner_event(
            "progress",
            "retry_pass",
            site_key=site_key,
            extra={"pass_index": pass_index, "total_passes": passes},
        )

        consumer_ready, consumer_status = await _check_retry_consumer_health()
        if not consumer_ready:
            logger.error(
                "[RetryQueue] Kafka consumer group '%s' chưa sẵn sàng: %s",
                app_config.KAFKA_GROUP_ID,
                consumer_status,
            )
            metrics_tracker.record_retry_queue_alert(
                site_key=site_key,
                reason=consumer_status,
            )

            # Track retry queue alert in database
            await track_retry_queue_alert(site_key=site_key, reason=consumer_status)

            await send_telegram_notify(
                "[ALERT][RETRY_QUEUE] Kafka consumer không hoạt động, tạm dừng enqueue chương retry.",
                status="error",
                extra={
                    "site_key": site_key,
                    "group_id": app_config.KAFKA_GROUP_ID,
                    "details": consumer_status,
                },
            )
            _emit_runner_event(
                "failed",
                "retry_passes",
                site_key=site_key,
                started_at=started_at,
                status="consumer_unhealthy",
                extra={
                    "pass_index": pass_index,
                    "total_passes": passes,
                    "details": consumer_status,
                },
            )
            return

        dead_files = glob.glob(os.path.join(app_config.DATA_FOLDER, "**", "dead_chapters.json"), recursive=True)
        if not dead_files:
            logger.info("Không tìm thấy chương nào bị đánh dấu 'dead'. Bỏ qua lượt này.")
            _emit_runner_event(
                "completed",
                "retry_pass",
                site_key=site_key,
                started_at=pass_started_at,
                status="no_dead_files",
                extra={"pass_index": pass_index, "total_passes": passes},
            )
            completion_status = "no_dead_files"
            passes_run = pass_index if passes_run < pass_index else passes_run
            break

        total_queued = 0
        files_processed = 0
        for dead_file in dead_files:
            try:
                with open(dead_file, encoding='utf-8') as f:
                    dead_chapters = json.load(f)

                story_folder = os.path.dirname(dead_file)
                metadata_path = os.path.join(story_folder, "metadata.json")
                with open(metadata_path, encoding='utf-8') as f:
                    metadata = json.load(f)

                files_processed += 1
                for chapter in dead_chapters:
                    job = {
                        "type": "retry_chapter",
                        "site_key": metadata.get("site_key"),
                        "chapter_url": chapter.get("url"),
                        "chapter_title": chapter.get("title"),
                        "story_title": metadata.get("title"),
                        "filename": os.path.join(story_folder, get_chapter_filename(chapter.get("title"), chapter.get("index")))
                    }
                    await send_job(job)
                    total_queued += 1

                # Xóa file sau khi đã gửi job
                os.remove(dead_file)
                logger.info(f"Đã gửi {len(dead_chapters)} job retry từ file {dead_file} và xóa file.")

            except Exception as e:
                logger.error(f"Lỗi khi xử lý file dead_chapters: {dead_file} - {e}")

        if total_queued == 0:
            logger.info("Không có chương nào cần retry trong lượt này.")
            _emit_runner_event(
                "completed",
                "retry_pass",
                site_key=site_key,
                started_at=pass_started_at,
                status="no_jobs_enqueued",
                extra={"pass_index": pass_index, "total_passes": passes},
            )
            completion_status = "no_jobs_enqueued"
            passes_run = pass_index if passes_run < pass_index else passes_run
            break

        metrics_tracker.record_retry_queue_enqueue(
            site_key=site_key,
            chapters=total_queued,
            files=files_processed,
            pass_index=i + 1,
            total_passes=passes,
        )

        # Track retry queue enqueue in database
        await track_retry_queue_enqueue(
            site_key=site_key,
            enqueued_count=total_queued,
            file_count=files_processed,
            pass_index=i + 1,
            total_passes=passes,
        )
        await send_telegram_notify(
            "[RetryQueue] Đã enqueue các chương lỗi để retry.",
            status="warning",
            extra={
                "site_key": site_key,
                "chapters": total_queued,
                "files": files_processed,
                "pass": f"{i + 1}/{passes}",
            },
        )

        if i < passes - 1:
            logger.info(f"Đã gửi {total_queued} job. Chờ {app_config.RETRY_SLEEP_SECONDS} giây trước khi bắt đầu lượt tiếp theo...")
            await asyncio.sleep(app_config.RETRY_SLEEP_SECONDS)

        total_enqueued += total_queued
        passes_run = pass_index
        _emit_runner_event(
            "completed",
            "retry_pass",
            site_key=site_key,
            started_at=pass_started_at,
            extra={
                "pass_index": pass_index,
                "total_passes": passes,
                "chapters_enqueued": total_queued,
                "files_processed": files_processed,
            },
        )

    logger.info("=== KẾT THÚC CÁC LƯỢT RETRY ===")
    _emit_runner_event(
        "succeeded",
        "retry_passes",
        site_key=site_key,
        started_at=started_at,
        status=completion_status,
        extra={
            "passes_run": passes_run,
            "total_passes": passes,
            "chapters_enqueued": total_enqueued,
        },
    )


async def run_single_site(
    site_key: str,
    env_overrides: dict[str, str] | None = None,
    crawl_mode: str | None = None,
    *,
    manage_post_crawl_workers: bool = True,
    missing_single_url: str | None = None,
    missing_single_title: str | None = None,
):
    from flowcore_story.config.proxy_provider import shuffle_proxies

    if env_overrides:
        apply_env_overrides({"env_override": env_overrides})

    logger.info(f"[MAIN] Đang chạy crawler cho site: {site_key} với mode={crawl_mode}")
    site_started_at = time.time()
    _emit_runner_event(
        "started",
        "single_site",
        site_key=site_key,
        extra={
            "crawl_mode": crawl_mode,
            "manage_post_crawl_workers": manage_post_crawl_workers,
        },
    )
    load_skipped_stories()
    merge_all_missing_workers_to_main(site_key)
    homepage_url, crawl_state = await initialize_and_log_setup_with_state(site_key)

    background_started = False
    if manage_post_crawl_workers:
        await start_post_crawl_workers()
    try:
        await start_missing_background_loop()
        background_started = True
    except Exception as ex:  # pragma: no cover - defensive guard
        logger.error(f"[MISSING][BACKGROUND] Không thể khởi động background loop: {ex}")

    settings = WorkerSettings(
        genre_batch_size=app_config.GENRE_BATCH_SIZE,
        genre_async_limit=app_config.GENRE_ASYNC_LIMIT,
        proxies_file=PROXIES_FILE,
        failed_genres_file=FAILED_GENRES_FILE,
        retry_genre_round_limit=app_config.RETRY_GENRE_ROUND_LIMIT,
        retry_sleep_seconds=app_config.RETRY_SLEEP_SECONDS,
    )

    try:
        if crawl_mode == "genres_only":
            await run_genres(site_key, settings, crawl_state)
            await retry_failed_genres(
                get_adapter(site_key), site_key, settings, shuffle_proxies
            )
        elif crawl_mode == "missing_only":
            await crawl_all_missing_stories(site_key, homepage_url)
        elif crawl_mode == "missing_single":
            if not missing_single_url:
                raise ValueError(
                    "missing_single_url is required when crawl_mode='missing_single'"
                )
            await crawl_single_story_worker(
                story_url=missing_single_url, title=missing_single_title
            )
            await crawl_all_missing_stories(site_key, homepage_url)
        else:
            await run_genres(site_key, settings, crawl_state)
            await retry_failed_genres(
                get_adapter(site_key), site_key, settings, shuffle_proxies
            )
            await crawl_all_missing_stories(site_key, homepage_url)

        # Cuối cùng, chạy các lượt retry cho các chương đã thất bại
        await run_retry_passes(site_key)
    except Exception as exc:
        _emit_runner_event(
            "failed",
            "single_site",
            site_key=site_key,
            started_at=site_started_at,
            extra={
                "crawl_mode": crawl_mode,
                "manage_post_crawl_workers": manage_post_crawl_workers,
            },
            error=exc,
        )
        raise
    else:
        _emit_runner_event(
            "succeeded",
            "single_site",
            site_key=site_key,
            started_at=site_started_at,
            extra={
                "crawl_mode": crawl_mode,
                "manage_post_crawl_workers": manage_post_crawl_workers,
            },
        )
    finally:
        try:
            if manage_post_crawl_workers:
                await shutdown_post_crawl_workers()
        finally:
            if background_started:
                await stop_missing_background_loop()


def parse_cli_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="storyflow",
        description=(
            "StoryFlow crawler entry point. Provide a site key or 'all_sites' followed "
            "by an optional crawl mode such as 'genres_only', 'missing_only', or "
            "'missing_single'."
        ),
    )
    parser.add_argument(
        "mode",
        nargs="?",
        help="Site key to crawl or 'all_sites' to crawl every configured site.",
    )
    parser.add_argument(
        "crawl_mode",
        nargs="?",
        help="Optional crawl mode override (e.g. 'genres_only', 'missing_only').",
    )
    parser.add_argument(
        "--url",
        dest="url",
        help="Story URL used when crawl_mode='missing_single'.",
    )
    parser.add_argument(
        "--title",
        dest="title",
        help="Story title used when crawl_mode='missing_single'.",
    )
    return parser.parse_args(argv)


async def main(argv: Sequence[str] | None = None):
    args = parse_cli_args(argv)
    mode = app_config.DEFAULT_MODE or args.mode
    crawl_mode = app_config.DEFAULT_CRAWL_MODE or args.crawl_mode

    try:
        # If mode is 'full', treat it as 'all_sites' for backward compatibility and default runs
        if mode == 'full':
            mode = 'all_sites'

        if mode == "all_sites":
            await run_all_sites(crawl_mode=crawl_mode)
        elif mode and mode in app_config.BASE_URLS.keys():
            await run_single_site(
                site_key=mode,
                crawl_mode=crawl_mode,
                missing_single_url=args.url,
                missing_single_title=args.title,
            )
        else:
            valid_keys = ', '.join(app_config.BASE_URLS.keys())
            print(f"❌ Chế độ '{mode}' không hợp lệ. Hãy truyền một site_key hợp lệ ({valid_keys}) hoặc 'all_sites'.")
            sys.exit(1)
    finally:
        await close_producer()

if __name__ == "__main__":
    asyncio.run(main())

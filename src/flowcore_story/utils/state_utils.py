import asyncio
import glob
import inspect
import json
import os
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, TypeVar

import aiofiles

from flowcore_story.config.config import COMPLETED_FOLDER, DATA_FOLDER, get_state_file
from flowcore_story.utils.async_primitives import LoopBoundLock
from flowcore_story.utils.io_utils import safe_write_file
from flowcore_story.utils.logger import logger

# Import database storage adapter
try:
    from flowcore_story.storage.db_state_storage import DatabaseStateStorage
    DB_STORAGE_AVAILABLE = True
except ImportError:
    DB_STORAGE_AVAILABLE = False
    DatabaseStateStorage = None

# Import batch writer
try:
    from flowcore_story.storage.db_batch_writer import get_batch_writer
    BATCH_WRITER_AVAILABLE = True
except ImportError:
    BATCH_WRITER_AVAILABLE = False
    get_batch_writer = None

# Import metrics
try:
    from flowcore_story.storage.state_metrics import get_state_metrics
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    get_state_metrics = None

CSTATE_LOCK = LoopBoundLock()
_STATE_LOCKS: dict[int, asyncio.Lock] = {}
_SECTION_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}
_SECTION_SENTINEL = "__global__"

T = TypeVar("T")


def _should_use_file_state() -> bool:
    """
    Decide whether to use file-based state storage.

    The explicit USE_FILE_STATE flag wins. If it is not set, we default to file
    storage in pytest/CI runs (even if USE_FILE_STATE is explicitly false) to
    avoid long PostgreSQL connection retries when no database is available.
    """
    explicit = os.getenv("USE_FILE_STATE")
    if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("CI"):
        return True

    if explicit is not None:
        return explicit.lower() == "true"

    return False


async def load_crawl_state(state_file, site_key) -> dict[str, Any]:
    """
    Load crawl state from storage (file or database).

    Args:
        state_file: Path to state file (used for file-based storage)
        site_key: Site identifier

    Returns:
        Dict containing crawl state
    """
    # Check if we should use database storage
    use_file_state = _should_use_file_state()

    if not use_file_state and DB_STORAGE_AVAILABLE and DatabaseStateStorage:
        # Use database storage
        worker_id = os.getenv("WORKER_INSTANCE_ID") or os.getenv("MISSING_WORKER_ID") or "main"
        try:
            state = await DatabaseStateStorage.load_crawl_state(site_key, worker_id)
            logger.info(f"[DB] Đã tải trạng thái crawl từ database cho {site_key}/{worker_id}")
            return state
        except Exception as e:
            logger.error(f"[DB] Lỗi khi tải từ database: {e}, fallback to file")
            # Fall through to file-based storage

    # Use file-based storage (legacy or fallback)
    if not state_file:
        state_file = get_state_file(site_key)
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, state_file)
    if exists:
        try:
            async with aiofiles.open(state_file, encoding='utf-8') as f:
                data = await f.read()
            state = json.loads(data)
            stored_site = state.get("site_key")
            if stored_site and stored_site != site_key:
                logger.warning(
                    "State file %s belongs to site %s but was requested for %s. Ignoring stale state.",
                    state_file,
                    stored_site,
                    site_key,
                )
                return {}
            if site_key and stored_site != site_key:
                state["site_key"] = site_key
            logger.info(f"Đã tải trạng thái crawl từ {state_file}: {state}")
            return state
        except Exception as e:
            logger.error(f"Lỗi khi tải trạng thái crawl từ {state_file}: {e}. Bắt đầu crawl mới.")
    return {}


_last_save_time = {}
_state_hashes: dict[str, str] = {}

# Default debounce time from environment
DEFAULT_DEBOUNCE = float(os.getenv("STATE_SAVE_DEBOUNCE", "30.0"))


def _compute_state_hash(state: dict[str, Any]) -> str:
    """Compute hash of state to detect changes."""
    import hashlib
    state_json = json.dumps(state, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(state_json.encode()).hexdigest()


def _get_crawl_state_lock(state: dict[str, Any]) -> asyncio.Lock:
    lock = _STATE_LOCKS.get(id(state))
    if lock is None:
        lock = asyncio.Lock()
        _STATE_LOCKS[id(state)] = lock
    return lock


@asynccontextmanager
async def locked_crawl_state(state: dict[str, Any]):
    """Async context manager that serialises access to ``state``."""

    lock = _get_crawl_state_lock(state)
    async with lock:
        yield state


def _get_section_lock(state: dict[str, Any], component: str | None) -> asyncio.Lock:
    key = (id(state), component or _SECTION_SENTINEL)
    lock = _SECTION_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _SECTION_LOCKS[key] = lock
    return lock


async def update_crawl_state_section(
    state: dict[str, Any],
    updater: Callable[[dict[str, Any]], Awaitable[T] | T],
    *,
    component: str | None = None,
    state_file: str | None = None,
    site_key: str | None = None,
    save: bool = True,
    debounce: float | None = None,
) -> T:
    """Apply ``updater`` to ``state`` safely and persist the result.

    ``component`` allows callers to acquire a finer-grained lock when different
    parts of the state can be updated independently. If ``save`` is True and a
    ``state_file`` is provided, the helper persists the state using
    :func:`save_crawl_state` after the update.
    """

    lock = _get_section_lock(state, component)
    async with lock:
        result = updater(state)
        if inspect.isawaitable(result):
            result = await result
        if save and state_file:
            await save_crawl_state(
                state,
                state_file,
                debounce=debounce,
                site_key=site_key,
            )
        return result

async def save_crawl_state(
    state: dict[str, Any],
    state_file: str,
    debounce: float | None = None,
    *,
    site_key: str | None = None,
) -> None:
    """
    Save crawl state to storage (file or database).

    Args:
        state: State dictionary to save
        state_file: Path to state file (used for file-based storage)
        debounce: Minimum seconds between saves
        site_key: Site identifier
    """
    global _last_save_time

    # Use default debounce if not specified
    if debounce is None:
        debounce = DEFAULT_DEBOUNCE

    # Debounce check
    now = time.monotonic()
    cache_key = state_file if state_file else site_key
    last = _last_save_time.get(cache_key, 0)
    if now - last < debounce:
        time_remaining = debounce - (now - last)
        logger.debug(
            f"[State] Skip save (debounce): site={site_key}, "
            f"wait={time_remaining:.1f}s"
        )
        # Record skip metric
        if METRICS_AVAILABLE and get_state_metrics:
            get_state_metrics().record_skip("debounce", site_key)
        return

    # Dirty tracking - check if state actually changed
    enable_dirty_tracking = os.getenv("STATE_ENABLE_DIRTY_TRACKING", "true").lower() == "true"
    if enable_dirty_tracking:
        current_hash = _compute_state_hash(state)
        last_hash = _state_hashes.get(cache_key)

        if current_hash == last_hash:
            logger.debug(
                f"[State] Skip save (unchanged): site={site_key}, "
                f"hash={current_hash[:8]}"
            )
            # Record skip metric
            if METRICS_AVAILABLE and get_state_metrics:
                get_state_metrics().record_skip("unchanged", site_key)
            return

    # Ensure site_key is set
    if site_key:
        state["site_key"] = site_key
    elif "site_key" in state:
        site_key = state.get("site_key")

    # Check if we should use database storage
    use_file_state = _should_use_file_state()

    if not use_file_state and DB_STORAGE_AVAILABLE and DatabaseStateStorage and site_key:
        # Use database storage
        worker_id = os.getenv("WORKER_INSTANCE_ID") or os.getenv("MISSING_WORKER_ID") or "main"

        # Check if batch writer is enabled
        use_batch_writer = os.getenv("STATE_USE_BATCH_WRITER", "true").lower() == "true"

        if use_batch_writer and BATCH_WRITER_AVAILABLE and get_batch_writer:
            # Use batch writer for better performance
            try:
                enqueue_start = time.monotonic()
                writer = await get_batch_writer()
                await writer.enqueue(site_key, worker_id, state.copy())
                enqueue_duration = time.monotonic() - enqueue_start

                _last_save_time[cache_key] = now
                if enable_dirty_tracking:
                    _state_hashes[cache_key] = _compute_state_hash(state)

                # Record metric
                if METRICS_AVAILABLE and get_state_metrics:
                    get_state_metrics().record_save(
                        duration=enqueue_duration,
                        backend="batch",
                        site_key=site_key,
                        success=True
                    )

                logger.debug(
                    f"[State] Enqueued to batch writer: site={site_key}, worker={worker_id}"
                )
                return
            except Exception as e:
                logger.warning(
                    f"[State] Batch writer failed: {e}, falling back to direct save"
                )
                # Fall through to direct save

        async with CSTATE_LOCK:
            # Re-check debounce
            now = time.monotonic()
            last = _last_save_time.get(cache_key, 0)
            if now - last < debounce:
                logger.debug("Skip save state vì debounce (double-check)")
                return

            try:
                save_start = time.monotonic()
                success = await DatabaseStateStorage.save_crawl_state(state, site_key, worker_id)
                save_duration = time.monotonic() - save_start

                if success:
                    logger.info(
                        f"[State] Saved to DB: site={site_key}, worker={worker_id}, "
                        f"duration={save_duration*1000:.1f}ms"
                    )
                    _last_save_time[cache_key] = now
                    # Update hash after successful save
                    if enable_dirty_tracking:
                        _state_hashes[cache_key] = _compute_state_hash(state)
                    # Record metric
                    if METRICS_AVAILABLE and get_state_metrics:
                        get_state_metrics().record_save(
                            duration=save_duration,
                            backend="db",
                            site_key=site_key,
                            success=True
                        )
                else:
                    logger.warning(
                        f"[State] DB save failed, falling back to file: "
                        f"site={site_key}, duration={save_duration*1000:.1f}ms"
                    )
                    # Record failed DB save
                    if METRICS_AVAILABLE and get_state_metrics:
                        get_state_metrics().record_save(
                            duration=save_duration,
                            backend="db",
                            site_key=site_key,
                            success=False
                        )
                    # Fall through to file save
                    file_start = time.monotonic()
                    await _save_to_file(state, state_file)
                    file_duration = time.monotonic() - file_start
                    _last_save_time[cache_key] = now
                    if enable_dirty_tracking:
                        _state_hashes[cache_key] = _compute_state_hash(state)
                    # Record file save metric
                    if METRICS_AVAILABLE and get_state_metrics:
                        get_state_metrics().record_save(
                            duration=file_duration,
                            backend="file",
                            site_key=site_key,
                            success=True
                        )
            except Exception as e:
                logger.error(
                    f"[State] Exception during DB save: site={site_key}, "
                    f"error={type(e).__name__}: {e}, falling back to file"
                )
                await _save_to_file(state, state_file)
                _last_save_time[cache_key] = now
                if enable_dirty_tracking:
                    _state_hashes[cache_key] = _compute_state_hash(state)
    else:
        # Use file-based storage
        async with CSTATE_LOCK:
            # Re-check debounce
            now = time.monotonic()
            last = _last_save_time.get(cache_key, 0)
            if now - last < debounce:
                logger.debug("Skip save state vì debounce (double-check)")
                return

            file_start = time.monotonic()
            await _save_to_file(state, state_file)
            file_duration = time.monotonic() - file_start
            _last_save_time[cache_key] = now
            if enable_dirty_tracking:
                _state_hashes[cache_key] = _compute_state_hash(state)
            # Record file save metric
            if METRICS_AVAILABLE and get_state_metrics:
                get_state_metrics().record_save(
                    duration=file_duration,
                    backend="file",
                    site_key=site_key,
                    success=True
                )


async def _save_to_file(state: dict[str, Any], state_file: str) -> None:
    """Helper to save state to file."""
    try:
        logger.debug(f"[FILE] Bắt đầu lưu state vào {state_file}")
        content = json.dumps(state, ensure_ascii=False, indent=4)
        await safe_write_file(state_file, content)
        logger.debug(f"Đã lưu trạng thái crawl vào {state_file}")
    except Exception as e:
        logger.error(f"Lỗi khi lưu trạng thái crawl vào {state_file}: {e}")



async def clear_specific_state_keys(state: dict[str, Any], keys_to_remove: list[str], state_file: str, debounce: float | None = None) -> None:
    updated = False
    for key in keys_to_remove:
        if key in state:
            del state[key]
            updated = True
            logger.debug(f"Đã xóa key '{key}' khỏi trạng thái crawl.")
    if updated:
        await save_crawl_state(
            state,
            state_file,
            debounce=debounce,
            site_key=state.get("site_key"),
        )

async def clear_crawl_state_component(state: dict[str, Any], component_key: str, state_file: str) -> None:
    if component_key in state:
        del state[component_key]
        if component_key == "current_genre_url":
            state.pop("current_story_url", None)
            state.pop("current_story_index_in_genre", None)
            state.pop("processed_chapter_urls_for_current_story", None)
        elif component_key == "current_story_url":
            state.pop("processed_chapter_urls_for_current_story", None)
    await save_crawl_state(state, state_file, site_key=state.get("site_key"))

async def clear_all_crawl_state(state_file:str, site_key: str | None = None) -> None:
    """Clear all crawl state (file or database)."""
    # Check if we should use database storage
    use_file_state = _should_use_file_state()

    if not use_file_state and DB_STORAGE_AVAILABLE and DatabaseStateStorage and site_key:
        # Clear from database
        worker_id = os.getenv("WORKER_INSTANCE_ID") or os.getenv("MISSING_WORKER_ID") or "main"
        try:
            await DatabaseStateStorage.clear_all_crawl_state(site_key, worker_id)
            logger.info(f"[DB] Đã xóa trạng thái crawl: {site_key}/{worker_id}")
        except Exception as e:
            logger.error(f"[DB] Lỗi khi xóa state: {e}")
    else:
        # Clear file
        loop = asyncio.get_event_loop()
        exists = await loop.run_in_executor(None, os.path.exists, state_file)
        if exists:
            try:
                await loop.run_in_executor(None, os.remove, state_file)
                logger.info(f"Đã xóa file trạng thái crawl: {state_file}")
            except Exception as e:
                logger.error(f"Lỗi khi xóa file trạng thái crawl: {e}")

def is_genre_completed(genre_name):
    src_genre_folder = os.path.join(DATA_FOLDER, genre_name)
    completed_genre_folder = os.path.join(COMPLETED_FOLDER, genre_name)
    src_count = len(os.listdir(src_genre_folder)) if os.path.exists(src_genre_folder) else 0
    completed_count = len(os.listdir(completed_genre_folder)) if os.path.exists(completed_genre_folder) else 0
    return src_count == 0 and completed_count > 0



def merge_all_missing_workers_to_main(site_key):
    main_state_file = get_state_file(site_key)
    main_state = {}
    if os.path.exists(main_state_file):
        with open(main_state_file, encoding="utf-8") as f:
            try:
                main_state = json.load(f)
            except Exception as ex:
                logger.error(f"Lỗi đọc file state, sẽ reset lại state rỗng: {ex}")
                main_state = {}

    # Tìm tất cả file _missing_worker*.json (nếu nhiều worker phụ)
    files = glob.glob(f"{main_state_file.replace('.json', '')}_missing_worker*.json")
    all_completed = set(main_state.get("globally_completed_story_urls", []))
    for fname in files:
        with open(fname, encoding="utf-8") as f:
            missing_state = json.load(f)
        all_completed |= set(missing_state.get("globally_completed_story_urls", []))
        # Xóa file phụ sau khi merge
        os.remove(fname)
    main_state["globally_completed_story_urls"] = sorted(all_completed)
    main_state["site_key"] = site_key
    with open(main_state_file, "w", encoding="utf-8") as f:
        json.dump(main_state, f, ensure_ascii=False, indent=4)


def get_missing_worker_state_file(site_key):
    base = get_state_file(site_key)
    worker_id = os.getenv("MISSING_WORKER_ID") or os.getenv("WORKER_INSTANCE_ID") or ""
    if worker_id:
        trimmed = worker_id.strip()
        if trimmed:
            safe = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "-" for ch in trimmed)
            return base.replace(".json", f"_missing_worker_{safe}.json")
    return base.replace('.json', '_missing_worker.json')

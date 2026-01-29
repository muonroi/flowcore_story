import json
import os
import shutil
import sys
import time
from collections.abc import Iterator
from types import ModuleType
from typing import Any
from urllib.parse import urlparse

from flowcore_story.config import config as app_config
from flowcore_story.utils.chapter_utils import slugify_title
from flowcore_story.utils.io_utils import (
    ensure_backup_folder,
    ensure_directory_exists,
    safe_write_file,
    safe_write_json,
)
from flowcore_story.utils.logger import logger


# Provide a fallback metadata store factory for tests/CI that monkeypatch this symbol.
def get_metadata_store(path: str | None = None):  # pragma: no cover - test helper
    return None


def normalize_categories_field(meta: dict[str, Any], meta_path: str | None = None) -> dict[str, Any]:
    """Ensure categories is a list of dicts with a name key to avoid legacy string formats."""
    categories = meta.get("categories")
    normalized: list[dict[str, Any]] = []
    changed = False

    if isinstance(categories, list):
        for cat in categories:
            if isinstance(cat, dict):
                normalized.append(cat)
            elif isinstance(cat, str):
                normalized.append({"name": cat})
                changed = True
            else:
                changed = True
    elif isinstance(categories, str):
        normalized.append({"name": categories})
        changed = True
    elif categories is None:
        normalized = []
    else:
        normalized = []
        changed = True

    if categories is None or not isinstance(categories, list):
        changed = True

    if changed:
        meta["categories"] = normalized
        if meta_path:
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=4)
            logger.info("[AUTO-FIX] Đã chuẩn hoá categories cho '%s'", meta.get("title"))
    return meta


def get_primary_category_name(meta: dict[str, Any], default: str = "Unknown") -> str:
    categories = meta.get("categories")
    if isinstance(categories, list) and categories:
        first = categories[0]
        if isinstance(first, dict):
            return first.get("name") or first.get("title") or default
        if isinstance(first, str):
            return first or default
    if isinstance(categories, str):
        return categories or default
    return default


def _iter_config_modules(base_module: ModuleType) -> Iterator[ModuleType]:
    module_file = getattr(base_module, "__file__", None)
    if module_file is None:
        yield base_module
        return

    seen_ids = set()

    def _yield(module: ModuleType) -> Iterator[ModuleType]:
        if id(module) in seen_ids:
            return
        seen_ids.add(id(module))
        yield module

    for module in _yield(base_module):
        yield module
    for candidate in sys.modules.values():
        if isinstance(candidate, ModuleType) and getattr(candidate, "__file__", None) == module_file:
            for module in _yield(candidate):
                yield module
    import gc

    for obj in gc.get_objects():
        if isinstance(obj, ModuleType) and getattr(obj, "__file__", None) == module_file:
            for module in _yield(obj):
                yield module


def _resolve_metadata_db_path(base_module: ModuleType) -> str | None:
    """Return the configured metadata database path.

    Historically we attempted to walk every loaded copy of the configuration
    module and pick the first value that differed from the original default.
    During testing this led to surprising behaviour: monkeypatching the
    ``config`` module resulted in two module instances – the patched one and a
    stale copy still referenced elsewhere.  The discovery loop happened to see
    the stale copy first and therefore returned the *old* default path instead
    of the patched value.  As a consequence the metadata store continued to use
    the global default location which made the persistence test fail.

    To avoid this, prefer the value from the module we were given (the active
    configuration) and only fall back to other loaded modules when it does not
    define a path.  This keeps support for module reloading while ensuring
    runtime overrides – including monkeypatched values in the unit tests – are
    honoured consistently.
    """

    configured_path = getattr(base_module, "METADATA_DB_PATH", None)
    if configured_path:
        return configured_path

    for module in _iter_config_modules(base_module):
        candidate = getattr(module, "METADATA_DB_PATH", None)
        if candidate:
            return candidate

    return None


def derive_story_slug(title: str | None, url: str | None = None) -> str:
    """Return a safe folder slug for a story.

    When the title is missing or empty (common when metadata fetch fails due
    to anti-bot pages) we fall back to the URL's last segment or hostname so
    that files are not written to the root ``truyen_data`` folder.
    """

    primary_slug = slugify_title(title)
    if primary_slug:
        return primary_slug

    candidates = []
    if url:
        parsed = urlparse(url)
        if parsed.path:
            tail = parsed.path.rstrip("/").split("/")[-1]
            if tail:
                candidates.append(tail)
        if parsed.netloc:
            candidates.append(parsed.netloc)
        candidates.append(url)

    for candidate in candidates:
        if not candidate:
            continue
        slug_candidate = slugify_title(candidate)
        if slug_candidate:
            return slug_candidate
        sanitized = sanitize_filename(candidate)
        sanitized = slugify_title(sanitized) or sanitized.strip().strip("-_")
        if sanitized:
            return sanitized

    return "story"


async def save_story_metadata_file(
    story_base_data: dict[str, Any],
    current_discovery_genre_data: dict[str, Any] | None,
    story_folder_path: str,
    fetched_story_details: dict[str, Any] | None,
    existing_metadata: dict[str, Any] | None = None
) -> dict[str, Any]:
    await ensure_directory_exists(story_folder_path)
    metadata_file = os.path.join(story_folder_path, "metadata.json")
    metadata_to_save = existing_metadata.copy() if existing_metadata else {}

    # Fields quan trọng phải luôn được merge/ưu tiên lấy đủ
    FIELDS_MUST_HAVE = [
        "title", "url", "author", "cover", "description", "categories",
        "status", "source", "rating_value", "rating_count", "total_chapters_on_site"
    ]

    # Cập nhật fields cơ bản từ base data
    for key in ["title", "url", "author", "cover", "cover"]:
        if story_base_data.get(key):
            metadata_to_save[key] = story_base_data[key]

    metadata_to_save["crawled_by"] = "muonroi"

    # Merge categories
    current_cats = metadata_to_save.get("categories", [])

    # Sanitize current_cats to handle old string-only format
    sanitized_cats = []
    for cat in current_cats:
        if isinstance(cat, str):
            sanitized_cats.append({'name': cat, 'url': None})
        elif isinstance(cat, dict):
            sanitized_cats.append(cat)
    current_cats = sanitized_cats

    seen_urls = {cat.get("url") for cat in current_cats if cat.get("url")}
    if current_discovery_genre_data and current_discovery_genre_data.get("url") not in seen_urls:
        current_cats.append({
            "name": current_discovery_genre_data.get("name"),
            "url": current_discovery_genre_data.get("url")
        })
    metadata_to_save["categories"] = sorted(current_cats, key=lambda x: (x.get("name") or "").lower())

    # Cập nhật tất cả field lấy được từ fetched_story_details
    if fetched_story_details:
        for key in FIELDS_MUST_HAVE:
            # Ưu tiên lấy từ fetched_story_details nếu có (vì parser lấy trực tiếp từ HTML)
            if fetched_story_details.get(key) is not None:
                metadata_to_save[key] = fetched_story_details[key]

    # Fallback: các field chưa có thì set None (đảm bảo metadata đầy đủ field, tiện validate về sau)
    for key in FIELDS_MUST_HAVE:
        metadata_to_save.setdefault(key, None)

    missing_fields = [k for k in FIELDS_MUST_HAVE if not metadata_to_save.get(k)]
    if missing_fields:
        logger.warning(
            f"[META] Metadata autofix: thiếu các trường {missing_fields} cho '{story_base_data.get('title')}'"
        )

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    metadata_to_save["metadata_updated_at"] = now_str
    if "crawled_at" not in metadata_to_save:
        metadata_to_save["crawled_at"] = now_str

    try:
        await safe_write_file(metadata_file, json.dumps(metadata_to_save, ensure_ascii=False, indent=4))
        logger.info(f"Đã lưu/cập nhật metadata cho truyện vào: {metadata_file}")

        # Use metadata store factory for SQLite or MongoDB (async)
        from flowcore_story.utils.metadata_store_factory import get_metadata_store

        runtime_config = sys.modules.get("config.config", app_config)
        store_path = _resolve_metadata_db_path(runtime_config)

        # Get metadata store (async - supports both SQLite and MongoDB)
        store = await get_metadata_store(store_path)
        if store:
            fallback_key = (
                os.path.basename(story_folder_path.rstrip(os.sep))
                or metadata_to_save.get("title")
                or "unknown"
            )
            logger.debug(
                "[META][DB] Persisting metadata to %s with key=%s",
                getattr(store, 'db_path', 'mongodb'),
                metadata_to_save.get("url") or fallback_key,
            )
            # Await upsert (async for MongoDB, sync wrapper for SQLite)
            if hasattr(store.upsert, '__call__'):
                import asyncio
                if asyncio.iscoroutinefunction(store.upsert):
                    await store.upsert(metadata_to_save, fallback_key=fallback_key)
                else:
                    store.upsert(metadata_to_save, fallback_key=fallback_key)
        else:
            logger.debug("[META][DB] Metadata store not configured (SQLite path=%s)", store_path)

        return metadata_to_save
    except Exception as e:
        logger.error(f"LỖI khi lưu metadata '{metadata_file}': {e}")
        return metadata_to_save


def is_story_complete(story_folder_path: str, total_chapters_on_site: int) -> bool:
    """Kiểm tra số file .txt đã crawl có đủ không."""
    files = [f for f in os.listdir(story_folder_path) if f.endswith('.txt')]
    return len(files) >= total_chapters_on_site

def sanitize_filename(filename):
    # Đơn giản hóa tên file, tránh lỗi tên
    import re
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    return filename.strip()


async def add_missing_story(story_title, story_url, total_chapters, crawled_chapters, filename=None):
    """Thêm truyện thiếu chương vào file json."""
    if filename is None:
        path = app_config.MISSING_CHAPTERS_FILE
    else:
        path = filename
    # Đọc danh sách cũ
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"[ERROR] File {path} bị lỗi hoặc rỗng, sẽ tạo lại file mới.")
            data = {}
    else:
        data = []
    # Check đã tồn tại chưa
    for item in data:
        if item.get("url") == story_url:
            return  # Không thêm trùng
    data.append({
        "title": story_title,
        "url": story_url,
        "total_chapters": total_chapters,
        "crawled_chapters": crawled_chapters
    })
    await safe_write_json(path,data)


def backup_crawl_state(state_file='crawl_state.json', backup_folder=None):
    if backup_folder is None:
        backup_folder = app_config.BACKUP_FOLDER
    ensure_backup_folder(backup_folder)
    ts = time.strftime("%Y%m%d_%H%M%S")
    base_name = os.path.basename(state_file)
    backup_file = os.path.join(backup_folder, f"{base_name}.bak_{ts}")
    shutil.copy(state_file, backup_file)


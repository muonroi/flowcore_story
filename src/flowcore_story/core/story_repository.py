
import json
import os
import time
from typing import Any

# Assuming DATA_FOLDER is defined in config
from flowcore.config.config import DATA_FOLDER
from flowcore.utils.domain_utils import get_site_key_from_url, is_url_for_site
from flowcore.utils.logger import logger
from flowcore.utils.meta_utils import derive_story_slug


def get_story_folder(
    story_title: str | None,
    story_author: str | None = None,
    *,
    story_url: str | None = None,
) -> str:
    """
    Determines the storage folder for a story based on its title and author.
    Currently uses just the title slug.
    """
    # This can be enhanced later to use author as well.
    slug = derive_story_slug(story_title, story_url)
    return os.path.join(DATA_FOLDER, slug)


def load_metadata(story_folder_path: str) -> dict[str, Any] | None:
    """Loads metadata.json from a story's folder."""
    metadata_file = os.path.join(story_folder_path, "metadata.json")
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load metadata from {metadata_file}: {e}")
            return None
    return None


def save_metadata(story_folder_path: str, metadata: dict[str, Any], is_new: bool = False):
    """Saves metadata.json to a story's folder."""
    metadata_file = os.path.join(story_folder_path, "metadata.json")
    try:
        metadata.setdefault("metadata_updated_at", time.strftime("%Y-%m-%d %H:%M:%S"))
        if is_new:
            metadata.setdefault("crawled_at", time.strftime("%Y-%m-%d %H:%M:%S"))

        # Ensure we don't save chapter lists in the main metadata
        metadata_to_write = metadata.copy()
        metadata_to_write.pop("chapters", None)

        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata_to_write, f, ensure_ascii=False, indent=4)
    except OSError as e:
        logger.error(f"Failed to save metadata to {metadata_file}: {e}")


def sort_sources(sources):
    """Sorts a list of sources by priority."""
    return sorted(sources, key=lambda s: s.get("priority", 100))


def normalize_story_sources(
    story_data_item: dict[str, Any],
    current_site_key: str,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """
    Merge and normalize source list between story data and existing metadata.
    Returns True if the metadata was changed.
    """

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

    metadata_changed = False
    if metadata is not None:
        if metadata.get("sources") != normalized_sorted:
            metadata["sources"] = normalized_sorted
            metadata_changed = True

    # Also update the story_data_item in-memory for the current crawl session
    story_data_item["sources"] = normalized_sorted

    return metadata_changed

import asyncio
import json
import os
import time
from typing import Any

from flowcore_story.config.config import SKIPPED_STORIES_FILE
from flowcore_story.utils.logger import logger
from flowcore_story.utils.meta_utils import derive_story_slug
from flowcore_story.utils.metrics_tracker import metrics_tracker
from flowcore_story.utils.notifier import send_telegram_notify

SKIPPED_STORIES: dict[str, dict[str, Any]] = {}


def load_skipped_stories() -> dict[str, dict[str, Any]]:
    """Load skipped stories from file once at startup."""
    global SKIPPED_STORIES
    if SKIPPED_STORIES:
        return SKIPPED_STORIES
    if os.path.exists(SKIPPED_STORIES_FILE):
        try:
            with open(SKIPPED_STORIES_FILE, encoding="utf-8") as f:
                SKIPPED_STORIES = json.load(f)
        except Exception as ex:
            logger.error(f"[SKIP] Lỗi đọc file skip: {ex}")
            SKIPPED_STORIES = {}
    else:
        SKIPPED_STORIES = {}
    metrics_tracker.update_skipped_queue_size(len(SKIPPED_STORIES))
    return SKIPPED_STORIES


def save_skipped_stories() -> None:
    """Persist skip data to file."""
    try:
        with open(SKIPPED_STORIES_FILE, "w", encoding="utf-8") as f:
            json.dump(SKIPPED_STORIES, f, ensure_ascii=False, indent=2)
    except Exception as ex:
        logger.error(f"[SKIP] Không ghi được skipped_stories: {ex}")


def is_story_skipped(story: dict[str, Any]) -> bool:
    slug = derive_story_slug(story.get("title"), story.get("url"))
    return slug in SKIPPED_STORIES


def get_all_skipped_stories() -> dict[str, dict[str, Any]]:
    return SKIPPED_STORIES


def mark_story_as_skipped(story: dict[str, Any], reason: str = "") -> None:
    """Mark story as permanently skipped and notify."""
    slug = derive_story_slug(story.get("title"), story.get("url"))
    SKIPPED_STORIES[slug] = {
        "url": story.get("url"),
        "title": story.get("title"),
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "reason": reason,
    }
    save_skipped_stories()
    metrics_tracker.story_skipped(slug, story.get("title", slug), reason)
    metrics_tracker.update_skipped_queue_size(len(SKIPPED_STORIES))
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[SKIP] {story.get('title')} - {reason} ({now})".strip()
    logger.warning(msg)
    try:
        asyncio.create_task(send_telegram_notify(msg, status="warning"))
    except RuntimeError:
        # in case called outside event loop
        pass

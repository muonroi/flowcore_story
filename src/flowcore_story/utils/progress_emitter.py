"""Async helpers to push crawl progress updates to Kafka."""

from __future__ import annotations

import os
from typing import Any

from flowcore_story.utils.logger import get_logger

logger = get_logger("progress")

# Categories that are useful for dashboard - always enabled
_DASHBOARD_CATEGORIES: set[str] = {
    "dashboard",
    "genre_story",
    "story",
    "registry",
    "system",
    "missing_worker",  # Track missing chapter auto-fix
}

# Categories that can be disabled for optimization
_OPTIONAL_CATEGORIES: set[str] = {
    "worker",
    "retry_worker",
    "single_story_worker",
    "runner",
    "snapshot",  # Duplicates dashboard
}


def _parse_disabled_categories() -> set[str]:
    """Parse PROGRESS_DISABLE_CATEGORIES env var."""
    raw = os.environ.get("PROGRESS_DISABLE_CATEGORIES", "")
    if not raw or raw.strip().lower() in ("", "none", "0", "false"):
        return set()

    # Parse comma-separated list
    disabled = {cat.strip().lower() for cat in raw.split(",") if cat.strip()}

    # Warn if trying to disable critical categories
    critical_disabled = disabled & _DASHBOARD_CATEGORIES
    if critical_disabled:
        logger.warning(
            "[ProgressEmitter] Attempting to disable critical categories: %s. "
            "This may break dashboard functionality.",
            ", ".join(critical_disabled)
        )

    return disabled


def _parse_enabled_categories() -> set[str]:
    """Parse PROGRESS_ENABLED_CATEGORIES env var."""
    raw = os.environ.get("PROGRESS_ENABLED_CATEGORIES", "")
    if not raw or raw.strip().lower() in ("", "all", "*"):
        return set()  # Empty means all enabled

    # Parse comma-separated list
    enabled = {cat.strip().lower() for cat in raw.split(",") if cat.strip()}
    return enabled


# Load config once on module import
_DISABLED_CATEGORIES = _parse_disabled_categories()
_ENABLED_CATEGORIES = _parse_enabled_categories()


def _is_category_enabled(category: str) -> bool:
    """Check if a category should emit events."""
    if not category:
        return False

    category_lower = category.lower()

    # If explicit whitelist is set, only allow those
    if _ENABLED_CATEGORIES:
        return category_lower in _ENABLED_CATEGORIES

    # Otherwise check blacklist
    if category_lower in _DISABLED_CATEGORIES:
        return False

    return True


def _is_meaningful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def compact_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of ``data`` without ``None``/empty values."""

    return {key: value for key, value in data.items() if _is_meaningful(value)}


async def _send_event(event: dict[str, Any], topic: str) -> None:
    """Forward ``event`` to Kafka using the shared async producer."""

    try:
        from flowcore_story.kafka.kafka_producer import send_job

        await send_job(event, topic=topic)
    except Exception as exc:  # pragma: no cover - network/IO guard
        logger.warning("[ProgressEmitter] Không thể gửi event tiến trình: %s", exc)


def emit_progress_event(category: str, payload: dict[str, Any]) -> None:
    """No-op: dashboard progress emission is disabled."""
    # Dashboard progress emission is disabled to save resources.
    return


__all__ = ["emit_progress_event", "compact_payload"]

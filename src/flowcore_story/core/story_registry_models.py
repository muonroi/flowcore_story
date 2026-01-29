"""Shared models and constants for the story registry layer."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum

from flowcore_story.utils.logger import logger


class StoryCrawlStatus(str, Enum):
    """Enumeration of crawl states tracked in the story registry."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    COOLDOWN = "cooldown"
    NEEDS_RETRY = "needs_retry"
    PERMANENT_FAIL = "permanent_fail"


@dataclass(frozen=True)
class StoryRegistryEntry:
    """Snapshot of a story row in the registry."""

    story_id: int
    site_key: str
    status: StoryCrawlStatus
    last_category: str | None
    last_crawled_at: str | None
    last_result: str | None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> StoryRegistryEntry:
        # ``sqlite3.Row`` implements the mapping protocol but does not expose
        # ``get`` directly, so we normalise it into a dict first.
        mapping = dict(row)
        status_value = mapping["status"]
        try:
            status = StoryCrawlStatus(str(status_value))
        except ValueError:
            # Fallback to pending for unknown states so that older
            # deployments can continue functioning without crashing.
            logger.warning(
                "[REGISTRY] Unknown status '%s', treating as pending", status_value
            )
            status = StoryCrawlStatus.PENDING
        return cls(
            story_id=int(mapping["story_id"]),
            site_key=str(mapping["site_key"]),
            status=status,
            last_category=mapping.get("last_category"),
            last_crawled_at=mapping.get("last_crawled_at"),
            last_result=mapping.get("last_result"),
            created_at=str(mapping["created_at"]),
            updated_at=str(mapping["updated_at"]),
        )


ACQUIRABLE_STATUSES = (
    StoryCrawlStatus.PENDING,
    StoryCrawlStatus.NEEDS_RETRY,
    StoryCrawlStatus.FAILED,
    StoryCrawlStatus.COOLDOWN,
)


TERMINAL_STATUSES = (
    StoryCrawlStatus.COMPLETED,
    StoryCrawlStatus.PERMANENT_FAIL,
    StoryCrawlStatus.SKIPPED,
)


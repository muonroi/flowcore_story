"""Public API for interacting with the story crawl registry."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Sequence
from typing import Any

from flowcore_story.core.category_store import CategoryStore
from flowcore_story.core.story_registry_backend import (
    ShardedSQLiteStoryRegistryBackend,
    SQLiteStoryRegistryBackend,
    StoryRegistryBackend,
)
from flowcore_story.core.story_registry_models import (
    TERMINAL_STATUSES,
    StoryCrawlStatus,
    StoryRegistryEntry,
)
from flowcore.utils.logger import logger
from flowcore.utils.progress_emitter import emit_progress_event


class StoryRegistry:
    """Facade that delegates registry operations to a storage backend."""

    def __init__(
        self,
        db_path: str,
        category_store: CategoryStore,
        *,
        backend: StoryRegistryBackend | None = None,
        shard_by_site: bool = False,
        shard_directory: str | None = None,
    ) -> None:
        """Build a registry facade with a pluggable storage backend.

        Parameters
        ----------
        db_path:
            Default SQLite database path used by the built-in backend.
        category_store:
            Category store used for story metadata lookups.
        backend:
            Inject a custom backend implementation, e.g. a PostgreSQL or Redis
            service. When omitted a SQLite backend is provisioned.
        shard_by_site:
            Convenience flag that instantiates the sharded SQLite backend for
            environments running multiple crawler processes per site.
        shard_directory:
            Optional custom directory used when ``shard_by_site`` is enabled.
        """

        if backend is not None:
            self._backend = backend
        elif shard_by_site:
            self._backend = ShardedSQLiteStoryRegistryBackend(
                db_path,
                category_store,
                shard_directory=shard_directory,
            )
        else:
            self._backend = SQLiteStoryRegistryBackend(db_path, category_store)

    # ------------------------------------------------------------------
    # CRUD helpers

    def is_terminal_status(self, status: StoryCrawlStatus | str) -> bool:
        """Return ``True`` when ``status`` is considered terminal.

        Parameters
        ----------
        status:
            Either a :class:`StoryCrawlStatus` value or its string representation.

        Unknown status values are treated as non-terminal so that callers do
        not accidentally short-circuit new workflow states introduced in the
        future.
        """

        if not isinstance(status, StoryCrawlStatus):
            try:
                status = StoryCrawlStatus(str(status))
            except ValueError:
                return False

        return status in TERMINAL_STATUSES

    def ensure_entry(
        self,
        site_key: str,
        story: dict[str, Any],
        category_name: str | None = None,
    ) -> StoryRegistryEntry:
        """Ensure a registry entry exists for ``story`` and return it."""

        entry, should_emit = self._backend.ensure_entry(
            site_key, story, category_name=category_name
        )
        if should_emit:
            self._emit_registry_update_sync()
        return entry

    async def ensure_entry_async(
        self,
        site_key: str,
        story: dict[str, Any],
        category_name: str | None = None,
    ) -> StoryRegistryEntry:
        entry, should_emit = await asyncio.to_thread(
            self._backend.ensure_entry,
            site_key,
            story,
            category_name=category_name,
        )
        if should_emit:
            await self._emit_registry_update_async()
        return entry

    def try_acquire(
        self,
        story_id: int,
        *,
        category_name: str | None = None,
    ) -> StoryRegistryEntry | None:
        """Transition a story to ``IN_PROGRESS`` if it is not already busy."""

        entry, should_emit = self._backend.try_acquire(
            story_id, category_name=category_name
        )
        if should_emit:
            self._emit_registry_update_sync()
        return entry

    async def try_acquire_async(
        self,
        story_id: int,
        *,
        category_name: str | None = None,
    ) -> StoryRegistryEntry | None:
        entry, should_emit = await asyncio.to_thread(
            self._backend.try_acquire,
            story_id,
            category_name=category_name,
        )
        if should_emit:
            await self._emit_registry_update_async()
        return entry

    def mark_status(
        self,
        story_id: int,
        status: StoryCrawlStatus,
        *,
        result: str | None = None,
        category_name: str | None = None,
    ) -> StoryRegistryEntry | None:
        """Persist the final status for a story and return the updated entry."""

        entry, should_emit = self._backend.mark_status(
            story_id,
            status,
            result=result,
            category_name=category_name,
        )
        if should_emit:
            self._emit_registry_update_sync()
        return entry

    async def mark_status_async(
        self,
        story_id: int,
        status: StoryCrawlStatus,
        *,
        result: str | None = None,
        category_name: str | None = None,
    ) -> StoryRegistryEntry | None:
        entry, should_emit = await asyncio.to_thread(
            self._backend.mark_status,
            story_id,
            status,
            result=result,
            category_name=category_name,
        )
        if should_emit:
            await self._emit_registry_update_async()
        return entry

    def get_entry(self, story_id: int) -> StoryRegistryEntry | None:
        """Return the registry entry for ``story_id`` if it exists."""

        return self._backend.get_entry(story_id)

    async def get_entry_async(self, story_id: int) -> StoryRegistryEntry | None:
        return await asyncio.to_thread(self._backend.get_entry, story_id)

    # ------------------------------------------------------------------
    # Reporting helpers

    def get_backlog_overview(self) -> dict[str, Any]:
        """Return backlog statistics for the registry and active categories."""

        return self._backend.get_backlog_overview()

    async def get_backlog_overview_async(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._backend.get_backlog_overview)

    # ------------------------------------------------------------------
    # Reset helpers

    def reset_in_progress(
        self,
        *,
        site_key: str | None = None,
        older_than_seconds: float | None = None,
        story_ids: Iterable[int] | None = None,
    ) -> int:
        """Reset ``IN_PROGRESS`` entries back to ``NEEDS_RETRY``."""

        updated, should_emit = self._backend.reset_in_progress(
            site_key=site_key,
            older_than_seconds=older_than_seconds,
            story_ids=story_ids,
        )
        if should_emit:
            self._emit_registry_update_sync()
        return updated

    async def reset_in_progress_async(
        self,
        *,
        site_key: str | None = None,
        older_than_seconds: float | None = None,
        story_ids: Iterable[int] | None = None,
    ) -> int:
        updated, should_emit = await asyncio.to_thread(
            self._backend.reset_in_progress,
            site_key=site_key,
            older_than_seconds=older_than_seconds,
            story_ids=story_ids,
        )
        if should_emit:
            await self._emit_registry_update_async()
        return updated

    def mark_story_pending(
        self,
        *,
        story_id: int | None = None,
        site_key: str | None = None,
        story_url: str | None = None,
    ) -> StoryRegistryEntry | None:
        """Manually reset a single story back to ``PENDING``."""

        entry, should_emit = self._backend.mark_story_pending(
            story_id=story_id,
            site_key=site_key,
            story_url=story_url,
        )
        if should_emit:
            self._emit_registry_update_sync()
        return entry

    async def mark_story_pending_async(
        self,
        *,
        story_id: int | None = None,
        site_key: str | None = None,
        story_url: str | None = None,
    ) -> StoryRegistryEntry | None:
        entry, should_emit = await asyncio.to_thread(
            self._backend.mark_story_pending,
            story_id=story_id,
            site_key=site_key,
            story_url=story_url,
        )
        if should_emit:
            await self._emit_registry_update_async()
        return entry

    def mark_category_pending(
        self,
        site_key: str,
        category_name: str,
    ) -> int:
        """Reset all stories in ``category_name`` back to ``PENDING``."""

        updated, should_emit = self._backend.mark_category_pending(
            site_key, category_name
        )
        if should_emit:
            self._emit_registry_update_sync()
        return updated

    async def mark_category_pending_async(self, site_key: str, category_name: str) -> int:
        updated, should_emit = await asyncio.to_thread(
            self._backend.mark_category_pending,
            site_key,
            category_name,
        )
        if should_emit:
            await self._emit_registry_update_async()
        return updated

    # ------------------------------------------------------------------
    # Utilities

    @property
    def terminal_statuses(self) -> Sequence[StoryCrawlStatus]:
        return TERMINAL_STATUSES

    def ensure_entries(
        self,
        site_key: str,
        stories: Iterable[dict[str, Any]],
        *,
        category_name: str | None = None,
    ) -> None:
        """Bulk ensure registry entries exist for a list of stories."""

        for story in stories:
            try:
                self.ensure_entry(site_key, story, category_name)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error(
                    "[REGISTRY] Failed to ensure entry for story %s: %s",
                    story.get("url"),
                    exc,
                )

    # ------------------------------------------------------------------
    # Event publishing

    def _emit_registry_update_sync(self) -> None:
        """Publish the latest backlog snapshot to the realtime stream."""

        try:
            summary = self.get_backlog_overview()
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.warning("[REGISTRY] Không thể lấy backlog để phát realtime: %s", exc)
            return

        self._publish_registry_summary(summary)

    async def _emit_registry_update_async(self) -> None:
        """Publish the latest backlog snapshot using async-friendly calls."""

        try:
            summary = await self.get_backlog_overview_async()
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.warning("[REGISTRY] Không thể lấy backlog để phát realtime: %s", exc)
            return

        self._publish_registry_summary(summary)

    def _publish_registry_summary(self, summary: dict[str, Any]) -> None:
        payload = {
            "action": "summary",
            "summary": summary,
            "pending_jobs": {
                "total": summary.get("total_backlog", 0),
                "active": summary.get("stories_active", 0),
                "planned_total": summary.get("planned_total", 0),
                "generated_at": summary.get("generated_at"),
            },
        }

        emit_progress_event("registry", payload)

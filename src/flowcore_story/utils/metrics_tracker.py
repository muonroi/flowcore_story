"""Utility helpers to track crawl progress and expose a lightweight dashboard."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from flowcore_story.storage.db_tracking import site_genre_overview
from flowcore_story.utils.dashboard_utils import build_overview_signature
from flowcore_story.utils.io_utils import safe_write_json_sync
from flowcore_story.utils.logger import logger
from flowcore_story.utils.progress_emitter import emit_progress_event
# Maximum items to include in snapshot to prevent MessageSizeTooLargeError
MAX_STORIES_IN_SNAPSHOT = 100  # Limit stories list to prevent large payloads
MAX_EVENTS_PER_STORY = 10  # Limit events history per story
MAX_EVENT_ENTRIES = 50  # Limit total event entries
SNAPSHOT_COMPACT_MODE = True  # Use compact snapshot format


_DEFAULT_BACKLOG_SUMMARY: dict[str, Any] = {
    "generated_at": None,
    "total_backlog": 0,
    "planned_total": 0,
    "stories_active": 0,
    "total_genres": 0,
    "genres_done": 0,
    "by_category": [],
}


_UNSET = object()


def _coerce_positive_int(value: Any) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return 0
    return candidate if candidate > 0 else 0


def _normalise_backlog_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(summary, dict):
        return dict(_DEFAULT_BACKLOG_SUMMARY)

    normalized = dict(_DEFAULT_BACKLOG_SUMMARY)
    normalized["generated_at"] = summary.get("generated_at")
    normalized["total_backlog"] = _coerce_positive_int(summary.get("total_backlog"))
    normalized["planned_total"] = _coerce_positive_int(summary.get("planned_total"))
    normalized["stories_active"] = _coerce_positive_int(summary.get("stories_active"))
    normalized["total_genres"] = _coerce_positive_int(summary.get("total_genres"))
    normalized["genres_done"] = _coerce_positive_int(summary.get("genres_done"))

    categories: list[dict[str, Any]] = []
    for item in summary.get("by_category", []):
        if not isinstance(item, dict):
            continue
        categories.append(
            {
                "category_id": item.get("category_id"),
                "category_name": item.get("category_name"),
                "category_url": item.get("category_url"),
                "site_key": item.get("site_key"),
                "backlog": _coerce_positive_int(item.get("backlog")),
            }
        )
    normalized["by_category"] = categories
    return normalized

def _snapshot_checksum(snapshot: dict[str, Any]) -> str:
    serialized = json.dumps(
        snapshot,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()



@dataclass
class StoryProgress:
    story_id: str
    title: str
    total_chapters: int
    crawled_chapters: int = 0
    missing_chapters: int = 0
    status: str = "queued"
    primary_site: str | None = None
    genre_name: str | None = None
    genre_url: str | None = None
    genre_site_key: str | None = None
    last_source: str | None = None
    last_error: str | None = None
    started_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: float | None = None
    cooldown_until: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.story_id,
            "title": self.title,
            "total_chapters": self.total_chapters,
            "crawled_chapters": self.crawled_chapters,
            "missing_chapters": self.missing_chapters,
            "status": self.status,
            "primary_site": self.primary_site,
            "genre_name": self.genre_name,
            "genre_url": self.genre_url,
            "genre_site_key": self.genre_site_key,
            "last_source": self.last_source,
            "last_error": self.last_error,
            "started_at": _to_iso(self.started_at),
            "updated_at": _to_iso(self.updated_at),
            "completed_at": _to_iso(self.completed_at) if self.completed_at else None,
            "cooldown_until": _to_iso(self.cooldown_until) if self.cooldown_until else None,
        }


@dataclass
class SiteHealthSnapshot:
    site_key: str
    success: int = 0
    failure: int = 0
    last_error: str | None = None
    last_alert_at: float | None = None
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        total = self.success + self.failure
        failure_rate = (self.failure / total) if total else 0.0
        return {
            "site_key": self.site_key,
            "success": self.success,
            "failure": self.failure,
            "failure_rate": round(failure_rate, 4),
            "last_error": self.last_error,
            "last_alert_at": _to_iso(self.last_alert_at) if self.last_alert_at else None,
            "updated_at": _to_iso(self.updated_at),
        }


@dataclass
class GenreProgress:
    site_key: str
    genre_name: str
    genre_url: str
    position: int | None = None
    total_genres: int | None = None
    total_pages: int | None = None
    crawled_pages: int = 0
    current_page: int | None = None
    total_stories: int = 0
    processed_stories: int = 0
    active_stories: list[str] = field(default_factory=list)
    active_story_details: list[dict[str, Any]] = field(default_factory=list)
    current_story_title: str | None = None
    current_story_page: int | None = None
    current_story_position: int | None = None
    status: str = "queued"
    last_error: str | None = None
    started_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "site_key": self.site_key,
            "name": self.genre_name,
            "url": self.genre_url,
            "position": self.position,
            "total_genres": self.total_genres,
            "total_pages": self.total_pages,
            "crawled_pages": self.crawled_pages,
            "current_page": self.current_page,
            "total_stories": self.total_stories,
            "processed_stories": self.processed_stories,
            "active_stories": list(self.active_stories),
            "active_story_details": [
                {k: v for k, v in detail.items() if v is not None}
                for detail in self.active_story_details
            ],
            "current_story_title": self.current_story_title,
            "current_story_page": self.current_story_page,
            "current_story_position": self.current_story_position,
            "status": self.status,
            "last_error": self.last_error,
            "started_at": _to_iso(self.started_at),
            "updated_at": _to_iso(self.updated_at),
            "completed_at": _to_iso(self.completed_at) if self.completed_at else None,
        }

        # Add not_done_stories field for dashboard tracking
        if self.total_stories is not None and self.processed_stories is not None:
            result["not_done_stories"] = max(0, self.total_stories - self.processed_stories)

        return result


def _story_completion_percent(progress: StoryProgress) -> int:
    crawled = max(int(progress.crawled_chapters or 0), 0)
    missing = max(int(progress.missing_chapters or 0), 0)
    configured_total = max(int(progress.total_chapters or 0), 0)
    inferred_total = crawled + missing
    dynamic_total = configured_total if configured_total > 0 else inferred_total

    if progress.status == "completed":
        return 100

    if dynamic_total <= 0:
        return 0

    percent = int((crawled / dynamic_total) * 100)
    return max(0, min(percent, 100))


def _story_event_payload(progress: StoryProgress) -> dict[str, Any]:
    payload = progress.to_dict()
    payload.update(
        {
            "percent": _story_completion_percent(progress),
            "remaining_chapters": max(
                int(progress.total_chapters or 0) - int(progress.crawled_chapters or 0),
                0,
            ),
            "started_at_ts": progress.started_at,
            "updated_at_ts": progress.updated_at,
            "completed_at_ts": progress.completed_at,
            "cooldown_until_ts": progress.cooldown_until,
        }
    )
    return payload


def _emit_story_event(action: str, story_payload: dict[str, Any] | None) -> None:
    if not story_payload:
        return
    emit_progress_event("story", {"action": action, "story": story_payload})


def _enrich_active_story_detail(detail: dict[str, Any], story_progress_lookup: dict[str, StoryProgress]) -> dict[str, Any]:
    """Enrich active story detail with full required fields for dashboard."""
    if not story_progress_lookup:
        logger.warning("[Metrics] story_progress_lookup is empty, cannot enrich active stories")

    enriched = dict(detail)

    # Try to find story in progress tracker
    story_id = detail.get("id") or detail.get("story_id")
    if story_id and story_id in story_progress_lookup:
        story = story_progress_lookup[story_id]
        # Ensure required fields are present
        if "progress" not in enriched:
            enriched["progress"] = _story_completion_percent(story)
        if "crawled_chapters" not in enriched:
            enriched["crawled_chapters"] = story.crawled_chapters
        if "total_chapters" not in enriched:
            enriched["total_chapters"] = story.total_chapters
        if "status" not in enriched:
            enriched["status"] = story.status
        if "last_source" not in enriched:
            enriched["last_source"] = story.last_source
        if "primary_site" not in enriched:
            enriched["primary_site"] = story.primary_site

    # Filter out None values
    return {k: v for k, v in enriched.items() if v is not None}


def _build_genre_story_event(
    genre: GenreProgress | None,
    *,
    action: str,
    story_title: str | None = None,
    story_page: int | None = None,
    story_position: int | None = None,
    processed: bool | None = None,
    story_progress_lookup: dict[str, StoryProgress] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not genre:
        return None

    payload: dict[str, Any] = {
        "action": action,
        "site_key": genre.site_key,
        "genre_name": genre.genre_name,
        "genre_url": genre.genre_url,
        "status": genre.status,
        "processed_stories": genre.processed_stories,
        "total_stories": genre.total_stories,
        "active_stories": list(genre.active_stories),
        "timestamp": time.time(),
    }

    # Add computed not_done field for dashboard
    if genre.total_stories is not None and genre.processed_stories is not None:
        payload["not_done_stories"] = max(0, genre.total_stories - genre.processed_stories)

    if genre.position is not None:
        payload["position"] = genre.position
    if genre.total_genres is not None:
        payload["total_genres"] = genre.total_genres
    if processed is not None:
        payload["processed"] = bool(processed)

    story_payload: dict[str, Any] = {}
    if story_title:
        story_payload["title"] = story_title
    if story_page is not None:
        story_payload["page"] = story_page
    if story_position is not None:
        story_payload["position"] = story_position
    if story_payload:
        payload["story"] = story_payload

    if extra:
        payload.update({k: v for k, v in extra.items() if v is not None})

    # Enrich active_story_details with full information for dashboard
    if genre.active_story_details:
        lookup = story_progress_lookup or {}
        payload["active_story_details"] = [
            _enrich_active_story_detail(detail, lookup)
            for detail in genre.active_story_details
        ]

    if genre.current_story_title:
        payload.setdefault("story", {})
        payload["story"].setdefault("title", genre.current_story_title)
    if genre.current_story_page is not None:
        payload.setdefault("story", {})
        payload["story"].setdefault("page", genre.current_story_page)
    if genre.current_story_position is not None:
        payload.setdefault("story", {})
        payload["story"].setdefault("position", genre.current_story_position)

    return payload


def _to_iso(value: float | None) -> str | None:
    if value is None:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))
    except Exception:  # pragma: no cover - defensive guard for invalid timestamps
        return None


class CrawlMetricsTracker:
    """In-memory tracker that periodically dumps progress to ``state/dashboard.json``."""

    def __init__(self) -> None:
        dashboard_file = os.environ.get("STORYFLOW_DASHBOARD_FILE")
        if dashboard_file:
            self._dashboard_file = dashboard_file
            dashboard_dir = os.path.dirname(os.path.abspath(dashboard_file))
            if dashboard_dir and not os.path.exists(dashboard_dir):
                os.makedirs(dashboard_dir, exist_ok=True)
        else:
            state_dir = "state"
            os.makedirs(state_dir, exist_ok=True)
            self._dashboard_file = os.path.join(state_dir, "dashboard.json")

        self._lock = Lock()
        self._persistence_enabled = True
        self._stories_in_progress: dict[str, StoryProgress] = {}
        self._stories_completed: dict[str, StoryProgress] = {}
        self._stories_skipped: dict[str, StoryProgress] = {}
        self._queues: dict[str, Any] = {"skipped": 0}
        self._site_stats: dict[str, SiteHealthSnapshot] = {}
        self._genres_in_progress: dict[str, GenreProgress] = {}
        self._genres_completed: dict[str, GenreProgress] = {}
        self._genres_failed: dict[str, GenreProgress] = {}
        self._site_genre_overview: dict[str, dict[str, Any]] = {}
        self._story_events: dict[str, list[dict[str, Any]]] = {}
        self._story_event_summary: dict[str, int] = defaultdict(int)
        self._global_story_completed: int = 0
        self._global_story_total_estimate: int | None = None
        self._backlog_provider: Callable[[], dict[str, Any]] | None = None
        self._system_custom: dict[str, Any] = {}
        self._system_metrics: dict[str, Any] = {}
        self._last_system_signature: tuple[tuple[str, Any], ...] | None = None
        self._pending_system_event: dict[str, Any] | None = None
        self._last_snapshot_checksum: str | None = None
        self._last_snapshot_signature: tuple[Any, ...] | None = None

        # Challenge tracking for rate limit monitoring
        self._challenge_counts: dict[str, list[float]] = defaultdict(list)  # site_key -> [timestamps]

        self._load_existing()

    def disable_persistence(self) -> None:
        """Disable automatic persistence to disk for batch operations."""
        self._persistence_enabled = False

    def enable_persistence(self) -> None:
        """Enable automatic persistence to disk."""
        self._persistence_enabled = True

    def force_persist(self) -> None:
        """Force an immediate persistence of the current state to disk."""
        with self._lock:
            self._persist_locked(force=True)

    @staticmethod
    def _genre_key(site_key: str, genre_url: str) -> str:
        return f"{site_key}:{genre_url}"

    @staticmethod
    def _event_key(category_id: str, story_id: str) -> str:
        return f"{category_id}:{story_id}"

    # ------------------------------------------------------------------
    # Story tracking helpers
    # ------------------------------------------------------------------
    def story_started(
        self,
        story_id: str,
        title: str,
        total_chapters: int,
        *,
        primary_site: str | None = None,
        genre_name: str | None = None,
        genre_url: str | None = None,
        genre_site_key: str | None = None,
    ) -> None:
        progress = StoryProgress(
            story_id=story_id,
            title=title,
            total_chapters=total_chapters or 0,
            missing_chapters=max(total_chapters or 0, 0),
            status="running",
            primary_site=primary_site,
            genre_name=genre_name,
            genre_url=genre_url,
            genre_site_key=genre_site_key,
        )
        payload: dict[str, Any] | None = None
        with self._lock:
            self._stories_in_progress[story_id] = progress
            self._stories_completed.pop(story_id, None)
            self._stories_skipped.pop(story_id, None)
            self._persist_locked()
            payload = _story_event_payload(progress)

        _emit_story_event("started", payload)

    def update_story_progress(
        self,
        story_id: str,
        *,
        crawled_chapters: int | None = None,
        missing_chapters: int | None = None,
        status: str | None = None,
        last_source: str | None = None,
        last_error: str | None = None,
        cooldown_until: float | None = None,
        event_action: str = "progress",
    ) -> None:
        logger.debug(
            f"[Metrics] update_story_progress: story_id={story_id[:12]}..., "
            f"crawled={crawled_chapters}, missing={missing_chapters}, status={status}"
        )

        payload: dict[str, Any] | None = None
        with self._lock:
            story = self._stories_in_progress.get(story_id)
            if not story:
                logger.warning(
                    f"[Metrics] Cannot update progress for unknown story: {story_id[:12]}..."
                )
                return
            if crawled_chapters is not None:
                story.crawled_chapters = max(crawled_chapters, 0)
            if missing_chapters is not None:
                story.missing_chapters = max(missing_chapters, 0)
            if status:
                story.status = status
            if last_source:
                story.last_source = last_source
            if last_error:
                story.last_error = last_error
            story.cooldown_until = cooldown_until
            story.updated_at = time.time()
            self._persist_locked()
            payload = _story_event_payload(story)

            # Validate and log progress update
            if story.total_chapters > 0:
                percent = _story_completion_percent(story)
                logger.debug(
                    f"[Metrics] Story {story_id[:12]}... updated: "
                    f"{story.crawled_chapters}/{story.total_chapters} ({percent}%)"
                )

        _emit_story_event(event_action, payload)

    def story_on_cooldown(self, story_id: str, cooldown_until: float) -> None:
        self.update_story_progress(
            story_id,
            status="cooldown",
            cooldown_until=cooldown_until,
            event_action="cooldown",
        )

    def story_completed(self, story_id: str) -> None:
        payload: dict[str, Any] | None = None
        with self._lock:
            story = self._stories_in_progress.pop(story_id, None)
            if not story:
                return
            story.status = "completed"
            story.completed_at = time.time()
            story.updated_at = story.completed_at
            story.cooldown_until = None
            story.missing_chapters = 0
            self._stories_completed[story_id] = story
            self._persist_locked()
            payload = _story_event_payload(story)

        _emit_story_event("completed", payload)

    def story_failed(self, story_id: str, reason: str, *, final_status: str = "failed") -> None:
        payload: dict[str, Any] | None = None
        with self._lock:
            story = self._stories_in_progress.get(story_id)
            if story:
                story.status = final_status or "failed"
                story.last_error = reason
                story.updated_at = time.time()
                payload = _story_event_payload(story)
            self._persist_locked()

        _emit_story_event("failed", payload)

    def story_skipped(self, story_id: str, title: str, reason: str) -> None:
        payload: dict[str, Any] | None = None
        with self._lock:
            story = self._stories_in_progress.pop(story_id, None)
            if story is None:
                story = StoryProgress(
                    story_id=story_id,
                    title=title,
                    total_chapters=0,
                    missing_chapters=0,
                )
            story.status = "skipped"
            story.last_error = reason
            story.updated_at = time.time()
            story.completed_at = None
            story.cooldown_until = None
            self._stories_skipped[story_id] = story
            self._persist_locked()
            payload = _story_event_payload(story)

        _emit_story_event("skipped", payload)

    def record_story_event(
        self,
        category_id: str,
        story_id: str,
        event: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not category_id or not story_id or not event:
            return

        normalized_meta = {
            key: value
            for key, value in (metadata or {}).items()
            if value is not None
        }
        entry = {
            "category_id": str(category_id),
            "story_id": str(story_id),
            "event": str(event),
            "metadata": normalized_meta or None,
            "timestamp": time.time(),
        }
        event_key = self._event_key(str(category_id), str(story_id))

        with self._lock:
            events = self._story_events.setdefault(event_key, [])
            events.append(entry)
            if len(events) > 50:
                events[:] = events[-50:]
            self._story_events[event_key] = events
            self._story_event_summary[str(event)] += 1
            self._persist_locked()

    # ------------------------------------------------------------------
    # Genre tracking helpers
    # ------------------------------------------------------------------
    def site_genres_initialized(self, site_key: str, total_genres: int, genres_map: dict[str, dict[str, Any]]) -> None:
        """
        FIXED: Pass the full genres_map to the DB tracker so it saves all genres at once.
        """
        with self._lock:
            summary = self._site_genre_overview.setdefault(
                site_key,
                {"total_genres": 0, "genres": {}, "updated_at": time.time()},
            )
            # Use the passed genres_map directly
            summary["genres"] = genres_map
            summary["total_genres"] = max(int(total_genres), summary.get("total_genres", 0))
            summary["updated_at"] = time.time()
            self._persist_locked()

    def genre_started(
        self,
        site_key: str,
        genre_name: str,
        genre_url: str,
        *,
        position: int | None = None,
        total_genres: int | None = None,
    ) -> None:
        progress = GenreProgress(
            site_key=site_key,
            genre_name=genre_name,
            genre_url=genre_url,
            position=position,
            total_genres=total_genres,
            status="running",
        )
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            self._genres_in_progress[key] = progress
            self._genres_completed.pop(key, None)
            self._genres_failed.pop(key, None)
            if total_genres is not None:
                summary = self._site_genre_overview.setdefault(
                    site_key,
                    {"total_genres": int(total_genres), "genres": {}, "updated_at": time.time()},
                )
                summary["total_genres"] = max(int(total_genres), summary.get("total_genres", 0))
                summary["updated_at"] = time.time()
            self._persist_locked()

    def update_genre_pages(
        self,
        site_key: str,
        genre_url: str,
        *,
        crawled_pages: int | None = None,
        total_pages: int | None = None,
        current_page: int | None = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return
            if total_pages is not None:
                total = int(total_pages)
                genre.total_pages = total if total > 0 else None
            if crawled_pages is not None:
                genre.crawled_pages = max(int(crawled_pages), 0)
            if current_page is not None:
                genre.current_page = max(int(current_page), 0)
            genre.status = "fetching_pages"
            genre.updated_at = time.time()
            self._persist_locked()

    def set_genre_story_total(
        self,
        site_key: str,
        genre_url: str,
        total_stories: int,
    ) -> None:
        logger.debug(
            f"[Metrics] set_genre_story_total: site_key={site_key}, "
            f"genre_url={genre_url[:50]}..., total_stories={total_stories}"
        )

        key = self._genre_key(site_key, genre_url)
        event_payload: dict[str, Any] | None = None
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                logger.warning(
                    f"[Metrics] Cannot set story total for unknown genre: {genre_url[:50]}..."
                )
                return
            new_total = max(int(total_stories), 0)
            if genre.total_stories != new_total:
                genre.total_stories = new_total
                genre.updated_at = time.time()
                self._persist_locked()
                # Build a lightweight event so realtime dashboards can update totals immediately
                event_payload = _build_genre_story_event(
                    genre,
                    action="planned",
                    story_progress_lookup=self._stories_in_progress,
                )
        if event_payload:
            emit_progress_event("genre_story", event_payload)
            # Also emit an updated snapshot for single-source-of-truth consumers
            try:
                self.emit_dashboard_snapshot()
            except Exception:
                pass

    def genre_story_started(
        self,
        site_key: str,
        genre_url: str,
        story_title: str,
        story_page: int | None = None,
        story_position: int | None = None,
        *,
        story_id: str | None = None,
        story_slug: str | None = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        event_payload: dict[str, Any] | None = None
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return
            if story_title not in genre.active_stories:
                genre.active_stories.append(story_title)
                if len(genre.active_stories) > 5:
                    genre.active_stories = genre.active_stories[-5:]

            def _positive_int(value: int | None) -> int | None:
                if value is None:
                    return None
                try:
                    candidate = int(value)
                except (TypeError, ValueError):
                    return None
                return candidate if candidate > 0 else None

            detail: dict[str, Any] = {"title": story_title}
            story_identifier: str | None = None
            if story_id is not None:
                try:
                    story_identifier = str(story_id)
                except Exception:
                    story_identifier = None
            if story_identifier:
                detail["id"] = story_identifier
            if isinstance(story_slug, str) and story_slug:
                detail["slug"] = story_slug
            page_value = _positive_int(story_page)
            if page_value is not None:
                detail["page"] = page_value
                genre.current_story_page = page_value

            position_value = _positive_int(story_position)
            if position_value is not None:
                detail["position"] = position_value
                genre.current_story_position = position_value

            def _matches(detail_item: dict[str, Any]) -> bool:
                if story_identifier and str(detail_item.get("id")) == story_identifier:
                    return True
                return detail_item.get("title") == story_title

            genre.active_story_details = [
                item for item in genre.active_story_details if not _matches(item)
            ]
            genre.active_story_details.append(detail)
            if len(genre.active_story_details) > 5:
                genre.active_story_details = genre.active_story_details[-5:]

            genre.current_story_title = story_title
            genre.status = "processing_stories"
            genre.updated_at = time.time()
            self._persist_locked()
            event_payload = _build_genre_story_event(
                genre,
                action="started",
                story_title=story_title,
                story_page=page_value,
                story_position=position_value,
                story_progress_lookup=self._stories_in_progress,
            )
        if event_payload:
            emit_progress_event("genre_story", event_payload)
        # Push a fresh snapshot so dashboards have authoritative state
        try:
            self.emit_dashboard_snapshot()
        except Exception:
            pass

    def update_genre_story_progress(
        self,
        site_key: str,
        genre_url: str,
        story_title: str,
        progress: int,
        *,
        story_id: str | None = None,
        story_slug: str | None = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return

            story_identifier = str(story_id) if story_id else None

            for detail in genre.active_story_details:
                # Match by ID if available, otherwise title
                is_match = False
                if story_identifier and str(detail.get("id")) == story_identifier:
                    is_match = True
                elif detail.get("title") == story_title:
                    is_match = True

                if is_match:
                    detail["progress"] = max(0, min(100, int(progress)))
                    genre.updated_at = time.time()
                    self._persist_locked()
                    break

    def genre_story_finished(
        self,
        site_key: str,
        genre_url: str,
        story_title: str,
        *,
        processed: bool = False,
        story_id: str | None = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        event_payload: dict[str, Any] | None = None
        with self._lock:
            genre = self._genres_in_progress.get(key)
            if not genre:
                return

            if processed:
                genre.processed_stories += 1

            # active_stories is a list (not a set), so remove by title/id safely
            title_or_id = {story_title}
            if story_id:
                title_or_id.add(str(story_id))
            genre.active_stories = [
                s for s in genre.active_stories if s not in title_or_id
            ]

            # Remove from active details by title or ID
            genre.active_story_details = [
                d for d in genre.active_story_details 
                if d.get("title") != story_title and (not story_id or str(d.get("id")) != str(story_id))
            ]
            
            genre.updated_at = time.time()
            self._persist_locked()
            
            event_payload = _build_genre_story_event(
                genre,
                action="story_finished",
                story_progress_lookup=self._stories_in_progress,
                extra={"title": story_title, "processed": processed}
            )
        if event_payload:
            emit_progress_event("genre_story", event_payload)

    def genre_completed(
        self,
        site_key: str,
        genre_url: str,
        *,
        stories_processed: int | None = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        event_payload: dict[str, Any] | None = None
        with self._lock:
            genre = self._genres_in_progress.pop(key, None)
            if not genre:
                genre = self._genres_failed.pop(key, None)
                if not genre:
                    # If not found in progress/failed, check completed to update it? 
                    # Or just ignore. For now, let's try to find it in completed to support updates
                    genre = self._genres_completed.get(key)
                    if not genre:
                        return

            if stories_processed is not None:
                genre.processed_stories = max(int(stories_processed), 0)
            
            genre.status = "completed"
            genre.last_error = None
            genre.active_stories.clear()
            genre.active_story_details.clear()
            genre.current_story_title = None
            genre.current_story_page = None
            genre.current_story_position = None
            now = time.time()
            genre.completed_at = now
            genre.updated_at = now
            self._genres_completed[key] = genre

            summary = self._site_genre_overview.setdefault(
                site_key,
                {"total_genres": genre.total_genres or 0, "genres": {}, "updated_at": time.time()},
            )
            entry = summary.setdefault("genres", {})
            total_stories = genre.total_stories
            try:
                if total_stories is not None:
                    total_stories = int(total_stories)
            except Exception:
                total_stories = None
            entry[genre.genre_url] = {
                "name": genre.genre_name,
                "url": genre.genre_url,
                "stories": genre.processed_stories,
                "status": "completed",
                "updated_at": now,
                "total_stories": total_stories,
            }
            summary["updated_at"] = now
            if genre.total_genres:
                summary["total_genres"] = max(summary.get("total_genres", 0), int(genre.total_genres))
            self._persist_locked()
            event_payload = _build_genre_story_event(
                genre,
                action="completed",
                story_progress_lookup=self._stories_in_progress,
            )
        if event_payload:
            emit_progress_event("genre_story", event_payload)

    def genre_failed(
        self,
        site_key: str,
        genre_url: str,
        reason: str,
        *,
        genre_name: str | None = None,
    ) -> None:
        key = self._genre_key(site_key, genre_url)
        with self._lock:
            genre = self._genres_in_progress.pop(key, None)
            if not genre:
                genre = GenreProgress(
                    site_key=site_key,
                    genre_name=genre_name or genre_url,
                    genre_url=genre_url,
                )
            if genre_name:
                genre.genre_name = genre_name
            genre.status = "failed"
            genre.last_error = reason
            genre.active_stories.clear()
            genre.active_story_details.clear()
            genre.current_story_title = None
            genre.current_story_page = None
            genre.current_story_position = None
            now = time.time()
            genre.completed_at = now
            genre.updated_at = now
            self._genres_failed[key] = genre

            summary = self._site_genre_overview.setdefault(
                site_key,
                {"total_genres": genre.total_genres or 0, "genres": {}, "updated_at": time.time()},
            )
            entry = summary.setdefault("genres", {})
            total_stories = genre.total_stories
            try:
                if total_stories is not None:
                    total_stories = int(total_stories)
            except Exception:
                total_stories = None
            if isinstance(total_stories, int) and total_stories <= 0:
                total_stories = None
            entry[genre.genre_url] = {
                "name": genre.genre_name,
                "url": genre.genre_url,
                "stories": genre.processed_stories,
                "status": "failed",
                "updated_at": now,
                "error": reason,
                "total_stories": total_stories,
            }
            summary["updated_at"] = now
            if genre.total_genres:
                summary["total_genres"] = max(summary.get("total_genres", 0), int(genre.total_genres))
            self._persist_locked()

    def update_global_story_totals(
        self,
        *,
        completed: int | None = None,
        total_estimate: Any = _UNSET,
    ) -> None:
        """Update cached global story completion statistics."""

        with self._lock:
            dirty = False
            if completed is not None:
                try:
                    completed_value = int(completed)
                except (TypeError, ValueError):
                    completed_value = None
                if completed_value is not None:
                    if completed_value < 0:
                        completed_value = 0
                    if completed_value != self._global_story_completed:
                        self._global_story_completed = completed_value
                        dirty = True
            if total_estimate is not _UNSET:
                if total_estimate is None:
                    if self._global_story_total_estimate is not None:
                        self._global_story_total_estimate = None
                        dirty = True
                else:
                    try:
                        total_value = int(total_estimate)
                    except (TypeError, ValueError):
                        total_value = None
                    if total_value is not None:
                        if total_value < 0:
                            total_value = 0
                        if self._global_story_total_estimate != total_value:
                            self._global_story_total_estimate = total_value
                            dirty = True
            if dirty:
                self._persist_locked()
        if dirty:
            # Emit an updated snapshot so realtime dashboard reflects new totals
            try:
                self.emit_dashboard_snapshot()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Queue & site helpers
    # ------------------------------------------------------------------
    def update_skipped_queue_size(self, count: int) -> None:
        with self._lock:
            self._queues["skipped"] = max(int(count), 0)
            self._persist_locked()

    def record_retry_queue_enqueue(
        self,
        *,
        site_key: str,
        chapters: int,
        files: int,
        pass_index: int,
        total_passes: int,
    ) -> None:
        now = time.time()
        chapters = max(int(chapters), 0)
        files = max(int(files), 0)
        pass_index = max(int(pass_index), 0)
        total_passes = max(int(total_passes), 0)

        with self._lock:
            raw = self._queues.get("retry")
            entry: dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}

            entry.update(
                {
                    "last_site_key": site_key,
                    "last_enqueued_count": chapters,
                    "last_file_count": files,
                    "last_pass_index": pass_index,
                    "total_passes": total_passes,
                    "last_enqueued_at": now,
                }
            )
            total_enqueued = max(int(entry.get("total_enqueued", 0)), 0) + chapters
            entry["total_enqueued"] = total_enqueued
            self._queues["retry"] = entry
            self._persist_locked()

    def record_retry_queue_alert(self, *, site_key: str, reason: str) -> None:
        now = time.time()
        with self._lock:
            raw = self._queues.get("retry")
            entry: dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}

            entry.update(
                {
                    "last_alert_site_key": site_key,
                    "last_alert_reason": reason,
                    "last_alert_at": now,
                }
            )
            self._queues["retry"] = entry
            self._persist_locked()

    def update_system_metrics(self, **metrics: Any) -> None:
        """Merge ad-hoc system metrics and broadcast updates when changed."""

        sanitized: dict[str, Any] = {}
        for key, value in metrics.items():
            if value is None:
                sanitized[key] = None
            elif isinstance(value, (int, float, str, bool)):
                sanitized[key] = value
            elif isinstance(value, (Mapping, Sequence)) and not isinstance(
                value, (str, bytes, bytearray)
            ):
                try:
                    serialized = json.dumps(value, ensure_ascii=False)
                    sanitized[key] = json.loads(serialized)
                except (TypeError, ValueError):
                    sanitized[key] = str(value)
            else:
                sanitized[key] = str(value)

        with self._lock:
            changed = False
            for key, value in sanitized.items():
                if value is None:
                    if key in self._system_custom:
                        self._system_custom.pop(key, None)
                        changed = True
                    continue
                if self._system_custom.get(key) != value:
                    self._system_custom[key] = value
                    changed = True

            if not changed:
                return

            self._persist_locked()

    def update_site_health(
        self,
        site_key: str,
        *,
        success_delta: int = 0,
        failure_delta: int = 0,
        last_error: str | None = None,
        last_alert_at: float | None = None,
    ) -> None:
        with self._lock:
            snapshot = self._site_stats.get(site_key)
            if not snapshot:
                snapshot = SiteHealthSnapshot(site_key=site_key)
                self._site_stats[site_key] = snapshot
            snapshot.success = max(snapshot.success + success_delta, 0)
            snapshot.failure = max(snapshot.failure + failure_delta, 0)
            snapshot.updated_at = time.time()
            if last_error:
                snapshot.last_error = last_error
            if last_alert_at:
                snapshot.last_alert_at = last_alert_at
            self._persist_locked()

    def get_site_health_snapshot(self, site_key: str) -> SiteHealthSnapshot | None:
        """Return a copy of the tracked health snapshot for ``site_key``."""

        with self._lock:
            snapshot = self._site_stats.get(site_key)
            if not snapshot:
                return None
            return SiteHealthSnapshot(
                site_key=snapshot.site_key,
                success=snapshot.success,
                failure=snapshot.failure,
                last_error=snapshot.last_error,
                last_alert_at=snapshot.last_alert_at,
                updated_at=snapshot.updated_at,
            )

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------
    def get_snapshot(self) -> dict[str, Any]:
        backlog_summary = self._safe_collect_backlog_metrics()
        with self._lock:
            return self._build_snapshot_locked(backlog_summary, allow_emit=False)

    def emit_dashboard_snapshot(self, force: bool = False) -> None:
        """Emit a consolidated dashboard snapshot as the single source of truth.

        Sends a `category: "dashboard"` event containing the full snapshot so
        realtime consumers don't have to infer totals from individual messages.

        Args:
            force: If True, emit even if snapshot hasn't changed
        """
        try:
            snapshot = self.get_snapshot()
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.debug(f"[METRICS] Không thể tạo snapshot dashboard: {exc}")
            return

        # Calculate snapshot checksum for change detection
        import hashlib
        import json
        snapshot_sig = hashlib.md5(
            json.dumps(snapshot, sort_keys=True, default=str).encode()
        ).hexdigest()

        # Only emit if changed or forced
        if force or not hasattr(self, '_last_snapshot_sig') or snapshot_sig != self._last_snapshot_sig:
            emit_progress_event("dashboard", {"snapshot": snapshot})
            self._last_snapshot_sig = snapshot_sig
            logger.debug(f"[Metrics] Dashboard snapshot emitted (force={force})")
        else:
            logger.debug("[Metrics] Dashboard snapshot unchanged, skip emit")

    def set_backlog_provider(
        self, provider: Callable[[], dict[str, Any]] | None
    ) -> None:
        """Attach a callable that returns registry backlog information."""

        self._backlog_provider = provider

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_existing(self) -> None:
        if not os.path.exists(self._dashboard_file):
            return
        try:
            with open(self._dashboard_file, encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:  # pragma: no cover - defensive guard
            try:
                invalid_path = f"{self._dashboard_file}.invalid-{int(time.time())}"
                os.replace(self._dashboard_file, invalid_path)
                logger.warning(
                    "[METRICS] dashboard.json corrupted, moved to %s",
                    invalid_path,
                )
            except Exception:
                pass
            logger.warning(f"[METRICS] Không đọc được dashboard.json: {exc}")
            return

        stories = payload.get("stories", {})
        for item in stories.get("in_progress", []):
            progress = _story_from_payload(item)
            if progress:
                self._stories_in_progress[progress.story_id] = progress

        # In compact mode, completed/skipped are integers (counts), not lists
        completed = stories.get("completed", [])
        if isinstance(completed, list):
            for item in completed:
                progress = _story_from_payload(item)
                if progress:
                    self._stories_completed[progress.story_id] = progress

        skipped = stories.get("skipped", [])
        if isinstance(skipped, list):
            for item in skipped:
                progress = _story_from_payload(item)
                if progress:
                    self._stories_skipped[progress.story_id] = progress

        queues = payload.get("queues", {})
        if isinstance(queues, dict):
            for key, value in queues.items():
                if isinstance(value, dict):
                    entry: dict[str, Any] = dict(value)

                    def _coerce_int_field(field: str, _entry: dict[str, Any] = entry) -> None:
                        try:
                            _entry[field] = max(int(_entry.get(field, 0)), 0)
                        except Exception:
                            _entry.pop(field, None)

                    for field in (
                        "last_enqueued_count",
                        "last_file_count",
                        "last_pass_index",
                        "total_passes",
                        "total_enqueued",
                    ):
                        _coerce_int_field(field)

                    for field in ("last_alert_at", "last_enqueued_at"):
                        raw_value = entry.get(field)
                        if isinstance(raw_value, str):
                            parsed = _from_iso(raw_value)
                            if parsed is not None:
                                entry[field] = parsed
                                continue
                            try:
                                entry[field] = float(raw_value)
                            except Exception:
                                entry.pop(field, None)
                        elif isinstance(raw_value, (int, float)):
                            entry[field] = float(raw_value)
                        else:
                            entry.pop(field, None)

                    self._queues[key] = entry
                else:
                    try:
                        self._queues[key] = int(value)
                    except Exception:  # pragma: no cover - defensive guard
                        continue

        for item in payload.get("sites", []):
            if not isinstance(item, dict):
                continue
            site_key = item.get("site_key")
            if not site_key:
                continue
            snapshot = SiteHealthSnapshot(
                site_key=site_key,
                success=int(item.get("success", 0)),
                failure=int(item.get("failure", 0)),
                last_error=item.get("last_error"),
                last_alert_at=_from_iso(item.get("last_alert_at")),
            )
            snapshot.updated_at = time.time()
            self._site_stats[site_key] = snapshot

        genres_payload = payload.get("genres", {})
        for item in genres_payload.get("in_progress", []):
            progress = _genre_from_payload(item)
            if progress:
                key = self._genre_key(progress.site_key, progress.genre_url)
                self._genres_in_progress[key] = progress
        for item in genres_payload.get("completed", []):
            progress = _genre_from_payload(item)
            if progress:
                key = self._genre_key(progress.site_key, progress.genre_url)
                self._genres_completed[key] = progress
        for item in genres_payload.get("failed", []):
            progress = _genre_from_payload(item)
            if progress:
                key = self._genre_key(progress.site_key, progress.genre_url)
                self._genres_failed[key] = progress

        site_genres_payload = payload.get("site_genres", [])
        for site_entry in site_genres_payload:
            if not isinstance(site_entry, dict):
                continue
            site_key = site_entry.get("site_key")
            if not site_key:
                continue
            genres_map: dict[str, Any] = {}
            for genre in site_entry.get("genres", []):
                if not isinstance(genre, dict):
                    continue
                url = genre.get("url")
                if not url:
                    continue
                total_stories_val: int | None = None
                try:
                    candidate = genre.get("total_stories")
                    if candidate is not None:
                        total_stories_val = int(candidate)
                        if total_stories_val < 0:
                            total_stories_val = 0
                except Exception:
                    total_stories_val = None
                genres_map[url] = {
                    "name": genre.get("name", url),
                    "url": url,
                    "stories": int(genre.get("stories", 0)),
                    "status": genre.get("status", "completed"),
                    "updated_at": _from_iso(genre.get("updated_at")) or time.time(),
                    "error": genre.get("error"),
                    "total_stories": total_stories_val,
                }
            self._site_genre_overview[site_key] = {
                "total_genres": int(site_entry.get("total_genres", len(genres_map))),
                "completed_genres": len(genres_map), # Should be computed, not len(genres_map)
                "genres": genres_map,
                "updated_at": _from_iso(site_entry.get("updated_at")) or time.time(),
            }

        events_payload = payload.get("story_events", {})
        if isinstance(events_payload, dict):
            for item in events_payload.get("entries", []):
                if not isinstance(item, dict):
                    continue
                category_id = item.get("category_id")
                story_id = item.get("story_id")
                if not category_id or not story_id:
                    continue
                key = self._event_key(str(category_id), str(story_id))
                events: list[dict[str, Any]] = []
                for raw_event in item.get("events", []):
                    if not isinstance(raw_event, dict):
                        continue
                    timestamp = _from_iso(raw_event.get("timestamp")) or raw_event.get("timestamp")
                    try:
                        ts_value = float(timestamp)
                    except Exception:
                        ts_value = time.time()
                    events.append(
                        {
                            "category_id": str(category_id),
                            "story_id": str(story_id),
                            "event": str(raw_event.get("event", "")),
                            "metadata": raw_event.get("metadata"),
                            "timestamp": ts_value,
                        }
                    )
                if events:
                    self._story_events[key] = events[-50:]
            summary = events_payload.get("summary", {})
            if isinstance(summary, dict):
                for event_name, count in summary.items():
                    try:
                        self._story_event_summary[str(event_name)] = int(count)
                    except Exception:  # pragma: no cover - defensive guard
                        continue

        global_payload = payload.get("global_story_totals")
        if isinstance(global_payload, dict):
            try:
                completed_count = int(global_payload.get("completed", 0))
            except Exception:
                completed_count = 0
            self._global_story_completed = completed_count if completed_count >= 0 else 0
            try:
                total_estimate = global_payload.get("total_estimate")
                if total_estimate is None:
                    self._global_story_total_estimate = None
                else:
                    total_value = int(total_estimate)
                    if total_value < 0:
                        total_value = 0
                    self._global_story_total_estimate = total_value
            except Exception:
                self._global_story_total_estimate = None

        system_payload = payload.get("system")
        if isinstance(system_payload, dict):
            self._system_metrics = dict(system_payload)
            try:
                self._last_system_signature = tuple(sorted(self._system_metrics.items()))
            except Exception:  # pragma: no cover - defensive guard
                self._last_system_signature = None
        try:
            self._last_snapshot_checksum = _snapshot_checksum(payload)
        except Exception:  # pragma: no cover - defensive guard
            self._last_snapshot_checksum = None
        # Also prime the last snapshot signature from the existing payload so
        # that we don't emit redundant snapshot overview events immediately.
        try:
            self._last_snapshot_signature = build_overview_signature(payload)
        except Exception:  # pragma: no cover - defensive guard
            self._last_snapshot_signature = None

    def _persist_locked(self, force: bool = False) -> None:
        if not self._persistence_enabled and not force:
            return
        backlog_summary = self._safe_collect_backlog_metrics()
        snapshot = self._build_snapshot_locked(backlog_summary, allow_emit=True)
        pending_event = self._pending_system_event
        self._pending_system_event = None
        try:
            if not safe_write_json_sync(self._dashboard_file, snapshot):
                raise RuntimeError("safe_write_json_sync failed")
        except Exception as exc:  # pragma: no cover - IO guard
            logger.error(f"[METRICS] Không ghi được dashboard.json: {exc}")
        try:
            checksum = _snapshot_checksum(snapshot)
        except Exception:  # pragma: no cover - defensive guard
            checksum = None

        if checksum is None or checksum != self._last_snapshot_checksum:
            emit_progress_event("dashboard", {"snapshot": snapshot, "version": 1})
            # Emit a lightweight snapshot-overview event for realtime consumers
            # that want to track the source of the update without re-parsing.
            emit_progress_event(
                "snapshot",
                {"source": "metrics_tracker", "snapshot": snapshot},
            )
            self._last_snapshot_checksum = checksum
        if pending_event:
            emit_progress_event("system", {"metrics": pending_event})

    def _safe_collect_backlog_metrics(self) -> dict[str, Any]:
        provider = self._backlog_provider
        if not provider:
            return dict(_DEFAULT_BACKLOG_SUMMARY)
        try:
            summary = provider()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(f"[METRICS] Không thể lấy backlog từ registry: {exc}")
            return dict(_DEFAULT_BACKLOG_SUMMARY)
        return _normalise_backlog_summary(summary)

    def _build_snapshot_locked(
        self,
        backlog_summary: dict[str, Any] | None = None,
        *,
        allow_emit: bool = False,
    ) -> dict[str, Any]:
        backlog_payload = _normalise_backlog_summary(backlog_summary)
        updated_at = time.time()
        # Compact snapshot to prevent MessageSizeTooLargeError
        stories_in_progress_all = list(self._stories_in_progress.values())
        stories_completed_all = list(self._stories_completed.values())
        stories_skipped_all = list(self._stories_skipped.values())

        if SNAPSHOT_COMPACT_MODE:
            # Only include summary stats for in-progress stories
            stories_in_progress = [
                {
                    "title": story.title,
                    "site_key": story.primary_site,
                    "status": story.status,
                    "chapters_total": story.total_chapters,
                    "chapters_crawled": story.crawled_chapters,
                    "missing_chapters": story.missing_chapters,
                }
                for story in stories_in_progress_all[:MAX_STORIES_IN_SNAPSHOT]
            ]
            # Just counts for completed/skipped
            stories_completed = len(stories_completed_all)
            stories_skipped = len(stories_skipped_all)
        else:
            stories_in_progress = [story.to_dict() for story in stories_in_progress_all]
            stories_completed = [story.to_dict() for story in stories_completed_all]
            stories_skipped = [story.to_dict() for story in stories_skipped_all]
        total_missing = sum(story.missing_chapters for story in self._stories_in_progress.values())
        genres_in_progress = [genre.to_dict() for genre in self._genres_in_progress.values()]
        genres_completed = [genre.to_dict() for genre in self._genres_completed.values()]
        genres_failed = [genre.to_dict() for genre in self._genres_failed.values()]
        site_genres: list[dict[str, Any]] = []
        for site_key, overview in self._site_genre_overview.items():
            genres_map = overview.get("genres", {})
            genres_list = []
            for url, data in genres_map.items():
                total_stories = data.get("total_stories")
                try:
                    total_stories_int = (
                        int(total_stories)
                        if total_stories is not None
                        else None
                    )
                    if isinstance(total_stories_int, int) and total_stories_int < 0:
                        total_stories_int = 0
                except Exception:
                    total_stories_int = None
                genres_list.append(
                    {
                        "name": data.get("name", url),
                        "url": url,
                        "stories": int(data.get("stories", 0)),
                        "status": data.get("status", "completed"),
                        "updated_at": _to_iso(data.get("updated_at")),
                        "error": data.get("error"),
                        **(
                            {"total_stories": total_stories_int}
                            if total_stories_int is not None
                            else {}
                        ),
                    }
                )
            site_genres.append(
                {
                    "site_key": site_key,
                    "total_genres": int(overview.get("total_genres", len(genres_list))),
                    "completed_genres": len(genres_list),
                    "genres": sorted(genres_list, key=lambda item: item.get("name", "")),
                    "updated_at": _to_iso(overview.get("updated_at")),
                }
            )
        site_genres.sort(key=lambda item: item.get("site_key", ""))
        total_genres_known = sum(entry.get("total_genres", 0) for entry in site_genres)
        total_genres_completed = sum(entry.get("completed_genres", 0) for entry in site_genres)
        event_entries: list[dict[str, Any]] = []
        for story_id, events in list(self._story_events.items())[:MAX_EVENT_ENTRIES]:
            if not events:
                continue
            latest = events[-1]
            if SNAPSHOT_COMPACT_MODE:
                # Compact format: only recent events, no metadata
                event_entries.append(
                    {
                        "category_id": latest.get("category_id"),
                        "story_id": latest.get("story_id"),
                        "last_event": latest.get("event"),
                        "last_event_at": _to_iso(latest.get("timestamp")),
                        "event_count": len(events),
                        "recent_events": [
                            {
                                "event": item.get("event"),
                                "timestamp": _to_iso(item.get("timestamp")),
                            }
                            for item in events[-MAX_EVENTS_PER_STORY:]
                        ],
                    }
                )
            else:
                # Full format (original)
                event_entries.append(
                    {
                        "category_id": latest.get("category_id"),
                        "story_id": latest.get("story_id"),
                        "last_event": latest.get("event"),
                        "last_event_at": _to_iso(latest.get("timestamp")),
                        "events": [
                            {
                                "event": item.get("event"),
                                "timestamp": _to_iso(item.get("timestamp")),
                                "metadata": item.get("metadata"),
                            }
                            for item in events
                        ],
                    }
                )
        event_entries.sort(key=lambda item: (item.get("category_id", ""), item.get("story_id", "")))
        event_summary = {key: int(value) for key, value in self._story_event_summary.items()}
        for key in ("start", "success", "failure", "retry"):
            event_summary.setdefault(key, 0)

        queues_payload: dict[str, Any] = {}
        for key, value in self._queues.items():
            if isinstance(value, dict):
                payload = dict(value)
                alert_at = payload.get("last_alert_at")
                if isinstance(alert_at, (int, float)):
                    payload["last_alert_at"] = _to_iso(alert_at)
                enqueued_at = payload.get("last_enqueued_at")
                if isinstance(enqueued_at, (int, float)):
                    payload["last_enqueued_at"] = _to_iso(enqueued_at)
                queues_payload[key] = payload
            else:
                queues_payload[key] = value

        system_payload: dict[str, Any] = {
            "stories_in_progress": len(stories_in_progress),
            "stories_completed": len(stories_completed) if isinstance(stories_completed, list) else stories_completed,
            "stories_skipped": len(stories_skipped) if isinstance(stories_skipped, list) else stories_skipped,
            "skipped_queue_size": self._queues.get("skipped", 0),
            "registry_backlog": backlog_payload["total_backlog"],
        }

        retry_entry = self._queues.get("retry")
        if isinstance(retry_entry, dict):
            total_enqueued = retry_entry.get("total_enqueued")
            if isinstance(total_enqueued, int):
                system_payload["retry_queue_total_enqueued"] = max(total_enqueued, 0)
            last_alert = retry_entry.get("last_alert_reason")
            if last_alert:
                system_payload["retry_queue_last_alert"] = last_alert

        system_payload.update(self._system_custom)

        signature: tuple[tuple[str, Any], ...] = tuple(sorted(system_payload.items()))
        changed = signature != self._last_system_signature
        self._last_system_signature = signature
        self._system_metrics = dict(system_payload)
        if allow_emit and changed:
            self._pending_system_event = dict(system_payload)

        return {
            "updated_at": _to_iso(updated_at),
            "stories": {
                "in_progress": stories_in_progress,
                "completed": stories_completed,
                "skipped": stories_skipped,
            },
            "aggregates": {
                "stories_in_progress": len(stories_in_progress),
                "stories_completed": len(stories_completed) if isinstance(stories_completed, list) else stories_completed,
                "stories_skipped": len(stories_skipped) if isinstance(stories_skipped, list) else stories_skipped,
                "total_missing_chapters": total_missing,
                "skipped_queue_size": self._queues.get("skipped", 0),
                "genres_in_progress": len(genres_in_progress),
                "genres_completed": len(genres_completed),
                "genres_failed": len(genres_failed),
                "genres_total_configured": total_genres_known,
                "genres_total_completed": total_genres_completed,
                # Add not_done fields for easier dashboard consumption
                "genres_not_done": max(0, total_genres_known - total_genres_completed) if total_genres_known else None,
                "stories_not_done": backlog_payload["total_backlog"],  # stories remaining
                "registry_total_backlog": backlog_payload["total_backlog"],
                "registry_planned_total": backlog_payload["planned_total"],
                "registry_stories_active": backlog_payload["stories_active"],
                "registry_total_genres": backlog_payload["total_genres"],
                "registry_genres_done": backlog_payload["genres_done"],
            },
            "queues": queues_payload,
            "sites": [snapshot.to_dict() for snapshot in self._site_stats.values()],
            "genres": {
                "in_progress": genres_in_progress,
                "completed": genres_completed,
                "failed": genres_failed,
            },
            "site_genres": site_genres,
            "registry": backlog_payload,
            "global_story_totals": {
                "completed": self._global_story_completed,
                "total_estimate": self._global_story_total_estimate,
            },
            "story_events": {
                "entries": event_entries,
                "summary": event_summary,
            },
            "system": system_payload,
        }


def _story_from_payload(payload: dict[str, Any]) -> StoryProgress | None:
    if not isinstance(payload, dict):
        return None
    story_id = payload.get("id")
    title = payload.get("title")
    if not story_id or not title:
        return None
    progress = StoryProgress(
        story_id=story_id,
        title=title,
        total_chapters=int(payload.get("total_chapters", 0)),
        crawled_chapters=int(payload.get("crawled_chapters", 0)),
        missing_chapters=int(payload.get("missing_chapters", 0)),
        status=payload.get("status", "queued"),
        primary_site=payload.get("primary_site"),
        genre_name=payload.get("genre_name"),
        genre_url=payload.get("genre_url"),
        genre_site_key=payload.get("genre_site_key"),
        last_source=payload.get("last_source"),
        last_error=payload.get("last_error"),
    )
    progress.started_at = _from_iso(payload.get("started_at")) or time.time()
    progress.updated_at = _from_iso(payload.get("updated_at")) or progress.started_at
    progress.completed_at = _from_iso(payload.get("completed_at"))
    progress.cooldown_until = _from_iso(payload.get("cooldown_until"))
    return progress


def _genre_from_payload(payload: dict[str, Any]) -> GenreProgress | None:
    if not isinstance(payload, dict):
        return None
    site_key = payload.get("site_key")
    genre_url = payload.get("url")
    if not site_key or not genre_url:
        return None
    genre_name = payload.get("name") or genre_url
    def _coerce_int(value: Any) -> int | None:
        try:
            if value is None:
                return None
            return int(value)
        except Exception:  # pragma: no cover - defensive coercion
            return None

    progress = GenreProgress(
        site_key=site_key,
        genre_name=genre_name,
        genre_url=genre_url,
        position=_coerce_int(payload.get("position")),
        total_genres=_coerce_int(payload.get("total_genres")),
        total_pages=_coerce_int(payload.get("total_pages")),
        crawled_pages=int(payload.get("crawled_pages", 0)),
        current_page=_coerce_int(payload.get("current_page")),
        total_stories=int(payload.get("total_stories", 0)),
        processed_stories=int(payload.get("processed_stories", 0)),
        status=payload.get("status", "queued"),
        last_error=payload.get("last_error"),
    )
    active = payload.get("active_stories")
    if isinstance(active, list):
        progress.active_stories = [str(item) for item in active[:5]]

    raw_details = payload.get("active_story_details")
    details: list[dict[str, Any]] = []
    if isinstance(raw_details, list):
        for item in raw_details[:5]:
            if not isinstance(item, dict):
                continue
            entry: dict[str, Any] = {}
            title = item.get("title")
            if title:
                entry["title"] = str(title)
            page = _coerce_int(item.get("page"))
            if page is not None:
                entry["page"] = page
            position = _coerce_int(item.get("position"))
            if position is not None:
                entry["position"] = position
            if entry:
                details.append(entry)
    progress.active_story_details = details
    title = payload.get("current_story_title")
    progress.current_story_title = str(title) if isinstance(title, str) and title else None
    progress.current_story_page = _coerce_int(payload.get("current_story_page"))
    progress.current_story_position = _coerce_int(payload.get("current_story_position"))
    progress.started_at = _from_iso(payload.get("started_at")) or time.time()
    progress.updated_at = _from_iso(payload.get("updated_at")) or progress.started_at
    progress.completed_at = _from_iso(payload.get("completed_at"))
    return progress


def _from_iso(value: str | None) -> float | None:
    if not value:
        return None
    try:
        struct = time.strptime(value, "%Y-%m-%d %H:%M:%S")
        return time.mktime(struct)
    except Exception:  # pragma: no cover - defensive guard for malformed input
        return None


metrics_tracker = CrawlMetricsTracker()


__all__ = ["metrics_tracker", "CrawlMetricsTracker", "SiteHealthSnapshot"]

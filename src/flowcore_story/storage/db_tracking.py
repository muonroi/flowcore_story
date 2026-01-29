"""
Database tracking utilities for story progress, site health, and events.
These tables help reduce system load by tracking state in PostgreSQL.
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

try:  # pragma: no cover - optional dependency
    import asyncpg  # type: ignore[import]
except ModuleNotFoundError:  # pragma: no cover - optional dependency missing
    asyncpg = None  # type: ignore[assignment]

from flowcore_story.storage.db_pool import DatabasePool, get_db_pool
from flowcore_story.utils.logger import logger

_SITE_HEALTH_DISABLED_UNTIL = 0.0
_SITE_HEALTH_BOOTSTRAP_ATTEMPTED = False


def _site_health_enabled() -> bool:
    if os.getenv("SITE_HEALTH_DB_ENABLED", "true").lower() in ("0", "false", "no"):
        return False
    return time.time() >= _SITE_HEALTH_DISABLED_UNTIL


def _is_missing_table_error(exc: Exception) -> bool:
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "code", None)
    if sqlstate == "42P01":
        return True
    message = str(exc).lower()
    if "does not exist" in message and "site_health" in message:
        return True
    if asyncpg is not None:
        undefined = getattr(asyncpg.exceptions, "UndefinedTableError", None)
        if undefined and isinstance(exc, undefined):
            return True
    return False


def _backoff_site_health(exc: Exception) -> None:
    global _SITE_HEALTH_DISABLED_UNTIL
    backoff_seconds = int(os.getenv("SITE_HEALTH_DB_BACKOFF_SECONDS", "300"))
    _SITE_HEALTH_DISABLED_UNTIL = time.time() + backoff_seconds
    logger.warning(
        "[SiteHealth] Disabled DB health checks for %ss due to error: %r",
        backoff_seconds,
        exc,
    )


class StoryProgressTracker:
    """Track story crawl progress in PostgreSQL."""

    @staticmethod
    async def upsert_story_progress(
        story_id: str,
        title: str,
        total_chapters: int = 0,
        crawled_chapters: int = 0,
        missing_chapters: int = 0,
        status: str = "queued",
        primary_site: str | None = None,
        genre_name: str | None = None,
        genre_url: str | None = None,
        genre_site_key: str | None = None,
        last_error: str | None = None,
        cooldown_until: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """
        Insert or update story progress.

        Args:
            story_id: Story URL or unique ID
            title: Story title
            total_chapters: Total chapters expected
            crawled_chapters: Chapters successfully crawled
            missing_chapters: Chapters still missing
            status: queued, running, completed, failed, skipped, cooldown
            primary_site: Primary source site
            genre_name: Genre name
            genre_url: Genre URL
            genre_site_key: Site key for genre
            last_error: Last error message
            cooldown_until: Cooldown expiry time
            metadata: Additional metadata

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            logger.debug(f"[StoryProgress] Pool not available, skipping track for {story_id}")
            return False

        try:
            async with pool.acquire() as conn:
                # Check if story exists
                existing = await conn.fetchrow(
                    "SELECT status, completed_at FROM story_progress WHERE story_id = $1",
                    story_id,
                )

                # Don't update completed stories unless status changes
                if existing and existing["status"] == "completed" and status != "completed":
                    logger.debug(f"[StoryProgress] Story {story_id} already completed, skipping")
                    return True

                await conn.execute(
                    """
                    INSERT INTO story_progress (
                        story_id,
                        title,
                        total_chapters,
                        crawled_chapters,
                        missing_chapters,
                        status,
                        primary_site,
                        genre_name,
                        genre_url,
                        genre_site_key,
                        last_error,
                        cooldown_until,
                        metadata,
                        started_at,
                        updated_at,
                        completed_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW(), $14)
                    ON CONFLICT (story_id)
                    DO UPDATE SET
                        title = COALESCE(EXCLUDED.title, story_progress.title),
                        total_chapters = GREATEST(EXCLUDED.total_chapters, story_progress.total_chapters),
                        crawled_chapters = GREATEST(EXCLUDED.crawled_chapters, story_progress.crawled_chapters),
                        missing_chapters = EXCLUDED.missing_chapters,
                        status = EXCLUDED.status,
                        primary_site = COALESCE(EXCLUDED.primary_site, story_progress.primary_site),
                        genre_name = COALESCE(EXCLUDED.genre_name, story_progress.genre_name),
                        genre_url = COALESCE(EXCLUDED.genre_url, story_progress.genre_url),
                        genre_site_key = COALESCE(EXCLUDED.genre_site_key, story_progress.genre_site_key),
                        last_error = EXCLUDED.last_error,
                        cooldown_until = EXCLUDED.cooldown_until,
                        metadata = COALESCE(EXCLUDED.metadata, story_progress.metadata),
                        updated_at = NOW(),
                        completed_at = CASE
                            WHEN EXCLUDED.status = 'completed' THEN NOW()
                            ELSE story_progress.completed_at
                        END
                    """,
                    story_id,
                    title,
                    total_chapters,
                    crawled_chapters,
                    missing_chapters,
                    status,
                    primary_site,
                    genre_name,
                    genre_url,
                    genre_site_key,
                    last_error,
                    cooldown_until,
                    json.dumps(metadata) if metadata else None,
                    datetime.now(timezone.utc) if status == "completed" else None,
                )

            logger.debug(
                f"[StoryProgress] Updated: {story_id[:50]}... "
                f"status={status}, crawled={crawled_chapters}/{total_chapters}"
            )
            return True

        except Exception as e:
            logger.error(f"[StoryProgress] Error tracking {story_id}: {e}")
            return False

    @staticmethod
    async def increment_processed_stories(story_id: str, increment: int = 1) -> bool:
        """
        Increment crawled chapters count.

        Args:
            story_id: Story identifier
            increment: Number of chapters to add (default: 1)

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE story_progress
                    SET crawled_chapters = crawled_chapters + $1,
                        missing_chapters = GREATEST(total_chapters - (crawled_chapters + $1), 0),
                        updated_at = NOW()
                    WHERE story_id = $2
                    """,
                    increment,
                    story_id,
                )

                if result == "UPDATE 0":
                    logger.debug(f"[StoryProgress] Story {story_id} not found for increment")
                    return False

            logger.debug(f"[StoryProgress] Incremented chapters for {story_id}")
            return True

        except Exception as e:
            logger.error(f"[StoryProgress] Error incrementing chapters: {e}")
            return False

    @staticmethod
    async def get_story_progress(story_id: str) -> dict[str, Any] | None:
        """Get story progress info."""
        pool = await get_db_pool()
        if pool is None:
            return None

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT story_id, title, total_chapters, crawled_chapters,
                           missing_chapters, status, primary_site, genre_name,
                           cooldown_until, last_error
                    FROM story_progress
                    WHERE story_id = $1
                    """,
                    story_id,
                )

                if row is None:
                    return None

                return dict(row)

        except Exception as e:
            logger.error(f"[StoryProgress] Error getting progress: {e}")
            return None


class SiteHealthTracker:
    """Track site health and availability."""

    @staticmethod
    async def record_success(site_key: str) -> bool:
        """Record a successful request."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO site_health (site_key, success_count, failure_count)
                    VALUES ($1, 1, 0)
                    ON CONFLICT (site_key)
                    DO UPDATE SET
                        success_count = site_health.success_count + 1,
                        updated_at = NOW()
                    """,
                    site_key,
                )

            logger.debug(f"[SiteHealth] Recorded success for {site_key}")
            return True

        except Exception as e:
            logger.error(f"[SiteHealth] Error recording success: {e}")
            return False

    @staticmethod
    async def record_failure(site_key: str, error_message: str | None = None) -> bool:
        """Record a failed request."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO site_health (site_key, success_count, failure_count, last_error)
                    VALUES ($1, 0, 1, $2)
                    ON CONFLICT (site_key)
                    DO UPDATE SET
                        failure_count = site_health.failure_count + 1,
                        last_error = COALESCE($2, site_health.last_error),
                        updated_at = NOW()
                    """,
                    site_key,
                    error_message,
                )

            logger.debug(f"[SiteHealth] Recorded failure for {site_key}")
            return True

        except Exception as e:
            logger.error(f"[SiteHealth] Error recording failure: {e}")
            return False

    @staticmethod
    async def record_challenge(site_key: str, challenge_type: str = "cloudflare") -> bool:
        """Record a challenge (CAPTCHA, Cloudflare, etc.)."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO site_health (
                        site_key,
                        success_count,
                        failure_count,
                        challenge_count_1h,
                        challenge_count_24h,
                        challenge_timestamps
                    )
                    VALUES ($1, 0, 0, 1, 1, ARRAY[NOW()])
                    ON CONFLICT (site_key)
                    DO UPDATE SET
                        challenge_count_1h = site_health.challenge_count_1h + 1,
                        challenge_count_24h = site_health.challenge_count_24h + 1,
                        challenge_timestamps = array_append(site_health.challenge_timestamps, NOW()),
                        updated_at = NOW()
                    """,
                    site_key,
                )

            logger.warning(f"[SiteHealth] Recorded {challenge_type} challenge for {site_key}")
            return True

        except Exception as e:
            logger.error(f"[SiteHealth] Error recording challenge: {e}")
            return False

    @staticmethod
    async def cleanup_old_challenges(site_key: str) -> bool:
        """Clean up challenge timestamps older than 24h."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE site_health
                    SET challenge_timestamps = (
                        SELECT array_agg(ts)
                        FROM unnest(challenge_timestamps) AS ts
                        WHERE ts > NOW() - INTERVAL '24 hours'
                    ),
                    challenge_count_1h = (
                        SELECT COUNT(*)
                        FROM unnest(challenge_timestamps) AS ts
                        WHERE ts > NOW() - INTERVAL '1 hour'
                    ),
                    challenge_count_24h = (
                        SELECT COUNT(*)
                        FROM unnest(challenge_timestamps) AS ts
                        WHERE ts > NOW() - INTERVAL '24 hours'
                    )
                    WHERE site_key = $1
                    """,
                    site_key,
                )

            return True

        except Exception as e:
            logger.error(f"[SiteHealth] Error cleaning challenges: {e}")
            return False

    @staticmethod
    async def get_site_health(site_key: str) -> dict[str, Any] | None:
        """Get site health metrics."""
        if not _site_health_enabled():
            return None
        pool = await get_db_pool()
        if pool is None:
            return None

        for attempt in range(2):
            try:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT site_key, success_count, failure_count, failure_rate,
                               challenge_count_1h, challenge_count_24h, last_error
                        FROM site_health
                        WHERE site_key = $1
                        """,
                        site_key,
                    )

                    if row is None:
                        return None

                    return dict(row)
            except Exception as e:
                global _SITE_HEALTH_BOOTSTRAP_ATTEMPTED
                if attempt == 0 and not _SITE_HEALTH_BOOTSTRAP_ATTEMPTED and _is_missing_table_error(e):
                    _SITE_HEALTH_BOOTSTRAP_ATTEMPTED = True
                    try:
                        await DatabasePool._bootstrap_schema(pool)
                        continue
                    except Exception as bootstrap_err:
                        _backoff_site_health(bootstrap_err)
                        return None
                _backoff_site_health(e)
                return None


class StoryEventLogger:
    """Log story events for debugging and analytics."""

    @staticmethod
    async def log_event(
        story_id: str,
        event_type: str,
        category_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """
        Log a story event.

        Args:
            story_id: Story identifier
            event_type: Event type (start, success, failure, retry, skip)
            category_id: Category/genre identifier
            metadata: Additional event metadata

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO story_events (category_id, story_id, event_type, metadata, created_at)
                    VALUES ($1, $2, $3, $4, NOW())
                    """,
                    category_id or "unknown",
                    story_id,
                    event_type,
                    json.dumps(metadata) if metadata else None,
                )

            logger.debug(f"[StoryEvent] Logged {event_type} for {story_id[:50]}...")
            return True

        except Exception as e:
            logger.error(f"[StoryEvent] Error logging event: {e}")
            return False

    @staticmethod
    async def get_recent_events(
        story_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get recent events."""
        pool = await get_db_pool()
        if pool is None:
            return []

        try:
            async with pool.acquire() as conn:
                if story_id:
                    rows = await conn.fetch(
                        """
                        SELECT category_id, story_id, event_type, metadata, created_at
                        FROM story_events
                        WHERE story_id = $1
                        ORDER BY created_at DESC
                        LIMIT $2
                        """,
                        story_id,
                        limit,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT category_id, story_id, event_type, metadata, created_at
                        FROM story_events
                        ORDER BY created_at DESC
                        LIMIT $1
                        """,
                        limit,
                    )

                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"[StoryEvent] Error getting events: {e}")
            return []


class GenreProgressTracker:
    """Track genre crawling progress in PostgreSQL."""

    @staticmethod
    async def upsert_genre_progress(
        site_key: str,
        genre_name: str,
        genre_url: str,
        position: int | None = None,
        total_genres: int | None = None,
        total_pages: int | None = None,
        crawled_pages: int = 0,
        current_page: int | None = None,
        total_stories: int = 0,
        processed_stories: int = 0,
        active_stories: list[str] | None = None,
        active_story_details: dict[str, Any] | None = None,
        current_story_title: str | None = None,
        current_story_page: int | None = None,
        current_story_position: int | None = None,
        status: str = "queued",
        last_error: str | None = None,
    ) -> bool:
        """
        Insert or update genre progress.

        Args:
            site_key: Site identifier
            genre_name: Genre/category name
            genre_url: Genre URL
            position: Position in queue
            total_genres: Total genres in crawl
            total_pages: Total pages in genre
            crawled_pages: Pages already crawled
            current_page: Current page being processed
            total_stories: Total stories in genre
            processed_stories: Stories already processed
            active_stories: List of active story titles
            active_story_details: Detailed info about active stories (JSONB)
            current_story_title: Current story being processed
            current_story_page: Page of current story
            current_story_position: Position of current story
            status: Genre status (queued, running, fetching_pages, processing_stories, completed, failed)
            last_error: Last error message

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            logger.debug(f"[GenreProgress] Pool not available, skipping track for {genre_url}")
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO genre_progress (
                        site_key, genre_name, genre_url,
                        position, total_genres,
                        total_pages, crawled_pages, current_page,
                        total_stories, processed_stories,
                        active_stories, active_story_details,
                        current_story_title, current_story_page, current_story_position,
                        status, last_error,
                        started_at, updated_at,
                        completed_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::varchar, $17,
                        NOW(), NOW(),
                        CASE WHEN $16::varchar = 'completed' THEN NOW() ELSE NULL END
                    )
                    ON CONFLICT (site_key, genre_url)
                    DO UPDATE SET
                        genre_name = COALESCE(EXCLUDED.genre_name, genre_progress.genre_name),
                        position = COALESCE(EXCLUDED.position, genre_progress.position),
                        total_genres = COALESCE(EXCLUDED.total_genres, genre_progress.total_genres),
                        total_pages = COALESCE(EXCLUDED.total_pages, genre_progress.total_pages),
                        crawled_pages = GREATEST(EXCLUDED.crawled_pages, genre_progress.crawled_pages),
                        current_page = COALESCE(EXCLUDED.current_page, genre_progress.current_page),
                        total_stories = GREATEST(EXCLUDED.total_stories, genre_progress.total_stories),
                        processed_stories = GREATEST(EXCLUDED.processed_stories, genre_progress.processed_stories),
                        active_stories = COALESCE(EXCLUDED.active_stories, genre_progress.active_stories),
                        active_story_details = COALESCE(EXCLUDED.active_story_details, genre_progress.active_story_details),
                        current_story_title = EXCLUDED.current_story_title,
                        current_story_page = EXCLUDED.current_story_page,
                        current_story_position = EXCLUDED.current_story_position,
                        status = EXCLUDED.status,
                        last_error = EXCLUDED.last_error,
                        updated_at = NOW(),
                        completed_at = CASE
                            WHEN EXCLUDED.status = 'completed' THEN NOW()
                            ELSE genre_progress.completed_at
                        END
                    """,
                    site_key, genre_name, genre_url,
                    position, total_genres,
                    total_pages, crawled_pages, current_page,
                    total_stories, processed_stories,
                    active_stories or [],
                    json.dumps(active_story_details) if active_story_details else None,
                    current_story_title, current_story_page, current_story_position,
                    status, last_error,
                )

            logger.debug(
                f"[GenreProgress] Updated: {genre_name} ({genre_url[:50]}...) "
                f"status={status}, processed={processed_stories}/{total_stories}"
            )
            return True

        except Exception as e:
            logger.error(f"[GenreProgress] Error tracking {genre_url}: {e}")
            return False

    @staticmethod
    async def increment_processed_stories(site_key: str, genre_url: str, increment: int = 1) -> bool:
        """Increment processed stories count."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE genre_progress
                    SET processed_stories = processed_stories + $1,
                        updated_at = NOW()
                    WHERE site_key = $2 AND genre_url = $3
                    """,
                    increment, site_key, genre_url,
                )

                if result == "UPDATE 0":
                    logger.debug(f"[GenreProgress] Genre {genre_url} not found for increment")
                    return False

            return True

        except Exception as e:
            logger.error(f"[GenreProgress] Error incrementing stories: {e}")
            return False

    @staticmethod
    async def get_genre_progress(site_key: str, genre_url: str) -> dict[str, Any] | None:
        """Get genre progress info."""
        pool = await get_db_pool()
        if pool is None:
            return None

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT site_key, genre_name, genre_url, position, total_genres,
                           total_pages, crawled_pages, current_page,
                           total_stories, processed_stories, status,
                           updated_at, completed_at
                    FROM genre_progress
                    WHERE site_key = $1 AND genre_url = $2
                    """,
                    site_key,
                    genre_url,
                )

                if row is None:
                    return None

                return dict(row)

        except Exception as e:
            logger.error(f"[GenreProgress] Error getting progress: {e}")
            return None


class GlobalStoryTotalsTracker:
    """Track global story completion statistics (singleton table)."""

    @staticmethod
    async def update_totals(
        completed_count: int | None = None,
        total_estimate: int | None = None,
    ) -> bool:
        """
        Update global story totals (singleton record with id=1).

        Args:
            completed_count: Total completed stories
            total_estimate: Estimated total stories

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                # Build SET clause dynamically
                set_parts = []
                params = []
                param_idx = 1

                if completed_count is not None:
                    set_parts.append(f"completed_count = ${param_idx}")
                    params.append(completed_count)
                    param_idx += 1

                if total_estimate is not None:
                    set_parts.append(f"total_estimate = ${param_idx}")
                    params.append(total_estimate)
                    param_idx += 1

                if not set_parts:
                    logger.debug("[GlobalTotals] No fields to update")
                    return True

                set_parts.append("updated_at = NOW()")
                query = f"""
                    INSERT INTO global_story_totals (id, completed_count, total_estimate)
                    VALUES (1, {completed_count or 0}, {f'${param_idx}' if total_estimate is not None else 'NULL'})
                    ON CONFLICT (id)
                    DO UPDATE SET {', '.join(set_parts)}
                """

                if total_estimate is not None and total_estimate not in params:
                    params.append(total_estimate)

                await conn.execute(query, *params)

            logger.debug(
                f"[GlobalTotals] Updated: completed={completed_count}, estimate={total_estimate}"
            )
            return True

        except Exception as e:
            logger.error(f"[GlobalTotals] Error updating totals: {e}")
            return False

    @staticmethod
    async def increment_completed(increment: int = 1) -> bool:
        """Increment completed count by specified amount."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO global_story_totals (id, completed_count)
                    VALUES (1, $1)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        completed_count = global_story_totals.completed_count + $1,
                        updated_at = NOW()
                    """,
                    increment,
                )

            logger.debug(f"[GlobalTotals] Incremented completed by {increment}")
            return True

        except Exception as e:
            logger.error(f"[GlobalTotals] Error incrementing: {e}")
            return False


class QueueStatsTracker:
    """Track queue statistics for skipped and retry queues."""

    @staticmethod
    async def update_skipped_queue(skipped_count: int) -> bool:
        """Update skipped queue statistics."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO queue_stats (queue_type, skipped_count)
                    VALUES ('skipped', $1)
                    ON CONFLICT (queue_type)
                    DO UPDATE SET
                        skipped_count = EXCLUDED.skipped_count,
                        updated_at = NOW()
                    """,
                    skipped_count,
                )

            logger.debug(f"[QueueStats] Updated skipped queue: {skipped_count}")
            return True

        except Exception as e:
            logger.error(f"[QueueStats] Error updating skipped queue: {e}")
            return False

    @staticmethod
    async def update_retry_queue(
        site_key: str,
        enqueued_count: int,
        file_count: int,
        pass_index: int,
        total_passes: int,
    ) -> bool:
        """Update retry queue statistics."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO queue_stats (
                        queue_type,
                        last_site_key,
                        last_enqueued_count,
                        last_file_count,
                        last_pass_index,
                        total_passes,
                        total_enqueued,
                        last_enqueued_at
                    )
                    VALUES ('retry', $1, $2, $3, $4, $5, $2, NOW())
                    ON CONFLICT (queue_type)
                    DO UPDATE SET
                        last_site_key = EXCLUDED.last_site_key,
                        last_enqueued_count = EXCLUDED.last_enqueued_count,
                        last_file_count = EXCLUDED.last_file_count,
                        last_pass_index = EXCLUDED.last_pass_index,
                        total_passes = EXCLUDED.total_passes,
                        total_enqueued = queue_stats.total_enqueued + EXCLUDED.last_enqueued_count,
                        last_enqueued_at = NOW(),
                        updated_at = NOW()
                    """,
                    site_key, enqueued_count, file_count, pass_index, total_passes,
                )

            logger.debug(
                f"[QueueStats] Updated retry queue: site={site_key}, "
                f"enqueued={enqueued_count}, pass={pass_index}/{total_passes}"
            )
            return True

        except Exception as e:
            logger.error(f"[QueueStats] Error updating retry queue: {e}")
            return False

    @staticmethod
    async def record_retry_alert(site_key: str, reason: str) -> bool:
        """Record a retry queue alert."""
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE queue_stats
                    SET last_alert_site_key = $1,
                        last_alert_reason = $2,
                        last_alert_at = NOW(),
                        updated_at = NOW()
                    WHERE queue_type = 'retry'
                    """,
                    site_key, reason,
                )

            logger.warning(f"[QueueStats] Retry alert: {site_key} - {reason}")
            return True

        except Exception as e:
            logger.error(f"[QueueStats] Error recording alert: {e}")
            return False


class SiteGenreOverviewTracker:
    """Track overview of all genres per site."""

    @staticmethod
    async def update_site_overview(
        site_key: str,
        total_genres: int,
        genres_map: dict[str, dict[str, Any]], # <--- THIS IS THE EXPECTED TYPE
    ) -> bool:
        """
        Update site genre overview.

        Args:
            site_key: Site identifier
            total_genres: Total number of genres
            genres_map: Map of genre_url -> {name, url, stories, status, updated_at, error, total_stories}

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                # Count completed genres
                completed_genres = sum(
                    1 for data in genres_map.values()
                    if data.get("status") == "completed"
                )

                await conn.execute(
                    """
                    INSERT INTO site_genre_overview (site_key, total_genres, completed_genres, genres)
                    VALUES ($1, $2, $3, $4::jsonb)
                    ON CONFLICT (site_key)
                    DO UPDATE SET
                        total_genres = GREATEST(EXCLUDED.total_genres, site_genre_overview.total_genres),
                        completed_genres = EXCLUDED.completed_genres,
                        genres = EXCLUDED.genres,
                        updated_at = NOW()
                    """,
                    site_key,
                    total_genres,
                    completed_genres,
                    json.dumps(genres_map), # <--- Ensure genres_map is correctly passed as JSON string
                )

            logger.debug(
                f"[SiteGenreOverview] Updated {site_key}: "
                f"{completed_genres}/{total_genres} genres completed"
            )
            return True

        except Exception as e:
            logger.error(f"[SiteGenreOverview] Error updating {site_key}: {e}")
            return False

    @staticmethod
    async def update_genre_in_site(
        site_key: str,
        genre_url: str,
        genre_data: dict[str, Any],
    ) -> bool:
        """
        Update a single genre's data within a site's overview.

        Args:
            site_key: Site identifier
            genre_url: Genre URL (key in genres JSONB)
            genre_data: Genre data {name, url, stories, status, updated_at, error, total_stories}

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                # IMPORTANT: Use jsonb_set with the path to update a specific key within the JSONB map
                # The path should be an array of keys, e.g., ARRAY['genre_url_key']
                # The 'create_if_missing' flag (true) ensures the key is added if it doesn't exist
                await conn.execute(
                    """
                    INSERT INTO site_genre_overview (site_key, total_genres, completed_genres, genres)
                    VALUES ($1::text, 1, 0, jsonb_build_object($3::text, $2::jsonb)) -- Initial insert if no conflict
                    ON CONFLICT (site_key)
                    DO UPDATE SET
                        genres = jsonb_set(
                            COALESCE(site_genre_overview.genres, '{}'::jsonb),
                            ARRAY[$3::text], -- Correctly cast to text array type
                            $2::jsonb, -- The new genre_data
                            TRUE -- Create key if missing
                        ),
                        -- Recalculate completed_genres based on the updated JSONB
                        completed_genres = (
                            SELECT COUNT(*)
                            FROM jsonb_each(
                                jsonb_set(
                                    COALESCE(site_genre_overview.genres, '{}'::jsonb),
                                    ARRAY[$3::text],
                                    $2::jsonb,
                                    TRUE
                                )
                            ) AS genre_entry(key, value)
                            WHERE (value->>'status')::text = 'completed'
                        ),
                        updated_at = NOW()
                    """,
                    site_key,
                    json.dumps(genre_data),
                    genre_url,
                )

            logger.debug(f"[SiteGenreOverview] Updated genre {genre_url} in {site_key}")
            return True

        except Exception as e:
            logger.error(f"[SiteGenreOverview] Error updating genre: {e}")
            return False


class SystemMetricsTracker:
    """Track flexible system-wide metrics."""

    @staticmethod
    async def update_metric(metric_key: str, metric_value: dict[str, Any]) -> bool:
        """
        Update a system metric.

        Args:
            metric_key: Metric identifier (unique)
            metric_value: Metric data (stored as JSONB)

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO system_metrics (metric_key, metric_value)
                    VALUES ($1, $2)
                    ON CONFLICT (metric_key)
                    DO UPDATE SET
                        metric_value = EXCLUDED.metric_value,
                        updated_at = NOW()
                    """,
                    metric_key,
                    json.dumps(metric_value),
                )

            logger.debug(f"[SystemMetrics] Updated metric: {metric_key}")
            return True

        except Exception as e:
            logger.error(f"[SystemMetrics] Error updating metric {metric_key}: {e}")
            return False

    @staticmethod
    async def update_multiple_metrics(metrics: dict[str, dict[str, Any]]) -> bool:
        """
        Update multiple system metrics at once.

        Args:
            metrics: Map of metric_key -> metric_value

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                # Use batch insert with ON CONFLICT
                for metric_key, metric_value in metrics.items():
                    await conn.execute(
                        """
                        INSERT INTO system_metrics (metric_key, metric_value)
                        VALUES ($1, $2)
                        ON CONFLICT (metric_key)
                        DO UPDATE SET
                            metric_value = EXCLUDED.metric_value,
                            updated_at = NOW()
                        """,
                        metric_key,
                        json.dumps(metric_value),
                    )

            logger.debug(f"[SystemMetrics] Updated {len(metrics)} metrics")
            return True

        except Exception as e:
            logger.error(f"[SystemMetrics] Error updating multiple metrics: {e}")
            return False


# Convenience instances
story_progress = StoryProgressTracker()
site_health = SiteHealthTracker()
story_events = StoryEventLogger()
genre_progress = GenreProgressTracker()
global_story_totals = GlobalStoryTotalsTracker()
queue_stats = QueueStatsTracker()
site_genre_overview = SiteGenreOverviewTracker()
system_metrics = SystemMetricsTracker()

"""
Persistent story queue for crawl planning.

This module provides a PostgreSQL-backed queue for stories discovered during
the planning phase. Stories are enqueued during planning and dequeued during
processing, eliminating RAM bloat and enabling resume on restart.
"""

from __future__ import annotations

from datetime import timedelta
import json
from typing import Any

from flowcore_story.storage.db_pool import get_db_pool
from flowcore_story.utils.logger import logger


class StoryQueue:
    """Persistent queue operations for story crawling."""

    @staticmethod
    async def enqueue_story(
        site_key: str,
        genre_name: str,
        genre_url: str,
        story: dict[str, Any],
        *,
        priority: int = 0,
        discovery_index: int = 0,
        source_page: int | None = None,
    ) -> int | None:
        """
        Add a story to the queue.

        Args:
            site_key: Site identifier (e.g., 'xtruyen')
            genre_name: Genre/category name
            genre_url: Genre URL
            story: Full story dict from adapter
            priority: Priority value (lower = higher priority)
            discovery_index: Original discovery order
            source_page: Page number where story was discovered

        Returns:
            Queue entry ID if successful, None otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            logger.warning("[StoryQueue] Pool not available, cannot enqueue")
            return None

        story_url = story.get("url")
        if not story_url:
            logger.warning("[StoryQueue] Story missing URL, skipping: %s", story)
            return None

        story_title = story.get("title", "")

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT enqueue_story($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    site_key,
                    genre_name,
                    genre_url,
                    story_url,
                    story_title,
                    json.dumps(story),  # JSONB
                    priority,
                    discovery_index,
                    source_page,
                )
                queue_id = row[0] if row else None
                logger.debug(
                    f"[StoryQueue] Enqueued: {story_title[:50]}... (id={queue_id}, priority={priority})"
                )
                return queue_id

        except Exception as e:
            logger.error(f"[StoryQueue] Failed to enqueue story {story_url}: {e}")
            return None

    @staticmethod
    async def enqueue_batch(
        site_key: str,
        genre_name: str,
        genre_url: str,
        stories: list[dict[str, Any]],
        *,
        base_priority: int = 0,
        start_index: int = 0,
    ) -> int:
        """
        Batch enqueue multiple stories (more efficient than individual inserts).

        Args:
            site_key: Site identifier
            genre_name: Genre name
            genre_url: Genre URL
            stories: List of story dicts
            base_priority: Base priority offset
            start_index: Starting discovery index

        Returns:
            Number of stories successfully enqueued
        """
        pool = await get_db_pool()
        if pool is None:
            logger.warning("[StoryQueue] Pool not available for batch enqueue")
            return 0

        if not stories:
            logger.debug("[StoryQueue] No stories to enqueue")
            return 0

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    enqueued = 0
                    for idx, story in enumerate(stories):
                        story_url = story.get("url")
                        if not story_url:
                            logger.debug(f"[StoryQueue] Skipping story without URL at index {idx}")
                            continue

                        story_title = story.get("title", "")
                        source_page = story.get("_source_page")
                        priority = base_priority + idx
                        discovery_index = start_index + idx

                        await conn.execute(
                            """
                            INSERT INTO story_queue (
                                site_key, genre_name, genre_url, story_url,
                                story_title, story_data, priority, discovery_index,
                                source_page, status
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending')
                            ON CONFLICT (site_key, genre_url, story_url)
                            DO UPDATE SET
                                story_title = EXCLUDED.story_title,
                                story_data = EXCLUDED.story_data,
                                priority = EXCLUDED.priority,
                                discovery_index = EXCLUDED.discovery_index,
                                source_page = EXCLUDED.source_page,
                                status = CASE
                                    WHEN story_queue.status IN ('failed', 'skipped') THEN 'pending'
                                    ELSE story_queue.status
                                END,
                                retry_count = CASE
                                    WHEN story_queue.status IN ('failed', 'skipped') THEN 0
                                    ELSE story_queue.retry_count
                                END,
                                updated_at = NOW()
                            """,
                            site_key,
                            genre_name,
                            genre_url,
                            story_url,
                            story_title,
                            json.dumps(story),
                            priority,
                            discovery_index,
                            source_page,
                        )
                        enqueued += 1

                    logger.info(f"[StoryQueue] Batch enqueued {enqueued}/{len(stories)} stories for {genre_name}")
                    return enqueued

        except Exception as e:
            logger.error(f"[StoryQueue] Batch enqueue failed for {genre_url}: {e}")
            return 0

    @staticmethod
    async def dequeue_next(
        site_key: str,
        genre_url: str,
        worker_id: str = "main",
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        """
        Atomically claim and return next pending stories.

        Uses FOR UPDATE SKIP LOCKED for safe concurrent access.

        Args:
            site_key: Site identifier
            genre_url: Genre URL
            worker_id: Worker claiming these stories
            limit: Maximum number of stories to dequeue

        Returns:
            List of story dicts with queue metadata
        """
        pool = await get_db_pool()
        if pool is None:
            logger.warning("[StoryQueue] Pool not available for dequeue")
            return []

        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM dequeue_next_story($1, $2, $3, $4)
                    """,
                    site_key,
                    genre_url,
                    worker_id,
                    limit,
                )

                stories = []
                for row in rows:
                    # Parse JSONB story_data
                    story_data = json.loads(row["story_data"]) if isinstance(row["story_data"], str) else dict(row["story_data"])

                    # Add queue metadata
                    story_data["_queue_id"] = row["id"]
                    story_data["_queue_priority"] = row["priority"]
                    story_data["_queue_discovery_index"] = row["discovery_index"]
                    story_data["_queue_retry_count"] = row["retry_count"]

                    # Ensure URL and title are set
                    if not story_data.get("url"):
                        story_data["url"] = row["story_url"]
                    if not story_data.get("title"):
                        story_data["title"] = row["story_title"]

                    stories.append(story_data)

                if stories:
                    logger.debug(
                        f"[StoryQueue] Dequeued {len(stories)} stories for {genre_url[:50]}..."
                    )
                return stories

        except Exception as e:
            logger.error(f"[StoryQueue] Failed to dequeue from {genre_url}: {e}")
            return []

    @staticmethod
    async def complete_story(
        queue_id: int,
        status: str = "completed",
        error: str | None = None,
    ) -> bool:
        """
        Mark a queued story as completed/failed/skipped.

        Args:
            queue_id: Queue entry ID (from _queue_id in dequeued story)
            status: Final status (completed, failed, skipped)
            error: Error message if failed

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                if error:
                    await conn.execute(
                        """
                        UPDATE story_queue
                        SET status = $1, last_error = $2, completed_at = NOW(), updated_at = NOW()
                        WHERE id = $3
                        """,
                        status,
                        error,
                        queue_id,
                    )
                else:
                    await conn.execute(
                        """
                        SELECT complete_story($1::bigint, $2::varchar)
                        """,
                        queue_id,
                        status,
                    )

                logger.debug(f"[StoryQueue] Marked {queue_id} as {status}")
                return True

        except Exception as e:
            logger.error(f"[StoryQueue] Failed to complete {queue_id}: {e}")
            return False

    @staticmethod
    async def get_queue_stats(
        site_key: str,
        genre_url: str,
    ) -> dict[str, Any]:
        """
        Get queue statistics for a genre.

        Returns:
            Dict with total, pending, processing, completed, failed, skipped counts
        """
        pool = await get_db_pool()
        if pool is None:
            return {}

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM get_genre_queue_stats($1, $2)
                    """,
                    site_key,
                    genre_url,
                )

                if row:
                    return {
                        "total_stories": row["total_stories"],
                        "pending": row["pending_count"],
                        "processing": row["processing_count"],
                        "completed": row["completed_count"],
                        "failed": row["failed_count"],
                        "skipped": row["skipped_count"],
                    }
                return {}

        except Exception as e:
            logger.error(f"[StoryQueue] Failed to get stats for {genre_url}: {e}")
            return {}

    @staticmethod
    async def has_pending_stories(
        site_key: str,
        genre_url: str,
    ) -> bool:
        """
        Check if genre has pending stories in queue.

        Returns:
            True if pending stories exist, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM story_queue
                        WHERE site_key = $1 AND genre_url = $2 AND status = 'pending'
                    )
                    """,
                    site_key,
                    genre_url,
                )
                return row[0] if row else False

        except Exception as e:
            logger.error(f"[StoryQueue] Failed to check pending for {genre_url}: {e}")
            return False

    @staticmethod
    async def recover_stale_claims(
        stale_minutes: int = 30,
    ) -> int:
        """
        Recover stories claimed by dead workers.

        Args:
            stale_minutes: Consider claims older than this as stale

        Returns:
            Number of stories recovered
        """
        pool = await get_db_pool()
        if pool is None:
            return 0

        row = None
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT recover_stale_claims($1::integer)
                    """,
                    stale_minutes,
                )
        except Exception as primary_error:
            # Fallback for deployments where the DB function expects an INTERVAL
            try:
                async with pool.acquire() as conn:
                    stale_interval = timedelta(minutes=stale_minutes)
                    row = await conn.fetchrow(
                        """
                        SELECT recover_stale_claims($1::interval)
                        """,
                        stale_interval,
                    )
            except Exception as fallback_error:
                logger.error(
                    "[StoryQueue] Failed to recover stale claims: %s (primary), %s (fallback)",
                    primary_error,
                    fallback_error,
                )
                return 0

        recovered = row[0] if row else 0
        if recovered > 0:
            logger.info(f"[StoryQueue] Recovered {recovered} stale claims")
        return recovered

    @staticmethod
    async def cleanup_completed(
        retention_days: int = 7,
    ) -> int:
        """
        Remove old completed queue entries.

        Args:
            retention_days: Keep completed entries this many days

        Returns:
            Number of entries deleted
        """
        pool = await get_db_pool()
        if pool is None:
            return 0

        row = None
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT cleanup_completed_queue($1::integer)
                    """,
                    retention_days,
                )
        except Exception as primary_error:
            # Fallback for deployments where the DB function expects an INTERVAL
            try:
                async with pool.acquire() as conn:
                    retention_interval = timedelta(days=retention_days)
                    row = await conn.fetchrow(
                        """
                        SELECT cleanup_completed_queue($1::interval)
                        """,
                        retention_interval,
                    )
            except Exception as fallback_error:
                logger.error(
                    "[StoryQueue] Failed to cleanup completed: %s (primary), %s (fallback)",
                    primary_error,
                    fallback_error,
                )
                return 0

        deleted = row[0] if row else 0
        if deleted > 0:
            logger.info(f"[StoryQueue] Cleaned up {deleted} old queue entries")
        return deleted


class GenreQueueMetadata:
    """Manage genre-level queue metadata."""

    @staticmethod
    async def start_planning(
        site_key: str,
        genre_name: str,
        genre_url: str,
    ) -> bool:
        """
        Mark genre planning as started.

        Args:
            site_key: Site identifier
            genre_name: Genre name
            genre_url: Genre URL

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO genre_queue_metadata (
                        site_key, genre_name, genre_url, planning_status
                    ) VALUES ($1, $2, $3, 'planning')
                    ON CONFLICT (site_key, genre_url)
                    DO UPDATE SET
                        planning_status = 'planning',
                        planning_started_at = NOW(),
                        updated_at = NOW()
                    """,
                    site_key,
                    genre_name,
                    genre_url,
                )
                logger.debug(f"[GenreQueueMeta] Planning started for {genre_name}")
                return True

        except Exception as e:
            logger.error(f"[GenreQueueMeta] Failed to start planning: {e}")
            return False

    @staticmethod
    async def complete_planning(
        site_key: str,
        genre_url: str,
        *,
        total_stories: int,
        total_pages: int | None = None,
        crawled_pages: int | None = None,
        planned_story_total: int | None = None,
    ) -> bool:
        """
        Mark genre planning as completed.

        Args:
            site_key: Site identifier
            genre_url: Genre URL
            total_stories: Total stories enqueued
            total_pages: Total pages available
            crawled_pages: Pages crawled during planning
            planned_story_total: Expected total from planner

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE genre_queue_metadata
                    SET
                        planning_status = 'ready',
                        planning_completed_at = NOW(),
                        total_stories = $1,
                        total_pages = $2,
                        crawled_pages = $3,
                        planned_story_total = $4,
                        pending_count = $1,
                        updated_at = NOW()
                    WHERE site_key = $5 AND genre_url = $6
                    """,
                    total_stories,
                    total_pages,
                    crawled_pages,
                    planned_story_total,
                    site_key,
                    genre_url,
                )
                logger.info(
                    f"[GenreQueueMeta] Planning completed: {total_stories} stories queued for {genre_url[:50]}..."
                )
                return True

        except Exception as e:
            logger.error(f"[GenreQueueMeta] Failed to complete planning: {e}")
            return False

    @staticmethod
    async def reset_to_pending(
        site_key: str,
        genre_url: str,
    ) -> bool:
        """
        Reset planning_status to 'pending' to allow full discovery.

        This is used after discover_genre_total_only() which only fetches
        page 1 but inadvertently sets status to 'ready'.

        Args:
            site_key: Site identifier
            genre_url: Genre URL

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE genre_queue_metadata
                    SET
                        planning_status = 'pending',
                        updated_at = NOW()
                    WHERE site_key = $1 AND genre_url = $2
                    """,
                    site_key,
                    genre_url,
                )
                return True

        except Exception as e:
            logger.error(f"[GenreQueueMeta] Failed to reset to pending: {e}")
            return False

    @staticmethod
    async def get_metadata(
        site_key: str,
        genre_url: str,
    ) -> dict[str, Any] | None:
        """
        Get genre queue metadata.

        Args:
            site_key: Site identifier
            genre_url: Genre URL

        Returns:
            Dict with metadata if found, None otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return None

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM genre_queue_metadata
                    WHERE site_key = $1 AND genre_url = $2
                    """,
                    site_key,
                    genre_url,
                )

                if row:
                    return dict(row)
                return None

        except Exception as e:
            logger.error(f"[GenreQueueMeta] Failed to get metadata: {e}")
            return None

    @staticmethod
    async def update_counters(
        site_key: str,
        genre_url: str,
    ) -> bool:
        """
        Refresh cached counters from actual queue.

        Useful for reconciliation if counters get out of sync.

        Args:
            site_key: Site identifier
            genre_url: Genre URL

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT refresh_genre_queue_counters($1, $2)
                    """,
                    site_key,
                    genre_url,
                )
                success = row[0] if row else False
                if success:
                    logger.debug(f"[GenreQueueMeta] Updated counters for {genre_url[:50]}...")
                return success

        except Exception as e:
            logger.error(f"[GenreQueueMeta] Failed to update counters: {e}")
            return False


# Convenience instances for import
story_queue = StoryQueue()
genre_queue_metadata = GenreQueueMetadata()

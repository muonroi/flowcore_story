"""
Sequential Genre Completion Strategy

Process genres one at a time to completion before moving to next.
This optimizes resource usage and avoids redundant discovery.

Strategy:
1. Check genre completion status from database
2. If complete → Skip to next genre
3. If has queue → Process from queue
4. If needs discovery → Quick check total, then full discovery

Author: StoryFlow Team
Date: 2025-12-20
"""

from typing import Any
from flowcore_story.storage.db_pool import get_db_pool
from flowcore.utils.logger import logger


async def get_genre_completion_status(
    site_key: str,
    genre_url: str
) -> dict[str, Any]:
    """
    Get completion status for a genre.

    Returns:
        dict with keys:
        - total_stories: Total stories in genre (None if unknown)
        - completed: Number of completed stories
        - pending: Number of pending stories in queue
        - processing: Number of stories being processed
        - is_complete: True if genre is complete
        - has_queue: True if queue exists for this genre
        - needs_discovery: True if discovery needed
    """
    pool = await get_db_pool()
    if not pool:
        logger.error("[SEQUENTIAL] No database pool available")
        return {
            "total_stories": None,
            "completed": 0,
            "pending": 0,
            "processing": 0,
            "is_complete": False,
            "has_queue": False,
            "needs_discovery": True,
        }

    async with pool.acquire() as conn:
        # Get queue stats
        queue_stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'processing') as processing,
                COUNT(*) as total_in_queue
            FROM story_queue
            WHERE site_key = $1 AND genre_url = $2
            """,
            site_key,
            genre_url,
        )

        # Get genre metadata (total stories from last discovery)
        genre_meta = await conn.fetchrow(
            """
            SELECT total_stories, planning_status
            FROM genre_queue_metadata
            WHERE site_key = $1 AND genre_url = $2
            """,
            site_key,
            genre_url,
        )

        completed = queue_stats['completed'] if queue_stats else 0
        pending = queue_stats['pending'] if queue_stats else 0
        processing = queue_stats['processing'] if queue_stats else 0
        total_in_queue = queue_stats['total_in_queue'] if queue_stats else 0

        # Get total from metadata
        total_stories = None
        if genre_meta and genre_meta['total_stories'] and genre_meta['total_stories'] > 0:
            total_stories = genre_meta['total_stories']

        # Check if discovery was interrupted (planning_status = 'planning')
        discovery_interrupted = (
            genre_meta and genre_meta.get('planning_status') == 'planning'
        )

        has_queue = total_in_queue > 0

        # Determine if complete
        is_complete = False
        if total_stories is not None and total_stories > 0:
            # Known total - check if completed >= total
            is_complete = completed >= total_stories

            # Also check if no more pending/processing work
            if not is_complete and pending == 0 and processing == 0:
                # May have missed some stories, but queue is empty
                logger.warning(
                    f"[SEQUENTIAL] Genre has {completed}/{total_stories} completed "
                    f"but queue is empty. Marking as incomplete for re-discovery."
                )
        else:
            # Unknown total - conservative: only complete if queue empty and has some completed
            is_complete = (pending == 0 and processing == 0 and completed > 0)

        # Need discovery if:
        # 1. No queue exists, OR
        # 2. Queue exists but total unknown (need to get total), OR
        # 3. Queue exists but incomplete (total_in_queue < total_stories)
        #    This handles case where discovery was interrupted mid-way
        # 4. Total is clearly wrong (total < queue_count) - probably from first-page-only discovery
        # 5. Discovery was interrupted (planning_status = 'planning')
        needs_discovery = (
            (not has_queue) or
            (has_queue and total_stories is None) or
            (has_queue and total_stories and total_in_queue < total_stories * 0.95) or  # Allow 5% tolerance
            (has_queue and total_stories and total_stories < total_in_queue) or  # Wrong total - less than queue
            discovery_interrupted  # Discovery was interrupted, continue it
        )

        return {
            "total_stories": total_stories,
            "completed": completed,
            "pending": pending,
            "processing": processing,
            "is_complete": is_complete,
            "has_queue": has_queue,
            "needs_discovery": needs_discovery,
            "discovery_interrupted": discovery_interrupted,
        }


async def save_genre_total(
    site_key: str,
    genre_name: str,
    genre_url: str,
    total_stories: int,
) -> None:
    """Save estimated total stories for a genre to metadata."""
    pool = await get_db_pool()
    if not pool:
        return

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO genre_queue_metadata
                (site_key, genre_name, genre_url, total_stories, planning_status)
            VALUES ($1, $2, $3, $4, 'planning')
            ON CONFLICT (site_key, genre_url)
            DO UPDATE SET
                total_stories = $4,
                updated_at = NOW()
            """,
            site_key,
            genre_name,
            genre_url,
            total_stories,
        )

    logger.info(
        f"[SEQUENTIAL] Saved total stories for {genre_name}: {total_stories}"
    )


async def discover_genre_total_only(
    adapter,
    genre_data: dict[str, Any],
    site_key: str,
) -> int | None:
    """
    Quick discovery to get total stories count only (first page).

    Returns:
        Estimated total story count, or None if discovery failed

    NOTE: This function only fetches page 1 to get pagination info.
    It resets planning_status to 'pending' afterwards to allow full discovery later.
    """
    from flowcore_story.core.crawl_planner import build_category_plan
    from flowcore_story.storage.story_queue import genre_queue_metadata

    genre_url = genre_data.get('url') or genre_data.get('link') or genre_data.get('href')

    try:
        # Discover first page only to get total
        # Force discovery even if queue exists (we just need pagination info)
        plan = await build_category_plan(
            adapter,
            genre_data,
            site_key,
            max_pages=1,  # Only first page for quick check
            force_discovery=True,  # Bypass queue skip logic
        )

        if not plan:
            logger.warning(
                f"[SEQUENTIAL] Failed to discover total for {genre_data.get('name')}"
            )
            return None

        # IMPORTANT: Reset planning_status to 'pending' so full discovery can run
        # This is needed because build_category_plan() sets status to 'ready'
        if genre_url:
            await genre_queue_metadata.reset_to_pending(site_key, genre_url)
            logger.debug(
                f"[SEQUENTIAL] Reset planning_status to pending for {genre_data.get('name')}"
            )

        # Use planned_story_total if available (from adapter)
        if plan.planned_story_total:
            logger.info(
                f"[SEQUENTIAL] Got total from adapter: {plan.planned_story_total}"
            )
            return plan.planned_story_total

        # Use total_pages if available
        if plan.total_pages:
            stories_first_page = len(plan.stories)
            estimated_total = stories_first_page * plan.total_pages
            logger.info(
                f"[SEQUENTIAL] Estimated from pagination: "
                f"{stories_first_page} stories/page × {plan.total_pages} pages = {estimated_total}"
            )
            return estimated_total

        # Fallback: just return first page count (minimum)
        first_page_count = len(plan.stories)
        logger.info(
            f"[SEQUENTIAL] No total info available, using first page count: {first_page_count}"
        )
        return first_page_count

    except Exception as e:
        logger.error(
            f"[SEQUENTIAL] Error discovering total for {genre_data.get('name')}: {e}"
        )
        return None


def format_genre_status_log(
    genre_name: str,
    status: dict[str, Any],
) -> str:
    """Format a nice log line for genre status."""
    total = status['total_stories']
    total_str = str(total) if total is not None else "?"

    completed = status['completed']
    pending = status['pending']
    processing = status['processing']

    progress_pct = "?"
    if total and total > 0:
        progress_pct = f"{(completed / total) * 100:.1f}%"

    return (
        f"{genre_name}: "
        f"{completed}/{total_str} completed ({progress_pct}), "
        f"{pending} pending, {processing} processing"
    )

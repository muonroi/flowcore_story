"""
Helper functions to easily integrate tracking into existing code.
These are async-safe wrappers that won't break the flow if DB is unavailable.
"""

from typing import Any

from flowcore_story.storage.db_tracking import (
    genre_progress,
    global_story_totals,
    queue_stats,
    site_genre_overview,
    site_health,
    story_events,
    story_progress,
    system_metrics,
)
from flowcore_story.utils.logger import logger
from flowcore_story.utils.metrics_tracker import metrics_tracker


async def track_story_discovered(
    story_url: str,
    title: str,
    total_chapters: int,
    site_key: str,
    genre_name: str | None = None,
    genre_url: str | None = None,
) -> None:
    """
    Track when a story is first discovered.
    Safe to call - won't break flow if DB unavailable.
    """
    try:
        await story_progress.upsert_story_progress(
            story_id=story_url,
            title=title,
            total_chapters=total_chapters,
            crawled_chapters=0,
            missing_chapters=total_chapters,
            status="queued",
            primary_site=site_key,
            genre_name=genre_name,
            genre_url=genre_url,
            genre_site_key=site_key,
        )

        await story_events.log_event(
            story_id=story_url,
            event_type="discovered",
            category_id=genre_name or "unknown",
            metadata={"site_key": site_key, "total_chapters": total_chapters},
        )

        logger.debug(f"[Tracking] Story discovered: {title}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track story discovery: {e}")


async def track_story_start_crawl(story_url: str, title: str) -> None:
    """Track when story crawling starts."""
    try:
        await story_progress.upsert_story_progress(
            story_id=story_url,
            title=title,
            status="running",
        )

        await story_events.log_event(
            story_id=story_url,
            event_type="start",
        )

        logger.debug(f"[Tracking] Story crawl started: {title}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track story start: {e}")


async def track_chapter_crawled(story_url: str, chapter_number: int | None = None) -> None:
    """Track when a chapter is successfully crawled."""
    try:
        await story_progress.increment_crawled_chapters(story_url, increment=1)

        logger.debug(f"[Tracking] Chapter crawled for {story_url}: ch#{chapter_number}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track chapter: {e}")


async def track_story_completed(story_url: str, title: str, total_chapters: int) -> None:
    """Track when a story is fully crawled."""
    try:
        await story_progress.upsert_story_progress(
            story_id=story_url,
            title=title,
            total_chapters=total_chapters,
            crawled_chapters=total_chapters,
            missing_chapters=0,
            status="completed",
        )

        await story_events.log_event(
            story_id=story_url,
            event_type="success",
            metadata={"total_chapters": total_chapters},
        )

        logger.info(f"[Tracking] Story completed: {title} ({total_chapters} chapters)")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track story completion: {e}")


async def track_story_failed(
    story_url: str,
    title: str,
    error: str,
    crawled_chapters: int = 0,
    total_chapters: int = 0,
) -> None:
    """Track when a story fails to crawl."""
    try:
        await story_progress.upsert_story_progress(
            story_id=story_url,
            title=title,
            total_chapters=total_chapters,
            crawled_chapters=crawled_chapters,
            missing_chapters=total_chapters - crawled_chapters if total_chapters > 0 else 0,
            status="failed",
            last_error=error[:500],  # Limit error length
        )

        await story_events.log_event(
            story_id=story_url,
            event_type="failure",
            metadata={
                "error": error[:200],
                "crawled_chapters": crawled_chapters,
                "total_chapters": total_chapters,
            },
        )

        logger.warning(f"[Tracking] Story failed: {title} - {error[:100]}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track story failure: {e}")


async def track_story_skipped(story_url: str, title: str, reason: str) -> None:
    """Track when a story is skipped."""
    try:
        await story_progress.upsert_story_progress(
            story_id=story_url,
            title=title,
            status="skipped",
            last_error=reason,
        )

        await story_events.log_event(
            story_id=story_url,
            event_type="skip",
            metadata={"reason": reason},
        )

        logger.debug(f"[Tracking] Story skipped: {title} - {reason}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track story skip: {e}")


async def track_request_success(site_key: str) -> None:
    """Track a successful request to a site."""
    try:
        await site_health.record_success(site_key)

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track success: {e}")


async def track_request_failure(site_key: str, error: str) -> None:
    """Track a failed request to a site."""
    try:
        await site_health.record_failure(site_key, error[:200])

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track failure: {e}")


async def track_challenge_detected(site_key: str, challenge_type: str = "cloudflare") -> None:
    """Track when a challenge (CAPTCHA, Cloudflare) is detected."""
    try:
        await site_health.record_challenge(site_key, challenge_type)

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track challenge: {e}")


async def get_story_crawl_status(story_url: str) -> dict[str, Any] | None:
    """Get current crawl status of a story."""
    try:
        return await story_progress.get_story_progress(story_url)

    except Exception as e:
        logger.debug(f"[Tracking] Failed to get story status: {e}")
        return None


async def get_site_status(site_key: str) -> dict[str, Any] | None:
    """Get current health status of a site."""
    try:
        return await site_health.get_site_health(site_key)

    except Exception as e:
        logger.debug(f"[Tracking] Failed to get site status: {e}")
        return None


# ==============================================================================
# GENRE PROGRESS TRACKING
# ==============================================================================

async def track_genre_started(
    site_key: str,
    genre_name: str,
    genre_url: str,
    position: int | None = None,
    total_genres: int | None = None,
) -> None:
    """Track when genre crawling starts."""
    try:
        await genre_progress.upsert_genre_progress(
            site_key=site_key,
            genre_name=genre_name,
            genre_url=genre_url,
            position=position,
            total_genres=total_genres,
            status="running",
        )

        logger.debug(f"[Tracking] Genre started: {genre_name}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track genre start: {e}")


async def track_genre_pages_discovered(
    site_key: str,
    genre_url: str,
    total_pages: int,
    total_stories: int,
) -> None:
    """Track when genre pages are discovered."""
    try:
        await genre_progress.upsert_genre_progress(
            site_key=site_key,
            genre_name="",  # Will be preserved from initial insert
            genre_url=genre_url,
            total_pages=total_pages,
            total_stories=total_stories,
            status="processing_stories",
        )

        logger.debug(f"[Tracking] Genre pages: {total_pages} pages, {total_stories} stories")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track genre pages: {e}")


async def track_genre_story_processed(
    site_key: str,
    genre_url: str,
    story_title: str,
) -> None:
    """Track when a story in genre is processed."""
    try:
        await genre_progress.increment_processed_stories(site_key, genre_url, increment=1)

        logger.debug(f"[Tracking] Genre story processed: {story_title}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track genre story: {e}")


async def track_genre_completed(
    site_key: str,
    genre_name: str,
    genre_url: str,
    total_stories: int,
    processed_stories: int,
) -> None:
    """Track when genre crawling completes."""
    try:
        await genre_progress.upsert_genre_progress(
            site_key=site_key,
            genre_name=genre_name,
            genre_url=genre_url,
            total_stories=total_stories,
            processed_stories=processed_stories,
            status="completed",
        )

        # Also update site genre overview
        await site_genre_overview.update_genre_in_site(
            site_key=site_key,
            genre_url=genre_url,
            genre_data={
                "name": genre_name,
                "url": genre_url,
                "stories": processed_stories,
                "status": "completed",
                "updated_at": None,  # Will use NOW() in DB
                "total_stories": total_stories,
            },
        )

        logger.info(f"[Tracking] Genre completed: {genre_name} ({processed_stories}/{total_stories} stories)")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track genre completion: {e}")


async def track_genre_failed(
    site_key: str,
    genre_name: str,
    genre_url: str,
    error: str,
) -> None:
    """Track when genre crawling fails."""
    try:
        await genre_progress.upsert_genre_progress(
            site_key=site_key,
            genre_name=genre_name,
            genre_url=genre_url,
            status="failed",
            last_error=error[:500],
        )

        # Also update site genre overview
        await site_genre_overview.update_genre_in_site(
            site_key=site_key,
            genre_url=genre_url,
            genre_data={
                "name": genre_name,
                "url": genre_url,
                "stories": 0,
                "status": "failed",
                "updated_at": None,
                "error": error[:200],
            },
        )

        logger.warning(f"[Tracking] Genre failed: {genre_name} - {error[:100]}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track genre failure: {e}")


# ==============================================================================
# GLOBAL STORY TOTALS TRACKING
# ==============================================================================

async def track_global_story_completed(increment: int = 1) -> None:
    """Increment global completed story count."""
    try:
        await global_story_totals.increment_completed(increment)

        logger.debug(f"[Tracking] Global stories completed +{increment}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track global completion: {e}")


async def update_global_story_estimate(total_estimate: int) -> None:
    """Update estimated total stories."""
    try:
        await global_story_totals.update_totals(total_estimate=total_estimate)

        logger.debug(f"[Tracking] Global estimate updated: {total_estimate}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to update global estimate: {e}")


# ==============================================================================
# QUEUE STATS TRACKING
# ==============================================================================

async def track_skipped_queue_update(skipped_count: int) -> None:
    """Update skipped queue size."""
    try:
        await queue_stats.update_skipped_queue(skipped_count)

        logger.debug(f"[Tracking] Skipped queue: {skipped_count}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track skipped queue: {e}")


async def track_retry_queue_enqueue(
    site_key: str,
    enqueued_count: int,
    file_count: int,
    pass_index: int,
    total_passes: int,
) -> None:
    """Track retry queue enqueue operation."""
    try:
        await queue_stats.update_retry_queue(
            site_key=site_key,
            enqueued_count=enqueued_count,
            file_count=file_count,
            pass_index=pass_index,
            total_passes=total_passes,
        )

        logger.debug(f"[Tracking] Retry queue enqueued: {enqueued_count} items from {site_key}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track retry queue: {e}")


async def track_retry_queue_alert(site_key: str, reason: str) -> None:
    """Record retry queue alert."""
    try:
        await queue_stats.record_retry_alert(site_key, reason)

        logger.warning(f"[Tracking] Retry queue alert: {site_key} - {reason}")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track retry alert: {e}")


# ==============================================================================
# SITE GENRE OVERVIEW TRACKING
# ==============================================================================

async def track_site_genres_initialized(
    site_key: str,
    total_genres: int,
    genres_map: dict[str, dict[str, Any]], # <--- ADDED THIS
) -> None:
    """Track when a site's genres are initialized."""
    try:
        await site_genre_overview.update_site_overview(
            site_key=site_key,
            total_genres=total_genres,
            genres_map=genres_map, # <--- PASSED THIS
        )

        logger.debug(f"[Tracking] Site genres initialized: {site_key} - {total_genres} genres")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track site init: {e}")


# ==============================================================================
# SYSTEM METRICS TRACKING
# ==============================================================================

async def track_system_metrics(metrics: dict[str, Any]) -> None:
    """Track system-wide metrics."""
    try:
        # Convert flat metrics dict to nested structure
        metrics_map = {}
        for key, value in metrics.items():
            # Wrap each metric value in a dict for JSONB storage
            if isinstance(value, dict):
                metrics_map[key] = value
            else:
                metrics_map[key] = {"value": value, "type": type(value).__name__}

        await system_metrics.update_multiple_metrics(metrics_map)

        logger.debug(f"[Tracking] System metrics updated: {len(metrics)} metrics")

    except Exception as e:
        logger.debug(f"[Tracking] Failed to track system metrics: {e}")
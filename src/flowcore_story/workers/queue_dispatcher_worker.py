"""Queue Dispatcher Worker - Dispatches pending stories from PostgreSQL queue to Kafka.

This worker is specifically designed to consume pending xtruyen stories from the
story_queue table and dispatch them as crawl jobs to Kafka for processing by
the crawler consumer workers.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from datetime import datetime
from typing import Any

# Ensure proper path resolution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from flowcore_story.config import config as app_config
from flowcore_story.storage.db_pool import get_db_pool
from flowcore_story.utils.logger import logger

# Configuration
ENABLED_SITE_KEY = os.environ.get("DISPATCH_SITE_KEY", "xtruyen")
BATCH_SIZE = int(os.environ.get("DISPATCH_BATCH_SIZE", "50"))
DISPATCH_INTERVAL = float(os.environ.get("DISPATCH_INTERVAL", "10"))  # seconds between batches
MAX_CONCURRENT_DISPATCHES = int(os.environ.get("MAX_CONCURRENT_DISPATCHES", "100"))
WORKER_ID = os.environ.get("WORKER_ID", "queue-dispatcher-1")

# Kafka producer
_kafka_producer = None
_shutdown_event = asyncio.Event()


async def get_kafka_producer():
    """Get or create Kafka producer."""
    global _kafka_producer
    if _kafka_producer is None:
        from aiokafka import AIOKafkaProducer

        bootstrap_servers = app_config.KAFKA_BOOTSTRAP_SERVERS
        logger.info(f"[QueueDispatcher] Connecting to Kafka at {bootstrap_servers}")

        _kafka_producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            max_batch_size=16384,
            linger_ms=50,
        )
        await _kafka_producer.start()
        logger.info("[QueueDispatcher] Kafka producer ready")

    return _kafka_producer


async def close_kafka_producer():
    """Close Kafka producer."""
    global _kafka_producer
    if _kafka_producer is not None:
        await _kafka_producer.stop()
        _kafka_producer = None
        logger.info("[QueueDispatcher] Kafka producer closed")


async def fetch_pending_stories(pool, site_key: str, limit: int) -> list[dict[str, Any]]:
    """Fetch pending stories from the queue and mark them as processing."""
    async with pool.acquire() as conn:
        # Use FOR UPDATE SKIP LOCKED to avoid contention with other workers
        rows = await conn.fetch(
            """
            WITH pending AS (
                SELECT id, site_key, genre_name, genre_url, story_url, story_title,
                       priority, story_data, created_at
                FROM story_queue
                WHERE site_key = $1
                  AND status = 'pending'
                ORDER BY priority DESC, created_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE story_queue sq
            SET status = 'processing',
                updated_at = NOW(),
                worker_id = $3,
                claimed_at = NOW()
            FROM pending p
            WHERE sq.id = p.id
            RETURNING sq.id, sq.site_key, sq.genre_name, sq.genre_url,
                      sq.story_url, sq.story_title, sq.priority,
                      sq.story_data, sq.created_at
            """,
            site_key,
            limit,
            WORKER_ID,
        )

        return [dict(row) for row in rows]


async def dispatch_story_to_kafka(producer, story: dict[str, Any]) -> bool:
    """Dispatch a single story to Kafka for crawling."""
    try:
        from flowcore_story.config.config import get_topic_for_site
        site_key = story["site_key"]
        topic = get_topic_for_site(site_key, app_config.KAFKA_TOPIC)

        # Extract metadata from story_data if available
        story_data = story.get("story_data") or {}

        # Build the crawl job payload
        job_payload = {
            "type": "crawl_story",
            "queue_id": story["id"],
            "site_key": story["site_key"],
            "genre_name": story["genre_name"],
            "genre_url": story["genre_url"],
            "story_url": story["story_url"],
            "story_title": story["story_title"],
            "priority": story.get("priority", 0),
            "metadata": story_data,
            "dispatched_at": datetime.utcnow().isoformat(),
            "dispatcher_worker_id": WORKER_ID,
        }

        # Send to Kafka
        await producer.send_and_wait(topic, job_payload)

        logger.debug(
            f"[QueueDispatcher] Dispatched story: {story['story_title'][:50] if story['story_title'] else story['story_url'][:50]}... "
            f"(queue_id={story['id']})"
        )
        return True

    except Exception as e:
        logger.error(f"[QueueDispatcher] Failed to dispatch story {story['id']}: {e}")
        return False


async def mark_story_failed(pool, queue_id: int, error: str):
    """Mark a story as failed if dispatch failed."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE story_queue
                SET status = 'failed',
                    last_error = $2,
                    updated_at = NOW()
                WHERE id = $1
                """,
                queue_id,
                error[:500],  # Truncate error message
            )
    except Exception as e:
        logger.error(f"[QueueDispatcher] Failed to mark story {queue_id} as failed: {e}")


async def get_queue_stats(pool, site_key: str) -> dict[str, int]:
    """Get current queue statistics."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'processing') as processing,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed
            FROM story_queue
            WHERE site_key = $1
            """,
            site_key,
        )
        return dict(row) if row else {"pending": 0, "processing": 0, "completed": 0, "failed": 0}


async def recover_stale_processing(pool, site_key: str, stale_minutes: int = 1440) -> int:
    """Recover stories that have been stuck in 'processing' for too long."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE story_queue
            SET status = 'pending',
                worker_id = NULL,
                retry_count = COALESCE(retry_count, 0) + 1,
                updated_at = NOW()
            WHERE site_key = $1
              AND status = 'processing'
              AND updated_at < NOW() - INTERVAL '%s minutes'
            """ % stale_minutes,
            site_key,
        )
        count = int(result.split()[-1]) if result else 0
        if count > 0:
            logger.info(f"[QueueDispatcher] Recovered {count} stale processing stories")
        return count


async def dispatch_loop():
    """Main dispatch loop."""
    logger.info(f"[QueueDispatcher] Starting dispatcher for site_key={ENABLED_SITE_KEY}")
    logger.info(f"[QueueDispatcher] Batch size: {BATCH_SIZE}, Interval: {DISPATCH_INTERVAL}s")

    pool = await get_db_pool()
    if not pool:
        logger.error("[QueueDispatcher] Failed to get database pool")
        return

    producer = await get_kafka_producer()

    total_dispatched = 0
    last_stats_time = asyncio.get_event_loop().time()
    stats_interval = 60  # Log stats every 60 seconds

    # Initial recovery of stale processing entries
    await recover_stale_processing(pool, ENABLED_SITE_KEY)

    while not _shutdown_event.is_set():
        try:
            # Fetch pending stories
            stories = await fetch_pending_stories(pool, ENABLED_SITE_KEY, BATCH_SIZE)

            if not stories:
                # No pending stories, wait and check again
                logger.debug(f"[QueueDispatcher] No pending stories for {ENABLED_SITE_KEY}")
                await asyncio.sleep(DISPATCH_INTERVAL * 2)
                continue

            # Dispatch stories to Kafka
            dispatched_count = 0
            for story in stories:
                if _shutdown_event.is_set():
                    break

                success = await dispatch_story_to_kafka(producer, story)
                if success:
                    dispatched_count += 1
                else:
                    await mark_story_failed(pool, story["id"], "Failed to dispatch to Kafka")

            total_dispatched += dispatched_count
            logger.info(
                f"[QueueDispatcher] Dispatched {dispatched_count}/{len(stories)} stories "
                f"(total: {total_dispatched})"
            )

            # Periodic stats logging
            current_time = asyncio.get_event_loop().time()
            if current_time - last_stats_time >= stats_interval:
                stats = await get_queue_stats(pool, ENABLED_SITE_KEY)
                logger.info(
                    f"[QueueDispatcher] Queue stats for {ENABLED_SITE_KEY}: "
                    f"pending={stats['pending']}, processing={stats['processing']}, "
                    f"completed={stats['completed']}, failed={stats['failed']}"
                )
                last_stats_time = current_time

                # Periodic recovery of stale entries
                await recover_stale_processing(pool, ENABLED_SITE_KEY)

            # Wait before next batch
            await asyncio.sleep(DISPATCH_INTERVAL)

        except asyncio.CancelledError:
            logger.info("[QueueDispatcher] Dispatch loop cancelled")
            break
        except Exception as e:
            logger.error(f"[QueueDispatcher] Error in dispatch loop: {e}")
            await asyncio.sleep(DISPATCH_INTERVAL)

    logger.info(f"[QueueDispatcher] Shutting down. Total dispatched: {total_dispatched}")


def handle_shutdown(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"[QueueDispatcher] Received signal {signum}, initiating shutdown...")
    _shutdown_event.set()


async def main():
    """Main entry point."""
    # Setup signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("=" * 60)
    logger.info("[QueueDispatcher] Queue Dispatcher Worker Starting")
    logger.info(f"[QueueDispatcher] Site Key: {ENABLED_SITE_KEY}")
    logger.info(f"[QueueDispatcher] Worker ID: {WORKER_ID}")
    logger.info("=" * 60)

    try:
        await dispatch_loop()
    except Exception as e:
        logger.error(f"[QueueDispatcher] Fatal error: {e}")
        raise
    finally:
        await close_kafka_producer()
        logger.info("[QueueDispatcher] Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())

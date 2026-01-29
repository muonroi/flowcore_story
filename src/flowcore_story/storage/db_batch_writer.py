"""
Batch writer for state updates.
Reduces database load by batching multiple state writes into single transactions.
"""

import asyncio
import json
import os
from asyncio import Queue, create_task
from typing import Any

from flowcore_story.storage.db_pool import get_db_pool
from flowcore_story.utils.logger import logger


class StateBatchWriter:
    """Batch writer for state updates."""

    def __init__(
        self,
        flush_interval: float | None = None,
        max_batch_size: int | None = None
    ):
        self.flush_interval = flush_interval or float(
            os.getenv("STATE_BATCH_FLUSH_INTERVAL", "10.0")
        )
        self.max_batch_size = max_batch_size or int(
            os.getenv("STATE_BATCH_MAX_SIZE", "50")
        )

        self.queue: Queue = Queue()
        self._task: asyncio.Task | None = None
        self._running = False

        # Metrics
        self.total_enqueued = 0
        self.total_flushed = 0
        self.total_batches = 0
        self.last_flush_size = 0

        logger.info(
            f"[BatchWriter] Initialized: "
            f"flush_interval={self.flush_interval}s, "
            f"max_batch_size={self.max_batch_size}"
        )

    async def start(self) -> None:
        """Start background flush task."""
        if self._running:
            logger.warning("[BatchWriter] Already running")
            return

        self._running = True
        self._task = create_task(self._flush_loop())
        logger.info("[BatchWriter] Started background flush loop")

    async def stop(self) -> None:
        """Stop background flush task and flush remaining items."""
        if not self._running:
            return

        logger.info("[BatchWriter] Stopping, flushing remaining items...")
        self._running = False

        # Flush remaining items
        if not self.queue.empty():
            batch = []
            while not self.queue.empty():
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if batch:
                await self._flush_batch(batch)

        # Cancel task
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("[BatchWriter] Stopped")

    async def enqueue(
        self,
        site_key: str,
        worker_id: str,
        state: dict[str, Any]
    ) -> None:
        """Add state to write queue."""
        if not self._running:
            logger.warning("[BatchWriter] Not running, cannot enqueue")
            return

        await self.queue.put((site_key, worker_id, state))
        self.total_enqueued += 1

        logger.debug(
            f"[BatchWriter] Enqueued: site={site_key}, worker={worker_id}, "
            f"queue_size={self.queue.qsize()}"
        )

    async def _flush_loop(self) -> None:
        """Background task that flushes batches."""
        logger.info("[BatchWriter] Flush loop started")

        while self._running:
            batch = []

            try:
                # Wait for items or timeout
                try:
                    item = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=self.flush_interval
                    )
                    batch.append(item)
                except TimeoutError:
                    # Timeout - flush whatever we have
                    pass

                # Drain queue up to max_batch_size
                while not self.queue.empty() and len(batch) < self.max_batch_size:
                    try:
                        batch.append(self.queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                # Flush batch if not empty
                if batch:
                    await self._flush_batch(batch)

            except asyncio.CancelledError:
                logger.info("[BatchWriter] Flush loop cancelled")
                break
            except Exception as e:
                logger.error(f"[BatchWriter] Error in flush loop: {e}", exc_info=True)
                await asyncio.sleep(1)

        logger.info("[BatchWriter] Flush loop ended")

    async def _flush_batch(self, batch: list) -> None:
        """Write all states in batch to database."""
        if not batch:
            return

        pool = await get_db_pool()
        if pool is None:
            logger.warning(
                f"[BatchWriter] No DB pool available, "
                f"cannot flush batch of {len(batch)} items"
            )
            return

        flush_start = asyncio.get_event_loop().time()

        try:
            async with pool.acquire() as conn:
                # Use transaction for entire batch
                async with conn.transaction():
                    for site_key, worker_id, state in batch:
                        # Same UPSERT logic as DatabaseStateStorage
                        await conn.execute(
                            """
                            INSERT INTO crawl_states (
                                site_key,
                                worker_id,
                                current_genre_url,
                                current_genre_name,
                                current_story_url,
                                current_story_index_in_genre,
                                processed_genre_urls,
                                processed_story_urls,
                                globally_completed_story_urls,
                                processed_chapter_urls_for_current_story,
                                state_data,
                                last_save_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
                            ON CONFLICT (site_key, worker_id)
                            DO UPDATE SET
                                current_genre_url = EXCLUDED.current_genre_url,
                                current_genre_name = EXCLUDED.current_genre_name,
                                current_story_url = EXCLUDED.current_story_url,
                                current_story_index_in_genre = EXCLUDED.current_story_index_in_genre,
                                processed_genre_urls = EXCLUDED.processed_genre_urls,
                                processed_story_urls = EXCLUDED.processed_story_urls,
                                globally_completed_story_urls = EXCLUDED.globally_completed_story_urls,
                                processed_chapter_urls_for_current_story = EXCLUDED.processed_chapter_urls_for_current_story,
                                state_data = EXCLUDED.state_data,
                                last_save_at = NOW()
                            """,
                            site_key,
                            worker_id,
                            state.get("current_genre_url"),
                            state.get("current_genre_name"),
                            state.get("current_story_url"),
                            state.get("current_story_index_in_genre"),
                            state.get("processed_genre_urls", []),
                            state.get("processed_story_urls", []),
                            state.get("globally_completed_story_urls", []),
                            state.get("processed_chapter_urls_for_current_story", []),
                            json.dumps(state),
                        )

            flush_duration = (asyncio.get_event_loop().time() - flush_start) * 1000
            self.total_flushed += len(batch)
            self.total_batches += 1
            self.last_flush_size = len(batch)

            logger.info(
                f"[BatchWriter] Flushed batch: size={len(batch)}, "
                f"duration={flush_duration:.1f}ms, "
                f"total_batches={self.total_batches}, "
                f"total_flushed={self.total_flushed}"
            )

        except Exception as e:
            logger.error(
                f"[BatchWriter] Failed to flush batch of {len(batch)} items: {e}",
                exc_info=True
            )

    def get_stats(self) -> dict[str, Any]:
        """Get batch writer statistics."""
        return {
            "running": self._running,
            "queue_size": self.queue.qsize(),
            "total_enqueued": self.total_enqueued,
            "total_flushed": self.total_flushed,
            "total_batches": self.total_batches,
            "last_flush_size": self.last_flush_size,
            "config": {
                "flush_interval": self.flush_interval,
                "max_batch_size": self.max_batch_size,
            }
        }


# Global instance
_batch_writer: StateBatchWriter | None = None
_batch_writer_lock = asyncio.Lock()


async def get_batch_writer() -> StateBatchWriter:
    """Get or create global batch writer instance."""
    global _batch_writer

    if _batch_writer is None:
        async with _batch_writer_lock:
            # Double-check after acquiring lock
            if _batch_writer is None:
                _batch_writer = StateBatchWriter()
                await _batch_writer.start()

    return _batch_writer


async def stop_batch_writer() -> None:
    """Stop global batch writer instance."""
    global _batch_writer

    if _batch_writer is not None:
        await _batch_writer.stop()
        _batch_writer = None

"""
Data Cleanup Worker
===================

Removes duplicate chapters in MySQL by (story_id, title).
Strategy: Per-story cleanup to avoid locking the entire large table.
Keeps the newest row (max id) and deletes older duplicates.

Usage:
    python -m flowcore_story.workers.data_cleanup_worker
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

# Setup path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from flowcore_story.database.connection import db_manager
from flowcore_story.utils.logger import logger

class DataCleanupWorker:
    """Worker to remove duplicate chapter rows using per-story strategy."""

    def __init__(
        self,
        cleanup_interval: int = 86400,
        batch_size: int = 1000,
        enabled: bool = True,
    ) -> None:
        self.cleanup_interval = cleanup_interval
        self.batch_size = batch_size
        self.enabled = enabled
        self.running = False

    def _ensure_db_ready(self) -> None:
        db_manager.initialize()
        if not db_manager.engine:
            raise RuntimeError("Database engine not initialized")

    def cleanup_duplicate_chapters(self, dry_run: bool = False) -> dict[str, Any]:
        """Delete duplicate chapters by looping through stories.

        Strategy:
        - Process stories in batches to avoid loading all IDs into memory
        - Keep the row with MAX(id) for each (story_id, title) duplicate
        - Commit per story to balance transaction size and rollback safety
        - Handle errors per-story to allow cleanup to continue
        """
        if not self.enabled:
            logger.info("[DATA_CLEANUP] Cleanup disabled")
            return {"status": "disabled"}

        self._ensure_db_ready()
        start_time = time.time()
        total_deleted = 0
        stories_processed = 0
        stories_with_errors = 0
        batch_offset = 0

        logger.info("[DATA_CLEANUP] Starting per-story duplicate cleanup (Dry Run: %s)...", dry_run)

        try:
            # Process stories in batches to avoid memory issues
            while True:
                with db_manager.engine.connect() as conn:
                    # 1. Get batch of story IDs (pagination)
                    res = conn.execute(text(
                        "SELECT DISTINCT story_id FROM chapters "
                        "ORDER BY story_id LIMIT :limit OFFSET :offset"
                    ), {"limit": self.batch_size, "offset": batch_offset})
                    story_ids = [row[0] for row in res]

                    if not story_ids:
                        break  # No more stories to process

                    if batch_offset == 0:
                        # Get total count on first batch
                        total_res = conn.execute(text("SELECT COUNT(DISTINCT story_id) FROM chapters"))
                        total_stories = total_res.scalar()
                        logger.info("[DATA_CLEANUP] Found %d stories to check", total_stories)

                # 2. Process each story individually with error handling
                for sid in story_ids:
                    try:
                        deleted_in_story = self._cleanup_story_duplicates(sid, dry_run)
                        total_deleted += deleted_in_story
                        stories_processed += 1

                        if stories_processed % 100 == 0:
                            logger.info("[DATA_CLEANUP] Processed %d stories, deleted %d rows so far...",
                                      stories_processed, total_deleted)

                    except Exception as exc:
                        stories_with_errors += 1
                        logger.error("[DATA_CLEANUP] Error cleaning story_id=%s: %s", sid, exc)
                        # Continue processing other stories
                        continue

                batch_offset += self.batch_size

        except Exception as exc:
            logger.error("[DATA_CLEANUP] Fatal error during cleanup: %s", exc, exc_info=True)
            return {"status": "error", "message": str(exc)}

        duration = time.time() - start_time
        logger.info(
            "[DATA_CLEANUP] Cleanup finished: %s rows %s in %.2fs (%d stories checked, %d errors)",
            total_deleted, "would be deleted" if dry_run else "removed",
            duration, stories_processed, stories_with_errors
        )

        return {
            "status": "success",
            "deleted_rows": total_deleted,
            "stories_checked": stories_processed,
            "stories_with_errors": stories_with_errors,
            "duration_seconds": duration,
            "dry_run": dry_run
        }

    def _cleanup_story_duplicates(self, story_id: int, dry_run: bool) -> int:
        """Clean duplicates for a single story. Returns number of rows deleted.

        Uses a transaction per story for atomic cleanup with auto-rollback on error.
        Includes retry logic for Deadlocks (OperationalError).
        """
        max_retries = 3
        retry_delay = 1.0  # Start with 1 second

        for attempt in range(max_retries):
            try:
                deleted_count = 0
                # Use begin() for automatic transaction management with commit/rollback
                with db_manager.engine.begin() as conn:
                    # Find duplicate titles for this story
                    dup_res = conn.execute(text(
                        "SELECT title, COUNT(*) as cnt FROM chapters WHERE story_id = :sid "
                        "GROUP BY title HAVING COUNT(*) > 1"
                    ), {"sid": story_id})

                    dups = dup_res.fetchall()
                    if not dups:
                        return 0

                    for title, count in dups:
                        # Get the max ID to keep (newest by auto-increment)
                        id_res = conn.execute(text(
                            "SELECT MAX(id) FROM chapters WHERE story_id = :sid AND title = :title"
                        ), {"sid": story_id, "title": title})
                        max_id = id_res.scalar()

                        if not max_id:
                            continue

                        if dry_run:
                            # For dry run, count exact rows that would be deleted
                            count_res = conn.execute(text(
                                "SELECT COUNT(*) FROM chapters "
                                "WHERE story_id = :sid AND title = :title AND id < :max_id"
                            ), {"sid": story_id, "title": title, "max_id": max_id})
                            deleted_count += count_res.scalar()
                        else:
                            # Delete duplicate rows (keep only max_id)
                            del_res = conn.execute(text(
                                "DELETE FROM chapters "
                                "WHERE story_id = :sid AND title = :title AND id < :max_id"
                            ), {"sid": story_id, "title": title, "max_id": max_id})
                            deleted_count += del_res.rowcount

                # Transaction auto-commits on success, auto-rollbacks on exception
                return deleted_count

            except OperationalError as exc:
                if attempt < max_retries - 1:
                    logger.warning(
                        "[DATA_CLEANUP] Deadlock for story_id=%s (attempt %d/%d). Retrying in %.2fs...",
                        story_id, attempt + 1, max_retries, retry_delay
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("[DATA_CLEANUP] Failed to clean story_id=%s after %d attempts: %s",
                               story_id, max_retries, exc)
                    raise  # Re-raise to let the outer loop handle it (skip story)
            except Exception:
                raise  # Re-raise other errors immediately

    def run_forever(self) -> None:
        """Run cleanup loop."""
        self.running = True
        while self.running:
            try:
                self.cleanup_duplicate_chapters(dry_run=False)
                logger.info("[DATA_CLEANUP] Next run in %ss", self.cleanup_interval)
                time.sleep(self.cleanup_interval)
            except Exception as exc:
                logger.error("[DATA_CLEANUP] Loop error: %s", exc)
                time.sleep(300)

def main() -> None:
    interval = int(os.getenv("DATA_CLEANUP_INTERVAL", "86400"))
    worker = DataCleanupWorker(cleanup_interval=interval)
    worker.run_forever()

if __name__ == "__main__":
    main()
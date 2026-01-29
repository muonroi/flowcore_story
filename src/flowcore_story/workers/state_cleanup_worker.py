"""
State Cleanup Worker
====================

Automated worker for cleaning up outdated PostgreSQL state data.

Cleanup Strategy:
- 7 days: Story events logs
- 30 days: Inactive crawl states → Archive
- 60 days: Completed stories/genres → Archive
- 90 days: Archived data → Permanent deletion

Configuration (Environment Variables):
- CLEANUP_INTERVAL: Cleanup run interval in seconds (default: 86400 = daily)
- STATE_RETENTION_DAYS: Override default retention days (optional)
- ENABLE_STATE_CLEANUP: Enable/disable cleanup (default: true)
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD: PostgreSQL connection

Usage:
    python -m flowcore_story.workers.state_cleanup_worker

Docker:
    docker-compose up -d state-cleanup-worker
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import Any

import asyncpg

from flowcore_story.utils.logger import logger


class StateCleanupWorker:
    """Worker for automated state cleanup"""

    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
        cleanup_interval: int = 86400,  # Default: daily
        enabled: bool = True,
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.cleanup_interval = cleanup_interval
        self.enabled = enabled

        self.pool: asyncpg.Pool | None = None
        self.running = False

    async def connect(self):
        """Connect to PostgreSQL"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                min_size=1,
                max_size=3,
                command_timeout=60,
            )
            logger.info(f"[STATE_CLEANUP] Connected to PostgreSQL at {self.db_host}:{self.db_port}/{self.db_name}")
        except Exception as e:
            logger.error(f"[STATE_CLEANUP] Failed to connect to PostgreSQL: {e}")
            raise

    async def disconnect(self):
        """Disconnect from PostgreSQL"""
        if self.pool:
            await self.pool.close()
            logger.info("[STATE_CLEANUP] Disconnected from PostgreSQL")

    async def run_cleanup(self) -> dict[str, Any]:
        """Run comprehensive cleanup and return statistics"""
        if not self.enabled:
            logger.info("[STATE_CLEANUP] Cleanup is disabled (ENABLE_STATE_CLEANUP=false)")
            return {"status": "disabled"}

        logger.info("[STATE_CLEANUP] Starting comprehensive state cleanup...")
        start_time = datetime.now()
        stats = {}

        try:
            async with self.pool.acquire() as conn:
                # Run the comprehensive cleanup function
                result = await conn.fetchrow("SELECT * FROM run_comprehensive_state_cleanup()")

                stats = {
                    "status": "success",
                    "trimmed_states": result["trimmed_states"],
                    "deleted_events": result["deleted_events"],
                    "archived_stories": result["archived_stories"],
                    "archived_genres": result["archived_genres"],
                    "archived_states": result["archived_states"],
                    "deleted_archived": result["deleted_archived"],
                    "duration_seconds": (datetime.now() - start_time).total_seconds(),
                    "timestamp": datetime.now().isoformat(),
                }

                logger.info(
                    f"[STATE_CLEANUP] Cleanup completed successfully:\n"
                    f"  - Trimmed states: {stats['trimmed_states']}\n"
                    f"  - Deleted 7d events: {stats['deleted_events']}\n"
                    f"  - Archived 60d stories: {stats['archived_stories']}\n"
                    f"  - Archived 60d genres: {stats['archived_genres']}\n"
                    f"  - Archived 30d states: {stats['archived_states']}\n"
                    f"  - Deleted 90d archived: {stats['deleted_archived']}\n"
                    f"  - Duration: {stats['duration_seconds']:.2f}s"
                )

        except Exception as e:
            stats = {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }
            logger.error(f"[STATE_CLEANUP] Cleanup failed: {e}", exc_info=True)

        return stats

    async def get_cleanup_stats(self) -> dict[str, Any]:
        """Get current cleanup statistics"""
        try:
            async with self.pool.acquire() as conn:
                # Query the cleanup statistics view
                stats = await conn.fetchrow("SELECT * FROM v_state_cleanup_stats")
                archive_stats = await conn.fetchrow("SELECT * FROM v_archive_stats")

                return {
                    "cleanup_stats": dict(stats) if stats else {},
                    "archive_stats": dict(archive_stats) if archive_stats else {},
                    "timestamp": datetime.now().isoformat(),
                }
        except Exception as e:
            logger.error(f"[STATE_CLEANUP] Failed to get stats: {e}")
            return {"error": str(e)}

    async def get_cleanup_recommendations(self):
        """Get cleanup recommendations"""
        try:
            async with self.pool.acquire() as conn:
                recommendations = await conn.fetch("SELECT * FROM v_cleanup_recommendations")
                return [dict(r) for r in recommendations]
        except Exception as e:
            logger.error(f"[STATE_CLEANUP] Failed to get recommendations: {e}")
            return []

    async def run_forever(self):
        """Run cleanup worker continuously"""
        self.running = True
        logger.info(f"[STATE_CLEANUP] Worker started (interval: {self.cleanup_interval}s)")

        # Show initial statistics
        stats = await self.get_cleanup_stats()
        logger.info(f"[STATE_CLEANUP] Initial stats: {stats}")

        # Show recommendations
        recommendations = await self.get_cleanup_recommendations()
        if recommendations:
            logger.info("[STATE_CLEANUP] Recommendations:")
            for rec in recommendations:
                logger.info(f"  - {rec['recommendation']}")

        while self.running:
            try:
                # Run cleanup
                await self.run_cleanup()

                # Log statistics
                current_stats = await self.get_cleanup_stats()
                logger.info("[STATE_CLEANUP] Current stats after cleanup:")
                logger.info(f"  Cleanup: {current_stats.get('cleanup_stats', {})}")
                logger.info(f"  Archive: {current_stats.get('archive_stats', {})}")

                # Wait for next cleanup
                logger.info(f"[STATE_CLEANUP] Next cleanup in {self.cleanup_interval}s")
                await asyncio.sleep(self.cleanup_interval)

            except asyncio.CancelledError:
                logger.info("[STATE_CLEANUP] Worker cancelled")
                break
            except Exception as e:
                logger.error(f"[STATE_CLEANUP] Error in cleanup loop: {e}", exc_info=True)
                # Wait a bit before retrying
                await asyncio.sleep(300)  # 5 minutes

    async def stop(self):
        """Stop the worker gracefully"""
        logger.info("[STATE_CLEANUP] Stopping worker...")
        self.running = False


async def main():
    """Main entry point"""
    # Load configuration from environment
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME", "storyflow")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")

    cleanup_interval = int(os.getenv("CLEANUP_INTERVAL", "86400"))  # Default: daily
    enabled = os.getenv("ENABLE_STATE_CLEANUP", "true").lower() in ("true", "1", "yes")

    logger.info("=" * 80)
    logger.info("State Cleanup Worker Starting")
    logger.info("=" * 80)
    logger.info(f"Database: {db_host}:{db_port}/{db_name}")
    logger.info(f"Cleanup Interval: {cleanup_interval}s ({cleanup_interval / 3600:.1f}h)")
    logger.info(f"Enabled: {enabled}")
    logger.info("=" * 80)

    if not db_password:
        logger.error("[STATE_CLEANUP] DB_PASSWORD not set!")
        sys.exit(1)

    # Create worker
    worker = StateCleanupWorker(
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        cleanup_interval=cleanup_interval,
        enabled=enabled,
    )

    try:
        # Connect to database
        await worker.connect()

        # Run worker forever
        await worker.run_forever()

    except KeyboardInterrupt:
        logger.info("[STATE_CLEANUP] Received shutdown signal")
    except Exception as e:
        logger.error(f"[STATE_CLEANUP] Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await worker.stop()
        await worker.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

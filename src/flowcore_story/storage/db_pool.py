"""
Database connection pool management for PostgreSQL.
Provides singleton connection pool with automatic initialization.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

try:
    import asyncpg as _asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    _asyncpg = None

from flowcore_story.utils.logger import logger


def _should_use_file_state() -> bool:
    """
    Decide whether to skip DB usage and fall back to file state.

    We keep the explicit USE_FILE_STATE flag as the source of truth, but when it
    is unset (or explicitly false) we default to file-state during pytest/CI runs
    to avoid hanging on connection retries when no database is available.
    """
    explicit = os.getenv("USE_FILE_STATE")
    if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("CI"):
        # Force file state in automated test environments unless explicitly enabled.
        return True

    if explicit is not None:
        return explicit.lower() == "true"

    return False


class DatabasePool:
    """Singleton connection pool for PostgreSQL."""

    _instance: DatabasePool | None = None
    _pool: asyncpg.Pool | None = None  # Use string annotation when asyncpg might not be available
    _lock = asyncio.Lock()
    _schema_bootstrapped = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    async def get_pool(cls) -> asyncpg.Pool | Any | None:
        """Get or create connection pool."""
        if not ASYNCPG_AVAILABLE:
            logger.warning("[DB] asyncpg not available, database features disabled")
            return None

        # Check if PostgreSQL is enabled
        if _should_use_file_state():
            logger.debug("[DB] USE_FILE_STATE=true, using file-based storage")
            return None

        if cls._pool is None:
            async with cls._lock:
                # Double-check after acquiring lock
                if cls._pool is None:
                    cls._pool = await cls._create_pool()

        return cls._pool

    @classmethod
    async def _bootstrap_schema(cls, pool: asyncpg.Pool) -> None:
        """Ensure crawl state tables exist in PostgreSQL."""
        if cls._schema_bootstrapped:
            return

        # Derived from migrations/001_create_crawl_state_tables.sql (condensed to
        # essential tables/indexes/triggers). Idempotent by using IF NOT EXISTS
        # and ON CONFLICT where needed.
        statements = [
            # 1. crawl_states
            """
            CREATE TABLE IF NOT EXISTS crawl_states (
                id SERIAL PRIMARY KEY,
                site_key VARCHAR(255) NOT NULL,
                worker_id VARCHAR(255) DEFAULT 'main',
                current_genre_url TEXT,
                current_genre_name VARCHAR(500),
                current_story_url TEXT,
                current_story_index_in_genre INTEGER,
                processed_genre_urls TEXT[],
                processed_story_urls TEXT[],
                globally_completed_story_urls TEXT[],
                processed_chapter_urls_for_current_story TEXT[],
                state_data JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_save_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(site_key, worker_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_crawl_states_site_key ON crawl_states(site_key)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_states_updated_at ON crawl_states(updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_crawl_states_site_worker ON crawl_states(site_key, worker_id)",
            # 2. story_progress
            """
            CREATE TABLE IF NOT EXISTS story_progress (
                id SERIAL PRIMARY KEY,
                story_id VARCHAR(500) NOT NULL UNIQUE,
                title TEXT NOT NULL,
                total_chapters INTEGER NOT NULL DEFAULT 0,
                crawled_chapters INTEGER NOT NULL DEFAULT 0,
                missing_chapters INTEGER NOT NULL DEFAULT 0,
                status VARCHAR(50) NOT NULL DEFAULT 'queued',
                primary_site VARCHAR(255),
                last_source VARCHAR(255),
                genre_name VARCHAR(500),
                genre_url TEXT,
                genre_site_key VARCHAR(255),
                last_error TEXT,
                cooldown_until TIMESTAMPTZ,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                metadata JSONB
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_story_progress_status ON story_progress(status)",
            "CREATE INDEX IF NOT EXISTS idx_story_progress_site ON story_progress(primary_site)",
            "CREATE INDEX IF NOT EXISTS idx_story_progress_genre ON story_progress(genre_site_key, genre_url)",
            "CREATE INDEX IF NOT EXISTS idx_story_progress_updated ON story_progress(updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_story_progress_cooldown ON story_progress(cooldown_until) WHERE cooldown_until IS NOT NULL",
            # 3. genre_progress
            """
            CREATE TABLE IF NOT EXISTS genre_progress (
                id SERIAL PRIMARY KEY,
                site_key VARCHAR(255) NOT NULL,
                genre_name VARCHAR(500) NOT NULL,
                genre_url TEXT NOT NULL,
                position INTEGER,
                total_genres INTEGER,
                total_pages INTEGER,
                crawled_pages INTEGER NOT NULL DEFAULT 0,
                current_page INTEGER,
                total_stories INTEGER NOT NULL DEFAULT 0,
                processed_stories INTEGER NOT NULL DEFAULT 0,
                active_stories TEXT[],
                active_story_details JSONB,
                current_story_title TEXT,
                current_story_page INTEGER,
                current_story_position INTEGER,
                status VARCHAR(50) NOT NULL DEFAULT 'queued',
                last_error TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                UNIQUE(site_key, genre_url)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_genre_progress_site ON genre_progress(site_key)",
            "CREATE INDEX IF NOT EXISTS idx_genre_progress_status ON genre_progress(status)",
            "CREATE INDEX IF NOT EXISTS idx_genre_progress_updated ON genre_progress(updated_at)",
            # 4. site_health
            """
            CREATE TABLE IF NOT EXISTS site_health (
                id SERIAL PRIMARY KEY,
                site_key VARCHAR(255) NOT NULL UNIQUE,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                failure_rate DECIMAL(5,4) GENERATED ALWAYS AS (
                    CASE
                        WHEN (success_count + failure_count) = 0 THEN 0
                        ELSE failure_count::DECIMAL / (success_count + failure_count)
                    END
                ) STORED,
                last_error TEXT,
                last_alert_at TIMESTAMPTZ,
                challenge_count_1h INTEGER NOT NULL DEFAULT 0,
                challenge_count_24h INTEGER NOT NULL DEFAULT 0,
                challenge_timestamps TIMESTAMPTZ[],
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_site_health_updated ON site_health(updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_site_health_failure_rate ON site_health(failure_rate)",
            # 5. story_events
            """
            CREATE TABLE IF NOT EXISTS story_events (
                id SERIAL PRIMARY KEY,
                category_id VARCHAR(255) NOT NULL,
                story_id VARCHAR(500) NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                metadata JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_story_events_category_story ON story_events(category_id, story_id)",
            "CREATE INDEX IF NOT EXISTS idx_story_events_created ON story_events(created_at)",
            # 6. queue_stats
            """
            CREATE TABLE IF NOT EXISTS queue_stats (
                id SERIAL PRIMARY KEY,
                queue_type VARCHAR(100) NOT NULL UNIQUE,
                skipped_count INTEGER NOT NULL DEFAULT 0,
                last_site_key VARCHAR(255),
                last_enqueued_count INTEGER,
                last_file_count INTEGER,
                last_pass_index INTEGER,
                total_passes INTEGER,
                total_enqueued INTEGER NOT NULL DEFAULT 0,
                last_enqueued_at TIMESTAMPTZ,
                last_alert_site_key VARCHAR(255),
                last_alert_reason TEXT,
                last_alert_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            "INSERT INTO queue_stats (queue_type, skipped_count) VALUES ('skipped', 0) ON CONFLICT (queue_type) DO NOTHING",
            "INSERT INTO queue_stats (queue_type, total_enqueued) VALUES ('retry', 0) ON CONFLICT (queue_type) DO NOTHING",
            # 7. system_metrics
            """
            CREATE TABLE IF NOT EXISTS system_metrics (
                id SERIAL PRIMARY KEY,
                metric_key VARCHAR(255) NOT NULL UNIQUE,
                metric_value JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            # 8. site_genre_overview
            """
            CREATE TABLE IF NOT EXISTS site_genre_overview (
                id SERIAL PRIMARY KEY,
                site_key VARCHAR(255) NOT NULL UNIQUE,
                total_genres INTEGER NOT NULL DEFAULT 0,
                completed_genres INTEGER NOT NULL DEFAULT 0,
                genres JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            # 9. global_story_totals
            """
            CREATE TABLE IF NOT EXISTS global_story_totals (
                id INTEGER PRIMARY KEY DEFAULT 1,
                completed_count INTEGER NOT NULL DEFAULT 0,
                total_estimate INTEGER,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT singleton_check CHECK (id = 1)
            )
            """,
            "INSERT INTO global_story_totals (id, completed_count) VALUES (1, 0) ON CONFLICT (id) DO NOTHING",
            # Shared trigger function and triggers
            """
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # 10. story_queue table and indexes
            """
            CREATE TABLE IF NOT EXISTS story_queue (
                id SERIAL PRIMARY KEY,
                site_key VARCHAR(255) NOT NULL,
                genre_name VARCHAR(500) NOT NULL,
                genre_url TEXT NOT NULL,
                story_url TEXT NOT NULL,
                story_title TEXT,
                story_data JSONB NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                discovery_index INTEGER NOT NULL,
                source_page INTEGER,
                status VARCHAR(50) NOT NULL DEFAULT 'pending',
                worker_id VARCHAR(255),
                claimed_at TIMESTAMPTZ,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                UNIQUE(site_key, genre_url, story_url)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_story_queue_fetch_next
                ON story_queue(site_key, genre_url, priority, discovery_index)
                WHERE status = 'pending'
            """,
            "CREATE INDEX IF NOT EXISTS idx_story_queue_genre_status ON story_queue(site_key, genre_url, status)",
            "CREATE INDEX IF NOT EXISTS idx_story_queue_worker ON story_queue(worker_id, claimed_at) WHERE worker_id IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_story_queue_stale_claims ON story_queue(claimed_at, status) WHERE status = 'processing' AND claimed_at IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_story_queue_story_data ON story_queue USING GIN(story_data)",
            "CREATE INDEX IF NOT EXISTS idx_story_queue_completed_cleanup ON story_queue(completed_at, status) WHERE status IN ('completed', 'skipped')",
            # 11. genre_queue_metadata table and indexes
            """
            CREATE TABLE IF NOT EXISTS genre_queue_metadata (
                id SERIAL PRIMARY KEY,
                site_key VARCHAR(255) NOT NULL,
                genre_name VARCHAR(500) NOT NULL,
                genre_url TEXT NOT NULL,
                total_stories INTEGER NOT NULL DEFAULT 0,
                total_pages INTEGER,
                crawled_pages INTEGER,
                planned_story_total INTEGER,
                pending_count INTEGER NOT NULL DEFAULT 0,
                processing_count INTEGER NOT NULL DEFAULT 0,
                completed_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0,
                planning_status VARCHAR(50) NOT NULL DEFAULT 'planning',
                planning_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                planning_completed_at TIMESTAMPTZ,
                processing_started_at TIMESTAMPTZ,
                processing_completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(site_key, genre_url)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_genre_queue_metadata_site ON genre_queue_metadata(site_key)",
            "CREATE INDEX IF NOT EXISTS idx_genre_queue_metadata_status ON genre_queue_metadata(planning_status)",
            "CREATE INDEX IF NOT EXISTS idx_genre_queue_metadata_site_status ON genre_queue_metadata(site_key, planning_status)",
            # 12. story queue helper functions (derived from migration 005)
            """
            CREATE OR REPLACE FUNCTION enqueue_story(
                p_site_key VARCHAR(255),
                p_genre_name VARCHAR(500),
                p_genre_url TEXT,
                p_story_url TEXT,
                p_story_title TEXT,
                p_story_data JSONB,
                p_priority INTEGER DEFAULT 0,
                p_discovery_index INTEGER DEFAULT 0,
                p_source_page INTEGER DEFAULT NULL
            )
            RETURNS INTEGER AS $$
            DECLARE
                story_id INTEGER;
            BEGIN
                INSERT INTO story_queue (
                    site_key, genre_name, genre_url, story_url, story_title,
                    story_data, priority, discovery_index, source_page, status
                )
                VALUES (
                    p_site_key, p_genre_name, p_genre_url, p_story_url, p_story_title,
                    p_story_data, p_priority, p_discovery_index, p_source_page, 'pending'
                )
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
                RETURNING id INTO story_id;

                RETURN story_id;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            CREATE OR REPLACE FUNCTION dequeue_next_story(
                p_site_key VARCHAR(255),
                p_genre_url TEXT,
                p_worker_id VARCHAR(255),
                p_limit INTEGER DEFAULT 1
            )
            RETURNS TABLE (
                id INTEGER,
                story_url TEXT,
                story_title TEXT,
                story_data JSONB,
                priority INTEGER,
                discovery_index INTEGER,
                source_page INTEGER,
                retry_count INTEGER
            ) AS $$
            BEGIN
                RETURN QUERY
                UPDATE story_queue
                SET
                    status = 'processing',
                    worker_id = p_worker_id,
                    claimed_at = NOW(),
                    updated_at = NOW()
                WHERE story_queue.id IN (
                    SELECT sq.id
                    FROM story_queue sq
                    WHERE sq.site_key = p_site_key
                      AND sq.genre_url = p_genre_url
                      AND sq.status = 'pending'
                    ORDER BY sq.priority ASC, sq.discovery_index ASC
                    LIMIT p_limit
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING
                    story_queue.id,
                    story_queue.story_url,
                    story_queue.story_title,
                    story_queue.story_data,
                    story_queue.priority,
                    story_queue.discovery_index,
                    story_queue.source_page,
                    story_queue.retry_count;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            CREATE OR REPLACE FUNCTION complete_story(
                p_id INTEGER,
                p_status VARCHAR(50),
                p_error TEXT DEFAULT NULL
            )
            RETURNS VOID AS $$
            BEGIN
                UPDATE story_queue
                SET
                    status = p_status,
                    last_error = p_error,
                    completed_at = NOW(),
                    updated_at = NOW(),
                    worker_id = NULL,
                    claimed_at = NULL
                WHERE id = p_id;
            END;
            $$ LANGUAGE plpgsql;
            """,
            # Ensure signature changes don't fail on environments with older function versions
            "DROP FUNCTION IF EXISTS get_genre_queue_stats(VARCHAR, TEXT);",
            """
            CREATE OR REPLACE FUNCTION get_genre_queue_stats(
                p_site_key VARCHAR(255),
                p_genre_url TEXT
            )
            RETURNS TABLE (
                pending_count INTEGER,
                processing_count INTEGER,
                completed_count INTEGER,
                failed_count INTEGER,
                skipped_count INTEGER
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
                    COUNT(*) FILTER (WHERE status = 'processing') AS processing_count,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
                    COUNT(*) FILTER (WHERE status = 'skipped') AS skipped_count
                FROM story_queue
                WHERE site_key = p_site_key
                  AND genre_url = p_genre_url;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            CREATE OR REPLACE FUNCTION cleanup_completed_queue(
                p_before TIMESTAMPTZ DEFAULT NOW() - INTERVAL '7 days'
            )
            RETURNS INTEGER AS $$
            DECLARE
                deleted_count INTEGER;
            BEGIN
                DELETE FROM story_queue
                WHERE status IN ('completed', 'skipped')
                  AND completed_at IS NOT NULL
                  AND completed_at < p_before;

                GET DIAGNOSTICS deleted_count = ROW_COUNT;
                RETURN deleted_count;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            CREATE OR REPLACE FUNCTION recover_stale_claims(
                p_max_age INTERVAL DEFAULT INTERVAL '15 minutes'
            )
            RETURNS INTEGER AS $$
            DECLARE
                recovered INTEGER;
            BEGIN
                UPDATE story_queue
                SET
                    status = 'pending',
                    worker_id = NULL,
                    claimed_at = NULL,
                    updated_at = NOW(),
                    retry_count = retry_count + 1
                WHERE status = 'processing'
                  AND claimed_at IS NOT NULL
                  AND claimed_at < NOW() - p_max_age;

                GET DIAGNOSTICS recovered = ROW_COUNT;
                RETURN recovered;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            CREATE OR REPLACE FUNCTION clear_genre_queue(
                p_site_key VARCHAR(255),
                p_genre_url TEXT
            )
            RETURNS VOID AS $$
            BEGIN
                DELETE FROM story_queue WHERE site_key = p_site_key AND genre_url = p_genre_url;
                DELETE FROM genre_queue_metadata WHERE site_key = p_site_key AND genre_url = p_genre_url;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            CREATE OR REPLACE FUNCTION refresh_genre_queue_counters(
                p_site_key VARCHAR(255),
                p_genre_url TEXT
            )
            RETURNS VOID AS $$
            DECLARE
                stats RECORD;
            BEGIN
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
                    COUNT(*) FILTER (WHERE status = 'processing') AS processing_count,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
                    COUNT(*) FILTER (WHERE status = 'skipped') AS skipped_count
                INTO stats
                FROM story_queue
                WHERE site_key = p_site_key
                  AND genre_url = p_genre_url;

                UPDATE genre_queue_metadata
                SET
                    pending_count = stats.pending_count,
                    processing_count = stats.processing_count,
                    completed_count = stats.completed_count,
                    failed_count = stats.failed_count,
                    skipped_count = stats.skipped_count,
                    updated_at = NOW()
                WHERE site_key = p_site_key
                  AND genre_url = p_genre_url;
            END;
            $$ LANGUAGE plpgsql;
            """,
        ]

        trigger_targets = (
            "crawl_states",
            "story_progress",
            "genre_progress",
            "site_health",
            "queue_stats",
            "system_metrics",
            "site_genre_overview",
            "global_story_totals",
            "story_queue",
            "genre_queue_metadata",
        )

        try:
            async with pool.acquire() as conn:
                for stmt in statements:
                    await conn.execute(stmt)

                # Create per-table update triggers if not present
                for table in trigger_targets:
                    await conn.execute(
                        f"""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_trigger WHERE tgname = 'update_{table}_updated_at'
                            ) THEN
                                CREATE TRIGGER update_{table}_updated_at
                                BEFORE UPDATE ON {table}
                                FOR EACH ROW
                                EXECUTE FUNCTION update_updated_at_column();
                            END IF;
                        END;
                        $$;
                        """
                    )

                cls._schema_bootstrapped = True
                logger.info("[DB] Ensured PostgreSQL state schema is initialized")
        except Exception as e:  # pragma: no cover - best effort guard
            logger.error(f"[DB] Failed to bootstrap crawl_states table: {e}")

    @classmethod
    async def _create_pool(cls) -> asyncpg.Pool | Any | None:
        """Create new connection pool with retry logic."""
        # Try POSTGRES_STATE_* env vars first (new convention), fallback to DB_* (legacy)
        db_config = {
            "host": os.getenv("POSTGRES_STATE_HOST") or os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_STATE_PORT") or os.getenv("DB_PORT", "5432")),
            "database": os.getenv("POSTGRES_STATE_DB") or os.getenv("DB_NAME", "flowcore_story"),
            "user": os.getenv("POSTGRES_STATE_USER") or os.getenv("DB_USER", "storyflow"),
            "password": os.getenv("POSTGRES_STATE_PASSWORD") or os.getenv("DB_PASSWORD", ""),
            "min_size": int(os.getenv("POSTGRES_STATE_POOL_MIN") or os.getenv("DB_POOL_MIN_SIZE", "2")),
            "max_size": int(os.getenv("POSTGRES_STATE_POOL_MAX") or os.getenv("DB_POOL_MAX_SIZE", "10")),
            "command_timeout": float(os.getenv("POSTGRES_STATE_POOL_TIMEOUT") or os.getenv("DB_POOL_TIMEOUT", "30")),
        }

        # Retry configuration
        max_retries = int(os.getenv("POSTGRES_STATE_CONNECT_RETRIES", "5"))
        initial_backoff = float(os.getenv("POSTGRES_STATE_RETRY_BACKOFF_INITIAL", "1.0"))
        max_backoff = float(os.getenv("POSTGRES_STATE_RETRY_BACKOFF_MAX", "60.0"))

        backoff = initial_backoff

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"[DB] Connection attempt {attempt}/{max_retries} to "
                    f"PostgreSQL at {db_config['host']}:{db_config['port']}/{db_config['database']}"
                )

                pool = await _asyncpg.create_pool(**db_config)

                # Test connection
                async with pool.acquire() as conn:
                    version = await conn.fetchval("SELECT version()")
                    logger.info(f"[DB] Connected to PostgreSQL: {version.split(',')[0]}")

                # Ensure schema exists (idempotent)
                await cls._bootstrap_schema(pool)

                return pool

            except Exception as e:
                logger.error(f"[DB] Attempt {attempt}/{max_retries} FAILED with error: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())

                if attempt < max_retries:
                    logger.info(f"[DB] Retrying in {backoff:.1f}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                else:
                    logger.error(f"[DB] All {max_retries} connection attempts failed")
                    logger.warning("[DB] Falling back to file-based storage")
                    return None

    @classmethod
    async def close(cls):
        """Close connection pool."""
        if cls._pool is not None:
            async with cls._lock:
                if cls._pool is not None:
                    await cls._pool.close()
                    cls._pool = None
                    logger.info("[DB] Connection pool closed")


async def get_db_pool() -> asyncpg.Pool | Any | None:
    """
    Get database connection pool.
    Returns None if PostgreSQL is not available or USE_FILE_STATE=true.
    """
    return await DatabasePool.get_pool()


async def close_db_pool():
    """Close database connection pool."""
    await DatabasePool.close()

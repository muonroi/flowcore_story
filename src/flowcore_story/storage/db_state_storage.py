"""
Database-based state storage adapter for PostgreSQL.
Implements the same interface as file-based storage but uses PostgreSQL.
"""

import json
from typing import Any

from flowcore_story.storage.db_pool import get_db_pool
from flowcore_story.utils.logger import logger

try:
    from flowcore_story.storage.db_circuit_breaker import get_circuit_breaker
    CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    CIRCUIT_BREAKER_AVAILABLE = False
    get_circuit_breaker = None


class DatabaseStateStorage:
    """PostgreSQL-based state storage."""

    @staticmethod
    async def load_crawl_state(site_key: str, worker_id: str = "main") -> dict[str, Any]:
        """
        Load crawl state from PostgreSQL.

        Args:
            site_key: Site identifier (e.g., 'xtruyen')
            worker_id: Worker instance ID (default: 'main')

        Returns:
            Dict containing crawl state, empty dict if not found
        """
        pool = await get_db_pool()
        if pool is None:
            logger.debug(f"[DB] Pool not available for {site_key}, returning empty state")
            return {}

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        current_genre_url,
                        current_genre_name,
                        current_story_url,
                        current_story_index_in_genre,
                        processed_genre_urls,
                        processed_story_urls,
                        globally_completed_story_urls,
                        processed_chapter_urls_for_current_story,
                        state_data
                    FROM crawl_states
                    WHERE site_key = $1 AND worker_id = $2
                    """,
                    site_key,
                    worker_id,
                )

                if row is None:
                    logger.info(f"[DB] No state found for {site_key}/{worker_id}, returning empty state")
                    return {"site_key": site_key}

                # Build state dict from database row
                try:
                    # PostgreSQL JSONB is returned as dict by asyncpg, but if it's a string, parse it
                    if row["state_data"]:
                        if isinstance(row["state_data"], str):
                            # Parse JSON string
                            state = json.loads(row["state_data"])
                        elif isinstance(row["state_data"], dict):
                            # Already a dict
                            state = dict(row["state_data"])
                        else:
                            # Unknown type, try to convert
                            logger.warning(f"[DB] Unexpected state_data type: {type(row['state_data'])}")
                            state = dict(row["state_data"])
                    else:
                        state = {}
                except Exception as state_err:
                    logger.error(f"[DB] Error parsing state_data for {site_key}/{worker_id}: {state_err}")
                    logger.error(f"[DB] state_data type: {type(row['state_data'])}, value: {row['state_data'][:200] if row['state_data'] else None}")
                    state = {}

                # Helper function to convert PostgreSQL array to Python list
                def pg_array_to_list(pg_array):
                    """Convert PostgreSQL array to Python list."""
                    if pg_array is None:
                        return []
                    if isinstance(pg_array, list):
                        return pg_array
                    # If it's some other type, try to convert
                    try:
                        return list(pg_array)
                    except (TypeError, ValueError):
                        logger.warning(f"[DB] Could not convert {type(pg_array)} to list, returning empty")
                        return []

                # Override with explicit fields from database (with proper conversion)
                state["site_key"] = site_key
                state["current_genre_url"] = row["current_genre_url"]
                state["current_genre_name"] = row["current_genre_name"]
                state["current_story_url"] = row["current_story_url"]
                state["current_story_index_in_genre"] = row["current_story_index_in_genre"]
                state["processed_genre_urls"] = pg_array_to_list(row["processed_genre_urls"])
                state["processed_story_urls"] = pg_array_to_list(row["processed_story_urls"])
                state["globally_completed_story_urls"] = pg_array_to_list(row["globally_completed_story_urls"])
                state["processed_chapter_urls_for_current_story"] = pg_array_to_list(row["processed_chapter_urls_for_current_story"])

                logger.info(f"[DB] Loaded state for {site_key}/{worker_id}")
                return state

        except Exception as e:
            logger.error(f"[DB] Error loading state for {site_key}/{worker_id}: {e}")
            return {"site_key": site_key}

    @staticmethod
    async def save_crawl_state(
        state: dict[str, Any],
        site_key: str,
        worker_id: str = "main",
    ) -> bool:
        """
        Save crawl state to PostgreSQL.

        Args:
            state: State dictionary to save
            site_key: Site identifier
            worker_id: Worker instance ID

        Returns:
            True if successful, False otherwise
        """
        # Check circuit breaker
        if CIRCUIT_BREAKER_AVAILABLE and get_circuit_breaker:
            breaker = get_circuit_breaker()
            if not breaker.is_available():
                logger.debug(f"[DB] Circuit breaker OPEN, skipping save for {site_key}/{worker_id}")
                return False

        pool = await get_db_pool()
        if pool is None:
            logger.debug(f"[DB] Pool not available, skipping save for {site_key}")
            if CIRCUIT_BREAKER_AVAILABLE and get_circuit_breaker:
                get_circuit_breaker().record_failure()
            return False

        try:
            # Ensure site_key is in state
            if "site_key" not in state:
                state["site_key"] = site_key

            async with pool.acquire() as conn:
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

            logger.debug(f"[DB] Saved state for {site_key}/{worker_id}")

            # Record success in circuit breaker
            if CIRCUIT_BREAKER_AVAILABLE and get_circuit_breaker:
                get_circuit_breaker().record_success()

            return True

        except Exception as e:
            logger.error(f"[DB] Error saving state for {site_key}/{worker_id}: {e}")

            # Record failure in circuit breaker
            if CIRCUIT_BREAKER_AVAILABLE and get_circuit_breaker:
                get_circuit_breaker().record_failure(e)

            return False

    @staticmethod
    async def clear_crawl_state_keys(
        site_key: str,
        keys_to_remove: list[str],
        worker_id: str = "main",
    ) -> bool:
        """
        Clear specific keys from crawl state.

        Args:
            site_key: Site identifier
            keys_to_remove: List of keys to remove
            worker_id: Worker instance ID

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                # Load current state_data
                state_data = await conn.fetchval(
                    "SELECT state_data FROM crawl_states WHERE site_key = $1 AND worker_id = $2",
                    site_key,
                    worker_id,
                )

                if state_data is None:
                    logger.debug(f"[DB] No state to clear for {site_key}/{worker_id}")
                    return True

                # Remove keys
                for key in keys_to_remove:
                    if key in state_data:
                        del state_data[key]
                        logger.debug(f"[DB] Removed key '{key}' from state")

                    # Also clear specific columns if needed
                    if key == "current_genre_url":
                        await conn.execute(
                            """
                            UPDATE crawl_states
                            SET current_genre_url = NULL,
                                current_genre_name = NULL,
                                current_story_url = NULL,
                                current_story_index_in_genre = NULL,
                                processed_chapter_urls_for_current_story = ARRAY[]::TEXT[]
                            WHERE site_key = $1 AND worker_id = $2
                            """,
                            site_key,
                            worker_id,
                        )
                    elif key == "current_story_url":
                        await conn.execute(
                            """
                            UPDATE crawl_states
                            SET current_story_url = NULL,
                                processed_chapter_urls_for_current_story = ARRAY[]::TEXT[]
                            WHERE site_key = $1 AND worker_id = $2
                            """,
                            site_key,
                            worker_id,
                        )

                # Update state_data
                await conn.execute(
                    "UPDATE crawl_states SET state_data = $1, last_save_at = NOW() WHERE site_key = $2 AND worker_id = $3",
                    json.dumps(state_data),
                    site_key,
                    worker_id,
                )

            logger.debug(f"[DB] Cleared keys from state for {site_key}/{worker_id}")
            return True

        except Exception as e:
            logger.error(f"[DB] Error clearing state keys: {e}")
            return False

    @staticmethod
    async def clear_all_crawl_state(site_key: str, worker_id: str = "main") -> bool:
        """
        Delete crawl state completely.

        Args:
            site_key: Site identifier
            worker_id: Worker instance ID

        Returns:
            True if successful, False otherwise
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM crawl_states WHERE site_key = $1 AND worker_id = $2",
                    site_key,
                    worker_id,
                )

            logger.info(f"[DB] Deleted all state for {site_key}/{worker_id}")
            return True

        except Exception as e:
            logger.error(f"[DB] Error deleting state: {e}")
            return False

    @staticmethod
    async def merge_missing_workers_state(site_key: str) -> bool:
        """
        Merge all missing worker states into main worker state.

        Args:
            site_key: Site identifier

        Returns:
            True if successful
        """
        pool = await get_db_pool()
        if pool is None:
            return False

        try:
            async with pool.acquire() as conn:
                # Get all worker states for this site
                rows = await conn.fetch(
                    """
                    SELECT worker_id, globally_completed_story_urls
                    FROM crawl_states
                    WHERE site_key = $1
                    """,
                    site_key,
                )

                if not rows:
                    return True

                # Merge all globally_completed_story_urls
                all_completed = set()
                worker_ids_to_delete = []

                for row in rows:
                    if row["globally_completed_story_urls"]:
                        all_completed.update(row["globally_completed_story_urls"])

                    # Mark non-main workers for deletion
                    if row["worker_id"] != "main":
                        worker_ids_to_delete.append(row["worker_id"])

                # Update main worker with merged data
                if all_completed:
                    await conn.execute(
                        """
                        INSERT INTO crawl_states (
                            site_key, worker_id, globally_completed_story_urls, state_data
                        ) VALUES ($1, 'main', $2, $3)
                        ON CONFLICT (site_key, worker_id)
                        DO UPDATE SET
                            globally_completed_story_urls = EXCLUDED.globally_completed_story_urls,
                            state_data = crawl_states.state_data || EXCLUDED.state_data
                        """,
                        site_key,
                        sorted(all_completed),
                        json.dumps({"site_key": site_key}),
                    )

                # Delete missing worker states
                if worker_ids_to_delete:
                    await conn.execute(
                        "DELETE FROM crawl_states WHERE site_key = $1 AND worker_id = ANY($2)",
                        site_key,
                        worker_ids_to_delete,
                    )

            logger.info(f"[DB] Merged {len(worker_ids_to_delete)} worker states for {site_key}")
            return True

        except Exception as e:
            logger.error(f"[DB] Error merging worker states: {e}")
            return False


# Convenience aliases
db_load_state = DatabaseStateStorage.load_crawl_state
db_save_state = DatabaseStateStorage.save_crawl_state
db_clear_state_keys = DatabaseStateStorage.clear_crawl_state_keys
db_clear_all_state = DatabaseStateStorage.clear_all_crawl_state
db_merge_workers = DatabaseStateStorage.merge_missing_workers_state

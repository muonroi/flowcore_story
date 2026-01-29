"""
Metadata Store Factory
======================

Factory để chọn giữa SQLite và MongoDB metadata store.

Environment Variables:
    USE_MONGODB: Set to 'true' to use MongoDB (default: false - SQLite)
    MONGODB_URI: MongoDB connection string (required if USE_MONGODB=true)
    MONGODB_DB: Database name (default: storyflow)

Usage:
    from flowcore_story.utils.metadata_store_factory import get_metadata_store

    # Will return MongoMetadataStore or MetadataStore depending on USE_MONGODB
    store = await get_metadata_store()

    # Upsert (sync or async depending on backend)
    if asyncio.iscoroutinefunction(store.upsert):
        await store.upsert(metadata, fallback_key="story-123")
    else:
        store.upsert(metadata, fallback_key="story-123")
"""

import os
from typing import Optional, Union

from flowcore_story.utils.logger import logger

# Global cache for stores
_metadata_store_cache: Union["MetadataStore", "MongoMetadataStore"] | None = None


def _should_use_mongodb() -> bool:
    """Check if MongoDB should be used instead of SQLite"""
    use_mongo = os.getenv("USE_MONGODB", "false").lower() in ("true", "1", "yes")
    return use_mongo


async def get_metadata_store(
    db_path: str | None = None
) -> Union["MetadataStore", "MongoMetadataStore"] | None:
    """
    Get metadata store (SQLite or MongoDB depending on configuration)

    Args:
        db_path: SQLite database path (used only if USE_MONGODB=false)

    Returns:
        MetadataStore or MongoMetadataStore instance, or None if not configured
    """
    global _metadata_store_cache

    use_mongodb = _should_use_mongodb()

    if use_mongodb:
        # Use MongoDB
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            logger.warning("[META] USE_MONGODB=true but MONGODB_URI not set! Falling back to SQLite")
            use_mongodb = False
        else:
            from flowcore_story.utils.metadata_store_mongo import MongoMetadataStore

            if _metadata_store_cache is None or not isinstance(_metadata_store_cache, MongoMetadataStore):

                mongo_db = os.getenv("MONGODB_DB", "storyflow")
                _metadata_store_cache = MongoMetadataStore(
                    mongo_uri=mongo_uri,
                    db_name=mongo_db
                )

                # Ensure indexes
                await _metadata_store_cache.ensure_indexes()

                logger.info(f"[META] Using MongoDB: {mongo_db}")

            return _metadata_store_cache

    # Use SQLite
    if not db_path:
        return None

    from pathlib import Path

    from flowcore_story.utils.metadata_store import MetadataStore

    if _metadata_store_cache is None or not isinstance(_metadata_store_cache, MetadataStore):

        # Check if path changed
        if _metadata_store_cache and hasattr(_metadata_store_cache, 'db_path'):
            if Path(db_path) != _metadata_store_cache.db_path:
                _metadata_store_cache = MetadataStore(db_path)
        else:
            _metadata_store_cache = MetadataStore(db_path)

        logger.info(f"[META] Using SQLite: {db_path}")

    return _metadata_store_cache


def get_metadata_store_sync(db_path: str | None = None) -> Optional["MetadataStore"]:
    """
    Synchronous version - supports both SQLite and MongoDB via asyncio.run()

    Args:
        db_path: SQLite database path

    Returns:
        MetadataStore or MongoMetadataStore instance or None
    """
    if _should_use_mongodb():
        # Run async version in sync context
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If event loop is already running, return None and log
                logger.debug("[META] Event loop already running, cannot use sync function. Use async version.")
                return None
            else:
                return loop.run_until_complete(get_metadata_store(db_path))
        except RuntimeError:
            # No event loop, create one
            return asyncio.run(get_metadata_store(db_path))

    if not db_path:
        return None

    from flowcore_story.utils.metadata_store import get_metadata_store as get_sqlite_store
    return get_sqlite_store(db_path)


# For backward compatibility
def get_metadata_store_compat(db_path: str | None = None):
    """
    Backward compatible function that works like the old get_metadata_store

    Only supports SQLite (for code that hasn't been migrated yet)
    """
    return get_metadata_store_sync(db_path)


# Type imports for type hints (avoiding circular imports)
if False:  # TYPE_CHECKING
    from flowcore_story.utils.metadata_store import MetadataStore
    from flowcore_story.utils.metadata_store_mongo import MongoMetadataStore

"""
Category Store Factory
======================

Factory to choose between SQLite and MongoDB category store.

Environment Variables:
    USE_MONGODB: Set to 'true' to use MongoDB (default: false - SQLite)
    MONGODB_URI: MongoDB connection string (required if USE_MONGODB=true)
    MONGODB_DB: Database name (default: storyflow)

Usage:
    from flowcore_story.core.category_store_factory import create_category_store

    # Will return MongoCategoryStore or CategoryStore depending on USE_MONGODB
    store = await create_category_store(db_path="/app/state/categories.db")

    # Or synchronous version (SQLite only)
    store = create_category_store_sync(db_path="/app/state/categories.db")
"""

import os
from typing import Union

from flowcore_story.utils.logger import logger


def _should_use_mongodb() -> bool:
    """Check if MongoDB should be used instead of SQLite"""
    use_mongo = os.getenv("USE_MONGODB", "false").lower() in ("true", "1", "yes")
    return use_mongo


async def create_category_store(
    db_path: str
) -> Union["CategoryStore", "MongoCategoryStore"]:
    """
    Create category store (SQLite or MongoDB depending on configuration)

    Args:
        db_path: SQLite database path (used only if USE_MONGODB=false)

    Returns:
        CategoryStore or MongoCategoryStore instance
    """
    use_mongodb = _should_use_mongodb()

    if use_mongodb:
        # Use MongoDB
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            logger.warning("[CATEGORY] USE_MONGODB=true but MONGODB_URI not set! Falling back to SQLite")
            use_mongodb = False
        else:
            from flowcore_story.core.category_store_mongo import MongoCategoryStore

            mongo_db = os.getenv("MONGODB_DB", "storyflow")
            store = MongoCategoryStore(
                mongo_uri=mongo_uri,
                db_name=mongo_db
            )

            # Ensure indexes
            await store.ensure_indexes()

            logger.info(f"[CATEGORY] Using MongoDB: {mongo_db}")
            return store

    # Use SQLite
    from flowcore_story.core.category_store import CategoryStore

    logger.info(f"[CATEGORY] Using SQLite: {db_path}")
    return CategoryStore(db_path)


def create_category_store_sync(db_path: str) -> "CategoryStore":
    """
    Synchronous version - only returns SQLite store

    Args:
        db_path: SQLite database path

    Returns:
        CategoryStore instance

    Raises:
        RuntimeError: If USE_MONGODB=true (MongoDB requires async)
    """
    if _should_use_mongodb():
        raise RuntimeError(
            "USE_MONGODB=true but create_category_store_sync() called! "
            "Use async create_category_store() instead"
        )

    from flowcore_story.core.category_store import CategoryStore

    logger.info(f"[CATEGORY] Using SQLite: {db_path}")
    return CategoryStore(db_path)


# Type imports for type hints (avoiding circular imports)
if False:  # TYPE_CHECKING
    from flowcore_story.core.category_store import CategoryStore
    from flowcore_story.core.category_store_mongo import MongoCategoryStore

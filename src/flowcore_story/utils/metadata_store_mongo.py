"""
MongoDB Adapter for MetadataStore
==================================

Drop-in replacement for SQLite-based metadata_store.py

Benefits:
- Better concurrency (multiple writers)
- Native JSON/BSON storage
- Horizontal scalability
- Advanced indexing and querying
- TTL indexes for auto-cleanup

Configuration (Environment Variables):
- MONGODB_URI: MongoDB connection string (default: mongodb://localhost:27017)
- MONGODB_DB: Database name (default: storyflow)
- MONGODB_METADATA_COLLECTION: Collection name (default: story_metadata)

Usage:
    from flowcore_story.utils.metadata_store_mongo import MongoMetadataStore

    store = MongoMetadataStore(
        mongo_uri="mongodb://admin:password@mongodb:27017",
        db_name="storyflow"
    )

    # Ensure indexes (run once at startup)
    await store.ensure_indexes()

    # Upsert metadata
    await store.upsert(metadata, fallback_key="story-123")

    # Fetch metadata
    result = await store.fetch("story-123")
    result = await store.fetch_by_url("https://example.com/story")
"""

from datetime import datetime, timedelta
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from flowcore_story.utils.logger import logger


class MongoMetadataStore:
    """Persist story metadata in MongoDB"""

    def __init__(
        self,
        mongo_uri: str,
        db_name: str = "storyflow",
        collection_name: str = "story_metadata",
    ):
        """
        Initialize MongoDB metadata store

        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            collection_name: Collection name for metadata
        """
        self.client: AsyncIOMotorClient = AsyncIOMotorClient(mongo_uri)
        self.db: AsyncIOMotorDatabase = self.client[db_name]
        self.collection: AsyncIOMotorCollection = self.db[collection_name]

        logger.info(f"[META][MongoDB] Initialized with db={db_name}, collection={collection_name}")

    async def ensure_indexes(self):
        """Create indexes if not exist"""
        try:
            # Unique index on key
            await self.collection.create_index("key", unique=True, name="idx_key_unique")

            # Index on URL for fast lookups
            await self.collection.create_index("url", name="idx_url")

            # Index on slug
            await self.collection.create_index("slug", name="idx_slug")

            # Index on updated_at for time-based queries
            await self.collection.create_index([("updated_at", -1)], name="idx_updated_at")

            # Index on site_key for filtering by site
            await self.collection.create_index("metadata.site_key", name="idx_site_key")

            # Compound index for site + status queries
            await self.collection.create_index(
                [("metadata.site_key", 1), ("status", 1)],
                name="idx_site_status"
            )

            # TTL index for auto-deletion (optional - 180 days)
            # await self.collection.create_index(
            #     "updated_at",
            #     expireAfterSeconds=180 * 24 * 3600,
            #     name="idx_ttl_cleanup"
            # )

            logger.info("[META][MongoDB] Indexes ensured")

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to create indexes: {e}")
            raise

    def _normalize_key(self, metadata: dict[str, Any], fallback_key: str) -> str:
        """
        Extract key from metadata

        Priority: url > slug > fallback_key

        Args:
            metadata: Story metadata dictionary
            fallback_key: Key to use if metadata has no url or slug

        Returns:
            Normalized key string
        """
        return metadata.get("url") or metadata.get("slug") or fallback_key

    async def upsert(self, metadata: dict[str, Any], fallback_key: str) -> dict[str, Any]:
        """
        Insert or update metadata

        Args:
            metadata: Story metadata dictionary
            fallback_key: Key to use if metadata has no url or slug

        Returns:
            Dictionary with key and data
        """
        key = self._normalize_key(metadata, fallback_key)

        # Extract common fields for fast queries
        doc = {
            "key": key,
            "url": metadata.get("url"),
            "slug": metadata.get("slug"),
            "title": metadata.get("title"),
            "author": metadata.get("author"),
            "description": metadata.get("description"),
            "categories": metadata.get("categories", []),
            "cover": metadata.get("cover"),
            "status": metadata.get("status"),
            "metadata": metadata,  # Store full metadata as BSON
            "updated_at": datetime.utcnow(),
        }

        try:
            result = await self.collection.update_one(
                {"key": key},
                {
                    "$set": doc,
                    "$setOnInsert": {"created_at": datetime.utcnow()}
                },
                upsert=True
            )

            action = "inserted" if result.upserted_id else "updated"
            logger.debug(f"[META][MongoDB] {action.capitalize()} metadata for key={key}")

            return {"key": key, "data": metadata}

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to upsert metadata for key={key}: {e}")
            raise

    async def fetch(self, key: str) -> dict[str, Any] | None:
        """
        Fetch metadata by key

        Args:
            key: Story key (url, slug, or custom key)

        Returns:
            Dictionary with key and data, or None if not found
        """
        try:
            doc = await self.collection.find_one({"key": key})
            if not doc:
                logger.debug(f"[META][MongoDB] Key not found: {key}")
                return None

            return {
                "key": doc["key"],
                "data": doc.get("metadata", {})
            }

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to fetch metadata for key={key}: {e}")
            return None

    async def fetch_by_url(self, url: str) -> dict[str, Any] | None:
        """
        Fetch metadata by URL

        Args:
            url: Story URL

        Returns:
            Dictionary with key and data, or None if not found
        """
        try:
            doc = await self.collection.find_one({"url": url})
            if not doc:
                logger.debug(f"[META][MongoDB] URL not found: {url}")
                return None

            return {
                "key": doc["key"],
                "data": doc.get("metadata", {})
            }

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to fetch metadata by URL {url}: {e}")
            return None

    async def fetch_by_slug(self, slug: str) -> dict[str, Any] | None:
        """
        Fetch metadata by slug

        Args:
            slug: Story slug

        Returns:
            Dictionary with key and data, or None if not found
        """
        try:
            doc = await self.collection.find_one({"slug": slug})
            if not doc:
                logger.debug(f"[META][MongoDB] Slug not found: {slug}")
                return None

            return {
                "key": doc["key"],
                "data": doc.get("metadata", {})
            }

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to fetch metadata by slug {slug}: {e}")
            return None

    async def delete(self, key: str) -> bool:
        """
        Delete metadata by key

        Args:
            key: Story key

        Returns:
            True if deleted, False if not found
        """
        try:
            result = await self.collection.delete_one({"key": key})
            deleted = result.deleted_count > 0

            if deleted:
                logger.info(f"[META][MongoDB] Deleted metadata for key={key}")
            else:
                logger.debug(f"[META][MongoDB] Key not found for deletion: {key}")

            return deleted

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to delete metadata for key={key}: {e}")
            return False

    async def count(self) -> int:
        """
        Count total metadata records

        Returns:
            Total number of metadata records
        """
        try:
            return await self.collection.count_documents({})
        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to count documents: {e}")
            return 0

    async def count_by_site(self, site_key: str) -> int:
        """
        Count metadata records for a specific site

        Args:
            site_key: Site identifier

        Returns:
            Number of metadata records for the site
        """
        try:
            return await self.collection.count_documents({"metadata.site_key": site_key})
        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to count by site {site_key}: {e}")
            return 0

    async def cleanup_old(self, days: int = 180) -> int:
        """
        Delete metadata older than N days

        Args:
            days: Number of days to keep

        Returns:
            Number of deleted records
        """
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = await self.collection.delete_many({
                "updated_at": {"$lt": cutoff}
            })

            deleted_count = result.deleted_count
            if deleted_count > 0:
                logger.info(f"[META][MongoDB] Cleaned up {deleted_count} old metadata records (>{days} days)")

            return deleted_count

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to cleanup old metadata: {e}")
            return 0

    async def get_statistics(self) -> dict[str, Any]:
        """
        Get metadata store statistics

        Returns:
            Dictionary with statistics
        """
        try:
            total_count = await self.count()

            # Count by status
            pipeline = [
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }}
            ]
            status_counts = {}
            async for doc in self.collection.aggregate(pipeline):
                status_counts[doc["_id"] or "unknown"] = doc["count"]

            # Count by site
            pipeline = [
                {"$group": {
                    "_id": "$metadata.site_key",
                    "count": {"$sum": 1}
                }}
            ]
            site_counts = {}
            async for doc in self.collection.aggregate(pipeline):
                site_counts[doc["_id"] or "unknown"] = doc["count"]

            # Recent updates
            recent_cutoff = datetime.utcnow() - timedelta(days=7)
            recent_count = await self.collection.count_documents({
                "updated_at": {"$gte": recent_cutoff}
            })

            return {
                "total_count": total_count,
                "status_counts": status_counts,
                "site_counts": site_counts,
                "recent_updates_7d": recent_count,
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"[META][MongoDB] Failed to get statistics: {e}")
            return {"error": str(e)}

    async def close(self):
        """Close MongoDB connection"""
        self.client.close()
        logger.info("[META][MongoDB] Connection closed")

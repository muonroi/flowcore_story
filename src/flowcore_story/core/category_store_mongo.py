"""
MongoDB Adapter for CategoryStore
==================================

Drop-in replacement for SQLite-based category_store.py

Purpose: Track category → story discovery snapshots over time

Benefits over SQLite:
- Better concurrency (no single-writer limitation)
- Native BSON storage (no JSON serialization overhead)
- Advanced indexing for faster queries
- Horizontal scalability
- Aggregation pipeline for analytics

Configuration (Environment Variables):
- MONGODB_URI: MongoDB connection string (default: mongodb://localhost:27017)
- MONGODB_DB: Database name (default: storyflow)

Collections:
1. category_snapshots - Snapshot metadata
2. categories - Category information
3. stories - Story information
4. category_story_membership - Many-to-many relationship

Usage:
    from flowcore_story.core.category_store_mongo import MongoCategoryStore

    store = MongoCategoryStore(
        mongo_uri="mongodb://admin:password@mongodb:27017",
        db_name="storyflow"
    )

    # Ensure indexes
    await store.ensure_indexes()

    # Persist snapshot
    snapshot_info = await store.persist_snapshot(site_key, crawl_plan)
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import MongoClient

from flowcore.utils.logger import logger


@dataclass(frozen=True)
class SnapshotInfo:
    """Metadata returned after persisting a snapshot."""

    id: str  # MongoDB ObjectId as string
    site_key: str
    version: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "site_key": self.site_key,
            "version": self.version,
            "created_at": self.created_at,
        }


class MongoCategoryStore:
    """Persist category → story discovery snapshots in MongoDB"""

    def __init__(
        self,
        mongo_uri: str,
        db_name: str = "storyflow",
    ):
        """
        Initialize MongoDB category store

        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
        """
        self.client: AsyncIOMotorClient = AsyncIOMotorClient(mongo_uri)
        self.db: AsyncIOMotorDatabase = self.client[db_name]
        self._mongo_uri = mongo_uri
        self._mongo_db_name = db_name
        self._sync_client: MongoClient | None = None

        # Collections
        self.snapshots = self.db.category_snapshots
        self.categories = self.db.categories
        self.stories = self.db.stories
        self.membership = self.db.category_story_membership

        logger.info(f"[CATEGORY_STORE][MongoDB] Initialized with db={db_name}")

    async def ensure_indexes(self):
        """Create indexes if not exist"""
        try:
            # Snapshots indexes
            await self.snapshots.create_index(
                [("site_key", 1), ("version", 1)],
                unique=True,
                name="idx_site_version_unique"
            )
            await self.snapshots.create_index([("created_at", -1)], name="idx_created_at")

            # Categories indexes
            await self.categories.create_index(
                [("site_key", 1), ("url", 1)],
                unique=True,
                name="idx_site_url_unique"
            )
            await self.categories.create_index("site_key", name="idx_site_key")
            await self.categories.create_index("last_seen_snapshot_id", name="idx_last_seen")

            # Stories indexes
            await self.stories.create_index(
                [("site_key", 1), ("url", 1)],
                unique=True,
                name="idx_site_url_unique"
            )
            await self.stories.create_index("site_key", name="idx_site_key")
            await self.stories.create_index([("updated_at", -1)], name="idx_updated_at")

            # Membership indexes
            await self.membership.create_index(
                [("category_id", 1), ("story_id", 1), ("first_snapshot_id", 1)],
                unique=True,
                name="idx_category_story_snapshot_unique"
            )
            await self.membership.create_index("category_id", name="idx_category_id")
            await self.membership.create_index("story_id", name="idx_story_id")
            await self.membership.create_index(
                [("category_id", 1), ("ended_snapshot_id", 1)],
                name="idx_category_active"
            )

            logger.info("[CATEGORY_STORE][MongoDB] Indexes ensured")

        except Exception as e:
            logger.error(f"[CATEGORY_STORE][MongoDB] Failed to create indexes: {e}")
            raise

    def _now(self) -> str:
        """Get current timestamp in ISO format"""
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _generate_version(self) -> str:
        """Generate version string based on timestamp"""
        return datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

    def _dedupe_version(self, base_version: str) -> str:
        """Add unique suffix to version to avoid collision"""
        return f"{base_version}+{uuid.uuid4().hex[:8]}"

    async def persist_snapshot(
        self,
        site_key: str,
        crawl_plan,
        *,
        version: str | None = None,
    ) -> SnapshotInfo:
        """
        Persist a category snapshot

        Args:
            site_key: Site identifier
            crawl_plan: CrawlPlan instance with categories
            version: Optional version string (generated if not provided)

        Returns:
            SnapshotInfo with snapshot metadata
        """
        from flowcore_story.core.crawl_planner import CrawlPlan

        if not isinstance(crawl_plan, CrawlPlan):
            raise TypeError("crawl_plan must be a CrawlPlan instance")

        created_at = self._now()
        base_version = version or self._generate_version()

        # Ensure unique snapshot
        snapshot_id, version_value = await self._ensure_snapshot(
            site_key, base_version, created_at
        )

        seen_category_ids = []

        # Process each category
        for category in crawl_plan.categories:
            category_id = await self._upsert_category(
                site_key,
                category.name,
                category.url,
                category.metadata,
                category.raw_genre,
                len(category.stories),
                snapshot_id,
                created_at,
            )
            seen_category_ids.append(category_id)

            # Sync membership
            await self._sync_category_membership(
                category_id,
                site_key,
                category.stories,
                snapshot_id,
                created_at,
            )

        # Mark missing categories as ended
        await self._finalise_missing_categories(
            site_key,
            seen_category_ids,
            snapshot_id,
            created_at,
        )

        logger.info(
            "[CATEGORY_STORE][MongoDB] Persisted snapshot %s for %s with %d categories",
            version_value,
            site_key,
            len(crawl_plan.categories),
        )

        return SnapshotInfo(
            id=str(snapshot_id),
            site_key=site_key,
            version=version_value,
            created_at=created_at,
        )

    async def _ensure_snapshot(
        self,
        site_key: str,
        version: str,
        created_at: str,
    ) -> tuple[ObjectId, str]:
        """
        Insert snapshot, handling version collision

        Returns:
            (snapshot_id, final_version)
        """
        attempt = 0
        base_version = version

        while True:
            try_version = base_version if attempt == 0 else self._dedupe_version(base_version)

            try:
                doc = {
                    "site_key": site_key,
                    "version": try_version,
                    "created_at": created_at,
                    "metadata": {},
                }

                result = await self.snapshots.insert_one(doc)
                return result.inserted_id, try_version

            except Exception as e:
                if "duplicate key error" in str(e).lower() or "E11000" in str(e):
                    logger.warning(
                        "[CATEGORY_STORE][MongoDB] Snapshot version collision for %s/%s; generating unique suffix",
                        site_key,
                        try_version,
                    )
                    attempt += 1
                    if attempt > 5:
                        raise RuntimeError("Failed to create unique snapshot version after 5 attempts") from e
                else:
                    raise

    async def _upsert_category(
        self,
        site_key: str,
        name: str,
        url: str,
        metadata: dict[str, Any] | None,
        raw_data: dict[str, Any] | None,
        story_count: int,
        snapshot_id: ObjectId,
        timestamp: str,
    ) -> ObjectId:
        """
        Insert or update category

        Returns:
            category_id (ObjectId)
        """
        # Check if category exists
        existing = await self.categories.find_one({"site_key": site_key, "url": url})

        if existing:
            # Update existing category
            await self.categories.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "name": name,
                        "metadata": metadata,
                        "raw_data": raw_data,
                        "story_count": story_count,
                        "last_seen_snapshot_id": snapshot_id,
                        "ended_snapshot_id": None,
                        "updated_at": timestamp,
                    }
                }
            )
            return existing["_id"]

        # Insert new category
        doc = {
            "site_key": site_key,
            "name": name,
            "url": url,
            "metadata": metadata,
            "raw_data": raw_data,
            "story_count": story_count,
            "first_snapshot_id": snapshot_id,
            "last_seen_snapshot_id": snapshot_id,
            "ended_snapshot_id": None,
            "created_at": timestamp,
            "updated_at": timestamp,
        }

        result = await self.categories.insert_one(doc)
        return result.inserted_id

    async def _upsert_story(
        self,
        site_key: str,
        story: dict[str, Any],
        timestamp: str,
    ) -> ObjectId:
        """
        Insert or update story

        Returns:
            story_id (ObjectId)
        """
        story_url = story.get("url")
        if not story_url:
            raise ValueError("Story must have a url")

        # Check if story exists
        existing = await self.stories.find_one({"site_key": site_key, "url": story_url})

        if existing:
            # Update existing story
            await self.stories.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "title": story.get("title"),
                        "data": story,
                        "updated_at": timestamp,
                    }
                }
            )
            return existing["_id"]

        # Insert new story
        doc = {
            "site_key": site_key,
            "url": story_url,
            "title": story.get("title"),
            "data": story,
            "created_at": timestamp,
            "updated_at": timestamp,
        }

        result = await self.stories.insert_one(doc)
        return result.inserted_id

    async def _sync_category_membership(
        self,
        category_id: ObjectId,
        site_key: str,
        stories: Iterable[dict[str, Any]],
        snapshot_id: ObjectId,
        timestamp: str,
    ) -> None:
        """Sync category-story membership for a snapshot"""
        seen_story_ids = set()

        for index, story in enumerate(stories, start=1):
            if not isinstance(story, dict):
                continue

            story_url = story.get("url")
            if not story_url:
                logger.warning(
                    "[CATEGORY_STORE][MongoDB] Skipping story without URL in category %s",
                    category_id
                )
                continue

            # Upsert story
            story_id = await self._upsert_story(site_key, story, timestamp)
            seen_story_ids.add(story_id)

            # Check if membership exists
            existing = await self.membership.find_one({
                "category_id": category_id,
                "story_id": story_id,
                "ended_snapshot_id": None
            })

            if existing:
                # Update existing membership
                await self.membership.update_one(
                    {"_id": existing["_id"]},
                    {
                        "$set": {
                            "last_seen_snapshot_id": snapshot_id,
                            "position": index,
                            "updated_at": timestamp,
                        }
                    }
                )
            else:
                # Insert new membership
                doc = {
                    "category_id": category_id,
                    "story_id": story_id,
                    "first_snapshot_id": snapshot_id,
                    "last_seen_snapshot_id": snapshot_id,
                    "ended_snapshot_id": None,
                    "position": index,
                    "created_at": timestamp,
                    "updated_at": timestamp,
                }

                try:
                    await self.membership.insert_one(doc)
                except Exception as e:
                    if "duplicate key error" in str(e).lower() or "E11000" in str(e):
                        # Race condition - membership created by another process
                        logger.debug(
                            "[CATEGORY_STORE][MongoDB] Membership already exists (race condition): cat=%s, story=%s",
                            category_id,
                            story_id
                        )
                    else:
                        raise

        # Mark stories no longer in this category as ended
        await self.membership.update_many(
            {
                "category_id": category_id,
                "story_id": {"$nin": list(seen_story_ids)},
                "ended_snapshot_id": None
            },
            {
                "$set": {
                    "ended_snapshot_id": snapshot_id,
                    "updated_at": timestamp,
                }
            }
        )

    async def _finalise_missing_categories(
        self,
        site_key: str,
        seen_category_ids: list[ObjectId],
        snapshot_id: ObjectId,
        timestamp: str,
    ) -> None:
        """Mark categories not seen in this snapshot as ended"""
        await self.categories.update_many(
            {
                "site_key": site_key,
                "_id": {"$nin": seen_category_ids},
                "ended_snapshot_id": None
            },
            {
                "$set": {
                    "ended_snapshot_id": snapshot_id,
                    "updated_at": timestamp,
                }
            }
        )

    async def get_statistics(self) -> dict[str, Any]:
        """Get category store statistics"""
        try:
            total_snapshots = await self.snapshots.count_documents({})
            total_categories = await self.categories.count_documents({})
            total_stories = await self.stories.count_documents({})
            total_membership = await self.membership.count_documents({})

            # Active vs ended
            active_categories = await self.categories.count_documents({"ended_snapshot_id": None})
            active_membership = await self.membership.count_documents({"ended_snapshot_id": None})

            # Recent snapshots
            recent_cutoff = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            recent_snapshots = await self.snapshots.count_documents({
                "created_at": {"$gte": recent_cutoff}
            })

            return {
                "total_snapshots": total_snapshots,
                "total_categories": total_categories,
                "active_categories": active_categories,
                "total_stories": total_stories,
                "total_membership": total_membership,
                "active_membership": active_membership,
                "recent_snapshots": recent_snapshots,
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"[CATEGORY_STORE][MongoDB] Failed to get statistics: {e}")
            return {"error": str(e)}

    async def close(self):
        """Close MongoDB connection"""
        self.client.close()
        if self._sync_client is not None:
            self._sync_client.close()
            self._sync_client = None
        logger.info("[CATEGORY_STORE][MongoDB] Connection closed")

    # ------------------------------------------------------------------
    # Compatibility helpers for synchronous consumers

    def get_sync_db(self):
        """Return a synchronous PyMongo database handle for read-heavy tasks."""
        if self._sync_client is None:
            self._sync_client = MongoClient(self._mongo_uri)
        return self._sync_client[self._mongo_db_name]

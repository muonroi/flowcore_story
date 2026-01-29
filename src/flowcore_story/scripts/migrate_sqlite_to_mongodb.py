"""
SQLite to MongoDB Migration Script
===================================

Migrates data from SQLite databases to MongoDB:
1. MetadataStore: /app/state/metadata.db → story_metadata collection
2. CategoryStore: /app/state/categories.db → 4 collections

Usage:
    # Development (local):
    python scripts/migrate_sqlite_to_mongodb.py

    # Production (Docker):
    docker exec storyflow-app python scripts/migrate_sqlite_to_mongodb.py

Environment Variables:
    SQLITE_METADATA_PATH: Path to metadata.db (default: /app/state/metadata.db)
    SQLITE_CATEGORY_PATH: Path to categories.db (default: /app/state/categories.db)
    MONGODB_URI: MongoDB connection string
    MONGODB_DB: Database name (default: storyflow)
    DRY_RUN: If 'true', only show what would be migrated without writing

Output:
    - Migration statistics
    - Error reports
    - Validation results
"""

import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient


class MigrationStats:
    """Track migration statistics"""

    def __init__(self):
        self.metadata_migrated = 0
        self.metadata_errors = 0
        self.snapshots_migrated = 0
        self.categories_migrated = 0
        self.stories_migrated = 0
        self.membership_migrated = 0
        self.migration_errors = []

    def report(self):
        """Print migration report"""
        print("\n" + "=" * 80)
        print("MIGRATION REPORT")
        print("=" * 80)
        print("Metadata Store:")
        print(f"  [OK] Migrated: {self.metadata_migrated}")
        print(f"  [ERROR] Errors: {self.metadata_errors}")
        print()
        print("Category Store:")
        print(f"  [OK] Snapshots: {self.snapshots_migrated}")
        print(f"  [OK] Categories: {self.categories_migrated}")
        print(f"  [OK] Stories: {self.stories_migrated}")
        print(f"  [OK] Membership: {self.membership_migrated}")
        print()

        if self.migration_errors:
            print(f"Errors ({len(self.migration_errors)}):")
            for error in self.migration_errors[:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(self.migration_errors) > 10:
                print(f"  ... and {len(self.migration_errors) - 10} more")
        else:
            print("[OK] No errors!")

        print("=" * 80)


async def migrate_metadata_store(
    sqlite_path: str,
    mongo_uri: str,
    db_name: str,
    dry_run: bool = False
) -> MigrationStats:
    """
    Migrate MetadataStore from SQLite to MongoDB

    Args:
        sqlite_path: Path to metadata.db
        db_name: MongoDB database name
        dry_run: If True, don't write to MongoDB

    Returns:
        MigrationStats with results
    """
    stats = MigrationStats()

    print("\n" + "=" * 80)
    print("1. MIGRATING METADATA STORE")
    print("=" * 80)
    print(f"Source: {sqlite_path}")
    print(f"Target: {db_name}.story_metadata")
    print(f"Dry run: {dry_run}")
    print()

    # Check if SQLite file exists
    if not os.path.exists(sqlite_path):
        print(f"[WARN] SQLite file not found: {sqlite_path}")
        print("  Skipping metadata migration")
        return stats

    # Connect to MongoDB
    client = AsyncIOMotorClient(mongo_uri)
    db = client[db_name]
    collection = db.story_metadata

    if not dry_run:
        # Create indexes
        print("Creating indexes...")
        await collection.create_index("key", unique=True)
        await collection.create_index("url")
        await collection.create_index("updated_at")
        await collection.create_index("metadata.site_key")
        print("[OK] Indexes created")

    # Connect to SQLite
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    try:
        # Get total count
        cursor = conn.execute("SELECT COUNT(*) as count FROM story_metadata")
        total = cursor.fetchone()["count"]
        print(f"Total records to migrate: {total}")

        # Migrate each row
        cursor = conn.execute("SELECT * FROM story_metadata")

        for row in cursor:
            try:
                # Parse JSON metadata
                metadata = json.loads(row["data"])

                # Create document
                doc = {
                    "key": row["key"],
                    "url": metadata.get("url"),
                    "slug": metadata.get("slug"),
                    "title": metadata.get("title"),
                    "author": metadata.get("author"),
                    "description": metadata.get("description"),
                    "categories": metadata.get("categories", []),
                    "cover": metadata.get("cover"),
                    "status": metadata.get("status"),
                    "metadata": metadata,
                    "created_at": datetime.fromisoformat(row["updated_at"]),
                    "updated_at": datetime.fromisoformat(row["updated_at"]),
                }

                if not dry_run:
                    # Insert to MongoDB (upsert)
                    await collection.update_one(
                        {"key": row["key"]},
                        {"$set": doc},
                        upsert=True
                    )

                stats.metadata_migrated += 1

                if stats.metadata_migrated % 1000 == 0:
                    print(f"  Migrated {stats.metadata_migrated}/{total} records...")

            except Exception as e:
                stats.metadata_errors += 1
                stats.migration_errors.append(f"Metadata key={row['key']}: {e}")

        print(f"[OK] Migrated {stats.metadata_migrated} metadata records")

    except Exception as e:
        print(f"[ERROR] Fatal error migrating metadata: {e}")
        stats.migration_errors.append(f"Fatal metadata error: {e}")

    finally:
        conn.close()
        client.close()

    return stats


async def migrate_category_store(
    sqlite_path: str,
    mongo_uri: str,
    db_name: str,
    dry_run: bool = False
) -> MigrationStats:
    """
    Migrate CategoryStore from SQLite to MongoDB

    Args:
        sqlite_path: Path to categories.db
        mongo_uri: MongoDB connection string
        db_name: MongoDB database name
        dry_run: If True, don't write to MongoDB

    Returns:
        MigrationStats with results
    """
    stats = MigrationStats()

    print("\n" + "=" * 80)
    print("2. MIGRATING CATEGORY STORE")
    print("=" * 80)
    print(f"Source: {sqlite_path}")
    print(f"Target: {db_name}.category_* collections")
    print(f"Dry run: {dry_run}")
    print()

    # Check if SQLite file exists
    if not os.path.exists(sqlite_path):
        print(f"[WARN] SQLite file not found: {sqlite_path}")
        print("  Skipping category migration")
        return stats

    # Connect to MongoDB
    client = AsyncIOMotorClient(mongo_uri)
    db = client[db_name]

    snapshots_coll = db.category_snapshots
    categories_coll = db.categories
    stories_coll = db.stories
    membership_coll = db.category_story_membership

    if not dry_run:
        # Create indexes
        print("Creating indexes...")
        await snapshots_coll.create_index([("site_key", 1), ("version", 1)], unique=True)
        await categories_coll.create_index([("site_key", 1), ("url", 1)], unique=True)
        await stories_coll.create_index([("site_key", 1), ("url", 1)], unique=True)
        await membership_coll.create_index(
            [("category_id", 1), ("story_id", 1), ("first_snapshot_id", 1)],
            unique=True
        )
        print("[OK] Indexes created")

    # Connect to SQLite
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    # Track ID mappings: SQLite ID → MongoDB ObjectId
    snapshot_id_map: dict[int, ObjectId] = {}
    category_id_map: dict[int, ObjectId] = {}
    story_id_map: dict[int, ObjectId] = {}

    try:
        # Migrate snapshots
        print("\nMigrating snapshots...")
        cursor = conn.execute("SELECT COUNT(*) as count FROM category_snapshots")
        total = cursor.fetchone()["count"]
        print(f"  Total: {total}")

        cursor = conn.execute("SELECT * FROM category_snapshots")
        for row in cursor:
            try:
                doc = {
                    "site_key": row["site_key"],
                    "version": row["version"],
                    "created_at": row["created_at"],
                    "metadata": {},
                }

                if not dry_run:
                    result = await snapshots_coll.insert_one(doc)
                    snapshot_id_map[row["id"]] = result.inserted_id
                else:
                    snapshot_id_map[row["id"]] = ObjectId()

                stats.snapshots_migrated += 1

            except Exception as e:
                stats.migration_errors.append(f"Snapshot id={row['id']}: {e}")

        print(f"  [OK] Migrated {stats.snapshots_migrated} snapshots")

        # Migrate categories
        print("\nMigrating categories...")
        cursor = conn.execute("SELECT COUNT(*) as count FROM categories")
        total = cursor.fetchone()["count"]
        print(f"  Total: {total}")

        cursor = conn.execute("SELECT * FROM categories")
        for row in cursor:
            try:
                metadata = json.loads(row["metadata"]) if row["metadata"] else None
                raw_data = json.loads(row["raw_data"]) if row["raw_data"] else None

                doc = {
                    "site_key": row["site_key"],
                    "name": row["name"],
                    "url": row["url"],
                    "metadata": metadata,
                    "raw_data": raw_data,
                    "story_count": row["story_count"],
                    "first_snapshot_id": snapshot_id_map.get(row["first_snapshot_id"]),
                    "last_seen_snapshot_id": snapshot_id_map.get(row["last_seen_snapshot_id"]),
                    "ended_snapshot_id": snapshot_id_map.get(row["ended_snapshot_id"]) if row["ended_snapshot_id"] else None,
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }

                if not dry_run:
                    result = await categories_coll.insert_one(doc)
                    category_id_map[row["id"]] = result.inserted_id
                else:
                    category_id_map[row["id"]] = ObjectId()

                stats.categories_migrated += 1

                if stats.categories_migrated % 1000 == 0:
                    print(f"    Migrated {stats.categories_migrated}/{total}...")

            except Exception as e:
                stats.migration_errors.append(f"Category id={row['id']}: {e}")

        print(f"  [OK] Migrated {stats.categories_migrated} categories")

        # Migrate stories
        print("\nMigrating stories...")
        cursor = conn.execute("SELECT COUNT(*) as count FROM stories")
        total = cursor.fetchone()["count"]
        print(f"  Total: {total}")

        cursor = conn.execute("SELECT * FROM stories")
        for row in cursor:
            try:
                data = json.loads(row["data"]) if row["data"] else {}

                doc = {
                    "site_key": row["site_key"],
                    "url": row["url"],
                    "title": row["title"],
                    "data": data,
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }

                if not dry_run:
                    result = await stories_coll.insert_one(doc)
                    story_id_map[row["id"]] = result.inserted_id
                else:
                    story_id_map[row["id"]] = ObjectId()

                stats.stories_migrated += 1

                if stats.stories_migrated % 1000 == 0:
                    print(f"    Migrated {stats.stories_migrated}/{total}...")

            except Exception as e:
                stats.migration_errors.append(f"Story id={row['id']}: {e}")

        print(f"  [OK] Migrated {stats.stories_migrated} stories")

        # Migrate membership
        print("\nMigrating membership...")
        cursor = conn.execute("SELECT COUNT(*) as count FROM category_story_membership")
        total = cursor.fetchone()["count"]
        print(f"  Total: {total}")

        cursor = conn.execute("SELECT * FROM category_story_membership")
        for row in cursor:
            try:
                doc = {
                    "category_id": category_id_map.get(row["category_id"]),
                    "story_id": story_id_map.get(row["story_id"]),
                    "first_snapshot_id": snapshot_id_map.get(row["first_snapshot_id"]),
                    "last_seen_snapshot_id": snapshot_id_map.get(row["last_seen_snapshot_id"]),
                    "ended_snapshot_id": snapshot_id_map.get(row["ended_snapshot_id"]) if row["ended_snapshot_id"] else None,
                    "position": row["position"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }

                if not dry_run:
                    await membership_coll.insert_one(doc)

                stats.membership_migrated += 1

                if stats.membership_migrated % 5000 == 0:
                    print(f"    Migrated {stats.membership_migrated}/{total}...")

            except Exception as e:
                stats.migration_errors.append(f"Membership id={row['id']}: {e}")

        print(f"  [OK] Migrated {stats.membership_migrated} membership records")

    except Exception as e:
        print(f"[ERROR] Fatal error migrating category store: {e}")
        stats.migration_errors.append(f"Fatal category error: {e}")

    finally:
        conn.close()
        client.close()

    return stats


async def main():
    """Main entry point"""
    print("=" * 80)
    print("SQLite to MongoDB Migration")
    print("=" * 80)

    # Load configuration
    sqlite_metadata_path = os.getenv("SQLITE_METADATA_PATH", "/app/state/metadata.db")
    sqlite_category_path = os.getenv("SQLITE_CATEGORY_PATH", "/app/state/categories.db")
    mongo_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB", "storyflow")
    dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

    print(f"SQLite metadata: {sqlite_metadata_path}")
    print(f"SQLite categories: {sqlite_category_path}")
    print(f"MongoDB URI: {mongo_uri}")
    print(f"MongoDB DB: {db_name}")
    print(f"Dry run: {dry_run}")

    if not mongo_uri:
        print("\n[ERROR] ERROR: MONGODB_URI environment variable not set!")
        sys.exit(1)

    # Confirm before proceeding
    if not dry_run:
        print("\n[WARN] WARNING: This will write to MongoDB!")
        print("  Press Ctrl+C to cancel, or Enter to continue...")
        try:
            input()
        except KeyboardInterrupt:
            print("\n[ERROR] Cancelled by user")
            sys.exit(0)

    # Run migrations
    all_stats = MigrationStats()

    # Migrate metadata
    metadata_stats = await migrate_metadata_store(
        sqlite_metadata_path,
        mongo_uri,
        db_name,
        dry_run
    )
    all_stats.metadata_migrated = metadata_stats.metadata_migrated
    all_stats.metadata_errors = metadata_stats.metadata_errors
    all_stats.migration_errors.extend(metadata_stats.migration_errors)

    # Migrate categories
    category_stats = await migrate_category_store(
        sqlite_category_path,
        mongo_uri,
        db_name,
        dry_run
    )
    all_stats.snapshots_migrated = category_stats.snapshots_migrated
    all_stats.categories_migrated = category_stats.categories_migrated
    all_stats.stories_migrated = category_stats.stories_migrated
    all_stats.membership_migrated = category_stats.membership_migrated
    all_stats.migration_errors.extend(category_stats.migration_errors)

    # Print final report
    all_stats.report()

    # Exit with appropriate code
    if all_stats.migration_errors:
        sys.exit(1)
    else:
        print("\n[OK] Migration completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())

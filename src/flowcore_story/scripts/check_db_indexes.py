#!/usr/bin/env python
"""
Check and optimize PostgreSQL database indexes for state storage.
Provides recommendations for index improvements.
"""

import asyncio
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from flowcore_story.storage.db_pool import get_db_pool


async def check_indexes():
    """Check current indexes and provide recommendations."""
    print("\n" + "="*60)
    print("POSTGRESQL INDEX OPTIMIZATION CHECK")
    print("="*60)

    pool = await get_db_pool()
    if not pool:
        print("❌ Database pool not available")
        return False

    try:
        async with pool.acquire() as conn:
            # Check 1: List current indexes
            print("\n📊 Current Indexes on crawl_states:")
            print("-" * 60)

            indexes = await conn.fetch("""
                SELECT
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE tablename = 'crawl_states'
                ORDER BY indexname;
            """)

            for idx in indexes:
                print(f"✓ {idx['indexname']}")
                print(f"  {idx['indexdef']}")
                print()

            # Check 2: Table statistics
            print("\n📈 Table Statistics:")
            print("-" * 60)

            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT site_key) as unique_sites,
                    COUNT(DISTINCT worker_id) as unique_workers,
                    pg_size_pretty(pg_total_relation_size('crawl_states')) as total_size,
                    pg_size_pretty(pg_relation_size('crawl_states')) as table_size,
                    pg_size_pretty(pg_indexes_size('crawl_states')) as indexes_size
                FROM crawl_states;
            """)

            print(f"Total rows: {stats['total_rows']}")
            print(f"Unique sites: {stats['unique_sites']}")
            print(f"Unique workers: {stats['unique_workers']}")
            print(f"Total size: {stats['total_size']}")
            print(f"Table size: {stats['table_size']}")
            print(f"Indexes size: {stats['indexes_size']}")

            # Check 3: Index usage statistics
            print("\n📊 Index Usage Statistics:")
            print("-" * 60)

            usage = await conn.fetch("""
                SELECT
                    schemaname,
                    tablename,
                    indexname,
                    idx_scan as scans,
                    idx_tup_read as tuples_read,
                    idx_tup_fetch as tuples_fetched
                FROM pg_stat_user_indexes
                WHERE tablename = 'crawl_states'
                ORDER BY idx_scan DESC;
            """)

            for u in usage:
                print(f"{u['indexname']}:")
                print(f"  Scans: {u['scans']}")
                print(f"  Tuples read: {u['tuples_read']}")
                print(f"  Tuples fetched: {u['tuples_fetched']}")
                print()

            # Check 4: Missing indexes recommendations
            print("\n💡 Index Recommendations:")
            print("-" * 60)

            recommendations = []

            # Check if composite index exists for (site_key, worker_id)
            has_composite = any('site_key' in idx['indexdef'] and 'worker_id' in idx['indexdef']
                               for idx in indexes)

            if not has_composite:
                recommendations.append({
                    "priority": "HIGH",
                    "reason": "Primary key lookup optimization",
                    "sql": "CREATE INDEX CONCURRENTLY idx_crawl_states_site_worker ON crawl_states(site_key, worker_id);"
                })

            # Check if updated_at index exists
            has_updated_at = any('updated_at' in idx['indexdef'] for idx in indexes)

            if not has_updated_at:
                recommendations.append({
                    "priority": "MEDIUM",
                    "reason": "Query by time range optimization",
                    "sql": "CREATE INDEX CONCURRENTLY idx_crawl_states_updated_at ON crawl_states(updated_at DESC);"
                })

            # Check if partial index for active states exists
            has_partial_active = any('current_page IS NOT NULL' in idx['indexdef'] for idx in indexes)

            if not has_partial_active:
                recommendations.append({
                    "priority": "LOW",
                    "reason": "Filter active crawls efficiently",
                    "sql": "CREATE INDEX CONCURRENTLY idx_crawl_states_active ON crawl_states(site_key) WHERE current_page IS NOT NULL;"
                })

            if recommendations:
                for i, rec in enumerate(recommendations, 1):
                    print(f"{i}. [{rec['priority']}] {rec['reason']}")
                    print(f"   SQL: {rec['sql']}")
                    print()
            else:
                print("✓ All recommended indexes are present")

            # Check 5: Bloat detection
            print("\n🔍 Table Bloat Check:")
            print("-" * 60)

            bloat = await conn.fetchrow("""
                SELECT
                    pg_size_pretty(pg_total_relation_size('crawl_states')) as total_size,
                    pg_size_pretty(pg_relation_size('crawl_states')) as table_size,
                    (SELECT count(*) FROM crawl_states) as live_tuples,
                    (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'crawl_states') as dead_tuples
            """)

            dead_ratio = 0
            if bloat['live_tuples'] > 0:
                dead_ratio = (bloat['dead_tuples'] / bloat['live_tuples']) * 100

            print(f"Live tuples: {bloat['live_tuples']}")
            print(f"Dead tuples: {bloat['dead_tuples']}")
            print(f"Dead ratio: {dead_ratio:.2f}%")

            if dead_ratio > 20:
                print(f"\n⚠️  WARNING: High dead tuple ratio ({dead_ratio:.2f}%)")
                print("   Consider running VACUUM ANALYZE crawl_states;")
            elif dead_ratio > 10:
                print(f"\nℹ️  INFO: Moderate dead tuple ratio ({dead_ratio:.2f}%)")
                print("   VACUUM will run automatically, but manual VACUUM may help")
            else:
                print("\n✓ Table bloat is within acceptable range")

            # Check 6: Query performance test
            print("\n⚡ Query Performance Test:")
            print("-" * 60)

            import time

            # Test 1: Select by site_key and worker_id
            start = time.monotonic()
            await conn.fetchrow("""
                SELECT * FROM crawl_states
                WHERE site_key = 'xtruyen' AND worker_id = 'main'
            """)
            duration_ms = (time.monotonic() - start) * 1000
            print(f"Select by (site_key, worker_id): {duration_ms:.2f}ms")

            # Test 2: Select recent states
            start = time.monotonic()
            await conn.fetch("""
                SELECT site_key, worker_id, updated_at
                FROM crawl_states
                ORDER BY updated_at DESC
                LIMIT 10
            """)
            duration_ms = (time.monotonic() - start) * 1000
            print(f"Select 10 most recent: {duration_ms:.2f}ms")

            # Test 3: Upsert simulation
            start = time.monotonic()
            await conn.execute("""
                INSERT INTO crawl_states (site_key, worker_id, state_data)
                VALUES ('test_site', 'test_worker', '{}')
                ON CONFLICT (site_key, worker_id)
                DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            """)
            duration_ms = (time.monotonic() - start) * 1000
            print(f"Upsert operation: {duration_ms:.2f}ms")

            # Cleanup test data
            await conn.execute("DELETE FROM crawl_states WHERE site_key = 'test_site'")

            return True

    except Exception as e:
        print(f"\n❌ Error checking indexes: {e}")
        import traceback
        traceback.print_exc()
        return False


async def apply_recommended_indexes():
    """Apply recommended indexes if missing."""
    print("\n" + "="*60)
    print("APPLYING RECOMMENDED INDEXES")
    print("="*60)

    pool = await get_db_pool()
    if not pool:
        print("❌ Database pool not available")
        return False

    try:
        async with pool.acquire() as conn:
            # Check existing indexes
            indexes = await conn.fetch("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'crawl_states'
            """)

            # Index 1: Composite (site_key, worker_id)
            has_composite = any('site_key' in idx['indexdef'] and 'worker_id' in idx['indexdef']
                               for idx in indexes)

            if not has_composite:
                print("\n⏳ Creating composite index on (site_key, worker_id)...")
                await conn.execute("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crawl_states_site_worker
                    ON crawl_states(site_key, worker_id)
                """)
                print("✓ Created idx_crawl_states_site_worker")
            else:
                print("✓ Composite index already exists")

            # Index 2: updated_at for time-based queries
            has_updated_at = any('updated_at' in idx['indexdef'] for idx in indexes)

            if not has_updated_at:
                print("\n⏳ Creating index on updated_at...")
                await conn.execute("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crawl_states_updated_at
                    ON crawl_states(updated_at DESC)
                """)
                print("✓ Created idx_crawl_states_updated_at")
            else:
                print("✓ updated_at index already exists")

            print("\n✓ All recommended indexes are in place")
            return True

    except Exception as e:
        print(f"\n❌ Error applying indexes: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Check and optimize PostgreSQL indexes")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply recommended indexes"
    )

    args = parser.parse_args()

    if args.apply:
        success = await apply_recommended_indexes()
    else:
        success = await check_indexes()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())

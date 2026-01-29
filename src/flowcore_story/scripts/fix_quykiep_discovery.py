#!/usr/bin/env python3
"""
Fix quykiep discovery by resetting planning status.

The issue:
- genre_queue_metadata has planning_status='ready' for all quykiep genres
- But story_queue only has ~18 stories per genre (1 page)
- This means the initial planning was interrupted mid-way
- When crawler restarts, it sees 'ready' and skips rediscovery

Solution:
1. Reset planning_status to 'planning' for all quykiep genres
2. This forces the crawler to rediscover all stories on next run

Usage:
    docker exec crawler-producer python /app/scripts/fix_quykiep_discovery.py
"""
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, '/app/src')
os.chdir('/app/src')

from flowcore_story.storage.db_pool import get_db_pool
from flowcore_story.utils.logger import logger


async def reset_quykiep_genres():
    """Reset planning status for all quykiep genres."""
    print("\n" + "="*60)
    print("FIXING QUYKIEP DISCOVERY")
    print("="*60)

    pool = await get_db_pool()
    if not pool:
        print("❌ Database pool not available")
        return False

    async with pool.acquire() as conn:
        # 1. Check current state
        print("\n📊 Current genre_queue_metadata state:")
        rows = await conn.fetch("""
            SELECT genre_name, planning_status, total_stories, pending_count
            FROM genre_queue_metadata
            WHERE site_key = 'quykiep'
        """)
        for row in rows:
            print(f"  - {row['genre_name']}: status={row['planning_status']}, total={row['total_stories']}, pending={row['pending_count']}")

        # 2. Check story_queue
        row = await conn.fetchrow("""
            SELECT COUNT(*) as total FROM story_queue WHERE site_key = 'quykiep'
        """)
        print(f"\n📊 story_queue: {row['total']} entries")

        # 3. Reset planning status
        print("\n🔧 Resetting planning_status to 'pending' for all quykiep genres...")
        result = await conn.execute("""
            UPDATE genre_queue_metadata
            SET planning_status = 'pending',
                total_stories = 0,
                pending_count = 0,
                processing_count = 0,
                completed_count = 0,
                failed_count = 0,
                updated_at = NOW()
            WHERE site_key = 'quykiep'
        """)
        print(f"  ✅ Updated genre_queue_metadata: {result}")

        # 4. Delete existing queue entries (they're incomplete anyway)
        print("\n🗑️ Deleting incomplete story_queue entries for quykiep...")
        result = await conn.execute("""
            DELETE FROM story_queue
            WHERE site_key = 'quykiep' AND status IN ('pending', 'processing')
        """)
        print(f"  ✅ Deleted pending/processing entries: {result}")

        # 5. Reset genre_progress
        print("\n🔧 Resetting genre_progress for quykiep...")
        result = await conn.execute("""
            UPDATE genre_progress
            SET status = 'pending',
                processed_stories = 0,
                current_page = 1,
                crawled_pages = 0,
                updated_at = NOW(),
                completed_at = NULL
            WHERE site_key = 'quykiep'
        """)
        print(f"  ✅ Updated genre_progress: {result}")

        # 6. Verify final state
        print("\n📊 Final state after reset:")
        rows = await conn.fetch("""
            SELECT genre_name, planning_status, total_stories, pending_count
            FROM genre_queue_metadata
            WHERE site_key = 'quykiep'
        """)
        for row in rows:
            print(f"  - {row['genre_name']}: status={row['planning_status']}, total={row['total_stories']}, pending={row['pending_count']}")

        row = await conn.fetchrow("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'completed') as completed
            FROM story_queue WHERE site_key = 'quykiep'
        """)
        print(f"\n📊 story_queue: total={row['total']}, pending={row['pending']}, completed={row['completed']}")

    print("\n✅ Reset complete! Restart crawler-producer to rediscover quykiep stories.")
    print("="*60)
    return True


async def main():
    success = await reset_quykiep_genres()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

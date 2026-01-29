#!/usr/bin/env python3
"""
Test script để kiểm tra flow của crawl_planner.py

Kiểm tra:
1. build_category_plan() có được gọi không
2. story_queue.enqueue_batch() có lưu stories vào PostgreSQL không
3. genre_queue_metadata có được update đúng không

Usage:
    docker exec crawler-producer python /app/scripts/test_crawl_planner_flow.py
"""
import asyncio
import sys
import os

sys.path.insert(0, '/app/src')
os.chdir('/app/src')

from flowcore_story.adapters.quykiep_adapter import QuykiepAdapter
from flowcore_story.core.crawl_planner import build_category_plan
from flowcore_story.storage.story_queue import story_queue, genre_queue_metadata
from flowcore_story.storage.db_pool import get_db_pool
from flowcore_story.config import config as app_config


async def check_current_db_state():
    """Check current state of quykiep in database."""
    print("\n" + "="*70)
    print("CURRENT DATABASE STATE")
    print("="*70)

    pool = await get_db_pool()
    if not pool:
        print("❌ Database pool not available")
        return

    async with pool.acquire() as conn:
        # Check genre_queue_metadata
        print("\n📊 genre_queue_metadata:")
        rows = await conn.fetch("""
            SELECT genre_name, genre_url, planning_status, total_stories, pending_count
            FROM genre_queue_metadata
            WHERE site_key = 'quykiep'
        """)
        if rows:
            for row in rows:
                print(f"  - {row['genre_name']}: status={row['planning_status']}, "
                      f"total={row['total_stories']}, pending={row['pending_count']}")
        else:
            print("  (no entries)")

        # Check story_queue
        print("\n📊 story_queue:")
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'completed') as completed
            FROM story_queue
            WHERE site_key = 'quykiep'
        """)
        print(f"  total={row['total']}, pending={row['pending']}, completed={row['completed']}")


async def test_build_category_plan_with_force():
    """Test build_category_plan with force_discovery=True."""
    print("\n" + "="*70)
    print("TEST build_category_plan WITH force_discovery=True")
    print("="*70)

    adapter = QuykiepAdapter()
    site_key = "quykiep"

    # Get one genre to test
    genres = await adapter.get_genres()
    if not genres:
        print("❌ No genres found")
        return

    # Test with Kiếm Hiệp (small genre, ~11 pages)
    test_genre = next((g for g in genres if g['name'] == 'Kiếm Hiệp'), genres[0])
    print(f"\n📂 Testing genre: {test_genre['name']} ({test_genre['url']})")

    # Call build_category_plan with force_discovery=True
    print("\n🔄 Calling build_category_plan(force_discovery=True)...")

    plan = await build_category_plan(
        adapter,
        test_genre,
        site_key,
        position=1,
        total_genres=len(genres),
        max_pages=app_config.MAX_STORIES_PER_GENRE_PAGE,
        force_discovery=True,  # Force re-discovery
    )

    if plan:
        print(f"\n✅ Plan created:")
        print(f"  - name: {plan.name}")
        print(f"  - url: {plan.url}")
        print(f"  - stories: {len(plan.stories)}")
        print(f"  - planned_story_total: {plan.planned_story_total}")
        print(f"  - total_pages: {plan.total_pages}")
        print(f"  - crawled_pages: {plan.crawled_pages}")
    else:
        print("\n❌ Plan is None!")

    return plan


async def test_build_category_plan_without_force():
    """Test build_category_plan without force_discovery (default behavior)."""
    print("\n" + "="*70)
    print("TEST build_category_plan WITHOUT force_discovery (default)")
    print("="*70)

    adapter = QuykiepAdapter()
    site_key = "quykiep"

    genres = await adapter.get_genres()
    if not genres:
        print("❌ No genres found")
        return

    test_genre = next((g for g in genres if g['name'] == 'Kiếm Hiệp'), genres[0])
    print(f"\n📂 Testing genre: {test_genre['name']} ({test_genre['url']})")

    # Call build_category_plan without force_discovery
    print("\n🔄 Calling build_category_plan(force_discovery=False)...")

    plan = await build_category_plan(
        adapter,
        test_genre,
        site_key,
        position=1,
        total_genres=len(genres),
        max_pages=app_config.MAX_STORIES_PER_GENRE_PAGE,
        force_discovery=False,  # Default behavior
    )

    if plan:
        print(f"\n✅ Plan created:")
        print(f"  - name: {plan.name}")
        print(f"  - url: {plan.url}")
        print(f"  - stories: {len(plan.stories)} <- Check if this is 0 (skipped re-planning)")
        print(f"  - planned_story_total: {plan.planned_story_total}")
        print(f"  - total_pages: {plan.total_pages}")

        if len(plan.stories) == 0:
            print("\n⚠️ WARNING: stories=0 means re-planning was SKIPPED!")
            print("   This happens when genre_queue_metadata.planning_status = 'ready'")
    else:
        print("\n❌ Plan is None!")

    return plan


async def verify_stories_in_queue():
    """Verify stories were saved to PostgreSQL queue."""
    print("\n" + "="*70)
    print("VERIFY STORIES IN QUEUE AFTER PLANNING")
    print("="*70)

    pool = await get_db_pool()
    if not pool:
        print("❌ Database pool not available")
        return

    async with pool.acquire() as conn:
        # Check story_queue for Kiếm Hiệp
        print("\n📊 story_queue for Kiếm Hiệp:")
        rows = await conn.fetch("""
            SELECT story_url, story_title, status, discovery_index
            FROM story_queue
            WHERE site_key = 'quykiep' AND genre_name = 'Kiếm Hiệp'
            ORDER BY discovery_index
            LIMIT 10
        """)
        if rows:
            print(f"  Found {len(rows)} stories (showing first 10):")
            for row in rows:
                print(f"    [{row['discovery_index']}] {row['story_title'][:40]}... ({row['status']})")
        else:
            print("  (no stories)")

        # Count total
        row = await conn.fetchrow("""
            SELECT COUNT(*) as total FROM story_queue
            WHERE site_key = 'quykiep' AND genre_name = 'Kiếm Hiệp'
        """)
        print(f"\n  Total stories for Kiếm Hiệp: {row['total']}")


async def main():
    print("\n" + "="*70)
    print("CRAWL PLANNER FLOW TEST")
    print("="*70)
    print("Testing how build_category_plan saves stories to PostgreSQL queue")

    # Step 1: Check current state
    await check_current_db_state()

    # Step 2: Test without force (shows the bug if planning_status='ready')
    await test_build_category_plan_without_force()

    # Step 3: Verify queue state after test
    await verify_stories_in_queue()

    # Step 4: Test with force (should always re-discover)
    await test_build_category_plan_with_force()

    # Step 5: Verify queue state again
    await verify_stories_in_queue()

    # Final summary
    print("\n" + "="*70)
    print("ANALYSIS")
    print("="*70)
    print("""
The issue is in crawl_planner.py line 436:

    if not force_discovery and queue_meta.planning_status in ("ready", "processing"):
        # Return plan with stories=[] (skip re-planning)

When planning_status='ready':
1. build_category_plan() returns a plan with stories=[]
2. No new stories are enqueued to story_queue
3. The crawler tries to process from an empty plan

ROOT CAUSE: Initial planning was interrupted, only 1 page was crawled.
But planning_status was still set to 'ready'.

FIX OPTIONS:
1. Reset planning_status to 'pending' for quykiep genres
2. Or call build_category_plan with force_discovery=True
""")

    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Test script to verify database tracking is working correctly.
Run this after crawler has been running for a while.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from flowcore_story.storage.db_pool import get_db_pool, close_db_pool


async def test_crawl_states():
    """Test crawl_states table."""
    pool = await get_db_pool()
    if not pool:
        print("❌ Database pool not available")
        return False

    try:
        async with pool.acquire() as conn:
            # Check if current_genre_name is being populated
            rows = await conn.fetch(
                """
                SELECT site_key, worker_id, current_genre_url, current_genre_name,
                       array_length(processed_genre_urls, 1) as processed_genres,
                       array_length(globally_completed_story_urls, 1) as completed_stories
                FROM crawl_states
                ORDER BY updated_at DESC
                LIMIT 5
                """
            )

            print("\n" + "=" * 80)
            print("📊 CRAWL_STATES TABLE")
            print("=" * 80)

            if not rows:
                print("⚠️  No records found in crawl_states")
                return False

            for row in rows:
                print(f"\nSite: {row['site_key']} | Worker: {row['worker_id']}")
                print(f"  Current Genre URL: {row['current_genre_url'] or 'NULL'}")
                print(f"  Current Genre Name: {row['current_genre_name'] or '❌ NULL (PROBLEM!)'}")
                print(f"  Processed Genres: {row['processed_genres'] or 0}")
                print(f"  Completed Stories: {row['completed_stories'] or 0}")

            # Check for NULL current_genre_name
            null_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM crawl_states
                WHERE current_genre_url IS NOT NULL AND current_genre_name IS NULL
                """
            )

            if null_count > 0:
                print(f"\n❌ WARNING: {null_count} records have current_genre_url but NULL current_genre_name")
                print("   This indicates the fix is not yet applied or crawler needs restart")
                return False
            else:
                print("\n✅ All records with current_genre_url have current_genre_name")

            return True

    except Exception as e:
        print(f"❌ Error checking crawl_states: {e}")
        return False


async def test_story_progress():
    """Test story_progress table."""
    pool = await get_db_pool()
    if not pool:
        return False

    try:
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM story_progress")

            print("\n" + "=" * 80)
            print("📚 STORY_PROGRESS TABLE")
            print("=" * 80)
            print(f"Total Stories Tracked: {total}")

            if total == 0:
                print("⚠️  No records found - tracking not yet active")
                print("   Start crawler to populate this table")
                return False

            # Get status breakdown
            status_breakdown = await conn.fetch(
                """
                SELECT status, COUNT(*) as count,
                       SUM(crawled_chapters) as total_crawled,
                       SUM(total_chapters) as total_expected
                FROM story_progress
                GROUP BY status
                ORDER BY count DESC
                """
            )

            print("\nStatus Breakdown:")
            for row in status_breakdown:
                print(f"  {row['status']:12} : {row['count']:5} stories "
                      f"({row['total_crawled'] or 0}/{row['total_expected'] or 0} chapters)")

            # Show recent stories
            recent = await conn.fetch(
                """
                SELECT story_id, title, status, crawled_chapters, total_chapters,
                       missing_chapters, genre_name
                FROM story_progress
                ORDER BY updated_at DESC
                LIMIT 5
                """
            )

            print("\nRecent Stories:")
            for row in recent:
                title = row['title'][:50] if row['title'] else 'Unknown'
                progress = f"{row['crawled_chapters']}/{row['total_chapters']}"
                print(f"  [{row['status']:10}] {title:50} {progress:10} ({row['genre_name'] or 'N/A'})")

            return True

    except Exception as e:
        print(f"❌ Error checking story_progress: {e}")
        return False


async def test_site_health():
    """Test site_health table."""
    pool = await get_db_pool()
    if not pool:
        return False

    try:
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM site_health")

            print("\n" + "=" * 80)
            print("🏥 SITE_HEALTH TABLE")
            print("=" * 80)
            print(f"Total Sites Tracked: {total}")

            if total == 0:
                print("⚠️  No records found - health tracking not yet active")
                print("   This will be populated as requests succeed/fail")
                return False

            rows = await conn.fetch(
                """
                SELECT site_key, success_count, failure_count, failure_rate,
                       challenge_count_1h, challenge_count_24h, last_error
                FROM site_health
                ORDER BY (success_count + failure_count) DESC
                """
            )

            for row in rows:
                total_req = row['success_count'] + row['failure_count']
                rate = row['failure_rate'] or 0
                health_status = "❌ CRITICAL" if rate > 0.5 else "⚠️  WARNING" if rate > 0.3 else "✅ HEALTHY"

                print(f"\n{row['site_key']}:")
                print(f"  Status: {health_status}")
                print(f"  Requests: {row['success_count']} success / {row['failure_count']} failed "
                      f"(rate: {rate:.1%})")
                print(f"  Challenges: {row['challenge_count_1h'] or 0}/1h, "
                      f"{row['challenge_count_24h'] or 0}/24h")
                if row['last_error']:
                    print(f"  Last Error: {row['last_error'][:80]}...")

            return True

    except Exception as e:
        print(f"❌ Error checking site_health: {e}")
        return False


async def test_story_events():
    """Test story_events table."""
    pool = await get_db_pool()
    if not pool:
        return False

    try:
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM story_events")

            print("\n" + "=" * 80)
            print("📋 STORY_EVENTS TABLE")
            print("=" * 80)
            print(f"Total Events Logged: {total}")

            if total == 0:
                print("⚠️  No records found - event logging not yet active")
                return False

            # Event type breakdown
            event_breakdown = await conn.fetch(
                """
                SELECT event_type, COUNT(*) as count
                FROM story_events
                GROUP BY event_type
                ORDER BY count DESC
                """
            )

            print("\nEvent Type Breakdown:")
            for row in event_breakdown:
                print(f"  {row['event_type']:15} : {row['count']:6} events")

            # Recent events
            recent = await conn.fetch(
                """
                SELECT story_id, event_type, category_id, created_at
                FROM story_events
                ORDER BY created_at DESC
                LIMIT 10
                """
            )

            print("\nRecent Events:")
            for row in recent:
                story_id = row['story_id'][:60] if row['story_id'] else 'Unknown'
                print(f"  [{row['event_type']:10}] {story_id} ({row['category_id']})")

            return True

    except Exception as e:
        print(f"❌ Error checking story_events: {e}")
        return False


async def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("🔍 DATABASE TRACKING VERIFICATION")
    print("=" * 80)

    results = {
        "crawl_states": await test_crawl_states(),
        "story_progress": await test_story_progress(),
        "site_health": await test_site_health(),
        "story_events": await test_story_events(),
    }

    await close_db_pool()

    print("\n" + "=" * 80)
    print("📊 SUMMARY")
    print("=" * 80)

    for table, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL/PENDING"
        print(f"{table:20} : {status}")

    all_passed = all(results.values())

    print("\n" + "=" * 80)
    if all_passed:
        print("✅ All tracking tables are working correctly!")
    else:
        print("⚠️  Some tables need attention (see details above)")
        print("\nNext steps:")
        print("1. If crawl_states shows NULL current_genre_name, restart crawler")
        print("2. If other tables are empty, they will populate as crawler runs")
    print("=" * 80)

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(asyncio.run(main()))

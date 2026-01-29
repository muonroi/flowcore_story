#!/usr/bin/env python3
"""
Debug script for quykiep discovery issues.

This script tests the discovery process for quykiep site to identify
why only ~100 stories are being enqueued instead of thousands.

Usage:
    docker exec crawler-producer python /app/scripts/debug_quykiep_discovery.py
"""
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, '/app/src')
os.chdir('/app/src')

from flowcore_story.adapters.quykiep_adapter import QuykiepAdapter
from flowcore_story.apps.scraper import make_request
from flowcore_story.analyze.quykiep_parse import parse_story_list, extract_next_data
from flowcore_story.utils.logger import logger


async def test_direct_fetch(url: str) -> dict:
    """Test direct fetch with proxy."""
    print(f"\n{'='*60}")
    print(f"Testing URL: {url}")
    print('='*60)

    response = await make_request(url, site_key="quykiep", method="GET")

    if not response:
        print("❌ FAILED: No response received")
        return {"success": False, "error": "no_response"}

    if not response.text:
        print("❌ FAILED: Empty response text")
        return {"success": False, "error": "empty_text"}

    # Check for Cloudflare
    if "Just a moment" in response.text or "Enable JavaScript" in response.text:
        print("⚠️ WARNING: Cloudflare challenge detected!")
        return {"success": False, "error": "cloudflare_challenge"}

    # Check for __NEXT_DATA__
    next_data = extract_next_data(response.text)
    if not next_data:
        print("❌ FAILED: No __NEXT_DATA__ found")
        print(f"Response preview: {response.text[:500]}...")
        return {"success": False, "error": "no_next_data"}

    print("✅ SUCCESS: Got valid response with __NEXT_DATA__")
    return {"success": True, "next_data": next_data, "response": response}


async def test_genre_discovery():
    """Test genre list discovery."""
    print("\n" + "="*60)
    print("TESTING GENRE DISCOVERY")
    print("="*60)

    adapter = QuykiepAdapter()
    genres = await adapter.get_genres()

    print(f"\n📂 Found {len(genres)} genres:")
    for g in genres:
        print(f"  - {g['name']}: {g['url']}")

    return genres


async def test_story_list(genre_url: str, genre_name: str, max_pages: int = 3):
    """Test story list discovery with pagination."""
    print("\n" + "="*60)
    print(f"TESTING STORY LIST: {genre_name}")
    print("="*60)

    adapter = QuykiepAdapter()
    all_stories = []

    for page in range(1, max_pages + 1):
        print(f"\n📄 Page {page}:")

        stories, max_page = await adapter.get_stories_in_genre(genre_url, page)

        if not stories:
            print(f"  ❌ No stories on page {page}")
            break

        print(f"  ✅ Found {len(stories)} stories (max_page={max_page})")
        all_stories.extend(stories)

        # Show first few stories
        for s in stories[:3]:
            print(f"    - {s['title'][:50]}... (chapters: {s.get('total_chapters', '?')})")

        if page >= max_page:
            print(f"  ℹ️ Reached max page {max_page}")
            break

        await asyncio.sleep(1)  # Be nice to the server

    print(f"\n📊 Total stories found: {len(all_stories)}")
    return all_stories


async def check_database_state():
    """Check database state for quykiep."""
    print("\n" + "="*60)
    print("CHECKING DATABASE STATE")
    print("="*60)

    try:
        from flowcore_story.storage.db_pool import get_db_pool
        pool = await get_db_pool()

        if not pool:
            print("❌ Database pool not available")
            return

        async with pool.acquire() as conn:
            # Check genre_progress
            rows = await conn.fetch("""
                SELECT genre_name, genre_url, total_stories, processed_stories, status, updated_at
                FROM genre_progress
                WHERE site_key = 'quykiep'
                ORDER BY genre_name
            """)

            print(f"\n📊 genre_progress ({len(rows)} rows):")
            for row in rows:
                print(f"  - {row['genre_name']}: total={row['total_stories']}, processed={row['processed_stories']}, status={row['status']}")

            # Check story_queue
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE status = 'processing') as processing,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed
                FROM story_queue
                WHERE site_key = 'quykiep'
            """)

            print(f"\n📊 story_queue:")
            print(f"  - Total: {row['total']}")
            print(f"  - Pending: {row['pending']}")
            print(f"  - Processing: {row['processing']}")
            print(f"  - Completed: {row['completed']}")
            print(f"  - Failed: {row['failed']}")

            # Check genre_queue_metadata
            rows = await conn.fetch("""
                SELECT genre_name, genre_url, planning_status, total_stories, pending_count
                FROM genre_queue_metadata
                WHERE site_key = 'quykiep'
            """)

            print(f"\n📊 genre_queue_metadata ({len(rows)} rows):")
            for row in rows:
                print(f"  - {row['genre_name']}: status={row['planning_status']}, total={row['total_stories']}, pending={row['pending_count']}")

    except Exception as e:
        print(f"❌ Database check failed: {e}")
        import traceback
        traceback.print_exc()


async def test_parse_real_response():
    """Test parsing a real genre page response."""
    print("\n" + "="*60)
    print("TESTING PARSE WITH REAL RESPONSE")
    print("="*60)

    test_url = "https://quykiep.com/truyen-huyen-huyen-ln"
    result = await test_direct_fetch(test_url)

    if not result["success"]:
        print(f"❌ Cannot test parse: {result.get('error')}")
        return

    # Parse stories
    response = result["response"]
    stories, max_page = parse_story_list(response.text, "https://quykiep.com")

    print(f"\n📊 Parse results:")
    print(f"  - Stories found: {len(stories)}")
    print(f"  - Max page: {max_page}")

    if stories:
        print("\n📝 Sample stories:")
        for s in stories[:5]:
            print(f"  - {s['title'][:50]}... | chapters: {s.get('total_chapters', '?')}")

    # Check __NEXT_DATA__ structure
    next_data = result["next_data"]
    page_props = next_data.get('props', {}).get('pageProps', {})

    print(f"\n🔍 __NEXT_DATA__ structure:")
    print(f"  - pageProps keys: {list(page_props.keys())}")
    print(f"  - data count: {len(page_props.get('data', []))}")
    print(f"  - total: {page_props.get('total', 'N/A')}")


async def main():
    """Run all diagnostic tests."""
    print("\n" + "="*60)
    print("QUYKIEP DISCOVERY DEBUG SCRIPT")
    print("="*60)
    print(f"Working directory: {os.getcwd()}")
    print(f"Python path: {sys.path[:3]}")

    # 1. Test direct fetch
    await test_direct_fetch("https://quykiep.com/truyen-huyen-huyen-ln")

    # 2. Test parse
    await test_parse_real_response()

    # 3. Check database state
    await check_database_state()

    # 4. Test genre discovery
    genres = await test_genre_discovery()

    # 5. Test story list for first genre
    if genres:
        first_genre = genres[5] if len(genres) > 5 else genres[0]  # Huyền Huyễn
        await test_story_list(first_genre['url'], first_genre['name'], max_pages=5)

    print("\n" + "="*60)
    print("DEBUG COMPLETE")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

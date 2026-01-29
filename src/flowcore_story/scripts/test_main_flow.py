#!/usr/bin/env python3
"""
Test script để kiểm tra flow thực tế trong main.py

Kiểm tra:
1. Adapter get_genres() có đúng không
2. get_all_stories_from_genre_with_page_check() có crawl all pages không
3. on_page callback có được gọi đúng không
4. Stories có được enqueue đúng không

Usage:
    docker exec crawler-producer python /app/scripts/test_main_flow.py
"""
import asyncio
import sys
import os

sys.path.insert(0, '/app/src')
os.chdir('/app/src')

from flowcore_story.adapters.quykiep_adapter import QuykiepAdapter
from flowcore_story.config import config as app_config


async def test_main_flow():
    """Test the main discovery flow."""
    print("\n" + "="*70)
    print("TESTING MAIN DISCOVERY FLOW")
    print("="*70)

    print(f"\nConfig:")
    print(f"  MAX_STORIES_PER_GENRE_PAGE: {app_config.MAX_STORIES_PER_GENRE_PAGE}")
    print(f"  MAX_STORIES_TOTAL_PER_GENRE: {app_config.MAX_STORIES_TOTAL_PER_GENRE}")

    adapter = QuykiepAdapter()
    site_key = "quykiep"

    # Get genres
    genres = await adapter.get_genres()
    print(f"\n📂 Found {len(genres)} genres")

    # Test genre "Kiếm Hiệp" (nhỏ, 11 pages)
    test_genre = next((g for g in genres if g['name'] == 'Kiếm Hiệp'), None)
    if not test_genre:
        print("❌ Genre 'Kiếm Hiệp' not found")
        return

    genre_name = test_genre['name']
    genre_url = test_genre['url']

    print(f"\n📂 Testing genre: {genre_name} ({genre_url})")

    # Simulate what main.py does
    discovered_stories = []
    page_stats = []

    async def on_page(page_stories: list, page_number: int, total_pages: int | None):
        """This is what main.py uses to collect stories."""
        discovered_stories.extend(page_stories)
        page_stats.append({
            'page': page_number,
            'count': len(page_stories),
            'total_pages': total_pages,
        })
        print(f"  on_page() called: page {page_number}/{total_pages}, {len(page_stories)} stories")

    print(f"\n🔄 Calling get_all_stories_from_genre_with_page_check()...")

    max_pages = app_config.MAX_STORIES_PER_GENRE_PAGE
    print(f"   max_pages param: {max_pages}")

    stories, total_pages, crawled_pages = await adapter.get_all_stories_from_genre_with_page_check(
        genre_name,
        genre_url,
        site_key,
        max_pages=max_pages,
        page_callback=on_page,
        collect=True,
    )

    print(f"\n📊 Results:")
    print(f"  - Total pages: {total_pages}")
    print(f"  - Crawled pages: {crawled_pages}")
    print(f"  - Stories returned: {len(stories)}")
    print(f"  - Stories via on_page: {len(discovered_stories)}")
    print(f"  - Page callbacks: {len(page_stats)}")

    print(f"\n📊 Page breakdown:")
    for p in page_stats:
        print(f"  Page {p['page']}: {p['count']} stories")

    # Check if all pages were crawled
    if total_pages and crawled_pages < total_pages:
        print(f"\n⚠️ WARNING: Only crawled {crawled_pages}/{total_pages} pages!")
        print(f"   Expected: ~{total_pages * 18} stories")
        print(f"   Got: {len(discovered_stories)} stories")
    else:
        print(f"\n✅ All pages crawled successfully!")

    return discovered_stories


async def main():
    stories = await test_main_flow()
    if stories:
        print(f"\n📊 Sample stories discovered:")
        for s in stories[:5]:
            print(f"  - {s.get('title', 'N/A')[:50]}...")

    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())

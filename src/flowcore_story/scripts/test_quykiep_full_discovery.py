#!/usr/bin/env python3
"""
Test script để kiểm tra flow discovery hoàn chỉnh của quykiep.

Flow đúng:
1. Get all genres/categories từ site
2. Với mỗi genre, discovery ALL pages, lấy ALL stories
3. Đếm tổng số stories có thể discover

Usage:
    docker exec crawler-producer python /app/scripts/test_quykiep_full_discovery.py
"""
import asyncio
import sys
import os

sys.path.insert(0, '/app/src')
os.chdir('/app/src')

from flowcore_story.adapters.quykiep_adapter import QuykiepAdapter
from flowcore_story.apps.scraper import make_request
from flowcore_story.analyze.quykiep_parse import parse_story_list, extract_next_data


async def test_step1_get_genres():
    """Step 1: Test lấy danh sách genres từ site."""
    print("\n" + "="*70)
    print("STEP 1: GET ALL GENRES FROM SITE")
    print("="*70)

    adapter = QuykiepAdapter()
    genres = await adapter.get_genres()

    print(f"\n✅ Adapter trả về {len(genres)} genres:")
    for i, g in enumerate(genres, 1):
        print(f"  {i}. {g['name']}: {g['url']}")

    return genres


async def test_step2_genre_first_page(genre_name: str, genre_url: str):
    """Step 2: Test lấy trang đầu tiên của một genre."""
    print(f"\n--- Testing first page of '{genre_name}' ---")

    response = await make_request(genre_url, site_key="quykiep", method="GET")

    if not response or not response.text:
        print(f"  ❌ FAILED: No response")
        return None, 0, 0

    if "Just a moment" in response.text:
        print(f"  ❌ FAILED: Cloudflare challenge")
        return None, 0, 0

    stories, max_page = parse_story_list(response.text, "https://quykiep.com")

    # Get total from __NEXT_DATA__
    next_data = extract_next_data(response.text)
    total_from_api = 0
    if next_data:
        page_props = next_data.get('props', {}).get('pageProps', {})
        total_from_api = page_props.get('total', 0)

    if stories:
        print(f"  ✅ OK: {len(stories)} stories on page 1, max_page={max_page}, API total={total_from_api}")
    else:
        print(f"  ❌ EMPTY: No stories found")
        # Debug
        if next_data:
            pp = next_data.get('props', {}).get('pageProps', {})
            print(f"     pageProps keys: {list(pp.keys())}")

    return stories, max_page, total_from_api


async def test_step3_full_genre_discovery(genre_name: str, genre_url: str, max_test_pages: int = 10):
    """Step 3: Test discovery ALL pages của một genre."""
    print(f"\n--- Full discovery for '{genre_name}' (max {max_test_pages} pages) ---")

    adapter = QuykiepAdapter()
    all_stories = []
    page = 1

    while page <= max_test_pages:
        stories, max_page = await adapter.get_stories_in_genre(genre_url, page)

        if not stories:
            print(f"  Page {page}: 0 stories (stopping)")
            break

        all_stories.extend(stories)
        print(f"  Page {page}/{max_page}: {len(stories)} stories (total so far: {len(all_stories)})")

        if page >= max_page:
            print(f"  Reached max page {max_page}")
            break

        page += 1
        await asyncio.sleep(0.5)

    print(f"  📊 Total discovered: {len(all_stories)} stories in {page} pages")
    return all_stories, page


async def main():
    print("\n" + "="*70)
    print("QUYKIEP FULL DISCOVERY TEST")
    print("="*70)
    print("Testing the complete discovery flow step by step")

    # Step 1: Get genres
    genres = await test_step1_get_genres()

    if not genres:
        print("\n❌ FAILED: No genres found")
        return

    # Step 2: Test first page of each genre
    print("\n" + "="*70)
    print("STEP 2: TEST FIRST PAGE OF EACH GENRE")
    print("="*70)

    working_genres = []
    for g in genres:
        stories, max_page, total = await test_step2_genre_first_page(g['name'], g['url'])
        if stories:
            working_genres.append({
                'name': g['name'],
                'url': g['url'],
                'first_page_count': len(stories),
                'max_page': max_page,
                'api_total': total,
            })
        await asyncio.sleep(1)

    print(f"\n📊 Summary: {len(working_genres)}/{len(genres)} genres have stories")

    # Step 3: Full discovery for 2-3 genres (limited pages for testing)
    print("\n" + "="*70)
    print("STEP 3: FULL DISCOVERY TEST (LIMITED PAGES)")
    print("="*70)

    total_discovered = 0
    for g in working_genres[:3]:  # Test first 3 working genres
        stories, pages = await test_step3_full_genre_discovery(
            g['name'], g['url'], max_test_pages=10
        )
        total_discovered += len(stories)
        await asyncio.sleep(2)

    # Final summary
    print("\n" + "="*70)
    print("FINAL SUMMARY")
    print("="*70)

    print(f"\n📊 Genres found: {len(genres)}")
    print(f"📊 Working genres: {len(working_genres)}")

    estimated_total = 0
    print(f"\n📊 Estimated stories per genre:")
    for g in working_genres:
        estimated = g['api_total'] or (g['first_page_count'] * g['max_page'])
        estimated_total += estimated
        print(f"  - {g['name']}: ~{estimated} stories ({g['max_page']} pages)")

    print(f"\n📊 ESTIMATED TOTAL STORIES: ~{estimated_total}")
    print(f"📊 Stories discovered in test: {total_discovered}")

    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())

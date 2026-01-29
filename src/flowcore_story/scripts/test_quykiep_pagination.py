#!/usr/bin/env python3
"""
Test script để verify pagination của quykiep adapter
"""
import asyncio
import json

from flowcore_story.apps.scraper import make_request
from flowcore_story.analyze.quykiep_parse import extract_next_data, parse_story_list, parse_chapter_list_page

BASE_URL = "https://quykiep.com"
SITE_KEY = "quykiep"


async def test_genre_pagination():
    """Test genre/story list pagination"""
    print("\n" + "="*80)
    print("TEST 1: GENRE PAGINATION")
    print("="*80)

    genre_url = f"{BASE_URL}/truyen-dich-ds"

    # Fetch page 1 to get total
    response = await make_request(genre_url, site_key=SITE_KEY, method="GET")
    if not response or not response.text:
        print("ERROR: Cannot fetch page 1")
        return

    next_data = extract_next_data(response.text)
    if not next_data:
        print("ERROR: Cannot extract __NEXT_DATA__")
        return

    page_props = next_data.get('props', {}).get('pageProps', {})
    total = page_props.get('total', 0)
    data = page_props.get('data', [])
    page_index = page_props.get('pageIndex', 1)

    print(f"\nPage 1:")
    print(f"  total (from API): {total}")
    print(f"  items on this page: {len(data)}")
    print(f"  pageIndex: {page_index}")

    # Crawl all pages to count total stories
    all_stories = []
    page = 1
    max_pages = 20  # Safety limit

    while page <= max_pages:
        if page > 1:
            url = f"{genre_url}?page={page}"
        else:
            url = genre_url

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if not response or not response.text:
            print(f"  Page {page}: FAILED to fetch")
            break

        stories, max_page = parse_story_list(response.text, BASE_URL)

        if not stories:
            print(f"  Page {page}: No stories (end of pagination)")
            break

        all_stories.extend(stories)
        print(f"  Page {page}: {len(stories)} stories, calculated max_page={max_page}")

        if page >= max_page:
            print(f"  Reached max_page ({max_page})")
            break

        page += 1
        await asyncio.sleep(0.3)

    print(f"\nRESULT:")
    print(f"  API total: {total}")
    print(f"  Actually crawled: {len(all_stories)}")
    print(f"  Pages crawled: {page}")

    if len(all_stories) == total:
        print("  ✅ PAGINATION CORRECT!")
    else:
        print(f"  ❌ MISMATCH! Missing {total - len(all_stories)} stories")


async def test_chapter_pagination():
    """Test chapter list pagination"""
    print("\n" + "="*80)
    print("TEST 2: CHAPTER PAGINATION")
    print("="*80)

    story_slug = "lang-thien-kiem-de"
    chapter_list_url = f"{BASE_URL}/truyen/{story_slug}/danh-sach-chuong"

    # Fetch page 1 to get total
    response = await make_request(chapter_list_url, site_key=SITE_KEY, method="GET")
    if not response or not response.text:
        print("ERROR: Cannot fetch page 1")
        return

    next_data = extract_next_data(response.text)
    if not next_data:
        print("ERROR: Cannot extract __NEXT_DATA__")
        return

    page_props = next_data.get('props', {}).get('pageProps', {})
    total = page_props.get('total', 0)
    chapter_list = page_props.get('chapterList', [])
    page_index = page_props.get('pageIndex', 1)

    print(f"\nPage 1:")
    print(f"  total (from API): {total}")
    print(f"  chapters on this page: {len(chapter_list)}")
    print(f"  pageIndex: {page_index}")

    # Crawl all pages
    all_chapters = []
    page = 1
    chapters_per_page = len(chapter_list) if chapter_list else 50
    max_pages = (total + chapters_per_page - 1) // chapters_per_page + 2  # Safety margin

    while page <= max_pages:
        url = f"{chapter_list_url}?page={page}"

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if not response or not response.text:
            print(f"  Page {page}: FAILED to fetch")
            break

        chapters, api_total, current_page = parse_chapter_list_page(response.text, story_slug, BASE_URL)

        if not chapters:
            print(f"  Page {page}: No chapters (end of pagination)")
            break

        all_chapters.extend(chapters)
        print(f"  Page {page}: {len(chapters)} chapters")

        page += 1
        await asyncio.sleep(0.3)

    print(f"\nRESULT:")
    print(f"  API total: {total}")
    print(f"  Actually crawled: {len(all_chapters)}")
    print(f"  Pages crawled: {page - 1}")

    if len(all_chapters) == total:
        print("  ✅ PAGINATION CORRECT!")
    else:
        print(f"  ❌ MISMATCH! Missing {total - len(all_chapters)} chapters")

    # Verify first and last chapters
    if all_chapters:
        print(f"\n  First chapter: {all_chapters[0]['title']}")
        print(f"  Last chapter: {all_chapters[-1]['title']}")


async def test_multiple_genres():
    """Test pagination across multiple genres"""
    print("\n" + "="*80)
    print("TEST 3: MULTIPLE GENRES PAGINATION")
    print("="*80)

    genres = [
        ("Danh sách truyện dịch", f"{BASE_URL}/truyen-dich-ds"),
        ("Tiên Hiệp", f"{BASE_URL}/truyen-tien-hiep-ln"),
        ("Truyện Full", f"{BASE_URL}/truyen-full-ds"),
    ]

    for name, url in genres:
        print(f"\n{name}:")

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if not response or not response.text:
            print("  ERROR: Cannot fetch")
            continue

        next_data = extract_next_data(response.text)
        if not next_data:
            print("  ERROR: No __NEXT_DATA__")
            continue

        page_props = next_data.get('props', {}).get('pageProps', {})
        total = page_props.get('total', 0)
        data = page_props.get('data', [])

        stories, max_page = parse_story_list(response.text, BASE_URL)

        print(f"  API total: {total}")
        print(f"  Items on page 1: {len(data)}")
        print(f"  Calculated max_page: {max_page}")

        await asyncio.sleep(0.3)


async def main():
    print("\n" + "#"*80)
    print("# QUYKIEP PAGINATION TEST")
    print("#"*80)

    await test_multiple_genres()
    await test_genre_pagination()
    await test_chapter_pagination()

    print("\n" + "#"*80)
    print("# TEST COMPLETED")
    print("#"*80)


if __name__ == "__main__":
    asyncio.run(main())

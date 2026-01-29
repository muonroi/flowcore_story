#!/usr/bin/env python3
"""
Script khám phá cấu trúc site quykiep.com - Version 2
Phân tích Next.js __NEXT_DATA__ JSON
"""
import asyncio
import re
import json
from urllib.parse import urljoin, urlparse

from flowcore_story.apps.scraper import make_request
from bs4 import BeautifulSoup

BASE_URL = "https://quykiep.com"
SITE_KEY = "quykiep"


def extract_next_data(html: str) -> dict | None:
    """Extract __NEXT_DATA__ JSON from Next.js page"""
    soup = BeautifulSoup(html, 'html.parser')
    script = soup.select_one('script#__NEXT_DATA__')
    if script and script.string:
        try:
            return json.loads(script.string)
        except json.JSONDecodeError:
            pass
    return None


async def explore_the_loai_page():
    """Khám phá trang thể loại chính"""
    print("\n" + "="*80)
    print("1. KHÁM PHÁ TRANG THỂ LOẠI: /the-loai")
    print("="*80)

    response = await make_request(f"{BASE_URL}/the-loai", site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang thể loại")
        return None

    html = response.text
    next_data = extract_next_data(html)

    if next_data:
        print("\n--- __NEXT_DATA__ JSON ---")
        page_props = next_data.get('props', {}).get('pageProps', {})

        # Tìm danh sách thể loại
        for key, value in page_props.items():
            print(f"\n{key}: {type(value).__name__}")
            if isinstance(value, list) and len(value) > 0:
                print(f"  First item: {json.dumps(value[0], ensure_ascii=False, indent=2)[:500]}")
            elif isinstance(value, dict):
                print(f"  Keys: {list(value.keys())[:10]}")

        # Lưu JSON
        with open('/tmp/quykiep_theloai_json.json', 'w', encoding='utf-8') as f:
            json.dump(next_data, f, ensure_ascii=False, indent=2)
        print("\nSaved JSON to /tmp/quykiep_theloai_json.json")

    return next_data


async def explore_genre_list():
    """Khám phá trang danh sách truyện"""
    print("\n" + "="*80)
    print("2. KHÁM PHÁ TRANG DANH SÁCH: /truyen-dich-ds")
    print("="*80)

    response = await make_request(f"{BASE_URL}/truyen-dich-ds", site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang danh sách")
        return None

    html = response.text
    next_data = extract_next_data(html)

    if next_data:
        print("\n--- __NEXT_DATA__ JSON ---")
        page_props = next_data.get('props', {}).get('pageProps', {})

        # Hiển thị cấu trúc
        for key, value in page_props.items():
            print(f"\n{key}: {type(value).__name__}")
            if isinstance(value, list):
                print(f"  Length: {len(value)}")
                if len(value) > 0 and isinstance(value[0], dict):
                    print(f"  First item keys: {list(value[0].keys())}")
                    print(f"  First item: {json.dumps(value[0], ensure_ascii=False, indent=2)[:800]}")
            elif isinstance(value, dict):
                print(f"  Keys: {list(value.keys())}")

        # Lưu JSON
        with open('/tmp/quykiep_list_json.json', 'w', encoding='utf-8') as f:
            json.dump(next_data, f, ensure_ascii=False, indent=2)
        print("\nSaved JSON to /tmp/quykiep_list_json.json")

    return next_data


async def explore_story_page():
    """Khám phá trang chi tiết truyện"""
    print("\n" + "="*80)
    print("3. KHÁM PHÁ TRANG TRUYỆN: /truyen/lang-thien-kiem-de")
    print("="*80)

    response = await make_request(f"{BASE_URL}/truyen/lang-thien-kiem-de", site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang truyện")
        return None

    html = response.text
    next_data = extract_next_data(html)

    if next_data:
        print("\n--- __NEXT_DATA__ JSON ---")
        page_props = next_data.get('props', {}).get('pageProps', {})

        # Hiển thị cấu trúc
        for key, value in page_props.items():
            print(f"\n{key}: {type(value).__name__}")
            if isinstance(value, dict):
                print(f"  Keys: {list(value.keys())}")
                # Nếu là book, hiển thị chi tiết
                if key == 'book' and isinstance(value, dict):
                    print(f"\n  book details:")
                    for bk, bv in value.items():
                        if isinstance(bv, (str, int, float, bool, type(None))):
                            print(f"    {bk}: {bv}")
                        elif isinstance(bv, list):
                            print(f"    {bk}: list[{len(bv)}]")
                        elif isinstance(bv, dict):
                            print(f"    {bk}: dict{list(bv.keys())[:5]}")
            elif isinstance(value, list):
                print(f"  Length: {len(value)}")
                if len(value) > 0:
                    print(f"  First item: {json.dumps(value[0], ensure_ascii=False)[:300]}")

        # Lưu JSON
        with open('/tmp/quykiep_story_json.json', 'w', encoding='utf-8') as f:
            json.dump(next_data, f, ensure_ascii=False, indent=2)
        print("\nSaved JSON to /tmp/quykiep_story_json.json")

        # Tìm chapters
        book = page_props.get('book', {})
        chapters = book.get('chapters', [])
        archives = book.get('archives', [])

        print(f"\n--- CHAPTERS/ARCHIVES ---")
        print(f"chapters field: {type(chapters).__name__}, len={len(chapters) if isinstance(chapters, list) else 'N/A'}")
        print(f"archives field: {type(archives).__name__}, len={len(archives) if isinstance(archives, list) else 'N/A'}")

        if chapters and isinstance(chapters, list) and len(chapters) > 0:
            print(f"\nFirst chapter: {json.dumps(chapters[0], ensure_ascii=False)}")

        if archives and isinstance(archives, list) and len(archives) > 0:
            print(f"\nFirst archive: {json.dumps(archives[0], ensure_ascii=False)}")

    return next_data


async def explore_chapter_page():
    """Khám phá trang nội dung chương"""
    print("\n" + "="*80)
    print("4. KHÁM PHÁ TRANG CHƯƠNG: /truyen/lang-thien-kiem-de/chuong-1")
    print("="*80)

    response = await make_request(f"{BASE_URL}/truyen/lang-thien-kiem-de/chuong-1", site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang chương")
        return None

    html = response.text
    next_data = extract_next_data(html)

    if next_data:
        print("\n--- __NEXT_DATA__ JSON ---")
        page_props = next_data.get('props', {}).get('pageProps', {})

        for key, value in page_props.items():
            print(f"\n{key}: {type(value).__name__}")
            if isinstance(value, dict):
                print(f"  Keys: {list(value.keys())}")
                # Nếu là chapter, hiển thị chi tiết
                if 'content' in str(value).lower()[:1000]:
                    for ck, cv in value.items():
                        if isinstance(cv, str):
                            print(f"    {ck}: {cv[:200]}...")
                        else:
                            print(f"    {ck}: {type(cv).__name__}")

        # Lưu JSON
        with open('/tmp/quykiep_chapter_json.json', 'w', encoding='utf-8') as f:
            json.dump(next_data, f, ensure_ascii=False, indent=2)
        print("\nSaved JSON to /tmp/quykiep_chapter_json.json")

        # Tìm content
        for key, value in page_props.items():
            if isinstance(value, dict):
                content = value.get('content') or value.get('body') or value.get('text')
                if content and isinstance(content, str) and len(content) > 100:
                    print(f"\n--- CHAPTER CONTENT ({key}.content) ---")
                    print(f"Length: {len(content)} chars")
                    print(f"Preview: {content[:500]}...")

    # Thử parse HTML backup
    soup = BeautifulSoup(html, 'html.parser')
    content_selectors = [
        '#chapter-content', '.chapter-content', '.reading-content',
        'div[data-index]', 'article', '.content'
    ]

    print("\n--- HTML SELECTORS ---")
    for selector in content_selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text(strip=True)
            print(f"{selector}: found! Length={len(text)}")

    return next_data


async def explore_api_endpoints():
    """Khám phá các API endpoints có thể có"""
    print("\n" + "="*80)
    print("5. KHÁM PHÁ API ENDPOINTS")
    print("="*80)

    # Thử các API phổ biến
    api_endpoints = [
        "/api/books",
        "/api/novels",
        "/api/categories",
        "/_next/data",
    ]

    for endpoint in api_endpoints:
        url = f"{BASE_URL}{endpoint}"
        print(f"\nTrying: {url}")

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if response:
            print(f"  Status: {response.status_code}")
            if response.text:
                print(f"  Response preview: {response.text[:200]}...")


async def explore_genres_comprehensive():
    """Khám phá tất cả thể loại từ homepage"""
    print("\n" + "="*80)
    print("6. KHÁM PHÁ TẤT CẢ THỂ LOẠI TỪ HOMEPAGE")
    print("="*80)

    response = await make_request(BASE_URL, site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        return None

    html = response.text
    next_data = extract_next_data(html)

    if next_data:
        page_props = next_data.get('props', {}).get('pageProps', {})

        # Tìm categories
        categories = page_props.get('categories', [])
        genres = page_props.get('genres', [])
        tags = page_props.get('tags', [])

        print(f"\ncategories: {len(categories) if isinstance(categories, list) else 'N/A'}")
        print(f"genres: {len(genres) if isinstance(genres, list) else 'N/A'}")
        print(f"tags: {len(tags) if isinstance(tags, list) else 'N/A'}")

        for key in ['categories', 'genres', 'tags']:
            items = page_props.get(key, [])
            if isinstance(items, list) and len(items) > 0:
                print(f"\n{key}:")
                for item in items[:10]:
                    if isinstance(item, dict):
                        print(f"  - {item.get('name', item.get('title', item))}: {item.get('slug', '')}")
                    else:
                        print(f"  - {item}")


async def main():
    print("\n" + "#"*80)
    print("# SCRIPT KHÁM PHÁ CẤU TRÚC QUYKIEP.COM - VERSION 2")
    print("# (Phân tích Next.js __NEXT_DATA__)")
    print("#"*80)

    await explore_the_loai_page()
    await explore_genre_list()
    await explore_story_page()
    await explore_chapter_page()
    await explore_genres_comprehensive()

    print("\n" + "#"*80)
    print("# KHÁM PHÁ HOÀN TẤT!")
    print("# JSON files saved to /tmp/quykiep_*.json")
    print("#"*80)


if __name__ == "__main__":
    asyncio.run(main())

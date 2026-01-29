#!/usr/bin/env python3
"""
Script khám phá API chapters của quykiep.com
"""
import asyncio
import json

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


async def try_chapter_apis():
    """Thử tìm API endpoint cho chapters"""
    print("\n" + "="*80)
    print("TÌM KIẾM CHAPTER API ENDPOINTS")
    print("="*80)

    # Thử các endpoint có thể có
    story_slug = "lang-thien-kiem-de"
    book_id = 69432

    api_patterns = [
        f"/api/chapters/{book_id}",
        f"/api/book/{book_id}/chapters",
        f"/api/books/{book_id}/chapters",
        f"/api/truyen/{story_slug}/chapters",
        f"/api/get-chapters?bookId={book_id}",
        f"/api/get-chapters?slug={story_slug}",
        f"/_next/data/B3AtRJSJmn1m6zPU5dUpN/truyen/{story_slug}/danh-sach-chuong.json",
        f"/_next/data/B3AtRJSJmn1m6zPU5dUpN/truyen/{story_slug}.json",
    ]

    for endpoint in api_patterns:
        url = f"{BASE_URL}{endpoint}"
        print(f"\nTrying: {url}")

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if response:
            print(f"  Status: {response.status_code}")
            if response.status_code == 200 and response.text:
                try:
                    data = json.loads(response.text)
                    print(f"  JSON Response: {json.dumps(data, ensure_ascii=False)[:500]}...")
                except:
                    print(f"  Text Response: {response.text[:300]}...")


async def explore_chapter_list_page():
    """Thử tìm trang danh sách chương"""
    print("\n" + "="*80)
    print("TÌM TRANG DANH SÁCH CHƯƠNG")
    print("="*80)

    story_slug = "lang-thien-kiem-de"

    # Thử các URL pattern cho trang danh sách chương
    urls = [
        f"/truyen/{story_slug}/danh-sach-chuong",
        f"/truyen/{story_slug}/chapters",
        f"/truyen/{story_slug}/muc-luc",
        f"/truyen/{story_slug}?tab=chapters",
    ]

    for path in urls:
        url = f"{BASE_URL}{path}"
        print(f"\nTrying: {url}")

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if response:
            print(f"  Status: {response.status_code}")
            if response.status_code == 200 and response.text:
                next_data = extract_next_data(response.text)
                if next_data:
                    page_props = next_data.get('props', {}).get('pageProps', {})
                    print(f"  pageProps keys: {list(page_props.keys())}")

                    # Tìm chapters
                    for key, value in page_props.items():
                        if 'chapter' in key.lower():
                            if isinstance(value, list):
                                print(f"  {key}: list[{len(value)}]")
                                if len(value) > 0:
                                    print(f"    First: {json.dumps(value[0], ensure_ascii=False)[:200]}")


async def explore_chapter_content():
    """Khám phá chi tiết nội dung chương"""
    print("\n" + "="*80)
    print("KHÁM PHÁ NỘI DUNG CHƯƠNG")
    print("="*80)

    chapter_url = f"{BASE_URL}/truyen/lang-thien-kiem-de/chuong-1-thoi-doi-nong-lanh"

    print(f"\nFetching: {chapter_url}")
    response = await make_request(chapter_url, site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang chương")
        return

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    # Kiểm tra __NEXT_DATA__
    next_data = extract_next_data(html)
    if next_data:
        page_props = next_data.get('props', {}).get('pageProps', {})
        print(f"\npageProps keys: {list(page_props.keys())}")

        # Tìm chapter content trong JSON
        for key, value in page_props.items():
            print(f"\n{key}: {type(value).__name__}")
            if isinstance(value, dict):
                print(f"  Keys: {list(value.keys())}")
                # Kiểm tra content
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, str) and len(sub_value) > 100:
                        print(f"  {sub_key} (str): {sub_value[:300]}...")

    # Tìm content từ HTML
    print("\n--- HTML CONTENT SELECTORS ---")
    selectors = [
        'article', '#chapter-content', '.chapter-content',
        '.reading-content', 'div.content', 'main article',
        'div[data-chapter]', '.prose', '.entry-content'
    ]

    for selector in selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text()
            print(f"\n{selector}: found!")
            print(f"  Length: {len(text)} chars")
            print(f"  Preview: {text[:500]}...")

            # Lưu HTML element
            with open(f'/tmp/quykiep_chapter_content.html', 'w', encoding='utf-8') as f:
                f.write(str(elem))
            print(f"  Saved to /tmp/quykiep_chapter_content.html")
            break


async def explore_archives():
    """Khám phá archives (volumes)"""
    print("\n" + "="*80)
    print("KHÁM PHÁ ARCHIVES (VOLUMES)")
    print("="*80)

    # Trong book data có archiveCount = 120, có thể đây là volumes/tập
    # Thử tìm API archives

    story_slug = "lang-thien-kiem-de"
    book_id = 69432

    urls = [
        f"/api/archives/{book_id}",
        f"/api/book/{book_id}/archives",
        f"/truyen/{story_slug}/tập-1",
        f"/truyen/{story_slug}/volume-1",
    ]

    for path in urls:
        url = f"{BASE_URL}{path}"
        print(f"\nTrying: {url}")

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if response:
            print(f"  Status: {response.status_code}")
            if response.status_code == 200 and response.text:
                if response.text.startswith('{') or response.text.startswith('['):
                    try:
                        data = json.loads(response.text)
                        print(f"  JSON: {json.dumps(data, ensure_ascii=False)[:500]}...")
                    except:
                        print(f"  Text: {response.text[:300]}...")
                else:
                    next_data = extract_next_data(response.text)
                    if next_data:
                        page_props = next_data.get('props', {}).get('pageProps', {})
                        print(f"  pageProps keys: {list(page_props.keys())}")


async def explore_simple_chapter_pattern():
    """Kiểm tra chapter pattern đơn giản"""
    print("\n" + "="*80)
    print("KIỂM TRA CHAPTER PATTERN ĐƠN GIẢN")
    print("="*80)

    story_slug = "lang-thien-kiem-de"

    # Thử chapter 1 với slug đơn giản
    urls = [
        f"/truyen/{story_slug}/chuong-1",
        f"/truyen/{story_slug}/chuong-2",
        f"/truyen/{story_slug}/chuong-100",
    ]

    for path in urls:
        url = f"{BASE_URL}{path}"
        print(f"\nTrying: {url}")

        response = await make_request(url, site_key=SITE_KEY, method="GET")
        if response:
            print(f"  Status: {response.status_code}")
            if response.status_code == 200:
                next_data = extract_next_data(response.text)
                if next_data:
                    page_props = next_data.get('props', {}).get('pageProps', {})
                    chapter = page_props.get('chapter', {})
                    if chapter:
                        print(f"  Chapter found: {chapter.get('name', 'N/A')}")
                        print(f"  Chapter keys: {list(chapter.keys()) if isinstance(chapter, dict) else 'N/A'}")
            elif response.status_code == 404:
                print("  404 - Need full slug")


async def main():
    print("\n" + "#"*80)
    print("# KHÁM PHÁ API VÀ CHAPTERS CỦA QUYKIEP.COM")
    print("#"*80)

    await explore_simple_chapter_pattern()
    await explore_chapter_content()
    await try_chapter_apis()
    await explore_chapter_list_page()
    await explore_archives()

    print("\n" + "#"*80)
    print("# HOÀN TẤT!")
    print("#"*80)


if __name__ == "__main__":
    asyncio.run(main())

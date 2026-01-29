#!/usr/bin/env python3
"""
Script khám phá cấu trúc site quykiep.com
Chạy trong container: docker exec -it crawler-producer python /app/scripts/explore_quykiep.py
"""
import asyncio
import re
import json
from urllib.parse import urljoin, urlparse

# Import from existing codebase
from flowcore_story.apps.scraper import make_request
from bs4 import BeautifulSoup

BASE_URL = "https://quykiep.com"
SITE_KEY = "quykiep"


async def explore_homepage():
    """Khám phá trang chủ để tìm cấu trúc genres"""
    print("\n" + "="*80)
    print("1. KHÁM PHÁ TRANG CHỦ")
    print("="*80)

    response = await make_request(BASE_URL, site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang chủ")
        return None

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    print(f"\nPage title: {soup.title.string if soup.title else 'N/A'}")

    # Tìm navigation menu
    print("\n--- TÌM MENU THỂLOẠI ---")

    # Thử các selector phổ biến
    nav_selectors = [
        'nav', '.menu', '#menu', '.navbar',
        'ul.nav', '.main-menu', '.category-menu',
        '.the-loai', '.genres', 'a[href*="the-loai"]',
        'a[href*="genre"]', 'a[href*="truyen-"]'
    ]

    for selector in nav_selectors:
        elements = soup.select(selector)
        if elements:
            print(f"\n{selector}: found {len(elements)} elements")
            for i, elem in enumerate(elements[:5]):
                if elem.name == 'a':
                    print(f"  [{i}] href={elem.get('href')} | text={elem.get_text(strip=True)[:50]}")
                else:
                    links = elem.select('a[href]')
                    print(f"  [{i}] Contains {len(links)} links")
                    for link in links[:3]:
                        print(f"       - href={link.get('href')} | {link.get_text(strip=True)[:30]}")

    # Tìm tất cả links có thể là thể loại
    print("\n--- TẤT CẢ LINKS THỂ LOẠI ---")
    all_links = soup.select('a[href]')
    genre_patterns = ['/the-loai/', '/genre/', '/truyen-', '/danh-muc/', '/category/']

    potential_genres = []
    for link in all_links:
        href = link.get('href', '')
        text = link.get_text(strip=True)

        for pattern in genre_patterns:
            if pattern in href.lower():
                full_url = urljoin(BASE_URL, href)
                if full_url not in [g['url'] for g in potential_genres]:
                    potential_genres.append({
                        'name': text,
                        'url': full_url,
                        'pattern': pattern
                    })
                break

    print(f"\nFound {len(potential_genres)} potential genre links:")
    for g in potential_genres[:20]:
        print(f"  - {g['name']}: {g['url']}")

    # Lưu HTML để debug
    with open('/tmp/quykiep_homepage.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nSaved homepage HTML to /tmp/quykiep_homepage.html")

    return potential_genres


async def explore_genre_page(genre_url: str, genre_name: str):
    """Khám phá trang thể loại để tìm cấu trúc story list"""
    print(f"\n" + "="*80)
    print(f"2. KHÁM PHÁ TRANG THỂ LOẠI: {genre_name}")
    print(f"   URL: {genre_url}")
    print("="*80)

    response = await make_request(genre_url, site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang thể loại")
        return None

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    # Tìm danh sách truyện
    print("\n--- TÌM DANH SÁCH TRUYỆN ---")

    story_selectors = [
        '.list-truyen', '.story-list', '.truyen-list',
        '.novel-list', 'div.row[itemscope]', '.book-list',
        '.item', '.story-item', 'article', '.post'
    ]

    for selector in story_selectors:
        elements = soup.select(selector)
        if elements:
            print(f"\n{selector}: found {len(elements)} elements")
            for i, elem in enumerate(elements[:2]):
                print(f"  [{i}] {str(elem)[:200]}...")

    # Tìm links đến truyện
    print("\n--- LINKS TRUYỆN ---")
    story_links = []

    # Patterns phổ biến cho story URL
    story_patterns = ['/truyen/', '/novel/', '/doc-truyen/', '/story/', '/.html']

    for link in soup.select('a[href]'):
        href = link.get('href', '')
        text = link.get_text(strip=True)

        for pattern in story_patterns:
            if pattern in href.lower() and text:
                full_url = urljoin(BASE_URL, href)
                if full_url not in [s['url'] for s in story_links]:
                    story_links.append({
                        'title': text[:50],
                        'url': full_url
                    })
                break

    print(f"\nFound {len(story_links)} potential story links:")
    for s in story_links[:10]:
        print(f"  - {s['title']}: {s['url']}")

    # Tìm pagination
    print("\n--- PAGINATION ---")
    pagination_selectors = [
        '.pagination', 'ul.page', '.paging',
        'a[href*="page"]', 'a[href*="trang"]'
    ]

    for selector in pagination_selectors:
        elements = soup.select(selector)
        if elements:
            print(f"\n{selector}: found {len(elements)} elements")
            for elem in elements[:3]:
                if elem.name == 'a':
                    print(f"  href={elem.get('href')}")
                else:
                    links = elem.select('a[href]')
                    for link in links[:5]:
                        print(f"  - href={link.get('href')} | {link.get_text(strip=True)}")

    # Lưu HTML
    with open('/tmp/quykiep_genre.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nSaved genre page HTML to /tmp/quykiep_genre.html")

    return story_links


async def explore_story_page(story_url: str, story_title: str):
    """Khám phá trang chi tiết truyện"""
    print(f"\n" + "="*80)
    print(f"3. KHÁM PHÁ TRANG TRUYỆN: {story_title}")
    print(f"   URL: {story_url}")
    print("="*80)

    response = await make_request(story_url, site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang truyện")
        return None

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    # Tìm title
    print("\n--- THÔNG TIN TRUYỆN ---")
    title_selectors = ['h1', 'h1.title', '.story-title', '.book-title', 'h1[itemprop="name"]']
    for selector in title_selectors:
        elem = soup.select_one(selector)
        if elem:
            print(f"Title ({selector}): {elem.get_text(strip=True)}")

    # Tìm author
    author_selectors = ['a[itemprop="author"]', '.author', '.tac-gia', 'a[href*="author"]']
    for selector in author_selectors:
        elem = soup.select_one(selector)
        if elem:
            print(f"Author ({selector}): {elem.get_text(strip=True)}")

    # Tìm description
    desc_selectors = ['div[itemprop="description"]', '.description', '.desc', '.story-desc', '.gioi-thieu']
    for selector in desc_selectors:
        elem = soup.select_one(selector)
        if elem:
            print(f"Description ({selector}): {elem.get_text(strip=True)[:100]}...")

    # Tìm cover image
    cover_selectors = ['img[itemprop="image"]', '.cover img', '.book-img img', 'img.lazy']
    for selector in cover_selectors:
        elem = soup.select_one(selector)
        if elem:
            src = elem.get('src') or elem.get('data-src') or elem.get('data-lazy-src')
            print(f"Cover ({selector}): {src}")

    # Tìm thể loại
    genre_selectors = ['a[itemprop="genre"]', '.genre a', '.the-loai a', 'a[href*="the-loai"]']
    for selector in genre_selectors:
        elems = soup.select(selector)
        if elems:
            genres = [e.get_text(strip=True) for e in elems[:5]]
            print(f"Genres ({selector}): {', '.join(genres)}")

    # Tìm danh sách chương
    print("\n--- DANH SÁCH CHƯƠNG ---")
    chapter_selectors = [
        'ul.list-chapter', '.chapter-list', '.list-chuong',
        'a[href*="chuong"]', 'a[href*="chapter"]'
    ]

    chapter_links = []
    for selector in chapter_selectors:
        elements = soup.select(selector)
        if elements:
            print(f"\n{selector}: found {len(elements)} elements")
            for elem in elements[:5]:
                if elem.name == 'a':
                    chapter_links.append({
                        'title': elem.get_text(strip=True),
                        'url': urljoin(BASE_URL, elem.get('href', ''))
                    })
                else:
                    for link in elem.select('a[href]')[:5]:
                        chapter_links.append({
                            'title': link.get_text(strip=True),
                            'url': urljoin(BASE_URL, link.get('href', ''))
                        })

    print(f"\nFound {len(chapter_links)} chapter links:")
    for ch in chapter_links[:5]:
        print(f"  - {ch['title']}: {ch['url']}")

    # Tìm chapter API/AJAX
    print("\n--- TÌM AJAX/API CHO CHAPTERS ---")
    for script in soup.select('script'):
        text = script.get_text()
        if 'ajax' in text.lower() or 'api' in text.lower() or 'chapter' in text.lower():
            print(f"Script with ajax/api/chapter: {text[:200]}...")

    # Lưu HTML
    with open('/tmp/quykiep_story.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nSaved story page HTML to /tmp/quykiep_story.html")

    return chapter_links


async def explore_chapter_page(chapter_url: str, chapter_title: str):
    """Khám phá trang nội dung chương"""
    print(f"\n" + "="*80)
    print(f"4. KHÁM PHÁ TRANG CHƯƠNG: {chapter_title}")
    print(f"   URL: {chapter_url}")
    print("="*80)

    response = await make_request(chapter_url, site_key=SITE_KEY, method="GET")

    if not response or not response.text:
        print("ERROR: Không thể tải trang chương")
        return None

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    # Tìm nội dung chương
    print("\n--- NỘI DUNG CHƯƠNG ---")
    content_selectors = [
        'div#chapter-c', '.chapter-c', '.chapter-content',
        '#content', '.content', '.noi-dung', '.reading-content',
        'article', '.post-content', '#chapter-content'
    ]

    for selector in content_selectors:
        elem = soup.select_one(selector)
        if elem:
            text = elem.get_text(strip=True)
            print(f"\n{selector}: found!")
            print(f"  Length: {len(text)} chars")
            print(f"  Preview: {text[:200]}...")

    # Kiểm tra anti-bot
    page_text = soup.get_text()
    antibot_signs = [
        'Just a moment', 'Enable JavaScript', 'Cloudflare',
        'captcha', 'verify', 'robot', 'ddos'
    ]

    print("\n--- KIỂM TRA ANTI-BOT ---")
    for sign in antibot_signs:
        if sign.lower() in page_text.lower():
            print(f"  WARNING: Found '{sign}' in page!")

    # Lưu HTML
    with open('/tmp/quykiep_chapter.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nSaved chapter page HTML to /tmp/quykiep_chapter.html")

    return True


async def main():
    print("\n" + "#"*80)
    print("# SCRIPT KHÁM PHÁ CẤU TRÚC QUYKIEP.COM")
    print("#"*80)

    # 1. Khám phá trang chủ
    genres = await explore_homepage()

    if not genres:
        print("\nKhông tìm thấy thể loại nào. Dừng khám phá.")
        return

    # 2. Khám phá trang thể loại đầu tiên
    first_genre = genres[0]
    stories = await explore_genre_page(first_genre['url'], first_genre['name'])

    if not stories:
        print("\nKhông tìm thấy truyện nào. Dừng khám phá.")
        return

    # 3. Khám phá trang truyện đầu tiên
    first_story = stories[0]
    chapters = await explore_story_page(first_story['url'], first_story['title'])

    if not chapters:
        print("\nKhông tìm thấy chương nào. Dừng khám phá.")
        return

    # 4. Khám phá trang chương đầu tiên
    first_chapter = chapters[0]
    await explore_chapter_page(first_chapter['url'], first_chapter['title'])

    print("\n" + "#"*80)
    print("# KHÁM PHÁ HOÀN TẤT!")
    print("# Kiểm tra các file HTML đã lưu:")
    print("#   /tmp/quykiep_homepage.html")
    print("#   /tmp/quykiep_genre.html")
    print("#   /tmp/quykiep_story.html")
    print("#   /tmp/quykiep_chapter.html")
    print("#"*80)


if __name__ == "__main__":
    asyncio.run(main())

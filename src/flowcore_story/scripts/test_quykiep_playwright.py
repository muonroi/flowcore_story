#!/usr/bin/env python3
"""
Test quykiep chapter access using Playwright (real browser).

This checks if JavaScript rendering can access chapter content that
curl/httpx cannot access due to redirects.
"""

import asyncio
import json
import re

from playwright.async_api import async_playwright


STORY_URL = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh"
CHAPTER_URL = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0"


async def test_browser_navigation():
    """Test if browser can navigate to chapter."""
    print("=" * 80)
    print("PLAYWRIGHT BROWSER TEST")
    print("=" * 80)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120",
            locale="vi-VN",
        )
        page = await context.new_page()

        # Test 1: Direct chapter URL
        print("\n1. Direct chapter URL navigation:")
        print(f"   URL: {CHAPTER_URL}")

        try:
            await page.goto(CHAPTER_URL, wait_until="networkidle", timeout=30000)
            final_url = page.url
            print(f"   Final URL: {final_url}")
            print(f"   Same URL: {final_url == CHAPTER_URL}")

            # Check page content
            content = await page.content()
            has_chapter = "chapter" in content.lower() and "content" in content.lower()
            print(f"   Has chapter content indicators: {has_chapter}")

            # Extract __NEXT_DATA__
            if "__NEXT_DATA__" in content:
                match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', content, re.DOTALL)
                if match:
                    data = json.loads(match.group(1))
                    page_props = data.get("props", {}).get("pageProps", {})
                    print(f"   pageProps keys: {list(page_props.keys())}")
                    if "chapter" in page_props:
                        ch_content = page_props["chapter"].get("content", "")
                        print(f"   ✅ Chapter content found! Length: {len(ch_content)}")
                    elif "__N_REDIRECT" in page_props:
                        print(f"   ❌ Redirect response in NEXT_DATA")
                    else:
                        print(f"   ❌ No chapter data")
        except Exception as e:
            print(f"   Error: {e}")

        # Test 2: Navigate from story page (simulate user click)
        print("\n2. Navigate from story page (user simulation):")

        try:
            # First go to story page
            await page.goto(STORY_URL, wait_until="networkidle", timeout=30000)
            print(f"   Story page loaded: {page.url}")

            # Look for chapter links
            chapter_links = await page.query_selector_all('a[href*="chuong-"]')
            print(f"   Found {len(chapter_links)} chapter links")

            if chapter_links:
                # Click first chapter link
                first_link = chapter_links[0]
                href = await first_link.get_attribute("href")
                print(f"   Clicking: {href}")

                await first_link.click()
                await page.wait_for_load_state("networkidle")

                final_url = page.url
                print(f"   After click URL: {final_url}")

                # Check if we're on chapter page
                content = await page.content()
                if "__NEXT_DATA__" in content:
                    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', content, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        page_props = data.get("props", {}).get("pageProps", {})
                        if "chapter" in page_props:
                            print(f"   ✅ Chapter page loaded!")
                            ch = page_props["chapter"]
                            print(f"   Chapter name: {ch.get('name', 'N/A')}")
                            print(f"   Content length: {len(ch.get('content', ''))}")
                        elif "book" in page_props:
                            print(f"   ⚠️ Still on story page")
                        else:
                            print(f"   ❓ Unknown page type, keys: {list(page_props.keys())}")
        except Exception as e:
            print(f"   Error: {e}")

        # Test 3: Check if there's a "read" button that loads chapter differently
        print("\n3. Check for 'read' button on story page:")

        try:
            await page.goto(STORY_URL, wait_until="networkidle", timeout=30000)

            # Look for read buttons
            read_buttons = await page.query_selector_all('button:has-text("Đọc"), a:has-text("Đọc truyện"), [class*="read"]')
            print(f"   Found {len(read_buttons)} read buttons/links")

            for i, btn in enumerate(read_buttons[:3]):
                tag = await btn.evaluate("el => el.tagName")
                text = await btn.inner_text()
                print(f"   Button {i+1}: <{tag}> '{text[:30]}'")

        except Exception as e:
            print(f"   Error: {e}")

        # Test 4: Check network requests when clicking chapter
        print("\n4. Monitor network requests when navigating to chapter:")

        try:
            await page.goto(STORY_URL, wait_until="networkidle", timeout=30000)

            # Set up request monitoring
            requests_log = []

            def log_request(request):
                if "chuong" in request.url or "chapter" in request.url:
                    requests_log.append({
                        "url": request.url,
                        "method": request.method,
                    })

            page.on("request", log_request)

            # Find and click a chapter
            chapter_links = await page.query_selector_all('a[href*="chuong-"]')
            if chapter_links:
                await chapter_links[0].click()
                await asyncio.sleep(2)
                await page.wait_for_load_state("networkidle")

                print(f"   Captured {len(requests_log)} chapter-related requests:")
                for req in requests_log[:5]:
                    print(f"     {req['method']} {req['url'][:80]}")

        except Exception as e:
            print(f"   Error: {e}")

        await browser.close()


async def test_chapter_modal():
    """Check if chapter content loads via modal/AJAX."""
    print("\n" + "=" * 80)
    print("MODAL/AJAX CHAPTER LOADING TEST")
    print("=" * 80)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 Chrome/120",
            locale="vi-VN",
        )
        page = await context.new_page()

        # Monitor all network requests
        api_requests = []

        def capture_response(response):
            url = response.url
            if any(x in url for x in ["/api/", "/_next/data/", "chapter", "chuong"]):
                api_requests.append({
                    "url": url,
                    "status": response.status,
                })

        page.on("response", capture_response)

        try:
            # Go to story page
            await page.goto(STORY_URL, wait_until="networkidle", timeout=30000)

            # Look for chapter list button/section
            list_buttons = await page.query_selector_all('[class*="chapter"], [id*="chapter"], button:has-text("Chương")')
            print(f"Found {len(list_buttons)} chapter-related elements")

            # Click on "Danh sách chương" if exists
            ds_button = await page.query_selector('text="Danh sách chương"')
            if ds_button:
                print("Clicking 'Danh sách chương'...")
                await ds_button.click()
                await asyncio.sleep(1)

            # Now look for chapters and click one
            chapter_items = await page.query_selector_all('a[href*="chuong-"], [class*="chapterItem"]')
            print(f"Found {len(chapter_items)} chapter items")

            if chapter_items:
                await chapter_items[0].click()
                await asyncio.sleep(2)

            print(f"\nAPI requests captured: {len(api_requests)}")
            for req in api_requests:
                print(f"  {req['status']} {req['url'][:80]}")

        except Exception as e:
            print(f"Error: {e}")

        await browser.close()


async def main():
    print("=" * 80)
    print("QUYKIEP PLAYWRIGHT ANALYSIS")
    print("=" * 80)

    await test_browser_navigation()
    await test_chapter_modal()

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Test client-side navigation approach for Next.js sites like quykiep.

The key insight is that Next.js sites often require:
1. Loading the parent page first
2. Client-side navigation (clicking links) to access child pages
3. Direct URL access may be blocked/redirected

This script tests if clicking chapter links from the story page works.
"""

import asyncio
import json
import re
import sys

from playwright.async_api import async_playwright


PROXY = {
    "server": "http://103.183.119.19:8727",
    "username": "leanhO5BYQ",
    "password": "zrmk91ko",
}

STORY_URL = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh"


async def test_navigation_approach():
    """Test accessing chapter via client-side navigation."""
    print("=" * 80)
    print("NEXT.JS CLIENT-SIDE NAVIGATION TEST")
    print("=" * 80)

    async with async_playwright() as p:
        # Launch with proxy
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )

        context = await browser.new_context(
            proxy=PROXY,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="vi-VN",
            viewport={"width": 1920, "height": 1080},
        )

        # Anti-detection
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
        """)

        page = await context.new_page()

        try:
            # Step 1: Load story page
            print("\n1. Loading story page...")
            await page.goto(STORY_URL, wait_until="networkidle", timeout=60000)
            print(f"   URL: {page.url}")

            # Wait for React hydration
            await page.wait_for_timeout(2000)

            # Step 2: Find chapter links
            print("\n2. Looking for chapter links...")

            # First click on "Danh sách chương" if exists
            try:
                ds_button = await page.query_selector('button:has-text("Danh sách chương"), a:has-text("Danh sách chương")')
                if ds_button:
                    print("   Found 'Danh sách chương' button, clicking...")
                    await ds_button.click()
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

            # Find chapter links
            chapter_links = await page.query_selector_all('a[href*="/chuong-"]')
            print(f"   Found {len(chapter_links)} chapter links")

            if not chapter_links:
                print("   ERROR: No chapter links found!")
                await browser.close()
                return

            # Step 3: Click on first chapter
            print("\n3. Clicking first chapter link...")
            first_link = chapter_links[0]
            href = await first_link.get_attribute("href")
            print(f"   Chapter href: {href}")

            # Monitor for redirect
            current_url = page.url

            # Click and wait
            await first_link.click()

            try:
                # Wait for navigation
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception as e:
                print(f"   Navigation timeout: {e}")

            await page.wait_for_timeout(2000)

            new_url = page.url
            print(f"\n4. After click:")
            print(f"   Previous URL: {current_url}")
            print(f"   Current URL: {new_url}")

            # Check if we're on chapter page
            if "chuong-" in new_url:
                print("   ✅ Successfully navigated to chapter page!")
            else:
                print("   ❌ Still on story page or redirected elsewhere")

            # Step 4: Extract content
            print("\n5. Extracting page content...")
            content = await page.content()

            if "__NEXT_DATA__" in content:
                match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', content, re.DOTALL)
                if match:
                    data = json.loads(match.group(1))
                    page_props = data.get("props", {}).get("pageProps", {})

                    print(f"   pageProps keys: {list(page_props.keys())}")

                    if "chapter" in page_props:
                        chapter = page_props["chapter"]
                        chapter_content = chapter.get("content", "")
                        print(f"\n   ✅ CHAPTER CONTENT FOUND!")
                        print(f"   Chapter name: {chapter.get('name', 'N/A')}")
                        print(f"   Content length: {len(chapter_content)} characters")
                        print(f"\n   First 200 chars of content:")
                        print(f"   {chapter_content[:200]}...")
                    elif "book" in page_props:
                        print("   ❌ Got story page data instead of chapter")
                    elif "__N_REDIRECT" in page_props:
                        print("   ❌ Server returned redirect")
                    else:
                        print(f"   ❓ Unknown data structure")

            # Step 5: Try reading from visible content (fallback)
            print("\n6. Checking visible content on page...")
            try:
                # Look for common chapter content selectors
                for selector in ['#chapter-content', '.chapter-content', '.reading-content', '.text-content', 'article']:
                    elem = await page.query_selector(selector)
                    if elem:
                        text = await elem.inner_text()
                        if len(text) > 100:
                            print(f"   Found content in '{selector}': {len(text)} chars")
                            print(f"   Preview: {text[:150]}...")
                            break
            except Exception as e:
                print(f"   Error reading visible content: {e}")

            # Take screenshot for debugging
            await page.screenshot(path="/tmp/quykiep_chapter_test.png")
            print("\n   Screenshot saved to /tmp/quykiep_chapter_test.png")

        except Exception as e:
            print(f"\nError: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

        finally:
            await browser.close()


async def test_rapid_chapter_navigation():
    """Test navigating through multiple chapters quickly."""
    print("\n" + "=" * 80)
    print("RAPID CHAPTER NAVIGATION TEST")
    print("=" * 80)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            proxy=PROXY,
            user_agent="Mozilla/5.0 Chrome/120",
        )
        page = await context.new_page()

        try:
            # Load story page
            await page.goto(STORY_URL, wait_until="networkidle", timeout=60000)
            print("Story page loaded")

            # Click chapter list
            try:
                ds_button = await page.query_selector('button:has-text("Danh sách chương")')
                if ds_button:
                    await ds_button.click()
                    await page.wait_for_timeout(500)
            except Exception:
                pass

            # Get first few chapter links
            chapter_links = await page.query_selector_all('a[href*="/chuong-"]')
            print(f"Found {len(chapter_links)} chapters")

            # Navigate through first 3 chapters
            for i, link in enumerate(chapter_links[:3]):
                href = await link.get_attribute("href")
                print(f"\nNavigating to chapter {i+1}: {href}")

                await link.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                await page.wait_for_timeout(500)

                # Check URL
                current_url = page.url
                success = "chuong-" in current_url

                if success:
                    print(f"  ✅ On chapter page: {current_url}")

                    # Extract content
                    content = await page.content()
                    if "chapter" in content and "content" in content:
                        match = re.search(r'"content"\s*:\s*"([^"]{0,100})', content)
                        if match:
                            print(f"  Content preview: {match.group(1)[:50]}...")
                else:
                    print(f"  ❌ Not on chapter page: {current_url}")

                # Go back to story page for next chapter
                await page.go_back()
                await page.wait_for_load_state("networkidle")

        except Exception as e:
            print(f"Error: {e}")

        finally:
            await browser.close()


async def main():
    await test_navigation_approach()
    await test_rapid_chapter_navigation()

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

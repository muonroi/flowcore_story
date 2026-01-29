#!/usr/bin/env python3
"""
Deep analysis of quykiep chapter redirect behavior.

Key finding: Chapter URLs redirect to story page instead of showing chapter content.
This script investigates why and how to fix it.
"""

import asyncio
import json
import re
from urllib.parse import urljoin

from curl_cffi.requests import AsyncSession


PROXY = "http://leanhO5BYQ:zrmk91ko@103.183.119.19:8727"
BASE_URL = "https://quykiep.com"
STORY_SLUG = "ta-co-mot-quyen-do-nhan-kinh"


async def test_redirect_with_details():
    """Test redirect behavior with detailed analysis."""
    print("=" * 80)
    print("REDIRECT BEHAVIOR ANALYSIS")
    print("=" * 80)

    proxies = {"http": PROXY, "https": PROXY}

    async with AsyncSession(impersonate="chrome120") as session:
        # Test chapter URL without following redirects
        chapter_url = f"{BASE_URL}/truyen/{STORY_SLUG}/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0"

        print(f"\nTesting: {chapter_url}")
        print("\n1. WITHOUT following redirects:")

        resp = await session.get(
            chapter_url,
            proxies=proxies,
            timeout=30,
            allow_redirects=False,
        )

        print(f"   Status: {resp.status_code}")
        print(f"   Headers:")
        for key in ["location", "set-cookie", "cf-ray", "server"]:
            if key in resp.headers:
                print(f"     {key}: {resp.headers[key][:100]}")

        if resp.status_code in [301, 302, 303, 307, 308]:
            location = resp.headers.get("location", "")
            print(f"\n   REDIRECT DETECTED!")
            print(f"   Redirects to: {location}")

        print("\n2. WITH following redirects:")
        resp2 = await session.get(
            chapter_url,
            proxies=proxies,
            timeout=30,
            allow_redirects=True,
        )
        print(f"   Final URL: {resp2.url}")
        print(f"   Final Status: {resp2.status_code}")


async def test_chapter_via_api():
    """Check if quykiep has an API endpoint for chapters."""
    print("\n" + "=" * 80)
    print("API ENDPOINT ANALYSIS")
    print("=" * 80)

    proxies = {"http": PROXY, "https": PROXY}

    # Potential API endpoints to test
    api_endpoints = [
        f"{BASE_URL}/api/chapter/{STORY_SLUG}/1",
        f"{BASE_URL}/api/truyen/{STORY_SLUG}/chapter/1",
        f"{BASE_URL}/_next/data/*/truyen/{STORY_SLUG}/chuong-1.json",
    ]

    async with AsyncSession(impersonate="chrome120") as session:
        # First get the buildId from story page
        story_resp = await session.get(
            f"{BASE_URL}/truyen/{STORY_SLUG}",
            proxies=proxies,
            timeout=30,
        )

        # Extract buildId from __NEXT_DATA__
        build_id = None
        if "__NEXT_DATA__" in story_resp.text:
            match = re.search(r'"buildId"\s*:\s*"([^"]+)"', story_resp.text)
            if match:
                build_id = match.group(1)
                print(f"Found Next.js buildId: {build_id}")

        # Test Next.js data endpoint
        if build_id:
            next_data_url = f"{BASE_URL}/_next/data/{build_id}/truyen/{STORY_SLUG}/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0.json"
            print(f"\nTesting Next.js data endpoint:")
            print(f"  URL: {next_data_url}")

            try:
                resp = await session.get(
                    next_data_url,
                    proxies=proxies,
                    timeout=30,
                    allow_redirects=False,
                )
                print(f"  Status: {resp.status_code}")

                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        page_props = data.get("pageProps", {})
                        print(f"  pageProps keys: {list(page_props.keys())}")
                        if "chapter" in page_props:
                            ch = page_props["chapter"]
                            content = ch.get("content", "")
                            print(f"  ✅ HAS CHAPTER CONTENT! Length: {len(content)}")
                        else:
                            print(f"  ❌ No chapter in pageProps")
                    except Exception as e:
                        print(f"  JSON parse error: {e}")
                        print(f"  Response: {resp.text[:500]}")
                elif resp.status_code in [301, 302, 307, 308]:
                    print(f"  Redirects to: {resp.headers.get('location', 'unknown')}")
            except Exception as e:
                print(f"  Error: {e}")


async def analyze_chapter_list_data():
    """Analyze chapter list data to understand URL patterns."""
    print("\n" + "=" * 80)
    print("CHAPTER LIST DATA ANALYSIS")
    print("=" * 80)

    proxies = {"http": PROXY, "https": PROXY}

    async with AsyncSession(impersonate="chrome120") as session:
        # Get chapter list
        list_url = f"{BASE_URL}/truyen/{STORY_SLUG}/danh-sach-chuong?page=1"
        print(f"Fetching: {list_url}")

        resp = await session.get(
            list_url,
            proxies=proxies,
            timeout=30,
        )

        if "__NEXT_DATA__" in resp.text:
            match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', resp.text, re.DOTALL)
            if match:
                data = json.loads(match.group(1))
                page_props = data.get("props", {}).get("pageProps", {})

                build_id = data.get("buildId", "")
                print(f"\nBuild ID: {build_id}")

                chapters = page_props.get("chapterList", [])[:5]
                print(f"\nFirst 5 chapters:")

                for ch in chapters:
                    slug = ch.get("slug", "")
                    name = ch.get("name", "")
                    print(f"\n  Name: {name}")
                    print(f"  Slug: {slug}")

                    # Build full URL
                    full_url = f"{BASE_URL}/truyen/{STORY_SLUG}/{slug}"
                    print(f"  Full URL: {full_url}")

                    # Try Next.js data endpoint
                    next_data_url = f"{BASE_URL}/_next/data/{build_id}/truyen/{STORY_SLUG}/{slug}.json"
                    print(f"  Next.js data URL: {next_data_url}")

                    # Test the Next.js data endpoint
                    try:
                        ch_resp = await session.get(
                            next_data_url,
                            proxies=proxies,
                            timeout=30,
                            allow_redirects=False,
                        )
                        print(f"  Next.js data status: {ch_resp.status_code}")

                        if ch_resp.status_code == 200:
                            ch_data = ch_resp.json()
                            ch_props = ch_data.get("pageProps", {})
                            if "chapter" in ch_props:
                                content = ch_props["chapter"].get("content", "")
                                print(f"  ✅ Content length: {len(content)}")
                            else:
                                print(f"  ❌ No chapter in response")
                                print(f"  Available keys: {list(ch_props.keys())}")
                        elif ch_resp.status_code == 404:
                            print(f"  ❌ 404 Not Found")
                        elif ch_resp.status_code in [301, 302, 307, 308]:
                            print(f"  ⚠️ Redirect to: {ch_resp.headers.get('location', '')[:80]}")
                    except Exception as e:
                        print(f"  Error: {type(e).__name__}: {e}")

                    await asyncio.sleep(1)
                    break  # Only test first chapter


async def test_direct_chapter_access():
    """Test if chapters can be accessed directly through different methods."""
    print("\n" + "=" * 80)
    print("DIRECT CHAPTER ACCESS TEST")
    print("=" * 80)

    proxies = {"http": PROXY, "https": PROXY}

    # Known working chapter slug from chapter list
    chapter_slug = "chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0"

    tests = [
        # Standard URL patterns
        (f"{BASE_URL}/truyen/{STORY_SLUG}/{chapter_slug}", "Standard chapter URL"),
        (f"{BASE_URL}/{STORY_SLUG}/{chapter_slug}", "Without /truyen/"),
        (f"{BASE_URL}/doc-truyen/{STORY_SLUG}/{chapter_slug}", "With /doc-truyen/"),

        # Try chapter number only
        (f"{BASE_URL}/truyen/{STORY_SLUG}/chuong-1", "Simple chuong-1"),

        # Try with different prefixes
        (f"{BASE_URL}/truyen/{STORY_SLUG}/chapter/1", "chapter/1"),
    ]

    async with AsyncSession(impersonate="chrome120") as session:
        for url, desc in tests:
            print(f"\n{desc}:")
            print(f"  URL: {url}")

            try:
                resp = await session.get(
                    url,
                    proxies=proxies,
                    timeout=30,
                    allow_redirects=False,
                )
                print(f"  Status: {resp.status_code}")

                if resp.status_code in [301, 302, 307, 308]:
                    print(f"  Redirect: {resp.headers.get('location', '')}")
                elif resp.status_code == 200:
                    if "__NEXT_DATA__" in resp.text:
                        match = re.search(r'"pageProps"\s*:\s*\{([^}]+)', resp.text)
                        if match:
                            props_preview = match.group(1)[:100]
                            has_chapter = '"chapter"' in resp.text[:5000]
                            print(f"  Has __NEXT_DATA__, chapter key: {has_chapter}")
            except Exception as e:
                print(f"  Error: {type(e).__name__}")

            await asyncio.sleep(1)


async def compare_working_vs_broken():
    """Compare a working site (truyencom) with quykiep."""
    print("\n" + "=" * 80)
    print("COMPARISON: TRUYENCOM (works) vs QUYKIEP (broken)")
    print("=" * 80)

    proxies = {"http": PROXY, "https": PROXY}

    sites = {
        "truyencom": {
            "chapter_url": "https://truyencom.com/tien-phu-dao-do/chuong-1.html",
            "content_selector": "chapter-content",
        },
        "quykiep": {
            "chapter_url": f"{BASE_URL}/truyen/{STORY_SLUG}/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0",
            "content_selector": "__NEXT_DATA__",
        },
    }

    async with AsyncSession(impersonate="chrome120") as session:
        for site_name, config in sites.items():
            print(f"\n{site_name}:")
            url = config["chapter_url"]

            # Test without redirect
            resp_no_redirect = await session.get(
                url,
                proxies=proxies,
                timeout=30,
                allow_redirects=False,
            )

            print(f"  Without redirect: {resp_no_redirect.status_code}")
            if resp_no_redirect.status_code in [301, 302, 307, 308]:
                print(f"    Location: {resp_no_redirect.headers.get('location', '')}")

            # Test with redirect
            resp_redirect = await session.get(
                url,
                proxies=proxies,
                timeout=30,
                allow_redirects=True,
            )

            print(f"  With redirect: {resp_redirect.status_code}")
            print(f"  Final URL: {resp_redirect.url}")
            print(f"  Same URL: {str(resp_redirect.url) == url}")

            await asyncio.sleep(1)


async def test_session_and_cookies():
    """Test if session/cookies affect chapter access."""
    print("\n" + "=" * 80)
    print("SESSION & COOKIE TEST")
    print("=" * 80)

    proxies = {"http": PROXY, "https": PROXY}

    async with AsyncSession(impersonate="chrome120") as session:
        # Step 1: Visit homepage
        print("Step 1: Visit homepage")
        home_resp = await session.get(
            BASE_URL,
            proxies=proxies,
            timeout=30,
        )
        print(f"  Status: {home_resp.status_code}")
        print(f"  Cookies: {[c.name for c in session.cookies]}")

        await asyncio.sleep(1)

        # Step 2: Visit story page
        print("\nStep 2: Visit story page")
        story_url = f"{BASE_URL}/truyen/{STORY_SLUG}"
        story_resp = await session.get(
            story_url,
            proxies=proxies,
            timeout=30,
        )
        print(f"  Status: {story_resp.status_code}")
        print(f"  Cookies: {[c.name for c in session.cookies]}")

        await asyncio.sleep(1)

        # Step 3: Click on chapter (simulate user behavior)
        print("\nStep 3: Visit chapter with Referer")
        chapter_url = f"{BASE_URL}/truyen/{STORY_SLUG}/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0"
        chapter_resp = await session.get(
            chapter_url,
            proxies=proxies,
            timeout=30,
            allow_redirects=False,
            headers={
                "Referer": story_url,
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        print(f"  Status: {chapter_resp.status_code}")
        if chapter_resp.status_code in [301, 302, 307, 308]:
            print(f"  STILL REDIRECTS to: {chapter_resp.headers.get('location', '')}")
        print(f"  Cookies: {[c.name for c in session.cookies]}")


async def main():
    """Run all analysis tests."""
    print("=" * 80)
    print("QUYKIEP CHAPTER REDIRECT DEEP ANALYSIS")
    print("=" * 80)

    await test_redirect_with_details()
    await test_chapter_via_api()
    await analyze_chapter_list_data()
    await test_direct_chapter_access()
    await compare_working_vs_broken()
    await test_session_and_cookies()

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print("""
SUMMARY OF FINDINGS:
1. Chapter URLs return 301 redirect to story page
2. Need to check if Next.js data API works
3. Need to check if specific cookies/session required
4. May need to use JavaScript rendering for this site
""")


if __name__ == "__main__":
    asyncio.run(main())

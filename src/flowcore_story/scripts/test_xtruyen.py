#!/usr/bin/env python3
"""
Test script để diagnose vấn đề với xtruyen.vn
Kiểm tra: connectivity, proxy, cookies, anti-bot
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Test URLs
TEST_URLS = {
    "homepage": "https://xtruyen.vn/",
    "genre_page": "https://xtruyen.vn/the-loai/kiem-hiep/",
    "story_page": "https://xtruyen.vn/truyen/the-gioi-hoan-my/",
    "chapter_page": "https://xtruyen.vn/truyen/the-gioi-hoan-my/chuong-1/",
    "ajax_endpoint": "https://xtruyen.vn/wp-admin/admin-ajax.php",
}

# Proxies from proxies.txt
PROXIES_FILE = "/home/storyflow-core/proxies/proxies.txt"


def load_proxies():
    """Load proxies from file"""
    proxies = []
    if os.path.exists(PROXIES_FILE):
        with open(PROXIES_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    proxies.append(line)
    return proxies


async def test_direct_connection():
    """Test 1: Direct connection without proxy"""
    print("\n" + "=" * 60)
    print("TEST 1: DIRECT CONNECTION (NO PROXY)")
    print("=" * 60)

    try:
        import httpx

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for name, url in TEST_URLS.items():
                if name == "ajax_endpoint":
                    continue  # Skip AJAX for GET test
                try:
                    start = time.time()
                    resp = await client.get(url, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
                    })
                    elapsed = time.time() - start
                    print(f"  {name}: {resp.status_code} ({elapsed:.2f}s)")
                    if resp.status_code == 200:
                        # Check for anti-bot markers
                        content = resp.text[:5000]
                        if "cf-browser-verification" in content or "challenge-platform" in content:
                            print(f"    ⚠️ Cloudflare challenge detected!")
                        elif "403" in content or "Access Denied" in content:
                            print(f"    ⚠️ Access denied in content!")
                        else:
                            print(f"    ✅ Content looks OK (length: {len(resp.text)})")
                except Exception as e:
                    print(f"  {name}: ERROR - {type(e).__name__}: {str(e)[:50]}")
    except ImportError:
        print("  httpx not available, skipping direct test")


async def test_proxy_connection():
    """Test 2: Connection through proxies"""
    print("\n" + "=" * 60)
    print("TEST 2: PROXY CONNECTION")
    print("=" * 60)

    proxies = load_proxies()
    if not proxies:
        print("  No proxies found!")
        return

    print(f"  Found {len(proxies)} proxies")

    try:
        import httpx

        test_url = TEST_URLS["homepage"]

        for i, proxy in enumerate(proxies[:5]):  # Test first 5 proxies
            proxy_url = proxy if proxy.startswith("http") else f"http://{proxy}"
            print(f"\n  Proxy {i+1}: {proxy_url[:50]}...")

            try:
                async with httpx.AsyncClient(
                    proxy=proxy_url,
                    timeout=30.0,
                    follow_redirects=True
                ) as client:
                    start = time.time()
                    resp = await client.get(test_url, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    })
                    elapsed = time.time() - start
                    print(f"    Status: {resp.status_code} ({elapsed:.2f}s)")
                    if resp.status_code == 200:
                        print(f"    ✅ Proxy works!")
                    else:
                        print(f"    ⚠️ Status {resp.status_code}")
            except Exception as e:
                print(f"    ❌ ERROR: {type(e).__name__}: {str(e)[:60]}")
    except ImportError:
        print("  httpx not available")


async def test_curl_cffi():
    """Test 3: Using curl_cffi (browser impersonation)"""
    print("\n" + "=" * 60)
    print("TEST 3: CURL_CFFI (BROWSER IMPERSONATION)")
    print("=" * 60)

    try:
        from curl_cffi.requests import AsyncSession

        async with AsyncSession() as session:
            for name, url in TEST_URLS.items():
                if name == "ajax_endpoint":
                    continue
                try:
                    start = time.time()
                    resp = await session.get(url, impersonate="chrome120", timeout=30)
                    elapsed = time.time() - start
                    print(f"  {name}: {resp.status_code} ({elapsed:.2f}s)")
                    if resp.status_code == 200:
                        content = resp.text[:2000]
                        if "cf-browser-verification" in content:
                            print(f"    ⚠️ Cloudflare challenge!")
                        else:
                            print(f"    ✅ OK (length: {len(resp.text)})")
                except Exception as e:
                    print(f"  {name}: ERROR - {type(e).__name__}: {str(e)[:50]}")
    except ImportError:
        print("  curl_cffi not available")


async def test_challenge_harvester():
    """Test 4: Challenge harvester endpoint"""
    print("\n" + "=" * 60)
    print("TEST 4: CHALLENGE HARVESTER")
    print("=" * 60)

    try:
        import httpx

        harvester_url = "http://localhost:8099/harvest"

        async with httpx.AsyncClient(timeout=120.0) as client:
            # Test homepage
            payload = {
                "url": TEST_URLS["homepage"],
                "site_key": "xtruyen",
                "method": "GET"
            }

            print(f"  Testing: {payload['url']}")
            try:
                start = time.time()
                resp = await client.post(harvester_url, json=payload)
                elapsed = time.time() - start

                if resp.status_code == 200:
                    data = resp.json()
                    print(f"    Status: {data.get('status_code', 'N/A')} ({elapsed:.2f}s)")
                    if data.get("success"):
                        print(f"    ✅ Harvester worked!")
                        print(f"    Content length: {len(data.get('content', ''))}")
                        # Check for cookies
                        cookies = data.get("cookies", [])
                        if cookies:
                            print(f"    Cookies: {len(cookies)} cookies harvested")
                    else:
                        print(f"    ❌ Harvester failed: {data.get('error', 'Unknown')}")
                else:
                    print(f"    ❌ Harvester returned: {resp.status_code}")
            except Exception as e:
                print(f"    ❌ ERROR: {type(e).__name__}: {str(e)[:60]}")

            # Test chapter page
            print(f"\n  Testing: {TEST_URLS['chapter_page']}")
            payload["url"] = TEST_URLS["chapter_page"]
            try:
                start = time.time()
                resp = await client.post(harvester_url, json=payload)
                elapsed = time.time() - start

                if resp.status_code == 200:
                    data = resp.json()
                    print(f"    Status: {data.get('status_code', 'N/A')} ({elapsed:.2f}s)")
                    if data.get("success"):
                        content = data.get("content", "")
                        print(f"    ✅ OK! Content length: {len(content)}")
                        # Check if content has chapter text
                        if "chapter" in content.lower() or "chương" in content.lower():
                            print(f"    ✅ Chapter content detected!")
                    else:
                        print(f"    ❌ Failed: {data.get('error', 'Unknown')}")
                else:
                    print(f"    ❌ Status: {resp.status_code}")
            except Exception as e:
                print(f"    ❌ ERROR: {type(e).__name__}: {str(e)[:60]}")

    except ImportError:
        print("  httpx not available")


async def test_adapter_config():
    """Test 5: Check xtruyen adapter configuration"""
    print("\n" + "=" * 60)
    print("TEST 5: XTRUYEN ADAPTER CONFIGURATION")
    print("=" * 60)

    try:
        from flowcore_story.adapters.xtruyen_adapter import XtruyenAdapter

        adapter = XtruyenAdapter()
        print(f"  Adapter class: {adapter.__class__.__name__}")
        print(f"  Site key: {getattr(adapter, 'site_key', 'N/A')}")
        print(f"  Base URL: {getattr(adapter, 'base_url', 'N/A')}")

        # Check for special configs
        if hasattr(adapter, 'request_delay'):
            print(f"  Request delay: {adapter.request_delay}")
        if hasattr(adapter, 'use_playwright'):
            print(f"  Use Playwright: {adapter.use_playwright}")
        if hasattr(adapter, 'retry_attempts'):
            print(f"  Retry attempts: {adapter.retry_attempts}")

        print("  ✅ Adapter loaded successfully")

    except Exception as e:
        print(f"  ❌ ERROR loading adapter: {type(e).__name__}: {str(e)}")


async def test_connection_pool():
    """Test 6: Check if connection pool is saturated"""
    print("\n" + "=" * 60)
    print("TEST 6: CONNECTION POOL STATUS")
    print("=" * 60)

    try:
        import httpx

        # Create client with explicit pool settings
        limits = httpx.Limits(
            max_keepalive_connections=5,
            max_connections=10,
            keepalive_expiry=30
        )

        async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
            # Try concurrent requests
            urls = [TEST_URLS["homepage"]] * 3

            print("  Testing 3 concurrent requests...")
            start = time.time()

            tasks = [client.get(url, headers={"User-Agent": "Mozilla/5.0"}) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.time() - start

            success = sum(1 for r in results if not isinstance(r, Exception) and r.status_code == 200)
            errors = sum(1 for r in results if isinstance(r, Exception))

            print(f"  Results: {success} success, {errors} errors ({elapsed:.2f}s total)")

            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"    Request {i+1}: ❌ {type(r).__name__}")
                else:
                    print(f"    Request {i+1}: ✅ {r.status_code}")

    except ImportError:
        print("  httpx not available")


async def main():
    print("=" * 60)
    print("XTRUYEN.VN DIAGNOSTIC TEST")
    print("=" * 60)
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Run all tests
    await test_direct_connection()
    await test_proxy_connection()
    await test_curl_cffi()
    await test_challenge_harvester()
    await test_adapter_config()
    await test_connection_pool()

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)

    # Summary recommendations
    print("\n📊 ANALYSIS SUMMARY:")
    print("-" * 40)
    print("""
Based on the test results, check:
1. If direct connection works but proxy doesn't → Proxy issue
2. If curl_cffi works → Use curl_cffi for xtruyen
3. If challenge harvester works → Cookie/session issue in main crawler
4. If everything times out → Site may be blocking server IP
5. If Cloudflare detected → Need to use challenge harvester more
""")


if __name__ == "__main__":
    asyncio.run(main())

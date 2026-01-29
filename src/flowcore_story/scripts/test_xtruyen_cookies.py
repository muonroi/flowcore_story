#!/usr/bin/env python3
"""
Test if we can reuse cookies from challenge-harvester for faster subsequent requests.
This is the key to speeding up xtruyen crawling.
"""

import asyncio
import time
import aiohttp
from typing import Dict, Any, List


async def get_cookies_from_harvester(proxy: str = None) -> Dict[str, Any]:
    """Get cookies from challenge-harvester."""
    print("\n[1] Getting cookies from challenge-harvester...")
    start = time.time()

    harvester_url = "http://challenge-harvester:8099/harvest"
    payload = {
        "url": "https://xtruyen.vn/",
        "site_key": "xtruyen",
        "timeout": 120,
    }
    if proxy:
        payload["proxy"] = proxy

    async with aiohttp.ClientSession() as session:
        async with session.post(
            harvester_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=150)
        ) as resp:
            data = await resp.json()

    elapsed = time.time() - start
    print(f"   Time: {elapsed:.1f}s")
    print(f"   Success: {data.get('success')}")
    print(f"   Status: {data.get('status')}")
    print(f"   Cookies: {len(data.get('cookies', []))}")

    if data.get('cookies'):
        print("   Cookie names:", [c.get('name') for c in data['cookies']])

    return {
        "cookies": data.get("cookies", []),
        "user_agent": data.get("user_agent"),
        "headers": data.get("headers", {}),
    }


async def test_with_cookies(
    cookies_data: Dict[str, Any],
    proxy: str,
    num_requests: int = 5
) -> Dict[str, Any]:
    """Test if we can make requests using harvested cookies."""
    print(f"\n[2] Testing {num_requests} requests with harvested cookies...")

    # Build cookie header
    cookie_dict = {}
    for c in cookies_data.get("cookies", []):
        cookie_dict[c["name"]] = c["value"]

    headers = {
        "User-Agent": cookies_data.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Cookie": "; ".join([f"{k}={v}" for k, v in cookie_dict.items()]),
    }

    # Add any extra headers from harvester
    if cookies_data.get("headers"):
        for k, v in cookies_data["headers"].items():
            if k.lower() not in ["cookie", "host"]:
                headers[k] = v

    test_urls = [
        "https://xtruyen.vn/truyen/than-dao-dan-ton/",
        "https://xtruyen.vn/truyen/than-dao-dan-ton/chuong-1/",
        "https://xtruyen.vn/truyen/than-dao-dan-ton/chuong-2/",
        "https://xtruyen.vn/truyen/than-dao-dan-ton/chuong-3/",
        "https://xtruyen.vn/truyen/than-dao-dan-ton/chuong-4/",
    ]

    results = {
        "success": 0,
        "blocked": 0,
        "errors": 0,
        "times_ms": [],
    }

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i, url in enumerate(test_urls[:num_requests]):
            try:
                start = time.time()
                async with session.get(
                    url,
                    headers=headers,
                    proxy=proxy,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    elapsed = int((time.time() - start) * 1000)
                    content = await resp.text()

                    # Check result
                    if resp.status == 200 and "truyện" in content.lower():
                        results["success"] += 1
                        results["times_ms"].append(elapsed)
                        print(f"   ✅ Request {i+1}: {resp.status} in {elapsed}ms (content: {len(content)} chars)")
                    elif resp.status == 403 or "challenge" in content.lower():
                        results["blocked"] += 1
                        print(f"   ❌ Request {i+1}: BLOCKED ({resp.status})")
                    else:
                        results["errors"] += 1
                        print(f"   ⚠️ Request {i+1}: {resp.status} in {elapsed}ms")

            except Exception as e:
                results["errors"] += 1
                print(f"   ❌ Request {i+1}: ERROR - {str(e)[:50]}")

            await asyncio.sleep(1.5)  # Delay between requests

    return results


async def test_ajax_endpoint(cookies_data: Dict[str, Any], proxy: str) -> Dict[str, Any]:
    """Test AJAX endpoint which is used for fetching chapters."""
    print("\n[3] Testing AJAX endpoint (chapter fetching)...")

    cookie_dict = {}
    for c in cookies_data.get("cookies", []):
        cookie_dict[c["name"]] = c["value"]

    headers = {
        "User-Agent": cookies_data.get("user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Cookie": "; ".join([f"{k}={v}" for k, v in cookie_dict.items()]),
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }

    # Test fetching chapter list via AJAX
    ajax_url = "https://xtruyen.vn/wp-admin/admin-ajax.php"
    data = {
        "action": "tw_ajax",
        "type": "chapter",
        "id": "71877",  # Example story ID
    }

    results = {"success": 0, "blocked": 0, "times_ms": []}

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(3):
            try:
                start = time.time()
                async with session.post(
                    ajax_url,
                    headers=headers,
                    data=data,
                    proxy=proxy,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    elapsed = int((time.time() - start) * 1000)
                    content = await resp.text()

                    if resp.status == 200 and len(content) > 100:
                        results["success"] += 1
                        results["times_ms"].append(elapsed)
                        print(f"   ✅ AJAX {i+1}: {resp.status} in {elapsed}ms (response: {len(content)} chars)")
                    else:
                        results["blocked"] += 1
                        print(f"   ❌ AJAX {i+1}: {resp.status} - {content[:100]}")

            except Exception as e:
                print(f"   ❌ AJAX {i+1}: ERROR - {str(e)[:50]}")

            await asyncio.sleep(1)

    return results


async def main():
    print("=" * 60)
    print("XTRUYEN COOKIE REUSE TEST")
    print("=" * 60)

    # Load proxy
    proxy = None
    try:
        with open("/app/proxies/proxies.txt") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    proxy = line
                    break
    except:
        pass

    print(f"\nUsing proxy: {proxy[:50] if proxy else 'None'}...")

    # Step 1: Get cookies from harvester
    try:
        cookies_data = await get_cookies_from_harvester(proxy)
    except Exception as e:
        print(f"\n❌ Failed to get cookies: {e}")
        return

    if not cookies_data.get("cookies"):
        print("\n❌ No cookies received from harvester")
        return

    # Step 2: Test page requests with cookies
    page_results = await test_with_cookies(cookies_data, proxy, num_requests=5)

    # Step 3: Test AJAX endpoint
    ajax_results = await test_ajax_endpoint(cookies_data, proxy)

    # Summary
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)

    print(f"\nPage Requests:")
    print(f"  Success: {page_results['success']}/5")
    print(f"  Blocked: {page_results['blocked']}/5")
    if page_results['times_ms']:
        avg = sum(page_results['times_ms']) / len(page_results['times_ms'])
        print(f"  Avg Time: {avg:.0f}ms")

    print(f"\nAJAX Requests:")
    print(f"  Success: {ajax_results['success']}/3")
    print(f"  Blocked: {ajax_results['blocked']}/3")
    if ajax_results['times_ms']:
        avg = sum(ajax_results['times_ms']) / len(ajax_results['times_ms'])
        print(f"  Avg Time: {avg:.0f}ms")

    # Recommendations
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)

    total_success = page_results['success'] + ajax_results['success']
    total_blocked = page_results['blocked'] + ajax_results['blocked']

    if total_success > 5:
        print("\n✅ Cookie reuse WORKS!")
        print("   → Current implementation should be caching cookies")
        print("   → Check if cookie cache TTL is configured correctly")
        avg_time = sum(page_results['times_ms'] + ajax_results['times_ms']) / max(1, len(page_results['times_ms'] + ajax_results['times_ms']))
        print(f"   → Expected speed: ~{60000/avg_time:.0f} requests/min with {avg_time:.0f}ms/request")
    elif total_blocked > 3:
        print("\n❌ Getting blocked even with cookies")
        print("   → Cookies may expire quickly")
        print("   → Need to refresh cookies more frequently")
        print("   → Consider using challenge-harvester for EVERY request")
    else:
        print("\n⚠️ Mixed results")
        print("   → Check proxy stability")
        print("   → May need to adjust request timing")


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Test script to verify site connectivity and response patterns.
Tests each crawler target to understand current status.
"""

import asyncio
import aiohttp
import time
import sys
from typing import Dict, Any

# Site configurations
SITES = {
    "truyencom": {
        "base_url": "https://truyencom.com",
        "test_endpoints": [
            "/",
            "/api/chapters/20913/1/50",  # API endpoint
        ],
        "expected_patterns": ["cloudflare", "truyencom"],
    },
    "xtruyen": {
        "base_url": "https://xtruyen.vn",
        "test_endpoints": [
            "/",
            "/theloai/tien-hiep/",
            "/truyen/ngao-the-dan-than/",  # Popular story
        ],
        "expected_patterns": ["xtruyen", "truyện"],
    },
    "tangthuvien": {
        "base_url": "https://tangthuvien.net",
        "test_endpoints": [
            "/",
            "/the-loai/tien-hiep",
        ],
        "expected_patterns": ["tangthuvien", "Tiên Hiệp"],
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}


async def test_endpoint(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    """Test a single endpoint."""
    start = time.time()
    result = {
        "url": url,
        "status": None,
        "time_ms": 0,
        "cloudflare": False,
        "anti_bot": False,
        "error": None,
        "content_length": 0,
    }

    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            result["status"] = resp.status
            result["time_ms"] = int((time.time() - start) * 1000)

            # Check headers for cloudflare
            cf_ray = resp.headers.get("cf-ray", "")
            if cf_ray:
                result["cloudflare"] = True

            content = await resp.text()
            result["content_length"] = len(content)

            # Check for anti-bot patterns
            anti_bot_patterns = [
                "challenge-platform",
                "cf-turnstile",
                "Just a moment",
                "Checking your browser",
                "captcha",
                "blocked",
            ]
            for pattern in anti_bot_patterns:
                if pattern.lower() in content.lower():
                    result["anti_bot"] = True
                    break

    except asyncio.TimeoutError:
        result["error"] = "TIMEOUT"
        result["time_ms"] = int((time.time() - start) * 1000)
    except aiohttp.ClientError as e:
        result["error"] = str(e)[:100]
        result["time_ms"] = int((time.time() - start) * 1000)
    except Exception as e:
        result["error"] = f"UNKNOWN: {str(e)[:80]}"
        result["time_ms"] = int((time.time() - start) * 1000)

    return result


async def test_site(site_key: str, config: Dict) -> Dict[str, Any]:
    """Test all endpoints for a site."""
    print(f"\n{'='*60}")
    print(f"Testing: {site_key.upper()}")
    print(f"Base URL: {config['base_url']}")
    print(f"{'='*60}")

    results = {
        "site": site_key,
        "base_url": config["base_url"],
        "endpoints": [],
        "summary": {
            "total": 0,
            "success": 0,
            "blocked": 0,
            "failed": 0,
            "avg_time_ms": 0,
        }
    }

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for endpoint in config["test_endpoints"]:
            url = f"{config['base_url']}{endpoint}"
            result = await test_endpoint(session, url)
            results["endpoints"].append(result)

            # Print result
            status_icon = "✅" if result["status"] == 200 and not result["anti_bot"] else "❌" if result["error"] or result["anti_bot"] else "⚠️"
            anti_bot_str = " [ANTI-BOT]" if result["anti_bot"] else ""
            error_str = f" [{result['error']}]" if result["error"] else ""

            print(f"  {status_icon} {endpoint}")
            print(f"     Status: {result['status']}, Time: {result['time_ms']}ms, Size: {result['content_length']}{anti_bot_str}{error_str}")

            # Update summary
            results["summary"]["total"] += 1
            if result["status"] == 200 and not result["anti_bot"]:
                results["summary"]["success"] += 1
            elif result["anti_bot"]:
                results["summary"]["blocked"] += 1
            else:
                results["summary"]["failed"] += 1

    # Calculate average time
    times = [r["time_ms"] for r in results["endpoints"] if r["time_ms"] > 0]
    results["summary"]["avg_time_ms"] = int(sum(times) / len(times)) if times else 0

    return results


async def main():
    print("=" * 60)
    print("STORYFLOW SITE CONNECTIVITY TEST")
    print("=" * 60)
    print(f"Testing {len(SITES)} sites...")

    all_results = []

    for site_key, config in SITES.items():
        result = await test_site(site_key, config)
        all_results.append(result)
        await asyncio.sleep(1)  # Small delay between sites

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for result in all_results:
        site = result["site"]
        s = result["summary"]
        status = "🟢 OK" if s["success"] == s["total"] else "🟡 PARTIAL" if s["success"] > 0 else "🔴 BLOCKED"
        print(f"\n{site.upper()}: {status}")
        print(f"  Success: {s['success']}/{s['total']}, Blocked: {s['blocked']}, Failed: {s['failed']}")
        print(f"  Avg Response Time: {s['avg_time_ms']}ms")

    # Recommendations
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)

    for result in all_results:
        site = result["site"]
        s = result["summary"]

        if s["blocked"] > 0:
            print(f"\n⚠️  {site.upper()}: Anti-bot protection detected")
            print(f"   → Need challenge-harvester or proxy rotation")
        elif s["failed"] > 0:
            print(f"\n❌ {site.upper()}: Connection issues")
            print(f"   → Check network/DNS settings")
        elif s["avg_time_ms"] > 2000:
            print(f"\n🐌 {site.upper()}: Slow response ({s['avg_time_ms']}ms)")
            print(f"   → Consider using CDN/proxy closer to server")
        else:
            print(f"\n✅ {site.upper()}: Working well")


if __name__ == "__main__":
    asyncio.run(main())

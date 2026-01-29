#!/usr/bin/env python3
"""
Test script to analyze xtruyen crawling speed and find optimal settings.
Tests different methods, concurrency levels, and delays.
"""

import asyncio
import time
import statistics
from typing import Dict, List, Any
import sys
import os

# Add src to path
sys.path.insert(0, '/app/src')

import aiohttp


class XtruyenSpeedTest:
    def __init__(self):
        self.results: Dict[str, List[float]] = {}
        self.errors: Dict[str, int] = {}

        # Test URLs - mix of different page types
        self.test_urls = [
            ("homepage", "https://xtruyen.vn/"),
            ("genre_page", "https://xtruyen.vn/theloai/tien-hiep/"),
            ("story_page", "https://xtruyen.vn/truyen/than-dao-dan-ton/"),
            ("chapter_page", "https://xtruyen.vn/truyen/than-dao-dan-ton/chuong-1/"),
            ("ajax_chapters", "https://xtruyen.vn/wp-admin/admin-ajax.php"),
        ]

        # Load proxies
        self.proxies = self._load_proxies()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

    def _load_proxies(self) -> List[str]:
        """Load proxies from file."""
        proxy_file = "/app/proxies/proxies.txt"
        proxies = []
        try:
            with open(proxy_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        proxies.append(line)
        except Exception as e:
            print(f"Warning: Could not load proxies: {e}")
        return proxies

    async def test_direct_request(self, session: aiohttp.ClientSession, url: str, proxy: str = None) -> Dict[str, Any]:
        """Test a direct HTTP request."""
        start = time.time()
        result = {
            "url": url,
            "method": "direct" + ("+proxy" if proxy else ""),
            "success": False,
            "status": None,
            "time_ms": 0,
            "anti_bot": False,
            "content_length": 0,
            "error": None,
        }

        try:
            async with session.get(
                url,
                headers=self.headers,
                proxy=proxy,
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=False
            ) as resp:
                result["status"] = resp.status
                result["time_ms"] = int((time.time() - start) * 1000)

                content = await resp.text()
                result["content_length"] = len(content)

                # Check for anti-bot
                anti_bot_patterns = ["challenge-platform", "cf-turnstile", "Just a moment", "Checking your browser"]
                for pattern in anti_bot_patterns:
                    if pattern.lower() in content.lower():
                        result["anti_bot"] = True
                        break

                result["success"] = resp.status == 200 and not result["anti_bot"]

        except asyncio.TimeoutError:
            result["error"] = "TIMEOUT"
            result["time_ms"] = int((time.time() - start) * 1000)
        except Exception as e:
            result["error"] = str(e)[:100]
            result["time_ms"] = int((time.time() - start) * 1000)

        return result

    async def test_challenge_harvester(self, url: str, proxy: str = None) -> Dict[str, Any]:
        """Test request via challenge-harvester."""
        start = time.time()
        result = {
            "url": url,
            "method": "harvester" + ("+proxy" if proxy else ""),
            "success": False,
            "status": None,
            "time_ms": 0,
            "anti_bot": False,
            "content_length": 0,
            "error": None,
        }

        harvester_url = "http://challenge-harvester:8099/harvest"
        payload = {
            "url": url,
            "site_key": "xtruyen",
            "timeout": 60,
        }
        if proxy:
            payload["proxy"] = proxy

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    harvester_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=90)
                ) as resp:
                    result["time_ms"] = int((time.time() - start) * 1000)
                    data = await resp.json()

                    result["status"] = data.get("status")
                    result["success"] = data.get("success", False)

                    if data.get("body"):
                        result["content_length"] = len(data["body"])
                        # Check content validity
                        if "xtruyen" in data["body"].lower() or "truyện" in data["body"].lower():
                            result["has_valid_content"] = True

                    if data.get("cookies"):
                        result["cookies_count"] = len(data["cookies"])

        except asyncio.TimeoutError:
            result["error"] = "TIMEOUT"
            result["time_ms"] = int((time.time() - start) * 1000)
        except Exception as e:
            result["error"] = str(e)[:100]
            result["time_ms"] = int((time.time() - start) * 1000)

        return result

    async def test_concurrency(self, concurrency: int, delay: float, num_requests: int = 10) -> Dict[str, Any]:
        """Test different concurrency levels."""
        print(f"\n  Testing concurrency={concurrency}, delay={delay}s, requests={num_requests}")

        url = "https://xtruyen.vn/wp-admin/admin-ajax.php"
        proxy = self.proxies[0] if self.proxies else None

        results = []
        errors = 0
        blocked = 0

        semaphore = asyncio.Semaphore(concurrency)

        async def make_request(i: int):
            nonlocal errors, blocked
            async with semaphore:
                if i > 0:
                    await asyncio.sleep(delay)

                try:
                    async with aiohttp.ClientSession() as session:
                        start = time.time()
                        async with session.get(
                            url,
                            headers=self.headers,
                            proxy=proxy,
                            timeout=aiohttp.ClientTimeout(total=30),
                            ssl=False
                        ) as resp:
                            elapsed = (time.time() - start) * 1000
                            content = await resp.text()

                            if resp.status == 403 or "challenge" in content.lower():
                                blocked += 1
                                return None

                            if resp.status == 200:
                                results.append(elapsed)
                                return elapsed
                            else:
                                errors += 1
                                return None
                except Exception as e:
                    errors += 1
                    return None

        start_total = time.time()
        await asyncio.gather(*[make_request(i) for i in range(num_requests)])
        total_time = time.time() - start_total

        success_count = len(results)
        avg_time = statistics.mean(results) if results else 0

        return {
            "concurrency": concurrency,
            "delay": delay,
            "total_requests": num_requests,
            "success": success_count,
            "errors": errors,
            "blocked": blocked,
            "avg_response_ms": int(avg_time),
            "total_time_s": round(total_time, 2),
            "requests_per_sec": round(success_count / total_time, 2) if total_time > 0 else 0,
        }

    async def run_all_tests(self):
        """Run all tests and print results."""
        print("=" * 60)
        print("XTRUYEN SPEED TEST")
        print("=" * 60)

        # Test 1: Direct requests (no proxy)
        print("\n[1] Testing Direct Requests (no proxy)...")
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            for name, url in self.test_urls[:3]:  # First 3 URLs
                result = await self.test_direct_request(session, url)
                status = "✅" if result["success"] else "❌"
                anti_bot = " [ANTI-BOT]" if result["anti_bot"] else ""
                print(f"  {status} {name}: {result['status']} in {result['time_ms']}ms{anti_bot}")
                await asyncio.sleep(1)

        # Test 2: Direct requests with proxy
        if self.proxies:
            print(f"\n[2] Testing Direct Requests (with proxy: {self.proxies[0][:40]}...)...")
            async with aiohttp.ClientSession(connector=connector) as session:
                for name, url in self.test_urls[:3]:
                    result = await self.test_direct_request(session, url, self.proxies[0])
                    status = "✅" if result["success"] else "❌"
                    anti_bot = " [ANTI-BOT]" if result["anti_bot"] else ""
                    print(f"  {status} {name}: {result['status']} in {result['time_ms']}ms{anti_bot}")
                    await asyncio.sleep(1)

        # Test 3: Challenge Harvester
        print("\n[3] Testing Challenge Harvester...")
        for name, url in self.test_urls[:2]:
            result = await self.test_challenge_harvester(url, self.proxies[0] if self.proxies else None)
            status = "✅" if result["success"] else "❌"
            cookies = f" [cookies: {result.get('cookies_count', 0)}]" if result.get('cookies_count') else ""
            print(f"  {status} {name}: {result['status']} in {result['time_ms']}ms{cookies}")
            await asyncio.sleep(2)

        # Test 4: Concurrency tests
        print("\n[4] Testing Different Concurrency Levels...")
        concurrency_results = []

        test_configs = [
            (1, 2.0),   # Conservative
            (2, 1.5),   # Moderate
            (3, 1.0),   # Aggressive
            (4, 0.8),   # Very aggressive
            (6, 0.5),   # Maximum
        ]

        for conc, delay in test_configs:
            result = await self.test_concurrency(conc, delay, num_requests=8)
            concurrency_results.append(result)
            print(f"    → Success: {result['success']}/{result['total_requests']}, "
                  f"Blocked: {result['blocked']}, "
                  f"Speed: {result['requests_per_sec']} req/s")
            await asyncio.sleep(3)  # Cooldown between tests

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY & RECOMMENDATIONS")
        print("=" * 60)

        # Find best concurrency
        best = max(concurrency_results, key=lambda x: x['requests_per_sec'] if x['blocked'] == 0 else 0)

        print(f"\n✅ Best Configuration (no blocking):")
        print(f"   Concurrency: {best['concurrency']}")
        print(f"   Delay: {best['delay']}s")
        print(f"   Speed: {best['requests_per_sec']} requests/second")

        # Check if we got blocked
        blocked_tests = [r for r in concurrency_results if r['blocked'] > 0]
        if blocked_tests:
            print(f"\n⚠️  Blocking detected at:")
            for r in blocked_tests:
                print(f"   - Concurrency {r['concurrency']}, Delay {r['delay']}s: {r['blocked']} blocked")

        # Recommendations
        print("\n📋 Recommendations for docker-compose.yml:")
        print(f"   SITE_DELAY_XTRUYEN: \"{best['delay']}\"")
        print(f"   STORY_ASYNC_LIMIT: \"{min(best['concurrency'], 4)}\"")
        print(f"   ASYNC_SEMAPHORE_LIMIT: \"{best['concurrency'] * 2}\"")

        print("\n📋 If still getting blocked:")
        print("   1. Enable challenge-harvester with proxy")
        print("   2. Increase SITE_DELAY_XTRUYEN to 3.0+")
        print("   3. Reduce STORY_ASYNC_LIMIT to 2")
        print("   4. Use rotating proxies with longer rotation interval")


async def main():
    tester = XtruyenSpeedTest()
    await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())

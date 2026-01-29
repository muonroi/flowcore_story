#!/usr/bin/env python3
"""
Explore quykiep.com anti-bot protection for chapter pages.

This script tests various bypass techniques to understand:
1. What triggers Cloudflare protection
2. What methods can bypass it
3. Differences between story pages (works) and chapter pages (blocked)
"""

import asyncio
import json
import re
import time
from dataclasses import dataclass
from typing import Any

import httpx
from curl_cffi.requests import AsyncSession


@dataclass
class TestResult:
    method: str
    url_type: str
    status: int
    blocked: bool
    has_content: bool
    response_size: int
    cf_ray: str | None
    notes: str
    duration: float


# Test URLs
STORY_URL = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh"
CHAPTER_URL = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh/chuong-1303-khong-lo-quan-doi-bi-qua-hoa-lieu-4k-516"
CHAPTER_LIST_URL = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh/danh-sach-chuong?page=1"
GENRE_URL = "https://quykiep.com/truyen-dich-ds"

# Proxies to test
PROXIES = [
    None,  # Direct
    "http://leanhO5BYQ:zrmk91ko@103.183.119.19:8727",
    "http://leanhGNPN2:Gci3bA78@103.74.107.58:8749",
]

# curl_cffi impersonation options
IMPERSONATE_OPTIONS = [
    "chrome110",
    "chrome116",
    "chrome120",
    "chrome124",
    "safari15_3",
    "safari17_0",
    "edge101",
    "edge120",
]


def is_blocked(text: str) -> bool:
    """Check if response is a Cloudflare block page."""
    block_signs = [
        "Just a moment",
        "Chờ một chút",
        "Enable JavaScript",
        "Checking your browser",
        "cf-spinner",
        "challenge-platform",
    ]
    return any(sign.lower() in text.lower() for sign in block_signs)


def has_real_content(text: str, url_type: str) -> bool:
    """Check if response has actual page content."""
    if "__NEXT_DATA__" not in text:
        return False

    try:
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', text, re.DOTALL)
        if not match:
            return False

        data = json.loads(match.group(1))
        page_props = data.get("props", {}).get("pageProps", {})

        if url_type == "chapter":
            # Chapter page should have chapter.content
            chapter = page_props.get("chapter", {})
            return bool(chapter.get("content"))
        elif url_type == "story":
            # Story page should have book info
            return "book" in page_props
        elif url_type == "chapter_list":
            return "chapterList" in page_props
        elif url_type == "genre":
            return "listBook" in page_props or "books" in page_props

        return True
    except Exception:
        return False


def get_cf_ray(headers: dict) -> str | None:
    """Extract Cloudflare Ray ID from headers."""
    for key, value in headers.items():
        if key.lower() == "cf-ray":
            return value
    return None


async def test_httpx_direct(url: str, url_type: str, proxy: str | None) -> TestResult:
    """Test with httpx (standard requests)."""
    start = time.time()
    try:
        async with httpx.AsyncClient(
            timeout=30,
            follow_redirects=True,
            proxy=proxy,
        ) as client:
            resp = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                },
            )

            text = resp.text
            blocked = is_blocked(text)
            has_content = has_real_content(text, url_type)

            return TestResult(
                method=f"httpx {'direct' if not proxy else 'proxy'}",
                url_type=url_type,
                status=resp.status_code,
                blocked=blocked,
                has_content=has_content,
                response_size=len(text),
                cf_ray=get_cf_ray(dict(resp.headers)),
                notes="redirect" if str(resp.url) != url else "",
                duration=time.time() - start,
            )
    except Exception as e:
        return TestResult(
            method=f"httpx {'direct' if not proxy else 'proxy'}",
            url_type=url_type,
            status=0,
            blocked=True,
            has_content=False,
            response_size=0,
            cf_ray=None,
            notes=f"error: {type(e).__name__}: {str(e)[:50]}",
            duration=time.time() - start,
        )


async def test_curl_cffi(url: str, url_type: str, impersonate: str, proxy: str | None) -> TestResult:
    """Test with curl_cffi impersonation."""
    start = time.time()
    try:
        async with AsyncSession(impersonate=impersonate) as session:
            proxies = {"http": proxy, "https": proxy} if proxy else None
            resp = await session.get(
                url,
                proxies=proxies,
                timeout=30,
                allow_redirects=True,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
                    "DNT": "1",
                    "Upgrade-Insecure-Requests": "1",
                },
            )

            text = resp.text
            blocked = is_blocked(text)
            has_content = has_real_content(text, url_type)

            return TestResult(
                method=f"curl_cffi/{impersonate}" + (" proxy" if proxy else ""),
                url_type=url_type,
                status=resp.status_code,
                blocked=blocked,
                has_content=has_content,
                response_size=len(text),
                cf_ray=resp.headers.get("cf-ray"),
                notes="redirect" if str(resp.url) != url else "",
                duration=time.time() - start,
            )
    except Exception as e:
        return TestResult(
            method=f"curl_cffi/{impersonate}" + (" proxy" if proxy else ""),
            url_type=url_type,
            status=0,
            blocked=True,
            has_content=False,
            response_size=0,
            cf_ray=None,
            notes=f"error: {type(e).__name__}: {str(e)[:50]}",
            duration=time.time() - start,
        )


async def test_curl_cffi_with_session(url: str, url_type: str, impersonate: str, proxy: str | None) -> TestResult:
    """Test with curl_cffi using session cookies from story page first."""
    start = time.time()
    try:
        async with AsyncSession(impersonate=impersonate) as session:
            proxies = {"http": proxy, "https": proxy} if proxy else None

            # First, visit story page to get cookies
            story_resp = await session.get(
                STORY_URL,
                proxies=proxies,
                timeout=30,
                allow_redirects=True,
            )

            # Small delay
            await asyncio.sleep(0.5)

            # Now visit chapter with session cookies
            resp = await session.get(
                url,
                proxies=proxies,
                timeout=30,
                allow_redirects=True,
                headers={
                    "Referer": STORY_URL,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )

            text = resp.text
            blocked = is_blocked(text)
            has_content = has_real_content(text, url_type)

            return TestResult(
                method=f"curl_cffi/{impersonate}+session" + (" proxy" if proxy else ""),
                url_type=url_type,
                status=resp.status_code,
                blocked=blocked,
                has_content=has_content,
                response_size=len(text),
                cf_ray=resp.headers.get("cf-ray"),
                notes=f"story:{story_resp.status_code}" + (" redirect" if str(resp.url) != url else ""),
                duration=time.time() - start,
            )
    except Exception as e:
        return TestResult(
            method=f"curl_cffi/{impersonate}+session" + (" proxy" if proxy else ""),
            url_type=url_type,
            status=0,
            blocked=True,
            has_content=False,
            response_size=0,
            cf_ray=None,
            notes=f"error: {type(e).__name__}: {str(e)[:50]}",
            duration=time.time() - start,
        )


def print_result(result: TestResult):
    """Print a single test result."""
    status_emoji = "✅" if result.has_content else ("⚠️" if not result.blocked else "❌")
    print(f"{status_emoji} {result.method:40} | {result.url_type:12} | {result.status:3} | "
          f"blocked={result.blocked!s:5} | content={result.has_content!s:5} | "
          f"size={result.response_size:6} | {result.duration:.2f}s | {result.notes}")


async def run_comparison_tests():
    """Run comparative tests between different URL types."""
    print("\n" + "=" * 120)
    print("COMPARISON: Story Page vs Chapter Page vs Chapter List")
    print("=" * 120)

    test_urls = [
        (STORY_URL, "story"),
        (CHAPTER_LIST_URL, "chapter_list"),
        (CHAPTER_URL, "chapter"),
    ]

    # Test with curl_cffi chrome120 + proxy
    proxy = PROXIES[1]
    impersonate = "chrome120"

    for url, url_type in test_urls:
        result = await test_curl_cffi(url, url_type, impersonate, proxy)
        print_result(result)
        await asyncio.sleep(1)


async def run_impersonate_tests():
    """Test different curl_cffi impersonation options."""
    print("\n" + "=" * 120)
    print("TEST: Different curl_cffi impersonation options for CHAPTER page")
    print("=" * 120)

    proxy = PROXIES[1]

    for imp in IMPERSONATE_OPTIONS:
        result = await test_curl_cffi(CHAPTER_URL, "chapter", imp, proxy)
        print_result(result)
        await asyncio.sleep(2)


async def run_session_tests():
    """Test using session cookies from story page."""
    print("\n" + "=" * 120)
    print("TEST: Session cookies (visit story first, then chapter)")
    print("=" * 120)

    proxy = PROXIES[1]

    for imp in ["chrome120", "chrome124", "safari17_0"]:
        result = await test_curl_cffi_with_session(CHAPTER_URL, "chapter", imp, proxy)
        print_result(result)
        await asyncio.sleep(2)


async def run_proxy_tests():
    """Test different proxies."""
    print("\n" + "=" * 120)
    print("TEST: Different proxies for CHAPTER page")
    print("=" * 120)

    for proxy in PROXIES:
        proxy_name = "direct" if not proxy else proxy.split("@")[1].split(":")[0]
        result = await test_curl_cffi(CHAPTER_URL, "chapter", "chrome120", proxy)
        print_result(result)
        await asyncio.sleep(2)


async def run_rate_limit_test():
    """Test rate limiting behavior."""
    print("\n" + "=" * 120)
    print("TEST: Rate limiting - Multiple requests in sequence")
    print("=" * 120)

    proxy = PROXIES[1]

    # Test 5 rapid requests
    print("\nRapid requests (0.5s delay):")
    for i in range(5):
        result = await test_curl_cffi(CHAPTER_URL, "chapter", "chrome120", proxy)
        print(f"  Request {i+1}: status={result.status}, blocked={result.blocked}")
        await asyncio.sleep(0.5)

    # Wait and test again
    print("\nAfter 5s pause:")
    await asyncio.sleep(5)
    result = await test_curl_cffi(CHAPTER_URL, "chapter", "chrome120", proxy)
    print_result(result)


async def analyze_blocked_response():
    """Analyze what the blocked response contains."""
    print("\n" + "=" * 120)
    print("ANALYSIS: Blocked response details")
    print("=" * 120)

    proxy = PROXIES[1]

    async with AsyncSession(impersonate="chrome120") as session:
        proxies = {"http": proxy, "https": proxy}

        # Get chapter page
        resp = await session.get(
            CHAPTER_URL,
            proxies=proxies,
            timeout=30,
            allow_redirects=True,
        )

        print(f"\nStatus: {resp.status_code}")
        print(f"Final URL: {resp.url}")
        print(f"\nHeaders:")
        for key, value in resp.headers.items():
            if key.lower() in ["cf-ray", "cf-cache-status", "server", "content-type", "set-cookie"]:
                print(f"  {key}: {value[:100] if len(value) > 100 else value}")

        text = resp.text
        print(f"\nResponse size: {len(text)}")

        # Check for specific Cloudflare elements
        cf_elements = [
            ("cf_clearance cookie", "cf_clearance" in str(resp.headers)),
            ("cf-ray header", "cf-ray" in str(resp.headers).lower()),
            ("challenge-platform script", "challenge-platform" in text),
            ("turnstile captcha", "turnstile" in text.lower()),
            ("managed challenge", "managed" in text.lower() and "challenge" in text.lower()),
            ("interactive challenge", "interactive" in text.lower()),
            ("JavaScript challenge", "jschl" in text.lower() or "js challenge" in text.lower()),
        ]

        print(f"\nCloudflare elements detected:")
        for name, present in cf_elements:
            print(f"  {'✓' if present else '✗'} {name}")

        # Extract challenge type if present
        if "cType" in text:
            match = re.search(r"cType['\"]?\s*[:=]\s*['\"]?(\w+)", text)
            if match:
                print(f"\nChallenge type: {match.group(1)}")


async def test_different_chapters():
    """Test different chapter URLs to see if specific chapters are blocked."""
    print("\n" + "=" * 120)
    print("TEST: Different chapter URLs")
    print("=" * 120)

    proxy = PROXIES[1]

    chapters = [
        ("chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0", "First chapter"),
        ("chuong-100-thanh-ton-co-te-nha-tu-roi-di-3J", "Chapter 100"),
        ("chuong-500-hac-co-tinh-gia-thien-mon-gian-1F4", "Chapter 500"),
        ("chuong-1000-quan-am-lam-da-tay-phuong-giai-tri-3E8", "Chapter 1000"),
        ("chuong-1303-khong-lo-quan-doi-bi-qua-hoa-lieu-4k-516", "Latest chapter"),
    ]

    base = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh"

    for slug, desc in chapters:
        url = f"{base}/{slug}"
        result = await test_curl_cffi(url, "chapter", "chrome120", proxy)
        print(f"{desc:20} | ", end="")
        print_result(result)
        await asyncio.sleep(2)


async def main():
    print("=" * 120)
    print("QUYKIEP ANTI-BOT EXPLORATION")
    print("=" * 120)
    print(f"Story URL: {STORY_URL}")
    print(f"Chapter URL: {CHAPTER_URL}")
    print(f"Chapter List URL: {CHAPTER_LIST_URL}")

    # Run all tests
    await run_comparison_tests()
    await run_impersonate_tests()
    await run_session_tests()
    await run_proxy_tests()
    await analyze_blocked_response()
    await test_different_chapters()
    await run_rate_limit_test()

    print("\n" + "=" * 120)
    print("EXPLORATION COMPLETE")
    print("=" * 120)


if __name__ == "__main__":
    asyncio.run(main())

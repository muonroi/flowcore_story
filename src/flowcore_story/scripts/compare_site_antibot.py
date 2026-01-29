#!/usr/bin/env python3
"""
Compare anti-bot behavior between sites:
- xtruyen (works)
- truyencom (works)
- quykiep (blocked)

Goal: Understand what makes quykiep different and harder to bypass.
"""

import asyncio
import json
import re
import time
from dataclasses import dataclass

from curl_cffi.requests import AsyncSession


@dataclass
class SiteAnalysis:
    site: str
    story_status: int
    story_blocked: bool
    chapter_status: int
    chapter_blocked: bool
    cloudflare_version: str | None
    challenge_type: str | None
    requires_js: bool
    cookies_set: list[str]
    special_headers: dict[str, str]
    notes: str


SITES = {
    "xtruyen": {
        "story": "https://xtruyen.vn/truyen/mang-hoang-ky/",
        "chapter": "https://xtruyen.vn/truyen/mang-hoang-ky/chuong-1/",
    },
    "truyencom": {
        "story": "https://truyencom.com/tien-phu-dao-do/",
        "chapter": "https://truyencom.com/tien-phu-dao-do/chuong-1.html",
    },
    "quykiep": {
        "story": "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh",
        "chapter": "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0",
    },
}

PROXY = "http://leanhO5BYQ:zrmk91ko@103.183.119.19:8727"


def is_blocked(text: str) -> bool:
    """Check if response is blocked."""
    block_signs = [
        "Just a moment",
        "Chờ một chút",
        "Enable JavaScript",
        "Checking your browser",
        "cf-spinner",
        "challenge-platform",
        "cf-turnstile",
    ]
    return any(sign.lower() in text.lower() for sign in block_signs)


def extract_cloudflare_info(text: str, headers: dict) -> dict:
    """Extract Cloudflare-related information."""
    info = {
        "cf_version": None,
        "challenge_type": None,
        "requires_js": False,
        "turnstile": False,
    }

    # Check for challenge type in cType
    if "cType" in text:
        match = re.search(r"cType['\"]?\s*[:=]\s*['\"]?(\w+)", text)
        if match:
            info["challenge_type"] = match.group(1)

    # Check for turnstile
    if "turnstile" in text.lower():
        info["turnstile"] = True

    # Check if JS is required
    if "Enable JavaScript" in text or "noscript" in text:
        info["requires_js"] = True

    # Check CF ray for version hints
    cf_ray = headers.get("cf-ray", "")
    if cf_ray:
        # Extract location from ray ID
        parts = cf_ray.split("-")
        if len(parts) > 1:
            info["cf_location"] = parts[1]

    # Check server header
    server = headers.get("server", "")
    if "cloudflare" in server.lower():
        info["cf_version"] = "cloudflare"

    return info


async def analyze_site(site_name: str, urls: dict) -> SiteAnalysis:
    """Analyze a single site's anti-bot behavior."""
    print(f"\n{'='*60}")
    print(f"Analyzing: {site_name}")
    print(f"{'='*60}")

    proxies = {"http": PROXY, "https": PROXY}

    async with AsyncSession(impersonate="chrome120") as session:
        # Test story page
        print(f"  Testing story page: {urls['story']}")
        try:
            story_resp = await session.get(
                urls["story"],
                proxies=proxies,
                timeout=30,
                allow_redirects=True,
            )
            story_status = story_resp.status_code
            story_text = story_resp.text
            story_blocked = is_blocked(story_text)
            story_headers = dict(story_resp.headers)
            story_cookies = [c.name for c in session.cookies]
            print(f"    Status: {story_status}, Blocked: {story_blocked}")
        except Exception as e:
            print(f"    Error: {e}")
            story_status = 0
            story_blocked = True
            story_text = ""
            story_headers = {}
            story_cookies = []

        await asyncio.sleep(1)

        # Test chapter page
        print(f"  Testing chapter page: {urls['chapter']}")
        try:
            chapter_resp = await session.get(
                urls["chapter"],
                proxies=proxies,
                timeout=30,
                allow_redirects=True,
            )
            chapter_status = chapter_resp.status_code
            chapter_text = chapter_resp.text
            chapter_blocked = is_blocked(chapter_text)
            chapter_headers = dict(chapter_resp.headers)
            chapter_cookies = [c.name for c in session.cookies]
            print(f"    Status: {chapter_status}, Blocked: {chapter_blocked}")
            print(f"    Final URL: {chapter_resp.url}")
        except Exception as e:
            print(f"    Error: {e}")
            chapter_status = 0
            chapter_blocked = True
            chapter_text = ""
            chapter_headers = {}

        # Extract Cloudflare info
        cf_info = extract_cloudflare_info(
            chapter_text if chapter_blocked else story_text,
            chapter_headers if chapter_blocked else story_headers,
        )

        # Analyze special headers
        special_headers = {}
        for key in ["cf-ray", "cf-cache-status", "server", "x-powered-by"]:
            if key in story_headers:
                special_headers[key] = story_headers[key]
            if key in chapter_headers:
                special_headers[f"chapter_{key}"] = chapter_headers[key]

        # Build notes
        notes = []
        if story_blocked != chapter_blocked:
            notes.append("story/chapter behave differently")
        if cf_info["turnstile"]:
            notes.append("uses turnstile captcha")
        if cf_info["challenge_type"]:
            notes.append(f"challenge: {cf_info['challenge_type']}")

        return SiteAnalysis(
            site=site_name,
            story_status=story_status,
            story_blocked=story_blocked,
            chapter_status=chapter_status,
            chapter_blocked=chapter_blocked,
            cloudflare_version=cf_info["cf_version"],
            challenge_type=cf_info["challenge_type"],
            requires_js=cf_info["requires_js"],
            cookies_set=list(set(story_cookies + chapter_cookies)),
            special_headers=special_headers,
            notes="; ".join(notes) if notes else "none",
        )


async def test_harvester_behavior():
    """Test how the challenge-harvester handles each site."""
    print("\n" + "=" * 80)
    print("HARVESTER BEHAVIOR TEST")
    print("=" * 80)

    import aiohttp

    for site_name, urls in SITES.items():
        print(f"\n{site_name}:")

        async with aiohttp.ClientSession() as http:
            # Test chapter via harvester
            try:
                async with http.post(
                    "http://localhost:8099/harvest",
                    json={"url": urls["chapter"], "site_key": site_name},
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    data = await resp.json()
                    body = data.get("body", "")
                    status = data.get("status", 0)
                    blocked = is_blocked(body)

                    print(f"  Harvester response:")
                    print(f"    Status from site: {status}")
                    print(f"    Body length: {len(body)}")
                    print(f"    Blocked: {blocked}")
                    print(f"    Has content: {'__NEXT_DATA__' in body or '<div' in body[:1000]}")
            except Exception as e:
                print(f"  Error: {e}")


async def analyze_cloudflare_rules():
    """Try to understand Cloudflare rules by testing patterns."""
    print("\n" + "=" * 80)
    print("CLOUDFLARE RULES ANALYSIS FOR QUYKIEP")
    print("=" * 80)

    proxies = {"http": PROXY, "https": PROXY}

    tests = [
        # Test if specific User-Agent matters
        ("Default UA", {"User-Agent": "Mozilla/5.0"}),
        ("Chrome UA", {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120"}),
        ("Mobile UA", {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"}),
        ("Bot UA", {"User-Agent": "Googlebot/2.1"}),

        # Test if Referer matters
        ("With Referer", {
            "User-Agent": "Mozilla/5.0 Chrome/120",
            "Referer": "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh",
        }),

        # Test if Accept-Language matters
        ("Vietnamese locale", {
            "User-Agent": "Mozilla/5.0 Chrome/120",
            "Accept-Language": "vi-VN,vi;q=0.9",
        }),

        # Test if specific cookies help
        ("With theme cookie", {
            "User-Agent": "Mozilla/5.0 Chrome/120",
            "Cookie": "theme=dark",
        }),
    ]

    url = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0"

    for test_name, headers in tests:
        try:
            async with AsyncSession(impersonate="chrome120") as session:
                resp = await session.get(
                    url,
                    proxies=proxies,
                    timeout=30,
                    headers=headers,
                    allow_redirects=True,
                )
                blocked = is_blocked(resp.text)
                print(f"  {test_name:25} | Status: {resp.status_code:3} | Blocked: {blocked}")
        except Exception as e:
            print(f"  {test_name:25} | Error: {type(e).__name__}")

        await asyncio.sleep(2)


async def test_tls_fingerprint():
    """Test if TLS fingerprint matters."""
    print("\n" + "=" * 80)
    print("TLS FINGERPRINT TEST")
    print("=" * 80)

    url = "https://quykiep.com/truyen/ta-co-mot-quyen-do-nhan-kinh/chuong-1-xem-mo-thieu-nien-do-nhan-kinh-cuon-0"
    proxies = {"http": PROXY, "https": PROXY}

    # Different browser impersonations have different TLS fingerprints
    fingerprints = [
        "chrome110",
        "chrome116",
        "chrome119",
        "chrome120",
        "chrome123",
        "chrome124",
        "safari15_3",
        "safari15_5",
        "safari17_0",
        "edge99",
        "edge101",
    ]

    print("\nTesting different TLS fingerprints (via curl_cffi impersonation):")

    for fp in fingerprints:
        try:
            async with AsyncSession(impersonate=fp) as session:
                resp = await session.get(
                    url,
                    proxies=proxies,
                    timeout=30,
                    allow_redirects=True,
                )
                blocked = is_blocked(resp.text)
                final_url = str(resp.url)
                redirected = final_url != url
                print(f"  {fp:15} | Status: {resp.status_code:3} | Blocked: {str(blocked):5} | Redirected: {redirected}")
        except Exception as e:
            print(f"  {fp:15} | Error: {type(e).__name__}")

        await asyncio.sleep(1.5)


async def main():
    print("=" * 80)
    print("SITE ANTI-BOT COMPARISON")
    print("=" * 80)

    # Analyze each site
    analyses = []
    for site_name, urls in SITES.items():
        analysis = await analyze_site(site_name, urls)
        analyses.append(analysis)
        await asyncio.sleep(2)

    # Print comparison table
    print("\n" + "=" * 80)
    print("COMPARISON SUMMARY")
    print("=" * 80)

    print(f"\n{'Site':<12} | {'Story':<15} | {'Chapter':<15} | {'Challenge':<12} | {'Cookies':<20}")
    print("-" * 80)

    for a in analyses:
        story_status = f"{a.story_status} {'BLOCK' if a.story_blocked else 'OK'}"
        chapter_status = f"{a.chapter_status} {'BLOCK' if a.chapter_blocked else 'OK'}"
        cookies = ", ".join(a.cookies_set[:3]) if a.cookies_set else "none"
        print(f"{a.site:<12} | {story_status:<15} | {chapter_status:<15} | {a.challenge_type or 'none':<12} | {cookies:<20}")

    print(f"\nNotes:")
    for a in analyses:
        if a.notes != "none":
            print(f"  {a.site}: {a.notes}")

    # Run additional tests
    await test_harvester_behavior()
    await analyze_cloudflare_rules()
    await test_tls_fingerprint()

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

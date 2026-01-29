"""Test script to reproduce and debug parsing errors."""
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from flowcore_story.adapters.xtruyen_adapter import XTruyenAdapter
from flowcore_story.adapters.tangthuvien_adapter import TangThuVienAdapter
from flowcore_story.adapters.truyencom_adapter import TruyenComAdapter


async def test_xtruyen_manga_id():
    """Test parsing manga_id from xtruyen story page."""
    print("\n=== Test 1: Xtruyen manga_id parsing ===")

    adapter = XTruyenAdapter()
    test_urls = [
        "https://xtruyen.vn/truyen/tam-quoc-than-thoai-the-gioi",
        "https://xtruyen.vn/truyen/bat-dau-ban-thuong-100-trieu-mang",
        "https://xtruyen.vn/truyen/chi-ton-dinh",
    ]

    for url in test_urls:
        print(f"\nTesting: {url}")
        try:
            # Fetch story page
            html = await adapter.fetch_with_retry(url)
            if not html:
                print(f"  ❌ Failed to fetch page")
                continue

            # Try to extract manga_id
            soup = adapter._make_soup(html)

            # Check for manga_id in various places
            manga_id = None

            # Method 1: Look for data-manga-id attribute
            story_div = soup.select_one('div.story-detail, div.manga-detail')
            if story_div and story_div.get('data-manga-id'):
                manga_id = story_div.get('data-manga-id')
                print(f"  ✅ Found manga_id in data attribute: {manga_id}")

            # Method 2: Look in JavaScript
            if not manga_id:
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string and 'manga_id' in script.string:
                        print(f"  📝 Found 'manga_id' in script: {script.string[:200]}...")
                        # Try to extract it
                        import re
                        match = re.search(r'manga_id["\']?\s*[:=]\s*["\']?(\d+)', script.string)
                        if match:
                            manga_id = match.group(1)
                            print(f"  ✅ Extracted manga_id from script: {manga_id}")
                        break

            if not manga_id:
                print(f"  ❌ No manga_id found")
                print(f"  📄 Page title: {soup.title.string if soup.title else 'N/A'}")

        except Exception as e:
            print(f"  ❌ Error: {e}")


async def test_genre_url_confusion():
    """Test for cross-site URL confusion."""
    print("\n=== Test 2: Cross-site URL confusion ===")

    # This is the error we saw: tangthuvien adapter trying to fetch xtruyen URL
    wrong_url = "https://xtruyen.vn/doc-truyen/thai-co-than-ton"
    correct_url = "https://tangthuvien.net/doc-truyen/thai-co-than-ton"

    print("\n[ERROR] Wrong: Using tangthuvien adapter for xtruyen URL")
    print(f"   URL: {wrong_url}")
    print("   Issue: 'doc-truyen' is tangthuvien pattern, but domain is xtruyen.vn")

    print("\n[OK] Correct: Should use xtruyen adapter for xtruyen URLs")
    print("   URL: https://xtruyen.vn/truyen/thai-co-than-ton")


async def test_story_url_patterns():
    """Test story URL pattern detection."""
    print("\n=== Test 3: Story URL pattern detection ===")

    test_cases = [
        ("tangthuvien", "https://tangthuvien.net/doc-truyen/story-name"),
        ("xtruyen", "https://xtruyen.vn/truyen/story-name"),
        ("truyencom", "https://truyencom.com/story-name/"),
        ("truyencom", "https://truyencom.com/story-name-12345/"),  # with ID
    ]

    for expected_site, url in test_cases:
        print(f"\nURL: {url}")

        # Detect site from URL
        if "tangthuvien.net" in url:
            detected = "tangthuvien"
        elif "xtruyen.vn" in url:
            detected = "xtruyen"
        elif "truyencom.com" in url:
            detected = "truyencom"
        else:
            detected = "unknown"

        if detected == expected_site:
            print(f"  [OK] Correctly detected as {detected}")
        else:
            print(f"  [ERROR] Expected {expected_site}, got {detected}")


async def test_truyencom_story_id():
    """Test extracting story ID from truyencom URLs."""
    print("\n=== Test 4: Truyencom story ID extraction ===")

    test_urls = [
        "https://truyencom.com/tam-quoc-than-thoai-the-gioi/",  # No ID
        "https://truyencom.com/thien-quan-tu-phuc-quan-troi-ban-phuc-12345/",  # With ID
        "https://truyencom.com/chi-ton-dinh-98765/",  # With ID
    ]

    import re
    for url in test_urls:
        print(f"\nURL: {url}")

        # Try to extract numeric ID
        match = re.search(r'/([a-z0-9-]+)-(\d+)/?$', url)
        if match:
            story_slug = match.group(1)
            story_id = match.group(2)
            print(f"  [OK] Found ID: {story_id} (slug: {story_slug})")
        else:
            print("  [WARN] No numeric ID found")


async def main():
    """Run all tests."""
    print("=" * 60)
    print("Testing Parse Errors")
    print("=" * 60)

    # await test_xtruyen_manga_id()
    await test_genre_url_confusion()
    await test_story_url_patterns()
    await test_truyencom_story_id()

    print("\n" + "=" * 60)
    print("Tests completed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

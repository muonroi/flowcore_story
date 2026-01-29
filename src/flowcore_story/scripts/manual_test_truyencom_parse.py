#!/usr/bin/env python
"""
Test script for TruyenCom parsing - verify URL ID, categories, and cover image.
Tests both live site and completed stories metadata.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from flowcore_story.adapters.truyencom_adapter import TruyenComAdapter


async def test_url_with_id(adapter: TruyenComAdapter, url_without_id: str, expected_id: str | None = None):
    """Test that URL gets ID appended"""
    print(f"\n{'='*80}")
    print("TEST: URL ID Resolution")
    print(f"{'='*80}")
    print(f"Input URL: {url_without_id}")

    # Extract title from URL for search
    slug = url_without_id.rstrip('/').split('/')[-1]
    title = slug.replace('-', ' ').title()

    print(f"Searching with title: {title}")

    # This should resolve the ID via search
    normalized = await adapter._ensure_story_url_has_id(url_without_id, title)

    print(f"Resolved URL: {normalized}")

    # Check if ID is present
    has_id = adapter._has_numeric_id(normalized)

    if has_id:
        # Extract ID
        import re
        match = re.search(r'\.(\d+)/?$', normalized)
        if match:
            found_id = match.group(1)
            print(f"✅ SUCCESS: Found ID = {found_id}")

            if expected_id and found_id != expected_id:
                print(f"⚠️  WARNING: Expected ID {expected_id}, got {found_id}")
                return False
            return True
        else:
            print("❌ FAIL: URL has ID pattern but couldn't extract")
            return False
    else:
        print("❌ FAIL: URL still missing ID after resolution")
        return False


async def test_story_parsing(adapter: TruyenComAdapter, story_url: str, story_title: str):
    """Test full story detail parsing"""
    print(f"\n{'='*80}")
    print("TEST: Story Detail Parsing")
    print(f"{'='*80}")
    print(f"Story: {story_title}")
    print(f"URL: {story_url}")

    details = await adapter.get_story_details(story_url, story_title)

    if not details:
        print("❌ FAIL: Could not fetch story details")
        return False

    print("\n📋 Parsed Details:")
    print(f"{'─'*80}")

    # Check required fields
    checks = {
        'title': details.get('title'),
        'url': details.get('url'),
        'author': details.get('author'),
        'status': details.get('status'),
        'description': details.get('description'),
        'story_id': details.get('story_id'),
        'categories': details.get('categories'),
        'genres': details.get('genres'),
        'cover': details.get('cover'),
        'cover_image': details.get('cover_image'),
        'total_chapters': details.get('total_chapters'),
    }

    all_pass = True

    for field, value in checks.items():
        if value:
            if isinstance(value, list):
                print(f"  ✅ {field}: {value} ({len(value)} items)")
            elif isinstance(value, str) and len(value) > 100:
                print(f"  ✅ {field}: {value[:100]}... ({len(value)} chars)")
            else:
                print(f"  ✅ {field}: {value}")
        else:
            status = "⚠️ " if field in ['cover', 'cover_image', 'story_id'] else "❌"
            print(f"  {status} {field}: MISSING")
            if field in ['title', 'url', 'categories', 'total_chapters']:
                all_pass = False

    # Specific checks
    print("\n🔍 Specific Checks:")
    print(f"{'─'*80}")

    # Check 1: URL has ID
    url = details.get('url', '')
    has_id = adapter._has_numeric_id(url)
    if has_id:
        import re
        match = re.search(r'\.(\d+)/?$', url)
        story_id = match.group(1) if match else None
        print(f"  ✅ URL has ID: {story_id}")
    else:
        print(f"  ❌ URL missing ID: {url}")
        all_pass = False

    # Check 2: Categories not empty
    categories = details.get('categories', [])
    if categories:
        print(f"  ✅ Categories: {', '.join(categories)}")
    else:
        print("  ❌ Categories EMPTY")
        all_pass = False

    # Check 3: Cover image exists
    cover = details.get('cover') or details.get('cover_image')
    if cover:
        print(f"  ✅ Cover image: {cover}")
    else:
        print("  ⚠️  Cover image: MISSING (not critical)")

    # Check 4: Chapters loaded
    chapters = details.get('chapters', [])
    total = details.get('total_chapters', 0)
    if total > 0:
        print(f"  ✅ Chapters: {len(chapters)} loaded, {total} total")
    else:
        print("  ❌ No chapters found")
        all_pass = False

    return all_pass


async def test_completed_story_metadata(story_slug: str):
    """Test metadata from completed_stories folder"""
    print(f"\n{'='*80}")
    print("TEST: Completed Story Metadata")
    print(f"{'='*80}")

    # Find the story folder
    base_path = Path(__file__).parent.parent / "completed_stories"

    # Search for story folder
    story_path = None
    for category_dir in base_path.iterdir():
        if not category_dir.is_dir():
            continue
        candidate = category_dir / story_slug
        if candidate.exists():
            story_path = candidate
            break

    if not story_path:
        print(f"❌ Story folder not found: {story_slug}")
        return False

    metadata_file = story_path / "metadata.json"
    if not metadata_file.exists():
        print(f"❌ metadata.json not found in {story_path}")
        return False

    print(f"Found: {metadata_file}")

    with open(metadata_file, encoding='utf-8') as f:
        metadata = json.load(f)

    print("\n📋 Current Metadata:")
    print(f"{'─'*80}")

    issues = []

    # Check URL
    url = metadata.get('url', '')
    print(f"  URL: {url}")
    if '.179' not in url and story_slug == 'cao-thu-tu-chan':
        issues.append("URL missing ID (should end with .179)")
    elif not re.search(r'\.\d+', url):
        issues.append("URL missing numeric ID")

    # Check categories
    categories = metadata.get('categories', [])
    print(f"  Categories: {categories}")
    if not categories:
        issues.append("Categories are empty")

    # Check cover
    cover = metadata.get('cover', '')
    print(f"  Cover: {cover if cover else 'EMPTY'}")
    if not cover:
        issues.append("Cover is empty")

    if issues:
        print("\n❌ Issues Found:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print("\n✅ All checks passed")
        return True


async def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("TRUYENCOM PARSING TEST SUITE")
    print("="*80)

    adapter = TruyenComAdapter()
    results = []

    # Test 1: URL ID resolution
    print("\n" + "▶"*40)
    print("TEST SET 1: URL ID Resolution")
    print("▶"*40)

    test_cases = [
        ("https://truyencom.com/cao-thu-tu-chan", "179"),
        ("https://truyencom.com/tien-nghich", "2"),
    ]

    for url, expected_id in test_cases:
        passed = await test_url_with_id(adapter, url, expected_id)
        results.append(("URL ID: " + url.split('/')[-1], passed))
        await asyncio.sleep(1)

    # Test 2: Full story parsing
    print("\n" + "▶"*40)
    print("TEST SET 2: Story Detail Parsing")
    print("▶"*40)

    test_stories = [
        ("https://truyencom.com/cao-thu-tu-chan.179", "Cao Thủ Tu Chân"),
    ]

    for url, title in test_stories:
        passed = await test_story_parsing(adapter, url, title)
        results.append((f"Parse: {title}", passed))
        await asyncio.sleep(2)

    # Test 3: Check completed story metadata
    print("\n" + "▶"*40)
    print("TEST SET 3: Completed Story Metadata")
    print("▶"*40)

    completed_stories = [
        "cao-thu-tu-chan",
    ]

    for slug in completed_stories:
        passed = await test_completed_story_metadata(slug)
        results.append((f"Metadata: {slug}", passed))

    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)

    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")

    total_passed = sum(1 for _, p in results if p)
    print(f"\n{total_passed}/{len(results)} tests passed")

    return total_passed == len(results)


if __name__ == "__main__":
    import re

    success = asyncio.run(main())
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""
Test script for quykiep.com adapter
Chạy trong container: docker exec crawler-producer python /app/scripts/test_quykiep_adapter.py
"""
import asyncio
import json

from flowcore_story.adapters.factory import get_adapter, available_site_keys
from flowcore_story.utils.logger import logger


async def test_adapter():
    print("\n" + "="*80)
    print("TEST QUYKIEP ADAPTER")
    print("="*80)

    # 1. Check available adapters
    print("\n1. Available site keys:")
    keys = available_site_keys()
    print(f"   {keys}")

    if 'quykiep' not in keys:
        print("\n   ERROR: quykiep adapter not found!")
        return False

    # 2. Get adapter
    print("\n2. Getting quykiep adapter...")
    adapter = get_adapter('quykiep')
    print(f"   Adapter: {adapter}")
    print(f"   Site key: {adapter.site_key}")
    print(f"   Base URL: {adapter.base_url}")

    # 3. Test get_genres
    print("\n3. Testing get_genres()...")
    genres = await adapter.get_genres()
    print(f"   Found {len(genres)} genres:")
    for g in genres[:5]:
        print(f"     - {g['name']}: {g['url']}")

    if not genres:
        print("   ERROR: No genres found!")
        return False

    # 4. Test get_stories_in_genre
    print("\n4. Testing get_stories_in_genre()...")
    first_genre = genres[0]
    stories, max_page = await adapter.get_stories_in_genre(first_genre['url'], page=1)
    print(f"   Genre: {first_genre['name']}")
    print(f"   Found {len(stories)} stories, max_page={max_page}")
    for s in stories[:3]:
        print(f"     - {s['title']}: {s['url']}")

    if not stories:
        print("   ERROR: No stories found!")
        return False

    # 5. Test get_story_details
    print("\n5. Testing get_story_details()...")
    first_story = stories[0]
    details = await adapter.get_story_details(first_story['url'], first_story['title'])

    if not details:
        print("   ERROR: Could not get story details!")
        return False

    print(f"   Title: {details.get('title')}")
    print(f"   Author: {details.get('author')}")
    print(f"   Categories: {details.get('categories')}")
    print(f"   Total chapters: {details.get('total_chapters')}")
    print(f"   Description: {details.get('description', '')[:100]}...")

    # 6. Test get_chapter_list
    print("\n6. Testing get_chapter_list()...")
    chapters = await adapter.get_chapter_list(
        first_story['url'],
        first_story['title'],
        'quykiep'
    )
    print(f"   Found {len(chapters)} chapters")
    if chapters:
        print(f"   First: {chapters[0]['title']} - {chapters[0]['url']}")
        print(f"   Last: {chapters[-1]['title']} - {chapters[-1]['url']}")

    # 7. Test get_chapter_content
    print("\n7. Testing get_chapter_content()...")
    if chapters:
        first_chapter = chapters[0]
        content = await adapter.get_chapter_content(
            first_chapter['url'],
            first_chapter['title'],
            'quykiep'
        )

        if content:
            print(f"   Content length: {len(content)} chars")
            print(f"   Preview: {content[:300]}...")
        else:
            print("   WARNING: Could not get chapter content!")

    print("\n" + "="*80)
    print("TEST COMPLETED SUCCESSFULLY!")
    print("="*80)
    return True


if __name__ == "__main__":
    success = asyncio.run(test_adapter())
    exit(0 if success else 1)

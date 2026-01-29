"""Diagnostic script to inspect genre discovery and first-page story fetch.

Run inside the container (env/proxy already configured):
    PYTHONPATH=src python3 scripts/debug_genre_fetch.py
"""

from __future__ import annotations

import asyncio
from typing import Iterable

from flowcore_story.adapters.factory import get_adapter
from flowcore_story.apps.scraper import initialize_scraper
from flowcore_story.config import config as app_config


async def debug_genres(site_key: str, limit: int = 10) -> list[dict[str, str]]:
    adapter = get_adapter(site_key)
    await initialize_scraper(site_key)
    genres = await adapter.get_genres()

    print(f"[{site_key}] genres found: {len(genres)}")
    for item in genres[:limit]:
        name = item.get("name")
        url = item.get("url")
        print(f"  - {name} -> {url}")
    if len(genres) > limit:
        print(f"  ... ({len(genres) - limit} more)")

    return genres


async def debug_xtruyen_problem_genres():
    adapter = get_adapter("xtruyen")
    await initialize_scraper("xtruyen")

    # Known problematic genres (previously reported empty)
    targets = [
        ("Cổ Đại", "https://xtruyen.vn/theloai/co-dai/?m_orderby=views"),
        ("Phương Tây", "https://xtruyen.vn/theloai/phuong-tay/?m_orderby=views"),
        ("Nữ Phụ", "https://xtruyen.vn/theloai/nu-phu/?m_orderby=views"),
    ]

    for name, url in targets:
        try:
            stories, max_page = await adapter.get_stories_in_genre(url, page=1)
        except Exception as exc:  # defensive guard for diagnostics
            print(f"[xtruyen] {name}: error -> {exc}")
            continue

        print(
            f"[xtruyen] {name}: first page stories={len(stories)}, max_page_hint={max_page}"
        )
        for story in stories[:5]:
            print(f"    - {story.get('title')} -> {story.get('url')}")
        if len(stories) > 5:
            print(f"    ... ({len(stories) - 5} more on page 1)")


async def main():
    print("=== Checking genre discovery ===")
    for site in app_config.BASE_URLS.keys():
        await debug_genres(site_key=site, limit=15)

    print("\n=== Checking XTruyen problematic genres (page 1) ===")
    await debug_xtruyen_problem_genres()


if __name__ == "__main__":
    asyncio.run(main())

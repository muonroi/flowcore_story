from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any
from urllib.parse import quote

from flowcore_story.adapters.factory import get_adapter
from flowcore_story.config import config as app_config
from flowcore_story.utils.chapter_utils import slugify_title
from flowcore_story.utils.logger import logger
from flowcore_story.utils.title_matcher import fuzzy_match_titles


# Fuzzy threshold for title matching (default: 0.8)
_FUZZY_THRESHOLD = float(os.getenv("ENRICHMENT_FUZZY_THRESHOLD", "0.8"))

# Sites that require search-based discovery (URL guessing doesn't work)
_SEARCH_REQUIRED_SITES = {"quykiep", "xtruyen", "truyencom"}


def _is_title_match(a: str | None, b: str | None) -> bool:
    """Check if two titles match using fuzzy matching.

    Uses the title_matcher module for Vietnamese-aware fuzzy matching.
    """
    if not isinstance(a, str) or not isinstance(b, str):
        return False
    if not a.strip() or not b.strip():
        return False

    is_match, _score = fuzzy_match_titles(a, b, threshold=_FUZZY_THRESHOLD)
    return is_match


_SITE_CANDIDATE_PATHS: dict[str, list[str]] = {
    # TangThuVien normaliser will rewrite to /doc-truyen/{slug} internally
    "tangthuvien": ["/doc-truyen/{slug}", "/{slug}"],
    # NOTE: xtruyen, quykiep, truyencom use search-based discovery
}


async def _search_on_quykiep(title: str, base_url: str) -> dict[str, Any] | None:
    """Search for story on quykiep using their search API."""
    from flowcore_story.apps.scraper import make_request

    # Create search variants to handle different convert naming (e.g. Võ vs Vũ)
    search_titles = [title]
    
    # Simple variants
    if "Võ" in title:
        search_titles.append(title.replace("Võ", "Vũ"))
    elif "Vũ" in title:
        search_titles.append(title.replace("Vũ", "Võ"))
        
    # Short prefix search (first 2-3 words) if original fails
    words = title.split()
    if len(words) > 3:
        prefix = " ".join(words[:2])
        if prefix not in search_titles:
            search_titles.append(prefix)

    for search_title in search_titles:
        search_url = f"{base_url}/tim-kiem?keyword={quote(search_title)}"
        try:
            response = await make_request(search_url, site_key="quykiep")
            if not response or not response.text:
                continue

            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                response.text,
            )
            if not match:
                continue

            data = json.loads(match.group(1))
            page_props = data.get("props", {}).get("pageProps", {})
            
            # 1. Check primary search results
            # 2. Fallback to popular/new lists (often contains the target if it's hot)
            results = page_props.get("result", []) or []
            popular = page_props.get("lstPopular", []) or []
            new_stories = page_props.get("lstNew", []) or []
            
            # Deduplicate items by slug while merging
            combined_pool = {item.get("slug"): item for item in (results + popular + new_stories) if item.get("slug")}.values()

            for result in combined_pool:
                result_title = result.get("name") or result.get("title", "")
                result_slug = result.get("slug", "")
                if result_title and result_slug and _is_title_match(title, result_title):
                    return {
                        "url": f"{base_url}/truyen/{result_slug}",
                        "title": result_title,
                        "site_key": "quykiep",
                    }
            
            # If we found items in primary results but no match, don't keep trying variants 
            # to avoid excessive requests, unless the first search was for a variant.
            if results and search_title == title:
                break
                
        except Exception as e:
            logger.debug(f"[MIRROR] Error searching quykiep: {e}")
            continue
            
    return None


async def _search_on_xtruyen(title: str, base_url: str) -> dict[str, Any] | None:
    """Search for story on xtruyen using WordPress search."""
    from flowcore_story.apps.scraper import make_request
    from flowcore_story.analyze.xtruyen_parse import parse_story_list

    search_url = f"{base_url}/?s={quote(title)}&post_type=wp-manga"
    try:
        response = await make_request(search_url, site_key="xtruyen")
        if not response or not response.text:
            return None

        stories, _ = parse_story_list(response.text, base_url)

        for story in stories:
            result_title = story.get("title", "")
            result_url = story.get("url", "")
            
            if result_title and result_url and _is_title_match(title, result_title):
                return {
                    "url": result_url,
                    "title": result_title,
                    "site_key": "xtruyen",
                }
        return None
    except Exception as e:
        logger.debug(f"[MIRROR] Error searching xtruyen: {e}")
        return None


async def _search_on_truyencom(title: str, base_url: str) -> dict[str, Any] | None:
    """Search for story on truyencom using their search page."""
    from flowcore_story.apps.scraper import make_request
    from flowcore_story.analyze.truyencom_parse import parse_story_list

    search_url = f"{base_url}/tim-kiem?tukhoa={quote(title)}"
    try:
        response = await make_request(search_url, site_key="truyencom")
        if not response or not response.text:
            return None

        stories, _ = parse_story_list(response.text, base_url)

        for story in stories:
            result_title = story.get("title", "")
            result_url = story.get("url", "")
            
            if result_title and result_url and _is_title_match(title, result_title):
                return {
                    "url": result_url,
                    "title": result_title,
                    "site_key": "truyencom",
                }
        return None
    except Exception as e:
        logger.debug(f"[MIRROR] Error searching truyencom: {e}")
        return None


_SEARCH_FUNCTIONS = {
    "quykiep": _search_on_quykiep,
    "xtruyen": _search_on_xtruyen,
    "truyencom": _search_on_truyencom,
}


async def discover_mirror_sources_by_title(
    metadata: dict[str, Any],
    *,
    prefer_sites: list[str] | None = None,
    max_add: int = 1,
) -> list[dict[str, Any]]:
    """Try to find mirror sources for a story by title.

    Returns a list of source dicts to be appended to metadata["sources"].
    Uses search API for sites that require it (quykiep, xtruyen, truyencom).
    """

    title = metadata.get("title")
    if not isinstance(title, str) or not title.strip():
        return []

    existing_sources = metadata.get("sources") or []
    existing_site_keys = {
        (s.get("site_key") or s.get("site") or "").strip()
        for s in existing_sources
        if isinstance(s, dict)
    }
    primary_site_key = (
        metadata.get("site_key") or (existing_sources[0].get("site_key") if existing_sources and isinstance(existing_sources[0], dict) else None)
    )

    enabled_sites = list(app_config.ENABLED_SITE_KEYS or [])
    candidate_sites = prefer_sites or enabled_sites
    discovered: list[dict[str, Any]] = []
    slug = slugify_title(title)

    for site_key in candidate_sites:
        if not site_key or site_key == primary_site_key:
            continue
        if site_key in existing_site_keys:
            continue

        base = app_config.BASE_URLS.get(site_key)
        if not base:
            continue

        # Use search-based discovery for sites that require it
        if site_key in _SEARCH_REQUIRED_SITES:
            search_func = _SEARCH_FUNCTIONS.get(site_key)
            if search_func:
                result = await search_func(title, base.rstrip("/"))
                if result:
                    discovered.append({
                        "url": result["url"],
                        "site_key": site_key,
                        "priority": 100 + len(discovered),
                    })
                    logger.debug(
                        f"[MIRROR] Found {site_key} via search: {result['title']}"
                    )
            if len(discovered) >= max_add:
                break
            continue

        # URL guessing for other sites
        path_patterns = _SITE_CANDIDATE_PATHS.get(site_key, ["/{slug}"])
        adapter = get_adapter(site_key)

        for pattern in path_patterns:
            path = pattern.format(slug=slug)
            url = base.rstrip("/") + path
            try:
                details = await adapter.get_story_details(url, title)
            except Exception:
                details = None

            if not details:
                continue

            cand_title = details.get("title") if isinstance(details, dict) else None
            if _is_title_match(title, cand_title):
                # Keep the adapter-normalised URL if present
                final_url = details.get("url") or url
                discovered.append({
                    "url": final_url,
                    "site_key": site_key,
                    "priority": 100 + len(discovered),
                })
                break

        if len(discovered) >= max_add:
            break

    return discovered


async def discover_and_update_mirrors(
    metadata: dict[str, Any],
    metadata_path: str | None = None,
    *,
    prefer_sites: list[str] | None = None,
    max_add: int = 1,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Discover mirrors and persist them into metadata if any found."""

    additions = await discover_mirror_sources_by_title(
        metadata, prefer_sites=prefer_sites, max_add=max_add
    )
    if not additions:
        return metadata, []

    sources = metadata.get("sources")
    if not isinstance(sources, list):
        sources = []
    sources.extend(additions)
    metadata["sources"] = sources

    if metadata_path:
        try:
            # Write back to disk lazily to avoid blocking event loop
            import json
            def _write() -> None:
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
            await asyncio.to_thread(_write)
        except Exception:
            pass

    return metadata, additions


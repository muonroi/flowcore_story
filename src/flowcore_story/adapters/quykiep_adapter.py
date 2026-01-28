"""
Adapter for quykiep.com - Light Novel site (Next.js based)

URL patterns:
- Homepage: https://quykiep.com
- Genre list: /the-loai
- Story list: /truyen-dich-ds, /truyen-{genre}-ln
- Story detail: /truyen/{slug}
- Chapter: /truyen/{slug}/chuong-{N}
"""
import asyncio
import json
import re
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlparse

from flowcore.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.analyze.quykiep_parse import (
    extract_next_data,
    parse_chapter_content,
    parse_chapter_list_page,
    parse_genres,
    parse_story_info,
    parse_story_list,
)
from flowcore.apps.scraper import _make_request_playwright
from flowcore_story.config.config import BASE_URLS
from flowcore.utils.logger import logger
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.site_config import load_site_config

_CANONICAL_URL_RE = re.compile(
    r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_OG_URL_RE = re.compile(
    r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)


class QuykiepAdapter(BaseSiteAdapter):
    """Adapter for quykiep.com Light Novel site."""

    site_key = "quykiep"

    def __init__(self) -> None:
        self.site_key = self.__class__.site_key
        site_config = load_site_config(self.site_key)

        configured_base_url = site_config.get("base_url")
        self.base_url = configured_base_url or BASE_URLS.get(self.site_key, "https://quykiep.com")

        self._items_per_page = site_config.get("items_per_page", 18)
        self._details_cache: dict[str, dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()

    def _is_homepage_url(self, url: str) -> bool:
        if not url:
            return False
        base = urlparse(self.base_url)
        parsed = urlparse(url)
        if base.netloc and parsed.netloc and parsed.netloc != base.netloc:
            return False
        return parsed.path.strip("/") == ""

    def _extract_meta_url(self, html_content: str) -> str | None:
        for pattern in (_CANONICAL_URL_RE, _OG_URL_RE):
            match = pattern.search(html_content)
            if match:
                return match.group(1).strip()
        return None

    def _looks_like_homepage(self, html_content: str) -> bool:
        meta_url = self._extract_meta_url(html_content)
        if meta_url and self._is_homepage_url(meta_url):
            return True
        base_root = f"{self.base_url.rstrip('/')}/".lower()
        html_lower = html_content.lower()
        if "rel=\"canonical\"" in html_lower and base_root in html_lower:
            return True
        if "property=\"og:url\"" in html_lower and base_root in html_lower:
            return True
        return False

    async def _fetch_text(self, url: str) -> str | None:
        """Fetch text with custom redirect check logic, using shared session."""
        from flowcore.apps.scraper import make_request
        
        # Use make_request directly to get ScraperResponse with URL info
        response = await make_request(url, site_key=self.site_key, method="GET")
        text = response.text if response else None

        if not text:
            return None

        # Analyze URL
        final_url = str(response.url)
        parsed_original = urlparse(url)
        parsed_final = urlparse(final_url)
        path = parsed_original.path or ""

        # 1. TRAP REDIRECT (To different story) -> Always FAIL/RETRY
        if "/truyen/" in parsed_original.path and "/truyen/" in parsed_final.path:
            parts_orig = parsed_original.path.split("/")
            parts_final = parsed_final.path.split("/")
            if len(parts_orig) > 2 and len(parts_final) > 2:
                original_slug = parts_orig[2]
                final_slug = parts_final[2]
                if original_slug != final_slug:
                    logger.warning(
                        f"[{self.site_key}] Trap redirect detected! Requested: {original_slug} -> Got: {final_slug}"
                    )
                    return None

        # Define helper for payload validation
        is_chapter_url = "/chuong-" in path
        is_chapter_list = "/danh-sach-chuong" in path

        def _has_expected_payload(page_props: dict[str, Any] | None) -> bool:
            if not isinstance(page_props, dict):
                return False
            if is_chapter_url:
                chapter = page_props.get("chapter")
                if not isinstance(chapter, dict) or not chapter:
                    return False
                content = chapter.get("content")
                if content and isinstance(content, str) and content.strip():
                    return True
                for fallback_key in ["data", "data1", "data2", "data3"]:
                    fallback_val = chapter.get(fallback_key)
                    if fallback_val and isinstance(fallback_val, str) and len(fallback_val.strip()) > 100:
                        return True
                return False
            if is_chapter_list:
                return bool(page_props.get("chapterList"))
            return bool(page_props.get("book") or page_props.get("data"))

        async def _playwright_fallback() -> str | None:
            # Helper to try Playwright if all else fails
            playwright_response = await _make_request_playwright(
                url,
                self.site_key,
                wait_for_selector="script#__NEXT_DATA__",
            )
            playwright_text: str | None = None
            if isinstance(playwright_response, str):
                playwright_text = playwright_response
            elif playwright_response is not None:
                playwright_text = getattr(playwright_response, "text", None)
            return playwright_text

        # 2. HOMEPAGE REDIRECT (Anti-bot or Content Locked) -> Try Next.js Data Fallback
        if self._looks_like_homepage(text) or parsed_final.path.strip("/") == "":
            logger.warning(f"[{self.site_key}] Anti-bot redirect to homepage detected for: {url}")
            
            # Attempt to fetch JSON data directly (bypass HTML check)
            if path and path.strip("/"):
                next_data = extract_next_data(text)
                build_id = next_data.get("buildId") if isinstance(next_data, dict) else None
                
                if build_id:
                    next_url = f"{self.base_url.rstrip('/')}/_next/data/{build_id}{path}.json"
                    if parsed_original.query:
                        next_url = f"{next_url}?{parsed_original.query}"
                    
                    logger.debug(f"[{self.site_key}] Trying Next.js data fallback: {next_url}")
                    json_resp = await make_request(next_url, site_key=self.site_key, method="GET")
                    json_payload = json_resp.text if json_resp else None
                    
                    if json_payload:
                        try:
                            payload = json.loads(json_payload)
                            if isinstance(payload, dict):
                                page_props = payload.get("pageProps")
                                # Validate if this payload actually has the content we need
                                if _has_expected_payload(page_props):
                                    logger.info(f"[{self.site_key}] Successfully recovered content via Next.js API")
                                    # Wrap in fake HTML for parser
                                    wrapped = {
                                        "props": {"pageProps": page_props},
                                        "page": payload.get("page"),
                                        "query": payload.get("query"),
                                        "buildId": build_id,
                                    }
                                    return (
                                        "<!DOCTYPE html><html><head></head><body>"
                                        f"<script id=\"__NEXT_DATA__\" type=\"application/json\">{json.dumps(wrapped)}</script>"
                                        "</body></html>"
                                    )
                        except Exception as e:
                            logger.debug(f"[{self.site_key}] JSON fallback parsing failed: {e}")

            # If fallback failed, try Playwright
            return await _playwright_fallback()

        return text

        return None

    def get_chapters_per_page_hint(self) -> int:
        """Return estimated chapters per page for this site."""
        return 100  # We generate all chapters at once

    async def get_genres(self) -> list[dict[str, str]]:
        """Fetch and parse genre list."""
        logger.debug(f"[{self.site_key}] Fetching genres")

        response_text = await self._fetch_text(f"{self.base_url}/the-loai")
        if not response_text:
            logger.error(f"[{self.site_key}] Failed to fetch genre page")
            return []

        genres = parse_genres(response_text, self.base_url)
        logger.info(f"[{self.site_key}] Found {len(genres)} genres")

        return genres

    async def get_stories_in_genre(
        self,
        genre_url: str,
        page: int = 1
    ) -> tuple[list[dict[str, str]], int]:
        """Fetch stories from a specific genre page."""
        # Add page parameter
        if page > 1:
            if '?' in genre_url:
                page_url = f"{genre_url}&page={page}"
            else:
                page_url = f"{genre_url}?page={page}"
        else:
            page_url = genre_url

        logger.debug(f"[{self.site_key}] Fetching genre page {page}: {page_url}")

        response_text = await self._fetch_text(page_url)
        if not response_text:
            logger.warning(f"[{self.site_key}] Failed to fetch genre page {page}")
            return [], 0

        stories, max_page = parse_story_list(response_text, self.base_url)
        logger.debug(f"[{self.site_key}] Found {len(stories)} stories on page {page}, max_page={max_page}")

        return stories, max_page

    async def get_all_stories_from_genre(
        self,
        genre_name: str,
        genre_url: str,
        max_pages: int | None = None
    ) -> list[dict[str, str]]:
        """Fetch all stories from a genre across all pages."""
        all_stories: list[dict[str, str]] = []
        page = 1

        while True:
            stories, detected_max = await self.get_stories_in_genre(genre_url, page)

            if not stories:
                break

            all_stories.extend(stories)
            logger.info(f"[{self.site_key}] {genre_name}: page {page} -> {len(stories)} stories")

            if max_pages and page >= max_pages:
                break

            if page >= detected_max:
                break

            page += 1
            await asyncio.sleep(0.5)

        logger.info(f"[{self.site_key}] {genre_name}: Total {len(all_stories)} stories")
        return all_stories

    async def get_all_stories_from_genre_with_page_check(
        self,
        genre_name: str,
        genre_url: str,
        site_key: str,
        max_pages: int | None = None,
        *,
        page_callback: Callable[[list[dict[str, Any]], int, int | None], Awaitable[None]] | None = None,
        collect: bool = True,
    ) -> tuple[list[dict[str, str]], int, int]:
        """Fetch stories from genre with optional page callback.

        Returns:
            Tuple of (stories, total_pages, crawled_pages)
        """
        logger.info(f"[{self.site_key}] Crawling stories for genre '{genre_name}'")

        try:
            metrics_tracker.genre_started(
                site_key=site_key,
                genre_name=genre_name,
                genre_url=genre_url,
            )
        except Exception:
            logger.debug(f"[{self.site_key}] Unable to register genre '{genre_name}' with metrics", exc_info=True)

        # Fetch first page
        first_page_stories, total_pages = await self.get_stories_in_genre(genre_url, page=1)
        if not first_page_stories:
            logger.warning(f"[{self.site_key}] No stories detected on first page of {genre_name}")
            return [], 0, 0

        # Update metrics
        metrics_tracker.update_genre_pages(
            site_key,
            genre_url,
            crawled_pages=1,
            total_pages=total_pages,
            current_page=1,
        )

        # Initialize tracking
        all_stories: list[dict[str, Any]] = list(first_page_stories) if collect else []
        for story in first_page_stories:
            story.setdefault('_source_page', 1)

        # Duplicate detection
        seen_urls: set[str] = set()
        for story in first_page_stories:
            url_key = story.get("url")
            if url_key:
                seen_urls.add(url_key)

        discovered_total = len(first_page_stories)
        metrics_tracker.set_genre_story_total(site_key, genre_url, discovered_total)

        if page_callback:
            await page_callback(list(first_page_stories), 1, total_pages)

        # Continue with remaining pages
        crawled_pages = 1
        page = 2
        limit = max_pages or total_pages or 1

        while page <= limit:
            stories, _ = await self.get_stories_in_genre(genre_url, page)
            crawled_pages += 1

            metrics_tracker.update_genre_pages(
                site_key,
                genre_url,
                crawled_pages=crawled_pages,
                total_pages=total_pages,
                current_page=page,
            )

            if not stories:
                logger.info(f"[{self.site_key}] Stop paging {genre_name}: empty page {page}")
                break

            # Filter duplicates and add source page
            new_batch: list[dict[str, Any]] = []
            for story in stories:
                story.setdefault('_source_page', page)
                url_key = story.get("url")
                if not url_key or url_key in seen_urls:
                    continue
                seen_urls.add(url_key)
                new_batch.append(story)

            if not new_batch:
                logger.info(f"[{self.site_key}] Stop paging {genre_name}: no new stories on page {page}")
                break

            if collect:
                all_stories.extend(new_batch)
                discovered_total = len(all_stories)
            else:
                discovered_total += len(new_batch)

            metrics_tracker.set_genre_story_total(site_key, genre_url, discovered_total)

            if page_callback:
                await page_callback(list(new_batch), page, total_pages)

            logger.info(f"[{self.site_key}] {genre_name}: page {page} -> {len(new_batch)} new stories")
            page += 1
            await asyncio.sleep(0.5)

        logger.info(f"[{self.site_key}] Total stories for {genre_name}: {discovered_total}")
        return all_stories, total_pages, crawled_pages

    async def _get_story_details_internal(self, story_url: str) -> dict[str, Any] | None:
        """Internal method to fetch and parse story details."""
        logger.debug(f"[{self.site_key}] Fetching story details: {story_url}")

        response_text = await self._fetch_text(story_url)
        if not response_text:
            logger.error(f"[{self.site_key}] Failed to fetch story page")
            return None

        details = parse_story_info(response_text, self.base_url)

        if not details or '_parse_error' in details:
            parse_error = details.get('_parse_error') if details else None
            if self._looks_like_homepage(response_text):
                logger.warning(
                    f"[{self.site_key}] Homepage content detected for {story_url}; likely anti-bot redirect."
                )
                return None
            logger.error(f"[{self.site_key}] Failed to parse story info: {story_url} (error: {parse_error})")
            return None

        # Fetch chapter list from dedicated page
        story_slug = details.get('slug', '')
        total_chapters = details.get('total_chapters', 0)

        if story_slug and total_chapters > 0:
            chapters = await self._fetch_all_chapters(story_slug, total_chapters)
            details['chapters'] = chapters
        else:
            details['chapters'] = []

        details['url'] = story_url
        details['sources'] = [
            {"url": story_url, "site_key": self.site_key, "priority": 1}
        ]

        return details

    async def _fetch_all_chapters(self, story_slug: str, total_chapters: int) -> list[dict[str, str]]:
        """Fetch all chapters from the chapter list pages."""
        all_chapters: list[dict[str, str]] = []
        page = 1
        chapters_per_page = 50  # Based on observed data
        max_pages = (total_chapters + chapters_per_page - 1) // chapters_per_page

        while page <= max_pages:
            chapter_list_url = f"{self.base_url}/truyen/{story_slug}/danh-sach-chuong?page={page}"
            logger.debug(f"[{self.site_key}] Fetching chapter list page {page}: {chapter_list_url}")

            response_text = await self._fetch_text(chapter_list_url)
            if not response_text:
                logger.warning(f"[{self.site_key}] Failed to fetch chapter list page {page}")
                break

            chapters, total, current_page = parse_chapter_list_page(
                response_text, story_slug, self.base_url
            )

            if not chapters:
                logger.debug(f"[{self.site_key}] No more chapters on page {page}")
                break

            all_chapters.extend(chapters)
            logger.debug(f"[{self.site_key}] Got {len(chapters)} chapters from page {page}")

            page += 1
            await asyncio.sleep(0.3)  # Be nice to the server

        logger.info(f"[{self.site_key}] Total {len(all_chapters)} chapters fetched for {story_slug}")
        return all_chapters

    async def get_story_details(
        self,
        story_url: str,
        story_title: str
    ) -> dict[str, Any] | None:
        """Get story details with caching."""
        async with self._details_lock:
            cached = self._details_cache.get(story_url)
            if cached:
                return cached

            details = await self._get_story_details_internal(story_url)

            if details:
                self._details_cache[story_url] = details

            return details

    async def get_chapter_list(
        self,
        story_url: str,
        story_title: str,
        site_key: str,
        max_pages: int | None = None,
        total_chapters: int | None = None,
        max_batches: int | None = None,
    ) -> list[dict[str, str]]:
        """Get chapter list for a story."""
        logger.debug(f"[{self.site_key}] Getting chapter list for '{story_title}'")

        details = await self.get_story_details(story_url, story_title)

        if not details:
            logger.error(f"[{self.site_key}] Could not load story details for {story_url}")
            return []

        chapters = details.get('chapters', [])
        logger.info(f"[{self.site_key}] Found {len(chapters)} chapters for '{story_title}'")

        return chapters

    async def get_chapter_content(
        self,
        chapter_url: str,
        chapter_title: str,
        site_key: str
    ) -> str | None:
        """Fetch and parse chapter content."""
        logger.debug(f"[{self.site_key}] Fetching chapter: {chapter_title}")

        response_text = await self._fetch_text(chapter_url)
        if not response_text:
            logger.error(f"[{self.site_key}] Failed to fetch chapter: {chapter_title}")
            return None

        content = parse_chapter_content(response_text)

        if content:
            logger.debug(f"[{self.site_key}] Successfully extracted content for: {chapter_title} (length: {len(content)} chars)")
            return content

        logger.warning(f"[{self.site_key}] No content extracted for: {chapter_title}")
        return None

    def extract_chapter_list(self, html: str, base_url: str | None = None) -> list[dict[str, str]]:
        story_slug = None
        source_url = base_url or ""
        if source_url:
            parsed = urlparse(source_url)
            parts = [part for part in parsed.path.split("/") if part]
            if "truyen" in parts:
                idx = parts.index("truyen")
                if idx + 1 < len(parts):
                    story_slug = parts[idx + 1]

        if not story_slug:
            details = parse_story_info(html, base_url or self.base_url)
            if isinstance(details, dict):
                story_slug = details.get("slug")

        if not story_slug:
            logger.warning(f"[{self.site_key}] Unable to determine story slug for chapter list parsing")
            return []

        chapters, _total, _page = parse_chapter_list_page(html, story_slug, base_url or self.base_url)
        return chapters

    def extract_chapter_content(self, html: str, base_url: str | None = None) -> str | None:
        content = parse_chapter_content(html)
        if content:
            return content
        logger.warning(f"[{self.site_key}] Failed to parse chapter content from HTML")
        return None

    async def search_story(self, query: str) -> list[dict[str, str]]:
        """Search for stories by title with strict matching on quykiep.com."""
        import urllib.parse
        from difflib import SequenceMatcher
        
        def similarity(a, b):
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()

        encoded_query = urllib.parse.quote(query)
        search_url = f"{self.base_url}/tim-kiem?keyword={encoded_query}"

        logger.info(f"[{self.site_key}] Searching: {search_url}")

        response_text = await self._fetch_text(search_url)
        if not response_text:
            return []

        next_data = extract_next_data(response_text)
        if not next_data:
            return []

        page_props = next_data.get('props', {}).get('pageProps', {})
        data = page_props.get('data', [])

        results = []
        for item in data:
            if not isinstance(item, dict):
                continue

            name = item.get('name')
            slug = item.get('slug')

            if not name or not slug:
                continue
            
            # Strict matching check
            score = similarity(query, name)
            if score < 0.85:
                logger.debug(f"[{self.site_key}] Skipping search result '{name}' (score {score:.2f} < 0.85)")
                continue

            results.append({
                'title': name,
                'url': f"{self.base_url}/truyen/{slug}",
                'site_key': self.site_key,
                'match_score': score
            })

        results.sort(key=lambda x: x.get("match_score", 0), reverse=True)
        return results

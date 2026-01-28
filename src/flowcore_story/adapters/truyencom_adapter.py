import asyncio
import os
import re
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlparse

from flowcore.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.analyze.truyencom_parse import (
    extract_story_slug,
    parse_chapter_content,
    parse_chapter_list,
    parse_chapter_list_api,
    parse_genres,
    parse_genre_api_response,
    parse_search_results,
    parse_story_info,
    parse_story_list,
)
from flowcore.apps.scraper import make_request, refresh_site_cookie
from flowcore.config.config import BASE_URLS
from flowcore_story.utils.chapter_utils import get_chapter_sort_key
from flowcore.utils.logger import logger
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.site_config import load_site_config


def _with_page_parameter(url: str, page: int) -> str:
    """Add page number to URL for truyencom.com pagination"""
    if page <= 1:
        return url

    # Pattern: /truyen-{genre}/full/ -> /truyen-{genre}/full/trang-{page}/
    # Pattern: /truyen-{genre}/full/trang-2/ -> /truyen-{genre}/full/trang-{page}/

    # Remove existing page parameter if present
    url = re.sub(r'/trang-\d+/?$', '/', url)

    # Ensure URL ends with /
    if not url.endswith('/'):
        url += '/'

    # Add page parameter
    url = url.rstrip('/') + f'/trang-{page}/'

    return url


class TruyenComAdapter(BaseSiteAdapter):
    site_key = "truyencom"
    _DEFAULT_CHAPTER_API_PER_PAGE = 50

    def __init__(self) -> None:
        self.site_key = self.__class__.site_key
        site_config = load_site_config(self.site_key)

        configured_base_url = site_config.get("base_url")
        self.base_url = configured_base_url or BASE_URLS.get(self.site_key, "https://truyencom.com")

        chapter_api_per_page_raw = site_config.get("chapter_api_per_page", self._DEFAULT_CHAPTER_API_PER_PAGE)
        self._chapter_api_per_page = self._coerce_positive_int(
            chapter_api_per_page_raw, self._DEFAULT_CHAPTER_API_PER_PAGE
        )

        self._details_cache: dict[str, dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()

    @staticmethod
    def _coerce_positive_int(value: Any, default: int) -> int:
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            return default
        return candidate if candidate > 0 else default

    _ID_PATTERN = re.compile(r"/[^/]+\.\d+/?$")

    def _normalize_story_url(self, url: str) -> str:
        """Normalize story URL to canonical form"""
        parsed = urlparse(url)

        # VALIDATION: Only normalize URLs from this site
        if parsed.netloc:
            allowed_domains = ["truyencom.com", "www.truyencom.com"]
            if parsed.netloc not in allowed_domains:
                # URL is from a different site, return as-is without normalization
                logger.warning(
                    f"[{self.site_key}] Attempted to normalize URL from different site: {url} "
                    f"(domain: {parsed.netloc}, expected: {allowed_domains})"
                )
                return url

        path = parsed.path.rstrip('/')
        return f"{parsed.scheme}://{parsed.netloc}{path}/"

    def _has_numeric_id(self, url: str) -> bool:
        return bool(self._ID_PATTERN.search(urlparse(url).path.rstrip("/")))

    async def _ensure_story_url_has_id(
        self,
        story_url: str,
        story_title: str | None = None,
    ) -> str:
        """Best effort to append '.<id>/' to story URLs that are missing it."""
        normalized = self._normalize_story_url(story_url)
        if self._has_numeric_id(normalized):
            return normalized

        title_hint = (story_title or "").strip()
        logger.info(
            "[%s] Story url '%s' lacks numeric id suffix; attempting to resolve via search (title='%s')",
            self.site_key,
            normalized,
            title_hint or "<unknown>",
        )

        search_terms: list[str] = []
        if title_hint:
            search_terms.append(title_hint)
        slug = normalized.rstrip("/").split("/")[-1]
        if slug:
            search_terms.append(slug.replace("-", " "))

        for term in search_terms:
            if not term:
                continue
            results = await self.search_by_title(term)
            for result in results:
                url = result.get("url")
                slug_with_id = result.get("slug_with_id")
                if url and self._has_numeric_id(url):
                    logger.info(
                        "[%s] Resolved story url '%s' -> '%s' via search result '%s'",
                        self.site_key,
                        normalized,
                        url,
                        term,
                    )
                    return self._normalize_story_url(url)
                if slug_with_id:
                    parts = slug_with_id.split(".", 1)
                    if len(parts) == 2:
                        candidate = f"{self.base_url.rstrip('/')}/{slug_with_id.strip('/')}/"
                        logger.info(
                            "[%s] Constructed story url '%s' -> '%s' from slug_with_id '%s'",
                            self.site_key,
                            normalized,
                            candidate,
                            slug_with_id,
                        )
                        return candidate

        logger.warning(
            "[%s] Could not resolve numeric id for story url '%s'; continuing without it.",
            self.site_key,
            normalized,
        )
        return normalized

    def get_chapters_per_page_hint(self) -> int:
        """Return the number of chapters per page for this site"""
        return self._chapter_api_per_page

    async def get_genres(self) -> list[dict[str, str]]:
        """Fetch and parse genre list from homepage"""
        logger.debug(f"[{self.site_key}] Fetching genres from homepage")

        response = await make_request(
            self.base_url,
            site_key=self.site_key,
            method="GET"
        )

        if not response or not response.text:
            logger.error(f"[{self.site_key}] Failed to fetch homepage for genres")
            return []

        genres = parse_genres(response.text, self.base_url)
        logger.info(f"[{self.site_key}] Found {len(genres)} genres")

        return genres

    async def get_stories_in_genre(
        self,
        genre_url: str,
        page: int = 1
    ) -> tuple[list[dict[str, str]], int]:
        """Fetch stories from a specific genre page"""
        
        # 1. Try API strategy if ID is embedded in URL (from parse_genres)
        # URL format: /the-loai/{id}/{slug}/
        match = re.search(r'/the-loai/(\d+)/', genre_url)
        if match:
            cat_id = match.group(1)
            # API: /api/list/{cat_id}/new/{page}/25
            api_url = f"{self.base_url}/api/list/{cat_id}/new/{page}/25"
            logger.debug(f"[{self.site_key}] Fetching genre API page {page}: {api_url}")
            
            response = await make_request(
                api_url,
                site_key=self.site_key,
                method="GET",
                extra_headers={'X-Requested-With': 'XMLHttpRequest'}
            )
            
            if response and response.text:
                return parse_genre_api_response(response.text, self.base_url)

        # 2. Fallback to HTML parsing (mainly for Full lists)
        page_url = _with_page_parameter(genre_url, page)

        logger.debug(f"[{self.site_key}] Fetching genre page {page}: {page_url}")

        response = await make_request(
            page_url,
            site_key=self.site_key,
            method="GET"
        )

        if not response or not response.text:
            logger.warning(f"[{self.site_key}] Failed to fetch genre page {page}")
            return [], 0

        stories, max_page = parse_story_list(response.text, self.base_url)
        logger.debug(f"[{self.site_key}] Found {len(stories)} stories on page {page}, max_page={max_page}")

        return stories, max_page

    async def get_all_stories_from_genre(
        self,
        genre_name: str,
        genre_url: str,
        max_pages: int | None = None
    ) -> list[dict[str, str]]:
        """Fetch all stories from a genre across all pages"""
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
        """Fetch stories from genre with optional page callback

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
                logger.info(
                    f"[{self.site_key}] Stop paging {genre_name}: no new stories on page {page}"
                )
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
        """Internal method to fetch and parse story details"""
        logger.debug(f"[{self.site_key}] Fetching story details: {story_url}")

        response = await make_request(
            story_url,
            site_key=self.site_key,
            method="GET"
        )

        if not response or not response.text:
            logger.error(f"[{self.site_key}] Failed to fetch story page")
            return None

        details = parse_story_info(response.text, self.base_url)

        # Check for parse errors
        if not details or '_parse_error' in details:
            parse_error = details.get('_parse_error') if details else None
            if parse_error == 'deleted_or_invalid_page':
                logger.warning(
                    f"[{self.site_key}] Story page is deleted or invalid (redirects to homepage): {story_url}"
                )
            elif parse_error == 'missing_title':
                logger.error(
                    f"[{self.site_key}] Failed to parse story info (missing title): {story_url}"
                )
            else:
                logger.error(f"[{self.site_key}] Failed to parse story info: {story_url}")
            return None

        # Parse initial chapter list from HTML
        html_chapters = parse_chapter_list(response.text, self.base_url)

        # Try to get more chapters via API if story_id is available
        story_id = details.get('story_id')
        story_slug = extract_story_slug(story_url)

        all_chapters = html_chapters[:]

        if not story_id:
            logger.warning(
                f"[{self.site_key}] No story_id found for {story_url}, "
                f"using HTML chapters only ({len(html_chapters)} chapters)"
            )
        elif story_id and story_slug:
            logger.debug(f"[{self.site_key}] Fetching chapters via API for story {story_id}")

            # Fetch chapters via API pagination
            page = 1
            api_chapters: list[dict[str, str]] = []
            seen_urls: set[str] = set()

            while True:
                api_url = f"{self.base_url}/api/chapters/{story_id}/{page}/{self._chapter_api_per_page}"

                logger.debug(f"[{self.site_key}] Fetching API page {page}: {api_url}")

                api_response = await make_request(
                    api_url,
                    site_key=self.site_key,
                    method="GET",
                    extra_headers={
                        'Accept': '*/*',
                        'Referer': story_url,
                    }
                )

                if not api_response or not api_response.text:
                    break

                page_chapters = parse_chapter_list_api(
                    api_response.text,
                    story_url,
                    story_slug=story_slug,
                    story_id=story_id,
                )

                if not page_chapters:
                    break

                # Add chapters to list
                new_count = 0
                for ch in page_chapters:
                    ch_url = ch.get('url', '')
                    if ch_url and ch_url not in seen_urls:
                        api_chapters.append(ch)
                        seen_urls.add(ch_url)
                        new_count += 1

                logger.debug(f"[{self.site_key}] API page {page}: {new_count} new chapters")

                if new_count == 0:
                    break

                page += 1

                # Safety limit
                if page > 100:
                    logger.warning(f"[{self.site_key}] Reached page limit for API")
                    break

            if api_chapters:
                logger.info(f"[{self.site_key}] Fetched {len(api_chapters)} chapters via API")
                all_chapters = api_chapters

        # Sort chapters by chapter number
        all_chapters.sort(key=lambda ch: get_chapter_sort_key(ch))

        # Map genres to categories for compatibility
        if 'genres' in details and not details.get('categories'):
            details['categories'] = details['genres']

        # Map cover_image to cover for compatibility
        if 'cover_image' in details and not details.get('cover'):
            details['cover'] = details['cover_image']

        total_chapter_count = len(all_chapters)
        details['chapters'] = all_chapters
        details['total_chapters'] = total_chapter_count
        details['total_chapters_on_site'] = total_chapter_count
        details['url'] = story_url
        details['sources'] = [
            {"url": story_url, "site_key": self.site_key, "priority": 1}
        ]

        return details

    async def get_story_details(
        self,
        story_url: str,
        story_title: str
    ) -> dict[str, Any] | None:
        """Get story details with caching"""
        normalized_url = await self._ensure_story_url_has_id(story_url, story_title)

        async with self._details_lock:
            cached = self._details_cache.get(normalized_url)
            if cached:
                return cached

            details = await self._get_story_details_internal(normalized_url)

            if details:
                self._details_cache[normalized_url] = details

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
        """Get chapter list for a story"""
        normalized_url = self._normalize_story_url(story_url)
        logger.debug(f"[{self.site_key}] Getting chapter list for '{story_title}'")

        details = await self.get_story_details(normalized_url, story_title)

        if not details:
            logger.error(f"[{self.site_key}] Could not load story details for {normalized_url}")
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
        """Fetch and parse chapter content

        Returns:
            str: Parsed HTML content (not plain text) if successful
            None: If parsing failed or anti-bot detected
            "": If chapter is verified empty/broken on the website

        Note: For truyencom, we return HTML content that still needs to be
        converted to plain text by _html_fragment_to_text() in chapter_utils.
        The HTML content may be short (< 500 chars) for brief chapters, which is valid.
        """
        logger.debug(f"[{self.site_key}] Fetching chapter: {chapter_title}")

        response = await make_request(
            chapter_url,
            site_key=self.site_key,
            method="GET"
        )

        if not response or not response.text:
            logger.error(f"[{self.site_key}] Failed to fetch chapter: {chapter_title}")
            return None

        content = parse_chapter_content(response.text)

        # FIX: Distinguish between empty content and parsing failure.
        # Empty content is only trusted after an additional verified refetch.
        empty_detected = (content == "")

        if content:  # Valid content (length > 0)
            # IMPORTANT: For truyencom, short content (< 500 chars) is VALID
            # Many chapters legitimately have only 300-500 characters
            # parse_chapter_content() already checked for anti-bot in HTML
            # If we got here, the content is legitimate (even if short)
            logger.debug(f"[{self.site_key}] Successfully extracted content for: {chapter_title} (length: {len(content)} chars)")
            return content

        if empty_detected:
            logger.warning(
                f"[{self.site_key}] Chapter is EMPTY/BROKEN on first fetch: {chapter_title}. "
                "Verifying with fingerprinted refetch before marking empty."
            )
        else:
            logger.warning(
                f"[{self.site_key}] No content extracted for: {chapter_title}; attempting fingerprinted refetch."
            )

        # In CI/pytest, avoid slow Playwright fingerprint refetch to prevent hangs.
        if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("CI") or os.getenv("DISABLE_TRUYENCOM_FALLBACK", "").lower() == "true":
            if empty_detected:
                logger.info("[%s] Skipping fingerprinted refetch in test/CI environment; returning empty string", self.site_key)
                return ""  # Empty content without verification in CI/test mode
            logger.info("[%s] Skipping fingerprinted refetch in test/CI environment", self.site_key)
            return None

        # Try to warm cookies via Playwright and refetch with stricter headers.
        await refresh_site_cookie(
            self.site_key,
            chapter_url,
            wait_for_selector="div#chapter-c, .chapter-c",
        )

        fallback_headers: dict[str, str] = {
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Cache-Control": "max-age=0",
        }
        referer = self._infer_story_url_from_chapter(chapter_url)
        if referer:
            fallback_headers["Referer"] = referer

        response = await make_request(
            chapter_url,
            site_key=self.site_key,
            method="GET",
            wait_for_selector="div#chapter-c, .chapter-c",
            extra_headers=fallback_headers,
        )
        if not response or not response.text:
            logger.error(f"[{self.site_key}] Fallback fetch failed for chapter: {chapter_title}")
            return None

        content = parse_chapter_content(response.text)

        if content:  # Valid content
            logger.debug(f"[{self.site_key}] Successfully extracted content (fallback) for: {chapter_title}")
            return content

        if content == "":
            if empty_detected:
                logger.warning(
                    f"[{self.site_key}] Chapter STILL EMPTY after fallback (verified broken): {chapter_title}"
                )
                return ""  # Empty content verified; skip retries
            logger.warning(
                f"[{self.site_key}] Fallback returned empty after initial parse failure; "
                "treating as retryable to avoid false empty."
            )
            return None

        logger.error(f"[{self.site_key}] Still unable to extract content for: {chapter_title}")
        return None

    def extract_chapter_list(self, html: str, base_url: str | None = None) -> list[dict[str, str]]:
        return parse_chapter_list(html, base_url or self.base_url)

    def extract_chapter_content(self, html: str, base_url: str | None = None) -> str | None:
        content = parse_chapter_content(html)

        if content:
            return content

        if content == "":
            logger.warning(f"[{self.site_key}] Chapter content is empty in HTML")
            return ""

        logger.warning(f"[{self.site_key}] Failed to parse chapter content from HTML")
        return None

    @staticmethod
    def _infer_story_url_from_chapter(chapter_url: str) -> str | None:
        """Derive the story detail URL from a chapter URL."""
        parsed = urlparse(chapter_url)
        if not parsed.scheme or not parsed.netloc:
            return None
        parts = parsed.path.rstrip("/").split("/")
        if len(parts) < 2:
            return None
        base_path = "/".join(parts[:2])  # e.g. /quyen-than.9012
        return f"{parsed.scheme}://{parsed.netloc}{base_path}/"

    async def search_by_title(self, title: str) -> list[dict[str, str]]:
        """Search for stories by title on truyencom.com with strict matching.

        Args:
            title: Story title to search for

        Returns:
            List of story dicts with 'title', 'url', 'slug', 'slug_with_id', 'id'
        """
        if not title or not title.strip():
            logger.warning(f"[{self.site_key}] Empty search title provided")
            return []

        # Build search URL
        from urllib.parse import quote
        from difflib import SequenceMatcher
        
        def similarity(a, b):
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()

        search_url = f"{self.base_url}/tim-kiem/?tukhoa={quote(title)}"

        logger.debug(f"[{self.site_key}] Searching for: '{title}' at {search_url}")

        response = await make_request(
            search_url,
            site_key=self.site_key,
            method="GET"
        )

        if not response or not response.text:
            logger.error(f"[{self.site_key}] Failed to fetch search results for: {title}")
            return []

        raw_results = parse_search_results(response.text, self.base_url)
        results = []
        
        for res in raw_results:
            res_title = res.get("title", "")
            score = similarity(title, res_title)
            if score < 0.85:
                logger.debug(f"[{self.site_key}] Skipping search result '{res_title}' (score {score:.2f} < 0.85)")
                continue
            
            res["match_score"] = score
            results.append(res)

        results.sort(key=lambda x: x.get("match_score", 0), reverse=True)
        logger.info(f"[{self.site_key}] Found {len(results)}/{len(raw_results)} matching search results for '{title}'")

        return results

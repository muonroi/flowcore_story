"""Site adapter for truyenfullmoi.com"""

import asyncio
import re
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from flowcore.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.analyze.truyenfullmoi_parse import (
    extract_story_slug,
    parse_chapter_content,
    parse_chapter_list,
    parse_genres,
    parse_story_info,
    parse_story_list,
)
from flowcore.apps.scraper import make_request
from flowcore_story.config.config import BASE_URLS
from flowcore.utils.chapter_utils import get_chapter_sort_key
from flowcore.utils.logger import logger
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.site_config import load_site_config


def _with_page_parameter(url: str, page: int) -> str:
    """Add page number to genre URL for truyenfullmoi.com pagination

    Pattern: /truyen-tien-hiep/ -> /truyen-tien-hiep/trang-2/

    Args:
        url: Base genre URL
        page: Page number (1-indexed)

    Returns:
        URL with page parameter
    """
    if page <= 1:
        return url

    # Remove existing page parameter if present
    url = re.sub(r'/trang-\d+/?$', '/', url)

    # Ensure URL ends with /
    if not url.endswith('/'):
        url += '/'

    # Add page parameter
    url = url.rstrip('/') + f'/trang-{page}/'

    return url


def _with_chapter_page_parameter(url: str, page: int) -> str:
    """Add page parameter for chapter list pagination

    Pattern: /story.123/ -> /story.123/?page=2

    Args:
        url: Story detail URL
        page: Page number (1-indexed)

    Returns:
        URL with page parameter for chapter pagination
    """
    if page <= 1:
        return url

    # Remove existing page parameter
    url = re.sub(r'\?page=\d+', '', url)

    # Add page parameter
    return f"{url.rstrip('/')}/?page={page}"


class TruyenFullMoiAdapter(BaseSiteAdapter):
    """Adapter for truyenfullmoi.com"""

    site_key = "truyenfullmoi"
    _DEFAULT_CHAPTERS_PER_PAGE = 50

    def __init__(self) -> None:
        self.site_key = self.__class__.site_key
        site_config = load_site_config(self.site_key)

        # Get base URL from config or fallback to default
        configured_base_url = site_config.get("base_url")
        self.base_url = configured_base_url or BASE_URLS.get(self.site_key, "https://truyenfullmoi.com")

        # Get chapters per page from config
        chapters_per_page_raw = site_config.get("chapters_per_page", self._DEFAULT_CHAPTERS_PER_PAGE)
        self._chapters_per_page = self._coerce_positive_int(
            chapters_per_page_raw, self._DEFAULT_CHAPTERS_PER_PAGE
        )

        # Story details cache
        self._details_cache: dict[str, dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()

    @staticmethod
    def _coerce_positive_int(value: Any, default: int) -> int:
        """Coerce value to positive int or return default"""
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            return default
        return candidate if candidate > 0 else default

    def _normalize_story_url(self, url: str) -> str:
        """Normalize story URL to canonical form

        Args:
            url: Story URL (may be relative or absolute)

        Returns:
            Normalized URL with trailing slash
        """
        parsed = urlparse(url)

        # Validation: Only normalize URLs from this site
        if parsed.netloc:
            allowed_domains = ["truyenfullmoi.com", "www.truyenfullmoi.com"]
            if parsed.netloc not in allowed_domains:
                logger.warning(
                    f"[{self.site_key}] Attempted to normalize URL from different site: {url} "
                    f"(domain: {parsed.netloc}, expected: {allowed_domains})"
                )
                return url

        path = parsed.path.rstrip('/')
        return f"{parsed.scheme}://{parsed.netloc}{path}/"

    def get_chapters_per_page_hint(self) -> int:
        """Return the number of chapters per page for this site"""
        return self._chapters_per_page

    async def get_genres(self) -> list[dict[str, str]]:
        """Fetch and parse genre list from homepage

        Returns:
            List of genre dicts with 'name', 'url', 'slug'
        """
        logger.info(f"[{self.site_key}] Fetching genres from {self.base_url} with Playwright")

        response = await make_request(
            self.base_url,
            site_key=self.site_key,
            method="GET",
            timeout=120,
            max_retries=5
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
        """Fetch stories from a specific genre page

        Args:
            genre_url: Genre URL
            page: Page number (1-indexed)

        Returns:
            Tuple of (stories, max_page)
        """
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
        """Fetch all stories from a genre across all pages

        Args:
            genre_name: Genre name for logging
            genre_url: Genre URL
            max_pages: Maximum pages to fetch (None = all)

        Returns:
            List of all stories in genre
        """
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

        Args:
            genre_name: Genre name for logging
            genre_url: Genre URL
            site_key: Site key (for metrics)
            max_pages: Maximum pages to fetch
            page_callback: Optional callback called for each page
            collect: Whether to collect and return all stories

        Returns:
            Tuple of (stories, total_pages, crawled_pages)
        """
        logger.info(f"[{self.site_key}] Crawling stories for genre '{genre_name}'")

        # Register with metrics
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
        """Internal method to fetch and parse story details

        This method fetches the story page and extracts:
        - Story metadata (title, author, genres, etc.)
        - Complete chapter list (across all paginated pages)

        Args:
            story_url: Story detail URL

        Returns:
            Dict with story details and complete chapter list
            None if fetch/parse failed
        """
        logger.debug(f"[{self.site_key}] Fetching story details: {story_url}")

        # Fetch first page of story details
        response = await make_request(
            story_url,
            site_key=self.site_key,
            method="GET"
        )

        if not response or not response.text:
            logger.error(f"[{self.site_key}] Failed to fetch story page")
            return None

        # Parse story metadata
        details = parse_story_info(response.text, self.base_url)

        # Check for parse errors
        if not details or '_parse_error' in details:
            parse_error = details.get('_parse_error') if details else None
            if parse_error == 'deleted_or_invalid_page':
                logger.debug(
                    f"[{self.site_key}] Story page is deleted or invalid (redirects to homepage): {story_url}"
                )
            elif parse_error == 'missing_title':
                logger.error(
                    f"[{self.site_key}] Failed to parse story info (missing title): {story_url}"
                )
            else:
                logger.error(f"[{self.site_key}] Failed to parse story info: {story_url}")
            return None

        # Parse chapter list from first page
        first_page_chapters = parse_chapter_list(response.text, self.base_url)
        all_chapters = first_page_chapters[:]

        logger.info(f"[{self.site_key}] Found {len(first_page_chapters)} chapters on first page")

        # Check if there are more pages of chapters
        # Look for chapter pagination
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find pagination links near chapter list
        # Pattern: <a href="?page=2">2</a>
        pagination_links = soup.select('a[href*="?page="]')
        chapter_pages = []

        for link in pagination_links:
            href = link.get('href', '')
            match = re.search(r'\?page=(\d+)', href)
            if match:
                chapter_pages.append(int(match.group(1)))

        if chapter_pages:
            max_chapter_page = max(chapter_pages)
            logger.info(f"[{self.site_key}] Chapter list has {max_chapter_page} pages")

            # Fetch remaining chapter pages
            for chapter_page in range(2, max_chapter_page + 1):
                page_url = _with_chapter_page_parameter(story_url, chapter_page)

                logger.debug(f"[{self.site_key}] Fetching chapter list page {chapter_page}: {page_url}")

                page_response = await make_request(
                    page_url,
                    site_key=self.site_key,
                    method="GET"
                )

                if not page_response or not page_response.text:
                    logger.warning(f"[{self.site_key}] Failed to fetch chapter page {chapter_page}")
                    break

                page_chapters = parse_chapter_list(page_response.text, self.base_url)

                if not page_chapters:
                    logger.info(f"[{self.site_key}] No chapters on page {chapter_page}, stopping")
                    break

                # Add to list, checking for duplicates
                seen_urls = {ch.get('url') for ch in all_chapters if ch.get('url')}
                new_chapters = []
                for ch in page_chapters:
                    ch_url = ch.get('url')
                    if ch_url and ch_url not in seen_urls:
                        new_chapters.append(ch)
                        seen_urls.add(ch_url)

                all_chapters.extend(new_chapters)
                logger.info(f"[{self.site_key}] Chapter page {chapter_page}: {len(new_chapters)} new chapters")

                # Safety limit
                if chapter_page > 20:
                    logger.warning(f"[{self.site_key}] Reached chapter page limit (20)")
                    break

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

        logger.info(f"[{self.site_key}] Total chapters for story: {total_chapter_count}")

        return details

    async def get_story_details(
        self,
        story_url: str,
        story_title: str
    ) -> dict[str, Any] | None:
        """Get story details with caching

        Args:
            story_url: Story URL
            story_title: Story title (for logging)

        Returns:
            Dict with story details or None
        """
        normalized_url = self._normalize_story_url(story_url)

        async with self._details_lock:
            cached = self._details_cache.get(normalized_url)
            if cached:
                logger.debug(f"[{self.site_key}] Using cached details for: {story_title}")
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
        """Get chapter list for a story

        Args:
            story_url: Story URL
            story_title: Story title
            site_key: Site key (for logging)
            max_pages: Unused (kept for interface compatibility)
            total_chapters: Unused (kept for interface compatibility)
            max_batches: Unused (kept for interface compatibility)

        Returns:
            List of chapter dicts
        """
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

        Args:
            chapter_url: Chapter URL
            chapter_title: Chapter title
            site_key: Site key (for logging)

        Returns:
            str: Parsed HTML content if successful
            None: If parsing failed
            "": If chapter is verified empty/broken
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

        if content:  # Valid content
            logger.debug(
                f"[{self.site_key}] Successfully extracted content for: {chapter_title} "
                f"(length: {len(content)} chars)"
            )
            return content

        if content == "":  # Empty chapter
            logger.warning(f"[{self.site_key}] Chapter is EMPTY: {chapter_title}")
            return ""

        # content is None - parsing failed
        logger.warning(f"[{self.site_key}] Failed to extract content for: {chapter_title}")
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

    async def search_story(self, query: str) -> list[dict[str, str]]:
        """Search for stories by title with strict matching.

        Args:
            query: Search query

        Returns:
            List of found stories with title, url, site_key
        """
        import urllib.parse
        from difflib import SequenceMatcher
        
        def similarity(a, b):
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()

        encoded_query = urllib.parse.quote(query)
        search_url = f"{self.base_url}/tim-kiem?tukhoa={encoded_query}"

        logger.info(f"[{self.site_key}] Searching: {search_url}")

        response = await make_request(
            search_url,
            site_key=self.site_key,
            method="GET",
        )

        if not response or not response.text:
            return []

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        results = []
        rows = soup.select('.list-truyen .row') or soup.select('.list-content .row')

        for row in rows:
            try:
                title_el = row.select_one('.truyen-title a') or row.select_one('h3.truyen-title a')
                if not title_el:
                    continue

                url = title_el.get('href')
                title = title_el.get_text(strip=True)

                if not url or not title:
                    continue
                
                # Strict matching check
                score = similarity(query, title)
                if score < 0.85:
                    logger.debug(f"[{self.site_key}] Skipping search result '{title}' (score {score:.2f} < 0.85)")
                    continue

                full_url = urljoin(self.base_url, url)

                results.append({
                    "title": title,
                    "url": full_url,
                    "site_key": self.site_key,
                    "match_score": score
                })
            except Exception:
                continue
        
        results.sort(key=lambda x: x.get("match_score", 0), reverse=True)
        return results

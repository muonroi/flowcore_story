import asyncio
import re
import unicodedata
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from flowcore_story.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.analyze.tangthuvien_parse import (
    _validate_required_fields,
    find_genre_listing_url,
    parse_chapter_content,
    parse_chapter_list,
    parse_genres,
    parse_story_info,
    parse_story_list,
)
from flowcore.apps.scraper import make_request
from flowcore_story.config.config import (
    BASE_URLS,
    GLOBAL_PROXY_PASSWORD,
    GLOBAL_PROXY_USERNAME,
    _is_desktop_user_agent,
    get_random_headers,
)
from flowcore_story.config.proxy_provider import get_proxy_url
from flowcore.utils.challenge_harvester_client import get_challenge_harvester_client
from flowcore.utils.chapter_utils import get_chapter_sort_key
from flowcore.utils.logger import logger
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.site_config import load_site_config


def _with_page_parameter(url: str, page: int) -> str:
    if page <= 1:
        return url
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    
    # For /tong-hop URLs, simply add/update 'page' query param
    if "/tong-hop" in parsed.path:
        query["page"] = [str(page)]
        new_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    path = parsed.path
    segment_pattern = re.compile(r'/(page|trang|p)/(\d+)$', re.IGNORECASE)
    suffix_pattern = re.compile(r'-(page|trang|p)-(\d+)$', re.IGNORECASE)

    if segment_pattern.search(path):
        path = segment_pattern.sub(lambda match: f"/{match.group(1)}/{page}", path)
        return urlunparse(parsed._replace(path=path))

    if suffix_pattern.search(path):
        path = suffix_pattern.sub(lambda match: f"-{match.group(1)}-{page}", path)
        return urlunparse(parsed._replace(path=path))

    replaced = False
    for key in ("page", "paged", "pageindex"):
        if key in query:
            query[key] = [str(page)]
            replaced = True
    if not replaced:
        query["page"] = [str(page)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

def _slugify(text: str) -> str:
    """
    Chuyển đổi chuỗi tiếng Việt có dấu thành slug không dấu.
    Ví dụ: "Đô Thị" -> "do-thi"
    """
    # Thay thế ký tự 'đ' thành 'd' trước khi chuẩn hóa
    text = text.replace('đ', 'd').replace('Đ', 'D')
    # Chuyển các ký tự có dấu thành không dấu
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    # Chuyển đổi các ký tự đặc biệt, khoảng trắng thành dấu gạch ngang
    text = re.sub(r'[^\w\s-]', '', text).strip().lower()
    # Loại bỏ các dấu gạch ngang thừa
    text = re.sub(r'[-\s]+', '-', text)
    return text

class TangThuVienAdapter(BaseSiteAdapter):
    site_key = "tangthuvien"
    _DEFAULT_CHAPTER_PAGE_SIZE = 75
    _DEFAULT_GENRE_PAGING_FALLBACK_STEP = 5
    _DEFAULT_GENRE_PAGING_FALLBACK_MAX = 50

    # STATIC MAP for Genres to ensure 100% coverage without parsing errors
    _STATIC_GENRES = {
        1: "Tiên Hiệp", 2: "Huyền Huyễn", 3: "Đô Thị", 4: "Khoa Huyễn",
        5: "Kỳ Huyễn", 6: "Võ Hiệp", 7: "Lịch Sử", 8: "Đồng Nhân",
        9: "Quân Sự", 10: "Du Hí", 11: "Cạnh Kỹ", 12: "Linh Dị",
    }

    def __init__(self) -> None:
        self.site_key = self.__class__.site_key
        site_config = load_site_config(self.site_key)
        env_base_url = BASE_URLS.get(self.site_key)
        raw_base_url = site_config.get("base_url", env_base_url)
        configured_base_url = raw_base_url if isinstance(raw_base_url, str) and raw_base_url.strip() else None
        if not configured_base_url and isinstance(env_base_url, str) and env_base_url.strip():
            configured_base_url = env_base_url
        configured_base_url = configured_base_url or "https://tangthuvien.net"
        
        # FORCE DESKTOP: Always prefer desktop URL to avoid SSL issues on mobile subdomain
        desktop_candidate = self._compute_desktop_base_url(configured_base_url) or "https://tangthuvien.net"
        
        self._configured_base_url = configured_base_url
        self.base_url = desktop_candidate
        self._desktop_base_url = desktop_candidate
        self._mobile_base_url = self._compute_mobile_base_url(configured_base_url)

        chapter_page_size_raw = site_config.get("chapter_page_size", self._DEFAULT_CHAPTER_PAGE_SIZE)
        self._chapter_page_size = self._coerce_positive_int(
            chapter_page_size_raw, self._DEFAULT_CHAPTER_PAGE_SIZE
        )
        genre_fallback_step_raw = site_config.get(
            "genre_paging_fallback_step", self._DEFAULT_GENRE_PAGING_FALLBACK_STEP
        )
        self._genre_paging_fallback_step = self._coerce_positive_int(
            genre_fallback_step_raw, self._DEFAULT_GENRE_PAGING_FALLBACK_STEP
        )
        genre_fallback_max_raw = site_config.get(
            "genre_paging_fallback_max", self._DEFAULT_GENRE_PAGING_FALLBACK_MAX
        )
        self._genre_paging_fallback_max = self._coerce_positive_int(
            genre_fallback_max_raw, self._DEFAULT_GENRE_PAGING_FALLBACK_MAX
        )

        self._details_cache: dict[str, dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()
        self._genre_listing_cache: dict[str, str] = {}
        self._genre_listing_lock = asyncio.Lock()

    @staticmethod
    def _compute_desktop_base_url(base_url: str) -> str | None:
        if not base_url or not isinstance(base_url, str):
            return None
        parsed = urlparse(base_url)
        hostname = parsed.hostname or ""
        replacement_host = None
        for prefix in ("m.", "mobile."):
            if hostname.startswith(prefix):
                replacement_host = hostname[len(prefix) :]
                break
        if not replacement_host or replacement_host == hostname:
            return base_url if not hostname.startswith("m.") else None
        port = f":{parsed.port}" if parsed.port else ""
        new_netloc = f"{replacement_host}{port}"
        return urlunparse(parsed._replace(netloc=new_netloc))

    @staticmethod
    def _compute_mobile_base_url(base_url: str) -> str | None:
        if not base_url or not isinstance(base_url, str):
            return None
        parsed = urlparse(base_url)
        hostname = parsed.hostname or ""
        if hostname.startswith("m."):
            return base_url
        mobile_host = f"m.{hostname}"
        port = f":{parsed.port}" if parsed.port else ""
        new_netloc = f"{mobile_host}{port}"
        return urlunparse(parsed._replace(netloc=new_netloc))

    def _convert_to_desktop_url(self, url: str) -> str:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
        if not hostname.startswith("m."):
            return url
        desktop_hostname = hostname[2:]
        port = f":{parsed.port}" if parsed.port else ""
        new_netloc = f"{desktop_hostname}{port}"
        return urlunparse(parsed._replace(netloc=new_netloc))

    def _normalize_category_url(self, category_url: str) -> str:
        if not category_url:
            return self.base_url
        return urljoin(self.base_url.rstrip("/") + "/", category_url)

    def _normalize_story_url(self, story_url: str) -> str:
        if not story_url:
            return story_url
        # Force convert to desktop URL to ensure consistency
        story_url = self._convert_to_desktop_url(story_url)
        
        absolute_url = urljoin(self.base_url.rstrip("/") + "/", story_url)
        parsed = urlparse(absolute_url)
        path = parsed.path or ""
        normalized_path = "/" + path.lstrip("/") if path else "/"

        if not normalized_path.lower().startswith("/doc-truyen/"):
            alias = normalized_path.strip("/")
            parts = [segment for segment in alias.split("/") if segment]
            if parts and parts[0].lower() == "tong-hop":
                parts = parts[1:]
            alias = "/".join(parts)
            if alias:
                normalized_path = f"/doc-truyen/{alias}"
            else:
                normalized_path = "/doc-truyen"

        normalized = parsed._replace(path=normalized_path)
        return urlunparse(normalized)

    def get_chapters_per_page_hint(self) -> int:
        return self._chapter_page_size

    @staticmethod
    def _coerce_positive_int(value: Any, default: int) -> int:
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            return default
        return candidate if candidate > 0 else default

    async def _build_request_headers(self, url: str, *, desktop_only: bool = True) -> dict[str, str]:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            referer = f"{parsed.scheme}://{parsed.netloc}/"
        else:
            referer = self.base_url.rstrip("/") + "/"

        headers = await get_random_headers(self.site_key, desktop_only=True)
        headers["Referer"] = referer
        return headers

    async def _fetch_text(self, url: str, wait_for_selector: str | None = None) -> str | None:
        # Ensure URL is desktop version
        url = self._convert_to_desktop_url(url)
        
        await asyncio.sleep(3.0) # Add a longer delay before each request to avoid rate limits

        headers = await self._build_request_headers(url)
        
        try:
            response = await make_request(
                url,
                self.site_key,
                wait_for_selector=wait_for_selector,
                extra_headers=headers,
                timeout=60, # Increase timeout for tangthuvien
            )
            
            if response and getattr(response, "text", None):
                return response.text
                
        except Exception as e:
            logger.warning(f"[{self.site_key}] Request failed for {url}: {e}")
            
        return None

    async def get_genres(self) -> list[dict[str, str]]:
        genres_list: list[dict[str, str]] = []
        genres_map_for_db: dict[str, dict[str, Any]] = {} 

        logger.info(f"[{self.site_key}] Using STATIC genre map to ensure full coverage (12 categories)")
        
        base = self.base_url.rstrip("/")
        
        for gid, name in self._STATIC_GENRES.items():
            # Reverted to query param style as requested by user to match DB legacy data
            url = f"{base}/tong-hop?ctg={gid}"
            genre_entry = {"name": name, "url": url}
            genres_list.append(genre_entry)
            genres_map_for_db[url] = { 
                "name": name,
                "url": url,
                "status": "pending", 
                "total_stories": 0,
                "processed_stories": 0,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            
        logger.info(f"[{self.site_key}] Generated {len(genres_list)} genre URLs.")

        metrics_tracker.site_genres_initialized(
            site_key=self.site_key,
            total_genres=len(genres_list),
            genres_map=genres_map_for_db 
        )

        return genres_list

    async def get_stories_in_genre(
        self,
        genre_url: str,
        page: int = 1,
        *,
        max_pages: int | None = None,
        **_: Any,
    ) -> tuple[list[dict[str, str]], int]:
        # Allow diagnostic helpers to cap pages without breaking existing callers
        if max_pages is not None and page > max_pages:
            return [], 0
        genre_url = self._convert_to_desktop_url(genre_url)
        target_url = _with_page_parameter(genre_url, page)
        logger.debug(f"[{self.site_key}] Fetching genre page {page}: {target_url}")
        
        html = await self._fetch_text(target_url, wait_for_selector="div.story-list")
        
        if not html:
            logger.warning(f"[{self.site_key}] Failed to fetch genre page {page}")
            return [], 0
            
        stories, detected_max_pages = parse_story_list(html, self.base_url)
        if not stories and page == 1:
            logger.warning(f"[{self.site_key}] No stories found on genre page {page} (URL: {target_url})")

        return stories, detected_max_pages or 999

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
        logger.info(f"[{self.site_key}] Crawling stories for genre '{genre_name}'")
        try:
            metrics_tracker.genre_started(
                site_key=site_key,
                genre_name=genre_name,
                genre_url=genre_url,
                total_genres=12, 
            )
        except Exception:
            logger.debug(f"[{self.site_key}] Unable to register genre '{genre_name}' with metrics", exc_info=True)
            
        first_page_stories, total_pages = await self.get_stories_in_genre(genre_url, page=1)
        if not first_page_stories:
            return [], 0, 0

        all_stories: list[dict[str, Any]] = list(first_page_stories) if collect else []
        if page_callback:
            await page_callback(list(first_page_stories), 1, total_pages)

        page_number = 2
        while True:
            if max_pages is not None and page_number > max_pages:
                break
            
            stories, _ = await self.get_stories_in_genre(genre_url, page_number)
            
            if not stories:
                break
                
            if collect:
                all_stories.extend(stories)
                
            if page_callback:
                await page_callback(list(stories), page_number, total_pages)
                
            page_number += 1
            await asyncio.sleep(0.5)

        return all_stories, total_pages, page_number - 1

    async def get_all_stories_from_genre(
        self, genre_name: str, genre_url: str, max_pages: int | None = None
    ) -> list[dict[str, str]]:
        stories, _, _ = await self.get_all_stories_from_genre_with_page_check(
            genre_name=genre_name,
            genre_url=genre_url,
            site_key=self.site_key,
            max_pages=max_pages,
        )
        return stories

    async def _get_story_details_internal(self, story_url: str) -> dict[str, Any] | None:
        # Normalize URL to ensure it has correct format (e.g., /truyen/slug/)
        story_url = self._normalize_story_url(story_url) # Force desktop
        
        # Don't wait for specific selector - content loads dynamically after Cloudflare bypass
        # Just wait for Cloudflare bypass and network idle
        html = await self._fetch_text(story_url, wait_for_selector=None)
        if not html:
            return None

        # Check for 404/Maintenance before parsing
        if "Không tìm thấy" in html or "Site Maintenance" in html:
            logger.error(f"[{self.site_key}] Story not found or maintenance page detected: {story_url}")
            return None
        
        details = parse_story_info(html, self.base_url)
        
        # IMPORTANT: Do NOT fallback to mobile if details are missing.
        # The desktop parser should be robust enough. 
        # If missing crucial fields, retry or log warning, but don't switch to broken mobile site.
        
        details['sources'] = [
            {'url': story_url, 'site_key': self.site_key, 'priority': 1}
        ]
        details['url'] = story_url
        return details

    async def get_story_details(self, story_url: str, story_title: str) -> dict[str, Any] | None:
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
        logger.debug(f"[{self.site_key}] Getting chapter list for '{story_title}'.")

        details = await self.get_story_details(story_url, story_title)
        if not details:
            logger.error(f"[{self.site_key}] Could not load story details for {story_url} to get chapters.")
            return []

        chapters = list(details.get('chapters') or [])

        if not chapters:
            html = await self._fetch_text(story_url)
            if not html:
                logger.error(f"[{self.site_key}] Fallback HTML fetch failed for {story_url}.")
                return []
            chapters = parse_chapter_list(html, self.base_url)
        
        for ch in chapters:
            ch.setdefault('site_key', self.site_key)

        async with self._details_lock:
            cached = self._details_cache.get(story_url)
            if cached is not None:
                cached['chapters'] = chapters
            else:
                self._details_cache[story_url] = details

        return chapters

    async def get_chapter_content(self, chapter_url: str, chapter_title: str, site_key: str) -> str | None:
        chapter_url = self._convert_to_desktop_url(chapter_url)
        html = await self._fetch_text(chapter_url, wait_for_selector="#chapter-c")
        if not html:
            return None

        parsed = parse_chapter_content(html)
        if not parsed:
            logger.warning(f"[{self.site_key}] Unable to parse content for chapter '{chapter_title}'")
            return None

        return parsed.get('content')

    def extract_chapter_list(self, html: str, base_url: str | None = None) -> list[dict[str, str]]:
        return parse_chapter_list(html, base_url or self.base_url)

    def extract_chapter_content(self, html: str, base_url: str | None = None) -> str | None:
        parsed = parse_chapter_content(html)
        if not parsed:
            logger.warning(f"[{self.site_key}] Unable to parse content from HTML")
            return None
        return parsed.get('content')

    async def search_story(self, query: str) -> list[dict[str, str]]:
        """Search for stories by title with strict matching on tangthuvien.net."""
        import urllib.parse
        from difflib import SequenceMatcher
        
        def similarity(a, b):
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()

        encoded_query = urllib.parse.quote(query)
        search_url = f"{self.base_url}/tong-hop?keyword={encoded_query}"

        logger.info(f"[{self.site_key}] Searching: {search_url}")

        html = await self._fetch_text(search_url, wait_for_selector="div.story-list")
        if not html:
            return []

        raw_stories, _ = parse_story_list(html, self.base_url)
        results = []

        for story in raw_stories:
            title = story.get("title", "")
            url = story.get("url")
            
            if not title or not url:
                continue
                
            score = similarity(query, title)
            if score < 0.85:
                logger.debug(f"[{self.site_key}] Skipping search result '{title}' (score {score:.2f} < 0.85)")
                continue

            story["url"] = self._normalize_story_url(url)
            story["match_score"] = score
            story["site_key"] = self.site_key
            results.append(story)

        results.sort(key=lambda x: x.get("match_score", 0), reverse=True)
        return results

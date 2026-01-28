import asyncio
import os
import re
import time
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from flowcore_story.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.analyze.xtruyen_parse import (
    parse_chapter_content,
    parse_chapter_list,
    parse_genres,
    parse_story_info,
    parse_story_list,
)
from flowcore_story.apps.scraper import (
    _make_request_playwright,
    discover_madara_chapter_ranges_via_playwright,
    make_request,
)
from flowcore_story.config.config import BASE_URLS, GLOBAL_PROXY_PASSWORD, GLOBAL_PROXY_USERNAME
from flowcore.utils.chapter_utils import get_chapter_sort_key
from flowcore.utils.logger import logger
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.site_config import load_site_config
from flowcore_story.apps.scraper import make_request as scraper_make_request

# PHASE 2 OPTIMIZATION: Adaptive rate limiting
try:
    from flowcore.utils.adaptive_rate_limiter import get_site_limiter
    _ADAPTIVE_LIMITER_AVAILABLE = True
except ImportError:
    _ADAPTIVE_LIMITER_AVAILABLE = False


def _calculate_exponential_backoff(retry_count: int, base_delay: float = 0.5, max_delay: float = 8.0) -> float:
    delay = base_delay * (2 ** retry_count)
    return min(delay, max_delay)


def _normalize_xtruyen_story_url(url: str, base_url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc:
        allowed_domains = ["xtruyen.vn", "www.xtruyen.vn"]
        if parsed.netloc not in allowed_domains:
            return url

    path = parsed.path.strip('/')
    if path and not path.startswith('truyen/') and not path.startswith('theloai/'):
        if '/' not in path:
            new_path = f"/truyen/{path}/"
            new_url = urlunparse(parsed._replace(path=new_path))
            return new_url
    return url


def _with_page_parameter(url: str, page: int) -> str:
    if page <= 1:
        return url
    parsed = urlparse(url)
    path = parsed.path
    segment_pattern = re.compile(r'/(page|trang|p)/(\d+)$', re.IGNORECASE)
    suffix_pattern = re.compile(r'-(page|trang|p)-(\d+)$', re.IGNORECASE)

    if segment_pattern.search(path):
        path = segment_pattern.sub(lambda match: f"/{match.group(1)}/{page}", path)
        return urlunparse(parsed._replace(path=path))

    if suffix_pattern.search(path):
        path = suffix_pattern.sub(lambda match: f"-{match.group(1)}-{page}", path)
        return urlunparse(parsed._replace(path=path))

    query = parse_qs(parsed.query, keep_blank_values=True)
    replaced = False
    for key in ("page", "paged", "pageindex"):
        if key in query:
            query[key] = [str(page)]
            replaced = True
    if not replaced:
        query["page"] = [str(page)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


class XTruyenAdapter(BaseSiteAdapter):
    site_key = "xtruyen"
    _DEFAULT_CHAPTER_LIST_BATCH = 100

    # FIX 2025-12-15: Enhanced rate limit handling for Cloudflare WAF
    # xtruyen.vn uses aggressive Cloudflare protection with strict rate limits
    # Proxy rotates IP every 60s, but Cloudflare may track fingerprints longer
    _PROXY_ROTATION_INTERVAL = 60  # Wait 1 full rotation for fresh IP
    _AJAX_REQUEST_DELAY = 8.0  # INCREASED: 8s between AJAX requests (from 5.0)
    _MAX_429_RETRIES = 5  # INCREASED: More retries before giving up (from 3)
    _CONCURRENT_AJAX_LIMIT = 2  # NEW: Limit concurrent AJAX requests
    _CHAPTER_BATCH_DELAY = 3.0  # NEW: Delay between chapter batches

    def __init__(self) -> None:
        self.site_key = self.__class__.site_key
        site_config = load_site_config(self.site_key)
        configured_base_url = site_config.get("base_url")
        self.base_url = configured_base_url or BASE_URLS.get(self.site_key, "https://xtruyen.vn")
        chapter_batch_raw = site_config.get("chapter_list_batch", self._DEFAULT_CHAPTER_LIST_BATCH)
        self._chapter_list_batch = self._coerce_positive_int(
            chapter_batch_raw, self._DEFAULT_CHAPTER_LIST_BATCH
        )
        self._ajax_endpoint = site_config.get("ajax_endpoint", "/wp-admin/admin-ajax.php")
        self._details_cache: dict[str, dict[str, Any]] = {}
        self._details_lock = asyncio.Lock()
        self._ajax_ranges_cache: dict[str, list[tuple[int, int]]] = {}
        self._last_429_time: float = 0  # Track last rate limit hit

    async def _wait_for_ip_rotation(self, context: str = "") -> None:
        """
        Wait for proxy IP rotation after hitting rate limit (429).

        Since proxy rotates every 60s, we calculate remaining time until next rotation
        and wait for it. This ensures we get a fresh IP before retrying.
        """
        now = time.time()
        elapsed_since_last_429 = now - self._last_429_time

        if elapsed_since_last_429 < self._PROXY_ROTATION_INTERVAL:
            wait_time = self._PROXY_ROTATION_INTERVAL - elapsed_since_last_429
            logger.info(
                f"[{self.site_key}] Rate limited{' (' + context + ')' if context else ''}. "
                f"Waiting {wait_time:.1f}s for proxy IP rotation..."
            )
            await asyncio.sleep(wait_time)
        else:
            # Already waited long enough, just a short delay
            logger.info(f"[{self.site_key}] IP should have rotated. Short delay before retry...")
            await asyncio.sleep(5.0)

        self._last_429_time = time.time()

    def _expand_ranges_for_fallback(
        self,
        base_ranges: list[tuple[int, int]],
        *,
        max_chapter_hint: int | None = None,
    ) -> list[tuple[int, int]]:
        if not base_ranges:
            return []

        cap = max_chapter_hint
        if cap is None:
            candidates = [end for _, end in base_ranges if isinstance(end, int)]
            if candidates:
                cap = max(candidates)

        expanded: list[tuple[int, int]] = []
        seen: set[tuple[int, int]] = set()

        for start, end in base_ranges:
            if cap is not None and start > cap:
                continue

            upper_bound = end
            if cap is not None:
                upper_bound = min(end, cap)

            if upper_bound < start:
                continue

            current = start
            while current <= upper_bound:
                sub_end = min(current + self._chapter_list_batch - 1, upper_bound)
                key = (current, sub_end)
                if key not in seen:
                    expanded.append(key)
                    seen.add(key)
                current = sub_end + 1

        return expanded

    @staticmethod
    def _normalize_ranges(raw_ranges: list[Any] | None) -> list[tuple[int, int]]:
        normalized: list[tuple[int, int]] = []
        if not raw_ranges:
            return normalized
        for item in raw_ranges:
            start: int | None = None
            end: int | None = None
            if isinstance(item, dict):
                start_val = item.get('from')
                end_val = item.get('to')
                if isinstance(start_val, int):
                    start = start_val
                elif isinstance(start_val, str) and start_val.isdigit():
                    start = int(start_val)
                if isinstance(end_val, int):
                    end = end_val
                elif isinstance(end_val, str) and end_val.isdigit():
                    end = int(end_val)
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                start_candidate, end_candidate = item[0], item[1]
                if isinstance(start_candidate, int):
                    start = start_candidate
                elif isinstance(start_candidate, str) and start_candidate.isdigit():
                    start = int(start_candidate)
                if isinstance(end_candidate, int):
                    end = end_candidate
                elif isinstance(end_candidate, str) and end_candidate.isdigit():
                    end = int(end_candidate)
            if start is None:
                continue
            if end is None:
                end = start
            if end < start:
                start, end = end, start
            normalized.append((start, end))
        normalized.sort()
        return normalized

    def get_chapters_per_page_hint(self) -> int:
        return self._chapter_list_batch

    @staticmethod
    def _coerce_positive_int(value: Any, default: int) -> int:
        try:
            candidate = int(value)
        except (TypeError, ValueError):
            return default
        return candidate if candidate > 0 else default

    async def _fetch_chapters_via_ajax(
        self,
        story_url: str,
        manga_id: str | None,
        ajax_nonce: str | None,
        total_expected: int | None,
        chapter_ranges: list[Any] | None = None,
        max_batches: int | None = None,
        *,
        _allow_custom_fallback: bool = True,
        prefer_discovered_ranges: bool = False,
    ) -> list[dict[str, str]]:
        if not manga_id:
            logger.warning(f"[{self.site_key}] Missing manga_id for story {story_url}, cannot load chapters via AJAX.")
            return []

        ajax_url = urljoin(self.base_url, self._ajax_endpoint)
        collected: list[dict[str, str]] = []
        seen_urls: set[str] = set()

        extra_headers = {
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': self.base_url,
            'Referer': story_url,
            'X-Requested-With': 'XMLHttpRequest',
        }

        # XTruyen uses a custom AJAX action that requires from/to rather than the
        # default madara "manga_get_chapters". Using the wrong action returns 400.
        base_payload: dict[str, Any]
        if self.site_key == "xtruyen":
            base_payload = {
                'action': 'load_chapter_list_from_to',
                'manga_id': manga_id,
            }
        else:
            base_payload = {
                'action': 'manga_get_chapters',
                'manga': manga_id,
            }
        
        logger.debug(f"[{self.site_key}] Preparing AJAX payload for {manga_id}. Nonce available: {bool(ajax_nonce)}")
        
        if ajax_nonce and self.site_key != "xtruyen":
            base_payload['security'] = ajax_nonce
            logger.debug(f"[{self.site_key}] Added security token to payload: {ajax_nonce}")
        elif not ajax_nonce and self.site_key != "xtruyen":
            logger.warning(f"[{self.site_key}] No AJAX nonce found for {manga_id}. Request may fail (400).")
        
        raw_passthrough_ranges: list[dict[str, Any]] | None = None
        if isinstance(chapter_ranges, list):
            for item in chapter_ranges:
                if isinstance(item, dict):
                    f_val = item.get('from')
                    t_val = item.get('to')
                    def _is_nondigit_string(v: Any) -> bool:
                        return isinstance(v, str) and not v.isdigit()
                    if _is_nondigit_string(f_val) or _is_nondigit_string(t_val):
                        raw_passthrough_ranges = [
                            {"from": it.get('from'), "to": it.get('to')}
                            for it in chapter_ranges if isinstance(it, dict) and it.get('from') is not None and it.get('to') is not None
                        ]
                        break

        ranges = self._normalize_ranges(chapter_ranges)
        has_custom_ranges = bool(ranges) or bool(raw_passthrough_ranges)

        # XTruyen splits chapters into ranges (e.g. 1-200). If parsing failed, fall back to a single window.
        if self.site_key == "xtruyen" and not ranges and not raw_passthrough_ranges:
            fallback_end = total_expected if total_expected and total_expected > 0 else self._chapter_list_batch
            ranges = [(1, fallback_end)]
        
        iterable_batches: list[Any] = []
        if not ranges and not raw_passthrough_ranges:
             iterable_batches = [None] 
        elif raw_passthrough_ranges:
            iterable_batches = raw_passthrough_ranges
        else:
             iterable_batches = ranges

        rate_limit_retries = 0

        for batch_index, item in enumerate(iterable_batches, start=1):
            if max_batches is not None and batch_index > max_batches:
                logger.debug(f"[{self.site_key}] Reached max_batches={max_batches}; stopping AJAX pagination.")
                break
            # FIX 2025-12-11: Increased throttle delay to avoid triggering rate limit
            await asyncio.sleep(self._AJAX_REQUEST_DELAY)

            payload = dict(base_payload)
            if item:
                # XTruyen expects explicit from/to to avoid fetching thousands of chapters in one go
                if isinstance(item, dict):
                    start = item.get("from") or item.get("start")
                    end = item.get("to") or item.get("end")
                elif isinstance(item, (tuple, list)) and len(item) >= 2:
                    start, end = item[0], item[1]
                else:
                    start = end = None

                if start is not None:
                    # FIX 2026-01-06: Keep 'm' suffix for last chunk (e.g. '1901m' = fetch to max)
                    # Only sanitize if it's malformed (not ending with 'm')
                    if isinstance(start, str) and not start.isdigit():
                        if not start.lower().endswith('m'):
                            start = re.sub(r'\D', '', start)
                    payload["from"] = start
                if end is not None:
                    if isinstance(end, str) and not end.isdigit():
                        # Preserve 'm' suffix - it signals "fetch to end" for last chunk
                        if not end.lower().endswith('m'):
                            end = re.sub(r'\D', '', end)
                    payload["to"] = end

                # Fallback: if range is malformed, default to batch-sized window
                if self.site_key == "xtruyen" and ("from" not in payload or "to" not in payload):
                    payload["from"] = 1
                    payload["to"] = self._chapter_list_batch

            # Force Playwright for AJAX (implicit via make_request fallback, but ensure headers are clean)
            max_retries = 4 if self.site_key == "xtruyen" else 2
            timeout = 60 if self.site_key == "xtruyen" else 45
            response = await make_request(
                ajax_url,
                self.site_key,
                method='POST',
                data=payload,
                extra_headers=extra_headers,
                max_retries=max_retries,  # Retry budget tuned per site; 429 handled separately
                timeout=timeout, # Longer timeout for Playwright
            )

            # FIX 2025-12-11: Handle rate limit (429) with IP rotation wait
            status_code = getattr(response, 'status_code', None)
            if status_code == 429:
                rate_limit_retries += 1
                if rate_limit_retries > self._MAX_429_RETRIES:
                    logger.error(
                        f"[{self.site_key}] Max rate limit retries ({self._MAX_429_RETRIES}) exceeded for {manga_id}. Giving up."
                    )
                    break

                logger.warning(
                    f"[{self.site_key}] Got 429 rate limit for AJAX (retry {rate_limit_retries}/{self._MAX_429_RETRIES})"
                )
                # Wait for proxy IP rotation (65s)
                await self._wait_for_ip_rotation(f"AJAX batch {batch_index}")
                # Refresh session after IP rotation
                await self._fetch_text(story_url)
                await asyncio.sleep(3.0)
                continue

            if not response or not getattr(response, 'text', None):
                logger.debug(f"[{self.site_key}] Empty AJAX response for manga {manga_id}. Response: {response}") # Thêm log chi tiết
                break

            html_snippet = response.text.strip()

            # FIX 2025-12-11: Better rate limit detection in response body
            is_rate_limited = (
                "bạn thao tác quá nhanh" in html_snippet.lower() or
                "just a moment" in html_snippet.lower() or
                "Cloudflare" in html_snippet or
                "Access denied" in html_snippet or
                status_code == 400  # Often means session expired due to IP change
            )

            if is_rate_limited:
                rate_limit_retries += 1
                if rate_limit_retries > self._MAX_429_RETRIES:
                    logger.error(
                        f"[{self.site_key}] Max rate limit retries exceeded. Giving up."
                    )
                    break

                logger.warning(
                    f"[{self.site_key}] Rate limit/Bot detected in AJAX response (status: {status_code}). "
                    f"Waiting for IP rotation... (retry {rate_limit_retries}/{self._MAX_429_RETRIES})"
                )
                # Wait for proxy IP rotation
                await self._wait_for_ip_rotation(f"response body check")
                # Refresh session cookies after IP rotation
                await self._fetch_text(story_url)
                await asyncio.sleep(3.0)
                continue

            # Reset rate limit counter on success
            rate_limit_retries = 0

            if not html_snippet:
                break

            batch_chapters = parse_chapter_list(html_snippet, self.base_url)
            new_items = []
            for chapter in batch_chapters:
                chapter_url = chapter.get('url')
                if not chapter_url or chapter_url in seen_urls:
                    continue
                seen_urls.add(chapter_url)
                chapter.setdefault('site_key', self.site_key)
                new_items.append(chapter)
            
            collected.extend(new_items)
            
            if not item and len(new_items) > 0:
                break

        collected.sort(key=get_chapter_sort_key)
        return collected

    async def _fetch_text(self, url: str, wait_for_selector: str | None = None) -> str | None:
        # Use make_request from scraper, which includes Playwright fallback logic
        logger.debug(f"[{self.site_key}] Calling scraper_make_request for {url} with Playwright capabilities...")
        response = await scraper_make_request(
            url,
            self.site_key,
            method='GET', # Explicitly set method to GET
            wait_for_selector=wait_for_selector,
            max_retries=5, # Allow retries within make_request
            timeout=120,   # Longer timeout for Playwright operations
        )
        logger.debug(f"[{self.site_key}] scraper_make_request returned. Response: {response}. Has text: {bool(response and response.text)}. Status: {getattr(response, 'status_code', 'N/A')}")
        if response and response.text:
            return response.text
        return None

    async def _fetch_text_playwright(self, url: str, wait_for_selector: str | None = None) -> str | None:
        response = await _make_request_playwright(
            url,
            self.site_key,
            timeout=120,
            max_retries=3,
            wait_for_selector=wait_for_selector,
        )
        if response is None:
            return None
        if isinstance(response, str):
            return response or None
        text_value = getattr(response, "text", None)
        if text_value is not None:
            return text_value or None
        return str(response)

    async def get_genres(self) -> list[dict[str, str]]:
        logger.info(f"[{self.site_key}] Fetching genres from {self.base_url} with Playwright.")
        logger.debug(f"[{self.site_key}] get_genres: About to call _fetch_text for {self.base_url}")
        html = await self._fetch_text(self.base_url, wait_for_selector="div.main-menu")
        if html:
            logger.debug(f"[{self.site_key}] get_genres: _fetch_text returned HTML (length: {len(html)})")
        else:
            logger.debug(f"[{self.site_key}] get_genres: _fetch_text returned None.")

        if not html:
            logger.error(f"[{self.site_key}] Could not fetch HTML for genres from {self.base_url}.")
            return []
        
        if "enable javascript and cookies" in html.lower():
            log_snippet = html[:500].replace('\n', ' ')
            logger.warning(f"[{self.site_key}] Anti-bot content detected in genre page HTML snippet: {log_snippet}")
            return []

        genres = parse_genres(html, self.base_url)
        
        # Filter out the problematic static content URL
        problematic_url = "https://xtruyen.vn/truyen-song-ngu-anh-viet-hay-va-de-doc/"
        initial_genre_count = len(genres)
        genres = [g for g in genres if g.get("url") != problematic_url]

        if not genres:
            log_snippet = html[:500].replace('\n', ' ')
            logger.warning(f"[{self.site_key}] No genres parsed from HTML. HTML snippet: {log_snippet}")
        else:
            if len(genres) < initial_genre_count:
                logger.info(f"[{self.site_key}] Filtered out 1 problematic genre URL. Remaining: {len(genres)} genres.")
            logger.info(f"[{self.site_key}] Successfully parsed {len(genres)} genres.")
        return genres

    async def get_stories_in_genre(self, genre_url: str, page: int = 1) -> tuple[list[dict[str, str]], int]:
        url = _with_page_parameter(genre_url, page)
        html = await self._fetch_text(url, wait_for_selector="div.popular-item-wrap")
        if not html: return [], 0
        stories, total_pages = parse_story_list(html, self.base_url)

        for story in stories:
            story_url = story.get("url")
            if story_url:
                story["url"] = _normalize_xtruyen_story_url(story_url, self.base_url)

        return stories, total_pages

    async def get_all_stories_from_genre(
        self,
        genre_name: str,
        genre_url: str,
        max_pages: int | None = None,
    ) -> list[dict[str, str]]:
        return await self.get_all_stories_from_genre_with_page_check(
            genre_name=genre_name,
            genre_url=genre_url,
            site_key=self.site_key,
            max_pages=max_pages,
        )

    async def get_chapter_list(
        self,
        story_url: str,
        story_title: str,
        site_key: str,
        max_pages: int | None = None,
        total_chapters: int | None = None,
        max_batches: int | None = None,
    ) -> list[dict[str, Any]]:
        details = await self.get_story_details(story_url, story_title, max_batches=max_batches)
        if details and "chapters" in details:
            return details["chapters"]
        return []

    async def get_all_stories_from_genre_with_page_check(
        self,
        genre_name: str,
        genre_url: str,
        site_key: str,
        max_pages: int | None = None,
        *,
        page_callback: Callable[[list[dict[str, Any]], int, int | None], Awaitable[None]] | None = None,
        collect: bool = True,
    ) -> tuple[list[dict[str, str]], int | None, int]:
        collected: list[dict[str, str]] = []
        page = 1
        total_pages: int | None = None
        while True:
            if max_pages is not None and page > max_pages:
                break

            current_page_stories, current_total_pages = await self.get_stories_in_genre(genre_url, page)
            if current_total_pages is not None:
                total_pages = current_total_pages # Update total_pages if available

            if not current_page_stories:
                break

            if collect:
                collected.extend(current_page_stories)

            if page_callback:
                await page_callback(current_page_stories, page, total_pages)

            if total_pages and page >= total_pages:
                break
            
            # Break if we don't have total_pages info but got stories? 
            # XTruyenAdapter.get_stories_in_genre usually returns 0 for total_pages if not parsed.
            
            page += 1

        return collected, total_pages, page - 1 # Return 3 values

    async def get_story_details(
        self,
        story_url: str,
        story_title: str,
        max_batches: int | None = None,
    ) -> dict[str, Any] | None:
        story_url = _normalize_xtruyen_story_url(story_url, self.base_url)
        # 1. Load Story Page (Full Playwright)
        html = await self._fetch_text(story_url)
        if not html: return None
        
        details = parse_story_info(html, self.base_url)
        
        # 2. Try to discover chapter ranges via Playwright Interaction if list is empty
        if not details.get("chapters") and not details.get("chapter_ranges"):
            logger.info(f"[{self.site_key}] Attempting interactive discovery for {story_url}")
            try:
                discovered = await discover_madara_chapter_ranges_via_playwright(
                    story_url,
                    self.site_key,
                    ajax_endpoint=self._ajax_endpoint
                )
                if discovered:
                    details['chapter_ranges'] = discovered
            except Exception as e:
                logger.warning(f"Discovery failed: {e}")

        # 3. Fetch Chapters via AJAX (using context from previous steps presumably handled by cookie manager)
        if not details.get("chapters") and details.get("manga_id"):
             ajax_chapters = await self._fetch_chapters_via_ajax(
                 story_url, 
                 details.get("manga_id"), 
                 details.get("ajax_nonce"), 
                 total_expected=None,
                 chapter_ranges=details.get('chapter_ranges'),
                 max_batches=max_batches,
             )
             if ajax_chapters:
                 details["chapters"] = ajax_chapters

        chapters = details.get("chapters") or []
        if chapters and not details.get("total_chapters_on_site"):
            details["total_chapters_on_site"] = len(chapters)
        if chapters and not details.get("total_chapters"):
            details["total_chapters"] = details.get("total_chapters_on_site")
                 
        return details

    async def get_chapter_content(self, chapter_url: str, chapter_title: str, site_key: str) -> str | None:
        html = await self._fetch_text(chapter_url, wait_for_selector="#chapter-reading-content")
        if not html:
            return None
        parsed = parse_chapter_content(html)
        content = parsed.get("content") if parsed else None

        empty_detected = (content == "")
        if content:
            return content

        if empty_detected:
            logger.warning(
                f"[{self.site_key}] Chapter is EMPTY on first fetch: {chapter_title}. "
                "Verifying with Playwright fetch before marking empty."
            )
        else:
            logger.warning(
                f"[{self.site_key}] No content extracted for: {chapter_title}; "
                "verifying with Playwright fetch."
            )

        if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("CI"):
            if empty_detected:
                logger.info("[%s] Skipping Playwright verification in test/CI; returning empty string", self.site_key)
                return ""
            logger.info("[%s] Skipping Playwright verification in test/CI", self.site_key)
            return None

        verified_html = await self._fetch_text_playwright(
            chapter_url,
            wait_for_selector="#chapter-reading-content",
        )
        if not verified_html:
            return "" if empty_detected else None

        verified = parse_chapter_content(verified_html)
        verified_content = verified.get("content") if verified else None

        if verified_content:
            return verified_content

        if verified_content == "":
            if empty_detected:
                logger.warning(
                    f"[{self.site_key}] Chapter STILL EMPTY after verification (confirmed empty): {chapter_title}"
                )
                return ""
            logger.warning(
                f"[{self.site_key}] Verification returned empty after initial failure; "
                "treating as retryable."
            )
            return None

        return None

    def extract_chapter_list(self, html: str, base_url: str | None = None) -> list[dict[str, str]]:
        return parse_chapter_list(html, base_url or self.base_url)

    def extract_chapter_content(self, html: str, base_url: str | None = None) -> str | None:
        parsed = parse_chapter_content(html)
        content = parsed.get("content") if parsed else None

        if content:
            return content

        if content == "":
            logger.warning(f"[{self.site_key}] Chapter content is empty in HTML")
            return ""

        logger.warning(f"[{self.site_key}] Failed to parse chapter content from HTML")
        return None

    async def search_story(self, query: str) -> list[dict[str, str]]:
        """Search for stories by title with strict matching on xtruyen.vn."""
        import urllib.parse
        from difflib import SequenceMatcher
        
        def similarity(a, b):
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()

        encoded_query = urllib.parse.quote(query)
        # Madara search URL
        search_url = f"{self.base_url}/?s={encoded_query}&post_type=wp-manga"

        logger.info(f"[{self.site_key}] Searching: {search_url}")

        html = await self._fetch_text(search_url, wait_for_selector="div.search-wrap")
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

            story["url"] = _normalize_xtruyen_story_url(url, self.base_url)
            story["match_score"] = score
            story["site_key"] = self.site_key
            results.append(story)

        results.sort(key=lambda x: x.get("match_score", 0), reverse=True)
        return results

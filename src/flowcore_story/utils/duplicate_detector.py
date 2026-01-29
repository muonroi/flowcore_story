"""Cross-site duplicate story detection and management.

This module provides mechanisms to:
1. Find potential duplicate stories across sites using fuzzy title matching
2. Verify duplicates by comparing additional fields (author, chapters)
3. Merge duplicates into a single story with combined sources

Special handling for sites with search:
- quykiep.com: /tim-kiem?keyword={title} -> __NEXT_DATA__ -> props.pageProps.result[]
- xtruyen.vn: ?s={title}&post_type=wp-manga -> HTML with href+title pattern
- truyencom.com: /tim-kiem?tukhoa={title} -> HTML with href+title pattern
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import quote

from flowcore_story.config import config as app_config
from flowcore_story.utils.logger import logger
from flowcore_story.utils.title_matcher import fuzzy_match_titles, normalize_vietnamese_title


@dataclass
class DuplicateMatch:
    """Represents a potential duplicate match."""

    source_folder: str
    source_title: str
    match_url: str
    match_site_key: str
    match_title: str
    confidence: float
    match_type: str  # "exact", "fuzzy", "author_confirmed"
    author_match: bool = False
    chapter_count_match: bool = False


@dataclass
class DuplicateReport:
    """Report of duplicate detection for a story."""

    story_folder: str
    story_title: str
    duplicates: list[DuplicateMatch] = field(default_factory=list)
    checked_sites: list[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class DuplicateDetector:
    """Detect and manage duplicate stories across sites."""

    def __init__(
        self,
        fuzzy_threshold: float = 0.8,
        enabled_sites: list[str] | None = None,
    ) -> None:
        self.fuzzy_threshold = fuzzy_threshold
        self.enabled_sites = enabled_sites or list(app_config.ENABLED_SITE_KEYS or [])

    async def find_duplicates(
        self,
        story_folder: str,
        metadata: dict[str, Any] | None = None,
    ) -> DuplicateReport:
        """Find potential duplicates for a story across all enabled sites.

        Args:
            story_folder: Path to story folder
            metadata: Story metadata (will be loaded from file if not provided)

        Returns:
            DuplicateReport with found duplicates
        """
        from flowcore_story.adapters.factory import get_adapter
        from flowcore_story.utils.chapter_utils import slugify_title

        # Load metadata if not provided
        if metadata is None:
            metadata_path = os.path.join(story_folder, "metadata.json")
            if not os.path.exists(metadata_path):
                return DuplicateReport(
                    story_folder=story_folder,
                    story_title="Unknown",
                )
            with open(metadata_path, encoding="utf-8") as f:
                metadata = json.load(f)

        title = metadata.get("title", "")
        author = metadata.get("author", "")
        total_chapters = metadata.get("total_chapters_on_site", 0)

        report = DuplicateReport(
            story_folder=story_folder,
            story_title=title,
        )

        if not title:
            logger.warning(f"[DUPLICATE] No title in {story_folder}")
            return report

        # Get existing sources to avoid checking them
        existing_sources = metadata.get("sources", [])
        existing_site_keys = {
            s.get("site_key") or s.get("site")
            for s in existing_sources
            if isinstance(s, dict)
        }
        primary_site_key = metadata.get("site_key")
        if primary_site_key:
            existing_site_keys.add(primary_site_key)

        # Generate slug for URL guessing
        slug = slugify_title(title)
        normalized_title = normalize_vietnamese_title(title)

        # Check each enabled site
        for site_key in self.enabled_sites:
            if site_key in existing_site_keys:
                continue

            report.checked_sites.append(site_key)

            try:
                base_url = app_config.BASE_URLS.get(site_key, "").rstrip("/")

                if not base_url:
                    continue

                # Special handling for quykiep: use search API instead of URL guessing
                # because quykiep has different slugs than other sites
                if site_key == "quykiep":
                    match = await self._search_on_quykiep(
                        title, author, total_chapters, base_url
                    )
                    if match:
                        match.source_folder = story_folder
                        report.duplicates.append(match)
                    continue

                # Special handling for xtruyen: use WordPress search
                # URL guessing with slugs often fails
                if site_key == "xtruyen":
                    match = await self._search_on_xtruyen(
                        title, author, total_chapters, base_url
                    )
                    if match:
                        match.source_folder = story_folder
                        report.duplicates.append(match)
                    continue

                # Special handling for truyencom: use search page
                # truyencom URLs require story ID which we don't have
                if site_key == "truyencom":
                    match = await self._search_on_truyencom(
                        title, author, total_chapters, base_url
                    )
                    if match:
                        match.source_folder = story_folder
                        report.duplicates.append(match)
                    continue

                # For other sites, try common URL patterns
                adapter = get_adapter(site_key)
                url_patterns = self._get_url_patterns(site_key, slug, base_url)

                for url in url_patterns:
                    try:
                        details = await adapter.get_story_details(url, title)

                        if not details or not isinstance(details, dict):
                            continue

                        match_title = details.get("title", "")
                        if not match_title:
                            continue

                        # Check title match
                        is_match, score = fuzzy_match_titles(
                            title, match_title, self.fuzzy_threshold
                        )

                        if not is_match:
                            continue

                        # Determine match type
                        if score >= 1.0:
                            match_type = "exact"
                        elif score >= 0.95:
                            match_type = "normalized_exact"
                        else:
                            match_type = "fuzzy"

                        # Check author match for confirmation
                        match_author = details.get("author", "")
                        author_match = self._authors_match(author, match_author)

                        if author_match:
                            match_type = "author_confirmed"
                            score = min(1.0, score + 0.1)

                        # Check chapter count similarity
                        match_chapters = details.get("total_chapters_on_site", 0)
                        chapter_match = self._chapter_counts_match(
                            total_chapters, match_chapters
                        )

                        if chapter_match:
                            score = min(1.0, score + 0.05)

                        duplicate = DuplicateMatch(
                            source_folder=story_folder,
                            source_title=title,
                            match_url=url,
                            match_site_key=site_key,
                            match_title=match_title,
                            confidence=score,
                            match_type=match_type,
                            author_match=author_match,
                            chapter_count_match=chapter_match,
                        )

                        report.duplicates.append(duplicate)
                        logger.info(
                            f"[DUPLICATE] Found match: '{title}' == '{match_title}' "
                            f"on {site_key} (confidence: {score:.2f}, type: {match_type})"
                        )

                        # Found a match on this site, move to next site
                        break

                    except Exception as e:
                        logger.debug(f"[DUPLICATE] Error checking {url}: {e}")
                        continue

            except Exception as e:
                logger.warning(f"[DUPLICATE] Error checking site {site_key}: {e}")
                continue

        return report

    def _get_url_patterns(
        self,
        site_key: str,
        slug: str,
        base_url: str,
    ) -> list[str]:
        """Get URL patterns to try for a site."""
        patterns: dict[str, list[str]] = {
            "xtruyen": [f"{base_url}/truyen/{slug}/"],
            "tangthuvien": [
                f"{base_url}/doc-truyen/{slug}",
                f"{base_url}/{slug}",
            ],
            "quykiep": [f"{base_url}/truyen/{slug}"],
            "truyencom": [],  # Requires ID, skip URL guessing
        }

        return patterns.get(site_key, [f"{base_url}/{slug}"])

    def _authors_match(self, author1: str | None, author2: str | None) -> bool:
        """Check if two authors match."""
        if not author1 or not author2:
            return False

        # Normalize authors
        norm1 = normalize_vietnamese_title(author1)
        norm2 = normalize_vietnamese_title(author2)

        if not norm1 or not norm2:
            return False

        # Exact match or containment
        return norm1 == norm2 or norm1 in norm2 or norm2 in norm1

    def _chapter_counts_match(
        self,
        count1: int | None,
        count2: int | None,
        tolerance: float = 0.1,
    ) -> bool:
        """Check if chapter counts are similar within tolerance."""
        if not count1 or not count2:
            return False

        # Allow 10% difference
        diff = abs(count1 - count2)
        max_count = max(count1, count2)

        return diff <= max_count * tolerance

    async def _search_on_xtruyen(
        self,
        title: str,
        author: str | None,
        total_chapters: int,
        base_url: str,
    ) -> DuplicateMatch | None:
        """Search for story on xtruyen using WordPress search.

        Xtruyen uses Madara theme with WordPress search.
        Search endpoint: ?s={title}&post_type=wp-manga

        Args:
            title: Story title to search for
            author: Author name for verification
            total_chapters: Chapter count for verification
            base_url: Base URL for xtruyen

        Returns:
            DuplicateMatch if found, None otherwise
        """
        from flowcore_story.apps.scraper import make_request

        # Search using WordPress search
        search_url = f"{base_url}/?s={quote(title)}&post_type=wp-manga"

        try:
            response = await make_request(search_url, site_key="xtruyen")
            if not response or not response.text:
                return None

            # Parse HTML to find story items
            # Xtruyen uses href="URL" title="TITLE" pattern in search results
            items = re.findall(
                r'href="(https://xtruyen\.vn/truyen/[^"]+/)"[^>]*title="([^"]+)"',
                response.text,
            )

            if not items:
                logger.debug(f"[DUPLICATE] No xtruyen search results for '{title}'")
                return None

            # Find best match from results
            for result_url, result_title in items:
                # Clean up HTML entities
                result_title = (
                    result_title.replace("&#8211;", "-")
                    .replace("&#8217;", "'")
                    .replace("&amp;", "&")
                    .strip()
                )

                # Check title match
                is_match, score = fuzzy_match_titles(
                    title, result_title, self.fuzzy_threshold
                )

                if not is_match:
                    continue

                # Determine match type
                if score >= 1.0:
                    match_type = "exact"
                elif score >= 0.95:
                    match_type = "normalized_exact"
                else:
                    match_type = "fuzzy"

                logger.info(
                    f"[DUPLICATE] Found xtruyen match via search: "
                    f"'{title}' == '{result_title}' (confidence: {score:.2f})"
                )

                return DuplicateMatch(
                    source_folder="",  # Will be set by caller
                    source_title=title,
                    match_url=result_url,
                    match_site_key="xtruyen",
                    match_title=result_title,
                    confidence=score,
                    match_type=match_type,
                    author_match=False,  # Can't get author from search results
                    chapter_count_match=False,
                )

            return None

        except Exception as e:
            logger.debug(f"[DUPLICATE] Error searching xtruyen: {e}")
            return None

    async def _search_on_quykiep(
        self,
        title: str,
        author: str | None,
        total_chapters: int,
        base_url: str,
    ) -> DuplicateMatch | None:
        """Search for story on quykiep using their search API.

        Quykiep has different slugs than other sites, so we use their
        search API to find matching stories by title.

        Args:
            title: Story title to search for
            author: Author name for verification
            total_chapters: Chapter count for verification
            base_url: Base URL for quykiep

        Returns:
            DuplicateMatch if found, None otherwise
        """
        from flowcore_story.apps.scraper import make_request

        # Search using title
        search_url = f"{base_url}/tim-kiem?keyword={quote(title)}"

        try:
            response = await make_request(search_url, site_key="quykiep")
            if not response or not response.text:
                return None

            # Extract __NEXT_DATA__ JSON
            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                response.text,
            )
            if not match:
                return None

            data = json.loads(match.group(1))
            results = data.get("props", {}).get("pageProps", {}).get("result", [])

            if not results:
                logger.debug(f"[DUPLICATE] No quykiep search results for '{title}'")
                return None

            # Find best match from results
            for result in results:
                result_title = result.get("name", "")
                result_slug = result.get("slug", "")
                result_author = result.get("author", "")
                result_chapters = result.get("chapterCount", 0)

                if not result_title or not result_slug:
                    continue

                # Check title match
                is_match, score = fuzzy_match_titles(
                    title, result_title, self.fuzzy_threshold
                )

                if not is_match:
                    continue

                # Determine match type
                if score >= 1.0:
                    match_type = "exact"
                elif score >= 0.95:
                    match_type = "normalized_exact"
                else:
                    match_type = "fuzzy"

                # Check author match for confirmation
                author_match = self._authors_match(author, result_author)
                if author_match:
                    match_type = "author_confirmed"
                    score = min(1.0, score + 0.1)

                # Check chapter count similarity
                chapter_match = self._chapter_counts_match(
                    total_chapters, result_chapters
                )
                if chapter_match:
                    score = min(1.0, score + 0.05)

                # Build story URL from slug
                story_url = f"{base_url}/truyen/{result_slug}"

                logger.info(
                    f"[DUPLICATE] Found quykiep match via search: "
                    f"'{title}' == '{result_title}' (confidence: {score:.2f})"
                )

                return DuplicateMatch(
                    source_folder="",  # Will be set by caller
                    source_title=title,
                    match_url=story_url,
                    match_site_key="quykiep",
                    match_title=result_title,
                    confidence=score,
                    match_type=match_type,
                    author_match=author_match,
                    chapter_count_match=chapter_match,
                )

            return None

        except Exception as e:
            logger.debug(f"[DUPLICATE] Error searching quykiep: {e}")
            return None

    async def _search_on_truyencom(
        self,
        title: str,
        author: str | None,
        total_chapters: int,
        base_url: str,
    ) -> DuplicateMatch | None:
        """Search for story on truyencom using their search page.

        Truyencom uses /tim-kiem?tukhoa={title} for search.

        Args:
            title: Story title to search for
            author: Author name for verification
            total_chapters: Chapter count for verification
            base_url: Base URL for truyencom

        Returns:
            DuplicateMatch if found, None otherwise
        """
        from flowcore_story.apps.scraper import make_request

        # Search using tukhoa parameter
        search_url = f"{base_url}/tim-kiem?tukhoa={quote(title)}"

        try:
            response = await make_request(search_url, site_key="truyencom")
            if not response or not response.text:
                return None

            # Parse HTML to find story items
            # Pattern: <a href="URL" title="TITLE">
            items = re.findall(
                r'<a[^>]+href="(https://truyencom\.com/[^"]+\.\d+/)"[^>]*title="([^"]+)"',
                response.text,
            )

            if not items:
                logger.debug(f"[DUPLICATE] No truyencom search results for '{title}'")
                return None

            # Find best match from results
            for result_url, result_title in items:
                # Clean up HTML entities
                result_title = (
                    result_title.replace("&#8211;", "-")
                    .replace("&#8217;", "'")
                    .replace("&amp;", "&")
                    .strip()
                )

                # Check title match
                is_match, score = fuzzy_match_titles(
                    title, result_title, self.fuzzy_threshold
                )

                if not is_match:
                    continue

                # Determine match type
                if score >= 1.0:
                    match_type = "exact"
                elif score >= 0.95:
                    match_type = "normalized_exact"
                else:
                    match_type = "fuzzy"

                logger.info(
                    f"[DUPLICATE] Found truyencom match via search: "
                    f"'{title}' == '{result_title}' (confidence: {score:.2f})"
                )

                return DuplicateMatch(
                    source_folder="",  # Will be set by caller
                    source_title=title,
                    match_url=result_url,
                    match_site_key="truyencom",
                    match_title=result_title,
                    confidence=score,
                    match_type=match_type,
                    author_match=False,  # Can't get author from search results
                    chapter_count_match=False,
                )

            return None

        except Exception as e:
            logger.debug(f"[DUPLICATE] Error searching truyencom: {e}")
            return None

    async def merge_duplicates(
        self,
        primary_folder: str,
        duplicates: list[DuplicateMatch],
        *,
        update_primary: bool = True,
    ) -> dict[str, Any]:
        """Merge duplicate stories into primary, preserving best data.

        Args:
            primary_folder: Path to primary story folder
            duplicates: List of duplicate matches to merge
            update_primary: Whether to update the primary metadata file

        Returns:
            Updated metadata dict
        """
        from flowcore_story.utils.quality_scorer import quality_scorer

        # Load primary metadata
        metadata_path = os.path.join(primary_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            logger.error(f"[MERGE] Primary metadata not found: {metadata_path}")
            return {}

        with open(metadata_path, encoding="utf-8") as f:
            metadata = json.load(f)

        # Get existing sources
        sources = metadata.get("sources", [])
        if not isinstance(sources, list):
            sources = []

        existing_urls = {
            s.get("url") for s in sources if isinstance(s, dict) and s.get("url")
        }

        # Add new sources from duplicates
        for dup in duplicates:
            if dup.match_url in existing_urls:
                continue

            new_source = {
                "url": dup.match_url,
                "site_key": dup.match_site_key,
                "priority": 100 + len(sources),
                "is_primary": False,
                "_discovered_as_duplicate": True,
                "_confidence": dup.confidence,
                "_match_type": dup.match_type,
            }

            sources.append(new_source)
            existing_urls.add(dup.match_url)

            logger.info(
                f"[MERGE] Added source from {dup.match_site_key} "
                f"(confidence: {dup.confidence:.2f})"
            )

        metadata["sources"] = sources

        # Record merge info
        if "_duplicate_merges" not in metadata:
            metadata["_duplicate_merges"] = []

        merge_record = {
            "timestamp": datetime.now().isoformat(),
            "duplicates_merged": [
                {
                    "url": d.match_url,
                    "site_key": d.match_site_key,
                    "confidence": d.confidence,
                }
                for d in duplicates
            ],
        }
        metadata["_duplicate_merges"].append(merge_record)

        # Save if requested
        if update_primary:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)

            logger.info(
                f"[MERGE] Updated {metadata.get('title')} with "
                f"{len(duplicates)} new sources"
            )

        return metadata


# Singleton instance
duplicate_detector = DuplicateDetector()


__all__ = [
    "DuplicateDetector",
    "DuplicateMatch",
    "DuplicateReport",
    "duplicate_detector",
]

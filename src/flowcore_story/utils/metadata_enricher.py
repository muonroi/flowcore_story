"""Metadata enrichment utilities for cross-site data completion.

This module provides mechanisms to:
1. Check metadata completeness
2. Search for missing fields across sites
3. Merge metadata with source tracking
4. Validate enriched data
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from flowcore_story.config import config as app_config
from flowcore_story.utils.logger import logger
from flowcore_story.utils.title_matcher import fuzzy_match_titles


# Priority fields for enrichment (in order of importance)
PRIORITY_FIELDS: list[str] = [
    "author",
    "description",
    "cover",
    "categories",
    "status",
    "rating_value",
    "rating_count",
]

# String values that indicate "empty"
EMPTY_STRING_VALUES: set[str] = {
    "",
    "Unknown",
    "unknown",
    "N/A",
    "n/a",
    "Không rõ",
    "không rõ",
    "Đang cập nhật",
    "đang cập nhật",
}


def _is_empty_value(value: Any) -> bool:
    """Check if a value is considered 'empty' for enrichment purposes."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in EMPTY_STRING_VALUES
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


@dataclass
class EnrichmentResult:
    """Result of a single enrichment operation."""

    field: str
    original_value: Any
    enriched_value: Any
    source_site: str
    source_url: str


@dataclass
class EnrichmentReport:
    """Report of metadata enrichment for a story."""

    story_folder: str
    story_title: str
    missing_fields_before: list[str] = field(default_factory=list)
    missing_fields_after: list[str] = field(default_factory=list)
    enrichments: list[EnrichmentResult] = field(default_factory=list)
    sources_checked: list[str] = field(default_factory=list)
    success: bool = False
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class MetadataEnricher:
    """Enrich metadata from cross-site sources."""

    def __init__(
        self,
        priority_fields: list[str] | None = None,
        enabled_sites: list[str] | None = None,
        fuzzy_threshold: float = 0.8,
    ) -> None:
        self.priority_fields = priority_fields or PRIORITY_FIELDS
        self.enabled_sites = enabled_sites or list(app_config.ENABLED_SITE_KEYS or [])
        self.fuzzy_threshold = fuzzy_threshold

    def get_missing_fields(
        self,
        metadata: dict[str, Any],
        fields: list[str] | None = None,
    ) -> list[str]:
        """Get list of missing or empty fields from metadata.

        Args:
            metadata: Story metadata dictionary
            fields: Fields to check (defaults to priority_fields)

        Returns:
            List of field names that are missing or empty
        """
        fields_to_check = fields or self.priority_fields
        missing: list[str] = []

        for field_name in fields_to_check:
            value = metadata.get(field_name)

            if self._is_empty(value):
                missing.append(field_name)

        return missing

    def _is_empty(self, value: Any) -> bool:
        """Check if a value is considered empty."""
        # Use module-level helper for basic checks
        if _is_empty_value(value):
            return True

        if isinstance(value, list):
            # Check if list has meaningful content
            if not value:
                return True
            # For categories, check if any have names
            if all(
                not (isinstance(item, dict) and item.get("name"))
                for item in value
            ):
                return True

        return False

    async def enrich_from_sources(
        self,
        story_folder: str,
        metadata: dict[str, Any] | None = None,
        *,
        save: bool = True,
    ) -> EnrichmentReport:
        """Enrich metadata from all available sources.

        Args:
            story_folder: Path to story folder
            metadata: Story metadata (will be loaded if not provided)
            save: Whether to save the enriched metadata

        Returns:
            EnrichmentReport with details of enrichment
        """
        from flowcore_story.adapters.factory import get_adapter

        # Load metadata if not provided
        metadata_path = os.path.join(story_folder, "metadata.json")
        if metadata is None:
            if not os.path.exists(metadata_path):
                return EnrichmentReport(
                    story_folder=story_folder,
                    story_title="Unknown",
                    success=False,
                )
            with open(metadata_path, encoding="utf-8") as f:
                metadata = json.load(f)

        title = metadata.get("title", "")
        sources = metadata.get("sources", [])

        report = EnrichmentReport(
            story_folder=story_folder,
            story_title=title,
            missing_fields_before=self.get_missing_fields(metadata),
        )

        if not title:
            logger.warning(f"[ENRICH] No title in {story_folder}")
            return report

        # If no missing fields, nothing to enrich
        if not report.missing_fields_before:
            report.success = True
            logger.debug(f"[ENRICH] No missing fields for '{title}'")
            return report

        # Collect all enrichment data from sources
        enriched_data: dict[str, EnrichmentResult] = {}

        for source in sources:
            if not isinstance(source, dict):
                continue

            site_key = source.get("site_key") or source.get("site")
            url = source.get("url")

            if not site_key or not url:
                continue

            # Skip if already primary
            if source.get("is_primary"):
                continue

            report.sources_checked.append(site_key)

            try:
                adapter = get_adapter(site_key)
                details = await adapter.get_story_details(url, title)

                if not details or not isinstance(details, dict):
                    continue

                # Verify title match
                match_title = details.get("title", "")
                is_match, _ = fuzzy_match_titles(
                    title, match_title, self.fuzzy_threshold
                )

                if not is_match:
                    logger.debug(
                        f"[ENRICH] Title mismatch: '{title}' vs '{match_title}'"
                    )
                    continue

                # Extract missing fields from this source
                for field_name in report.missing_fields_before:
                    # Skip if already enriched from better source
                    if field_name in enriched_data:
                        continue

                    value = details.get(field_name)
                    if not self._is_empty(value):
                        enriched_data[field_name] = EnrichmentResult(
                            field=field_name,
                            original_value=metadata.get(field_name),
                            enriched_value=value,
                            source_site=site_key,
                            source_url=url,
                        )

            except Exception as e:
                logger.warning(f"[ENRICH] Error fetching from {site_key}: {e}")
                continue

        # Apply enrichments
        if enriched_data:
            for field_name, result in enriched_data.items():
                metadata[field_name] = result.enriched_value
                report.enrichments.append(result)

                logger.info(
                    f"[ENRICH] '{title}': {field_name} <- {result.source_site}"
                )

            # Track enrichment sources
            if "_enrichment_sources" not in metadata:
                metadata["_enrichment_sources"] = {}

            for field_name, result in enriched_data.items():
                metadata["_enrichment_sources"][field_name] = {
                    "site_key": result.source_site,
                    "url": result.source_url,
                    "timestamp": report.timestamp,
                }

            metadata["_enriched_at"] = report.timestamp

            # Save if requested
            if save:
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)

        report.missing_fields_after = self.get_missing_fields(metadata)
        report.success = len(report.enrichments) > 0

        return report

    async def enrich_single_field(
        self,
        story_folder: str,
        field_name: str,
        *,
        save: bool = True,
    ) -> EnrichmentResult | None:
        """Enrich a single field from available sources.

        Args:
            story_folder: Path to story folder
            field_name: Name of field to enrich
            save: Whether to save the enriched metadata

        Returns:
            EnrichmentResult if successful, None otherwise
        """
        from flowcore_story.adapters.factory import get_adapter

        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            return None

        with open(metadata_path, encoding="utf-8") as f:
            metadata = json.load(f)

        title = metadata.get("title", "")
        sources = metadata.get("sources", [])

        if not title:
            return None

        # Check if field already has value
        current_value = metadata.get(field_name)
        if not self._is_empty(current_value):
            return None

        # Try each source
        for source in sources:
            if not isinstance(source, dict):
                continue

            site_key = source.get("site_key") or source.get("site")
            url = source.get("url")

            if not site_key or not url:
                continue

            if source.get("is_primary"):
                continue

            try:
                adapter = get_adapter(site_key)
                details = await adapter.get_story_details(url, title)

                if not details:
                    continue

                value = details.get(field_name)
                if not self._is_empty(value):
                    # Found value, apply it
                    metadata[field_name] = value

                    result = EnrichmentResult(
                        field=field_name,
                        original_value=current_value,
                        enriched_value=value,
                        source_site=site_key,
                        source_url=url,
                    )

                    # Track source
                    if "_enrichment_sources" not in metadata:
                        metadata["_enrichment_sources"] = {}
                    metadata["_enrichment_sources"][field_name] = {
                        "site_key": site_key,
                        "url": url,
                        "timestamp": datetime.now().isoformat(),
                    }

                    if save:
                        with open(metadata_path, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=4)

                    logger.info(
                        f"[ENRICH] '{title}': {field_name} <- {site_key}"
                    )
                    return result

            except Exception as e:
                logger.debug(f"[ENRICH] Error from {site_key}: {e}")
                continue

        return None

    def merge_metadata(
        self,
        base: dict[str, Any],
        enrichments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Merge metadata from multiple sources into base.

        Priority order: base values are kept if not empty,
        then enrichments are applied in order.

        Args:
            base: Base metadata dictionary
            enrichments: List of metadata dicts to merge from

        Returns:
            Merged metadata dictionary
        """
        result = dict(base)
        enrichment_sources: dict[str, str] = {}

        for field_name in self.priority_fields:
            # Skip if base already has good value
            if not self._is_empty(result.get(field_name)):
                continue

            # Try each enrichment source
            for enrich in enrichments:
                if not isinstance(enrich, dict):
                    continue

                value = enrich.get(field_name)
                source_site = enrich.get("_source_site", "unknown")

                if not self._is_empty(value):
                    result[field_name] = value
                    enrichment_sources[field_name] = source_site
                    break

        if enrichment_sources:
            result["_enrichment_sources"] = enrichment_sources
            result["_enriched_at"] = datetime.now().isoformat()

        return result

    def validate_enriched_metadata(
        self,
        metadata: dict[str, Any],
    ) -> tuple[bool, list[str]]:
        """Validate enriched metadata.

        Args:
            metadata: Metadata to validate

        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues: list[str] = []

        # Check required fields
        required_fields = ["title", "total_chapters_on_site"]
        for field_name in required_fields:
            if self._is_empty(metadata.get(field_name)):
                issues.append(f"Missing required field: {field_name}")

        # Check sources
        sources = metadata.get("sources", [])
        if not sources:
            issues.append("No sources defined")
        else:
            # Check for at least one primary
            has_primary = any(
                s.get("is_primary") for s in sources if isinstance(s, dict)
            )
            if not has_primary:
                issues.append("No primary source defined")

        # Check cover URL validity
        cover = metadata.get("cover")
        if cover and not cover.startswith(("http://", "https://")):
            issues.append(f"Invalid cover URL: {cover}")

        # Check total chapters is positive
        total = metadata.get("total_chapters_on_site", 0)
        if isinstance(total, (int, float)) and total <= 0:
            issues.append(f"Invalid total_chapters_on_site: {total}")

        return len(issues) == 0, issues


# Singleton instance
metadata_enricher = MetadataEnricher()


__all__ = [
    "MetadataEnricher",
    "EnrichmentResult",
    "EnrichmentReport",
    "metadata_enricher",
    "PRIORITY_FIELDS",
]

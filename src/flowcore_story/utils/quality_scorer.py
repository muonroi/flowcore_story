"""Quality scoring utilities for cross-site content comparison.

This module provides scoring mechanisms to evaluate:
1. Metadata completeness (author, description, cover, etc.)
2. Chapter content quality (length, formatting, no garbage)
3. Cross-source comparison and recommendation
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any

from flowcore_story.utils.logger import logger


# Metadata field weights for quality scoring
METADATA_WEIGHTS: dict[str, float] = {
    "title": 10.0,
    "author": 15.0,
    "description": 20.0,
    "cover": 10.0,
    "categories": 10.0,
    "total_chapters_on_site": 15.0,
    "rating_value": 10.0,
    "status": 10.0,
}

# Anti-bot/error patterns to detect in content
GARBAGE_PATTERNS: list[str] = [
    r"cloudflare",
    r"captcha",
    r"please wait",
    r"checking your browser",
    r"access denied",
    r"403 forbidden",
    r"404 not found",
    r"error loading",
    r"javascript required",
    r"enable javascript",
    r"bot detection",
    r"rate limit",
    r"too many requests",
]

# Compiled regex for garbage detection
_GARBAGE_REGEX = re.compile(
    "|".join(GARBAGE_PATTERNS),
    re.IGNORECASE,
)


@dataclass
class MetadataScore:
    """Result of metadata quality scoring."""

    total_score: float
    max_possible: float
    percentage: float
    field_scores: dict[str, float] = field(default_factory=dict)
    missing_fields: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


@dataclass
class ContentScore:
    """Result of content quality scoring."""

    total_score: float
    length_score: float
    formatting_score: float
    garbage_score: float
    issues: list[str] = field(default_factory=list)


@dataclass
class SourceQualityReport:
    """Quality report for a single source."""

    site_key: str
    url: str
    metadata_score: float
    content_score: float
    sample_chapters_checked: int
    issues: list[str] = field(default_factory=list)


@dataclass
class QualityComparisonReport:
    """Full quality comparison report across sources."""

    story_title: str
    story_folder: str
    sources: dict[str, SourceQualityReport] = field(default_factory=dict)
    recommended_primary: str | None = None
    recommendation_reason: str = ""


class QualityScorer:
    """Score and compare content quality across sites."""

    def __init__(self) -> None:
        self._weights = METADATA_WEIGHTS.copy()

    def score_metadata(self, metadata: dict[str, Any]) -> MetadataScore:
        """Score metadata completeness.

        Args:
            metadata: Story metadata dictionary

        Returns:
            MetadataScore with detailed breakdown
        """
        field_scores: dict[str, float] = {}
        missing_fields: list[str] = []
        issues: list[str] = []
        total_score = 0.0
        max_possible = sum(self._weights.values())

        for field_name, weight in self._weights.items():
            value = metadata.get(field_name)
            score = self._score_field(field_name, value)
            field_scores[field_name] = score * weight

            if score == 0:
                missing_fields.append(field_name)
            elif score < 1.0:
                issues.append(f"{field_name}: partial ({score:.0%})")

            total_score += score * weight

        percentage = (total_score / max_possible * 100) if max_possible > 0 else 0

        return MetadataScore(
            total_score=total_score,
            max_possible=max_possible,
            percentage=percentage,
            field_scores=field_scores,
            missing_fields=missing_fields,
            issues=issues,
        )

    def _score_field(self, field_name: str, value: Any) -> float:
        """Score a single metadata field.

        Returns:
            Score from 0.0 (missing/empty) to 1.0 (complete)
        """
        if value is None:
            return 0.0

        if isinstance(value, str):
            value = value.strip()
            if not value:
                return 0.0

            # Check for placeholder values
            if value.lower() in ("unknown", "n/a", "không rõ", "đang cập nhật"):
                return 0.3

            # Score based on length for description
            if field_name == "description":
                if len(value) < 50:
                    return 0.5
                if len(value) < 200:
                    return 0.8
                return 1.0

            # Check for valid URL for cover
            if field_name == "cover":
                if value.startswith(("http://", "https://")):
                    return 1.0
                return 0.5

            return 1.0

        if isinstance(value, (int, float)):
            if field_name == "total_chapters_on_site":
                return 1.0 if value > 0 else 0.0
            if field_name == "rating_value":
                return 1.0 if 0 < value <= 5 else 0.5
            return 1.0 if value else 0.0

        if isinstance(value, list):
            if not value:
                return 0.0
            # For categories, check if they have names
            if field_name == "categories":
                valid_cats = sum(
                    1 for cat in value
                    if isinstance(cat, dict) and cat.get("name")
                )
                return valid_cats / len(value) if value else 0.0
            return 1.0

        return 1.0 if value else 0.0

    def score_chapter_content(self, content: str) -> ContentScore:
        """Score chapter content quality.

        Factors:
        - Length (longer is better, up to threshold)
        - Formatting (paragraphs, proper line breaks)
        - No garbage/error content

        Args:
            content: Chapter content text

        Returns:
            ContentScore with detailed breakdown
        """
        issues: list[str] = []

        if not content or not isinstance(content, str):
            return ContentScore(
                total_score=0.0,
                length_score=0.0,
                formatting_score=0.0,
                garbage_score=0.0,
                issues=["Empty content"],
            )

        content = content.strip()

        # Length scoring (0-40 points)
        length = len(content)
        if length < 100:
            length_score = 0.0
            issues.append("Content too short (<100 chars)")
        elif length < 500:
            length_score = 10.0
            issues.append("Content short (<500 chars)")
        elif length < 2000:
            length_score = 25.0
        elif length < 5000:
            length_score = 35.0
        else:
            length_score = 40.0

        # Formatting scoring (0-30 points)
        paragraphs = content.count("\n\n") + content.count("\r\n\r\n")
        lines = content.count("\n") + 1

        if paragraphs >= 5:
            formatting_score = 30.0
        elif paragraphs >= 2:
            formatting_score = 20.0
        elif lines >= 5:
            formatting_score = 15.0
        else:
            formatting_score = 5.0
            issues.append("Poor formatting (few paragraphs)")

        # Garbage detection (0-30 points)
        garbage_matches = _GARBAGE_REGEX.findall(content.lower())
        if garbage_matches:
            garbage_score = 0.0
            issues.append(f"Garbage content detected: {garbage_matches[:3]}")
        else:
            garbage_score = 30.0

        # Check for excessive whitespace
        whitespace_ratio = content.count(" ") / len(content) if content else 0
        if whitespace_ratio > 0.3:
            garbage_score = max(0, garbage_score - 10)
            issues.append("Excessive whitespace")

        total_score = length_score + formatting_score + garbage_score

        return ContentScore(
            total_score=total_score,
            length_score=length_score,
            formatting_score=formatting_score,
            garbage_score=garbage_score,
            issues=issues,
        )

    async def compare_sources(
        self,
        story_folder: str,
        metadata: dict[str, Any],
    ) -> QualityComparisonReport:
        """Compare quality across all sources for a story.

        Args:
            story_folder: Path to story folder
            metadata: Story metadata with sources

        Returns:
            QualityComparisonReport with rankings
        """
        from flowcore_story.adapters.factory import get_adapter

        title = metadata.get("title", os.path.basename(story_folder))
        sources = metadata.get("sources", [])

        report = QualityComparisonReport(
            story_title=title,
            story_folder=story_folder,
        )

        if not sources:
            report.recommendation_reason = "No sources available"
            return report

        best_score = -1.0
        best_source: str | None = None

        for source in sources:
            if not isinstance(source, dict):
                continue

            site_key = source.get("site_key") or source.get("site")
            url = source.get("url")

            if not site_key or not url:
                continue

            try:
                adapter = get_adapter(site_key)
                details = await adapter.get_story_details(url, title)

                if not details:
                    report.sources[site_key] = SourceQualityReport(
                        site_key=site_key,
                        url=url,
                        metadata_score=0.0,
                        content_score=0.0,
                        sample_chapters_checked=0,
                        issues=["Failed to fetch details"],
                    )
                    continue

                # Score metadata
                meta_result = self.score_metadata(details)

                # Sample content scoring (first chapter if available)
                content_scores: list[float] = []
                chapters = details.get("chapters", [])[:3]  # Sample first 3 chapters

                for chapter in chapters:
                    chapter_url = chapter.get("url")
                    chapter_title = chapter.get("title", "")

                    if not chapter_url:
                        continue

                    try:
                        content = await adapter.get_chapter_content(
                            chapter_url, chapter_title, site_key
                        )
                        if content:
                            score = self.score_chapter_content(content)
                            content_scores.append(score.total_score)
                    except Exception as e:
                        logger.debug(f"Error scoring chapter {chapter_url}: {e}")

                avg_content_score = (
                    sum(content_scores) / len(content_scores)
                    if content_scores
                    else 0.0
                )

                source_report = SourceQualityReport(
                    site_key=site_key,
                    url=url,
                    metadata_score=meta_result.percentage,
                    content_score=avg_content_score,
                    sample_chapters_checked=len(content_scores),
                    issues=meta_result.issues[:5],  # Limit issues
                )

                report.sources[site_key] = source_report

                # Calculate combined score for ranking
                combined_score = meta_result.percentage * 0.6 + avg_content_score * 0.4

                if combined_score > best_score:
                    best_score = combined_score
                    best_source = site_key

            except Exception as e:
                logger.warning(f"Error comparing source {site_key}: {e}")
                report.sources[site_key] = SourceQualityReport(
                    site_key=site_key,
                    url=url,
                    metadata_score=0.0,
                    content_score=0.0,
                    sample_chapters_checked=0,
                    issues=[f"Error: {str(e)[:100]}"],
                )

        if best_source:
            report.recommended_primary = best_source
            best_report = report.sources.get(best_source)
            if best_report:
                report.recommendation_reason = (
                    f"Highest combined score: "
                    f"metadata={best_report.metadata_score:.1f}%, "
                    f"content={best_report.content_score:.1f}"
                )

        return report

    def recommend_primary_source(
        self,
        quality_report: QualityComparisonReport,
    ) -> str | None:
        """Recommend which source should be primary based on quality.

        Args:
            quality_report: Quality comparison report

        Returns:
            Recommended site_key or None
        """
        return quality_report.recommended_primary


# Singleton instance
quality_scorer = QualityScorer()


__all__ = [
    "QualityScorer",
    "MetadataScore",
    "ContentScore",
    "SourceQualityReport",
    "QualityComparisonReport",
    "quality_scorer",
    "METADATA_WEIGHTS",
]

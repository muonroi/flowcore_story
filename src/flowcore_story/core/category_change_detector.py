"""Change detection utilities for category-level crawl planning.

This module encapsulates the logic for comparing two discovery snapshots of a
category and deciding whether a refresh should be scheduled.  The detection is
based on two signals:

* A checksum of the discovered story URLs (order agnostic).
* A content signature derived from stable story metadata fields.

By packaging the functionality in its own module we keep :mod:`main` focused on
orchestration while making the change detection easily testable in isolation.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass

_DEFAULT_CONTENT_KEYS = (
    "title",
    "name",
    "slug",
    "id",
    "latest_chapter",
    "chapter",
    "chapter_count",
    "total_chapters",
    "author",
    "status",
)


def _normalise_url(value: object) -> str | None:
    """Return a normalised URL string when ``value`` looks like one."""

    if isinstance(value, str):
        candidate = value.strip()
        if candidate:
            return candidate
    return None


def _hash_strings(values: Iterable[str]) -> str:
    """Return a deterministic digest for ``values`` ignoring order."""

    digest = hashlib.blake2s()
    for item in sorted(values):
        digest.update(item.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _serialise_story(story: Mapping[str, object]) -> str:
    """Return a stable serialisation of ``story`` for signature purposes."""

    payload: MutableMapping[str, str] = {}
    for key in _DEFAULT_CONTENT_KEYS:
        raw = story.get(key)
        if isinstance(raw, (str, int, float)):
            text = str(raw).strip()
            if text:
                payload[key] = text
    url = _normalise_url(story.get("url"))
    if url:
        payload.setdefault("url", url)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


@dataclass(frozen=True)
class CategorySignature:
    """Summary of a category discovery snapshot."""

    url_checksum: str
    content_signature: str
    story_count: int


@dataclass(frozen=True)
class CategoryChangeResult:
    """Detailed outcome of a change detection comparison."""

    signature: CategorySignature
    urls: list[str]
    added_urls: list[str]
    removed_urls: list[str]
    change_ratio: float
    change_count: int
    content_changed: bool
    requires_refresh: bool


class CategoryChangeDetector:
    """Detect large discovery changes between crawl runs."""

    def __init__(
        self,
        *,
        ratio_threshold: float,
        absolute_threshold: int,
        min_story_count: int,
    ) -> None:
        self._ratio_threshold = max(0.0, float(ratio_threshold))
        self._absolute_threshold = max(0, int(absolute_threshold))
        self._min_story_count = max(0, int(min_story_count))

    def evaluate(
        self,
        stories: Iterable[Mapping[str, object]],
        previous_entry: Mapping[str, object] | None = None,
    ) -> CategoryChangeResult:
        """Compare ``stories`` against ``previous_entry`` and summarise the change."""

        urls: list[str] = []
        content_fragments: list[str] = []
        for story in stories:
            if not isinstance(story, Mapping):
                continue
            url = _normalise_url(story.get("url"))
            if not url:
                continue
            urls.append(url)
            content_fragments.append(_serialise_story(story))

        urls = sorted(set(urls))
        content_fragments = sorted(content_fragments)

        signature = CategorySignature(
            url_checksum=_hash_strings(urls),
            content_signature=_hash_strings(content_fragments),
            story_count=len(urls),
        )

        previous_urls: list[str] = []
        prev_signature: CategorySignature | None = None
        if isinstance(previous_entry, Mapping):
            raw_urls = previous_entry.get("urls")
            if isinstance(raw_urls, list):
                previous_urls = sorted(
                    {candidate.strip() for candidate in raw_urls if isinstance(candidate, str) and candidate.strip()}
                )
            prev_url_checksum = previous_entry.get("url_checksum")
            prev_content_signature = previous_entry.get("content_signature")
            prev_story_count = previous_entry.get("story_count")
            if isinstance(prev_url_checksum, str) and isinstance(prev_content_signature, str):
                prev_signature = CategorySignature(
                    url_checksum=prev_url_checksum,
                    content_signature=prev_content_signature,
                    story_count=int(prev_story_count or len(previous_urls)),
                )

        added_urls = sorted(set(urls) - set(previous_urls))
        removed_urls = sorted(set(previous_urls) - set(urls))
        change_count = len(added_urls) + len(removed_urls)

        if previous_urls:
            baseline = max(len(previous_urls), 1)
            change_ratio = change_count / baseline
        else:
            change_ratio = 1.0 if change_count else 0.0

        content_changed = bool(prev_signature) and prev_signature.content_signature != signature.content_signature

        requires_refresh = False
        if prev_signature and signature.story_count >= self._min_story_count:
            if change_count >= self._absolute_threshold:
                requires_refresh = True
            elif change_ratio >= self._ratio_threshold and change_count > 0:
                requires_refresh = True
            elif content_changed and change_count >= max(
                1,
                min(self._absolute_threshold, max(5, signature.story_count // 4)),
            ):
                requires_refresh = True

        return CategoryChangeResult(
            signature=signature,
            urls=urls,
            added_urls=added_urls,
            removed_urls=removed_urls,
            change_ratio=change_ratio,
            change_count=change_count,
            content_changed=content_changed,
            requires_refresh=requires_refresh,
        )


__all__ = [
    "CategoryChangeDetector",
    "CategoryChangeResult",
    "CategorySignature",
]


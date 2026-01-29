"""Fuzzy title matching utilities for cross-site story identification.

This module provides Vietnamese-aware title matching using multiple strategies:
1. Exact match (fastest)
2. Normalized containment (handles suffixes like "- Convert")
3. Fuzzy token matching with rapidfuzz (handles typos, word order)
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import TYPE_CHECKING

from unidecode import unidecode

if TYPE_CHECKING:
    from collections.abc import Sequence

# Try to import rapidfuzz, fallback to difflib if not available
try:
    from rapidfuzz import fuzz as rapidfuzz_fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    import difflib

# Default fuzzy matching threshold (0.0 - 1.0)
DEFAULT_THRESHOLD = 0.8


@lru_cache(maxsize=10000)
def normalize_vietnamese_title(title: str) -> str:
    """Normalize a Vietnamese title for matching.

    Steps:
    1. Convert to lowercase
    2. Remove Vietnamese diacritics using unidecode
    3. Remove special characters
    4. Collapse whitespace

    Args:
        title: The title to normalize

    Returns:
        Normalized title string
    """
    if not title or not isinstance(title, str):
        return ""

    # Lowercase
    normalized = title.lower().strip()

    # Handle common Vietnamese character đ/Đ explicitly before unidecode
    normalized = normalized.replace('đ', 'd').replace('Đ', 'D')

    # Remove diacritics using unidecode
    normalized = unidecode(normalized)

    # Remove special characters, keep only alphanumeric and spaces
    normalized = re.sub(r'[^a-z0-9\s]', ' ', normalized)

    # Collapse multiple spaces
    normalized = ' '.join(normalized.split())

    return normalized


def _exact_match(title1: str, title2: str) -> bool:
    """Check for exact match after basic normalization."""
    return title1.lower().strip() == title2.lower().strip()


def _containment_match(norm1: str, norm2: str) -> bool:
    """Check if one normalized title contains the other.

    This handles cases like:
    - "Đấu Phá Thương Khung" vs "Đấu Phá Thương Khung - Convert"
    - "ĐPTK" vs "Đấu Phá Thương Khung ĐPTK"
    """
    if not norm1 or not norm2:
        return False

    # Check if either is contained in the other
    if norm1 in norm2 or norm2 in norm1:
        return True

    # Check word-level containment for shorter titles
    words1 = set(norm1.split())
    words2 = set(norm2.split())

    if len(words1) <= 2 or len(words2) <= 2:
        # For very short titles, require exact containment
        return False

    # Check if all words from shorter title are in longer title
    shorter, longer = (words1, words2) if len(words1) < len(words2) else (words2, words1)
    if len(shorter) >= 2 and shorter.issubset(longer):
        return True

    return False


def _fuzzy_score(norm1: str, norm2: str) -> float:
    """Calculate fuzzy match score between two normalized titles.

    Returns:
        Score from 0.0 to 1.0
    """
    if not norm1 or not norm2:
        return 0.0

    if RAPIDFUZZ_AVAILABLE:
        # Use token_sort_ratio for better handling of word order differences
        # Returns 0-100, convert to 0-1
        return rapidfuzz_fuzz.token_sort_ratio(norm1, norm2) / 100.0
    else:
        # Fallback to difflib SequenceMatcher
        return difflib.SequenceMatcher(None, norm1, norm2).ratio()


def fuzzy_match_titles(
    title1: str,
    title2: str,
    threshold: float = DEFAULT_THRESHOLD,
) -> tuple[bool, float]:
    """Check if two titles match using fuzzy matching.

    Uses a multi-step approach for efficiency:
    1. Exact match (fastest, returns 1.0)
    2. Normalized containment (fast, returns 0.95)
    3. Fuzzy token matching (slower, returns actual score)

    Args:
        title1: First title to compare
        title2: Second title to compare
        threshold: Minimum score to consider a match (0.0 - 1.0)

    Returns:
        Tuple of (is_match, score)
    """
    if not title1 or not title2:
        return False, 0.0

    # Step 1: Exact match (fastest)
    if _exact_match(title1, title2):
        return True, 1.0

    # Step 2: Normalize titles
    norm1 = normalize_vietnamese_title(title1)
    norm2 = normalize_vietnamese_title(title2)

    if not norm1 or not norm2:
        return False, 0.0

    # Exact match after normalization
    if norm1 == norm2:
        return True, 1.0

    # Step 3: Containment check
    if _containment_match(norm1, norm2):
        return True, 0.95

    # Step 4: Fuzzy matching
    score = _fuzzy_score(norm1, norm2)
    return score >= threshold, score


def find_best_match(
    title: str,
    candidates: Sequence[str],
    threshold: float = DEFAULT_THRESHOLD,
) -> tuple[str, float] | None:
    """Find the best matching title from a list of candidates.

    Args:
        title: The title to match
        candidates: List of candidate titles to search
        threshold: Minimum score to consider a match

    Returns:
        Tuple of (best_match_title, score) or None if no match found
    """
    if not title or not candidates:
        return None

    best_match: str | None = None
    best_score: float = 0.0

    norm_title = normalize_vietnamese_title(title)

    for candidate in candidates:
        if not candidate:
            continue

        is_match, score = fuzzy_match_titles(title, candidate, threshold)

        if is_match and score > best_score:
            best_score = score
            best_match = candidate

            # Early exit on exact match
            if score >= 1.0:
                break

    if best_match is not None and best_score >= threshold:
        return best_match, best_score

    return None


def batch_find_matches(
    title: str,
    candidates: Sequence[str],
    threshold: float = DEFAULT_THRESHOLD,
    max_results: int = 5,
) -> list[tuple[str, float]]:
    """Find all matching titles above threshold, sorted by score.

    Args:
        title: The title to match
        candidates: List of candidate titles to search
        threshold: Minimum score to consider a match
        max_results: Maximum number of results to return

    Returns:
        List of (title, score) tuples, sorted by score descending
    """
    if not title or not candidates:
        return []

    matches: list[tuple[str, float]] = []

    for candidate in candidates:
        if not candidate:
            continue

        is_match, score = fuzzy_match_titles(title, candidate, threshold)
        if is_match:
            matches.append((candidate, score))

    # Sort by score descending
    matches.sort(key=lambda x: x[1], reverse=True)

    return matches[:max_results]


def clear_cache() -> None:
    """Clear the normalization cache."""
    normalize_vietnamese_title.cache_clear()


# Module-level exports
__all__ = [
    "normalize_vietnamese_title",
    "fuzzy_match_titles",
    "find_best_match",
    "batch_find_matches",
    "clear_cache",
    "DEFAULT_THRESHOLD",
    "RAPIDFUZZ_AVAILABLE",
]

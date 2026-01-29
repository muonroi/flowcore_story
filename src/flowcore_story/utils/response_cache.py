"""
PHASE 1 OPTIMIZATION: In-memory caching layer for HTTP responses
Reduces challenge harvester calls for frequently accessed pages (genre lists, metadata)
"""
import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger("storyflow.response_cache")


@dataclass
class CacheEntry:
    """Cache entry with TTL support"""
    key: str
    value: Any
    created_at: float
    ttl_seconds: float

    def is_expired(self) -> bool:
        """Check if entry is expired"""
        if self.ttl_seconds <= 0:
            return False  # No expiration
        return (time.time() - self.created_at) > self.ttl_seconds


class ResponseCache:
    """
    In-memory LRU cache with TTL for HTTP responses.

    PHASE 1 OPTIMIZATION: Reduces redundant challenge harvester calls by caching:
    - Genre pages (TTL: 1 hour) - genre lists change infrequently
    - Story metadata (TTL: 30 minutes) - story info relatively static
    - Chapter lists (TTL: 15 minutes) - new chapters appear periodically

    Cost savings:
    - Avoid repeated Playwright launches for same pages
    - Reduce Cloudflare challenge solves (even with cookies, challenges still occur)
    - Lower proxy bandwidth usage
    """

    def __init__(self, max_size: int = 1000, default_ttl: float = 3600):
        """
        Initialize cache.

        Args:
            max_size: Maximum number of entries (LRU eviction when exceeded)
            default_ttl: Default TTL in seconds (0 = no expiration)
        """
        self._cache: dict[str, CacheEntry] = {}
        self._access_times: dict[str, float] = {}  # For LRU
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()

        # Statistics
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def _generate_key(self, url: str, **kwargs) -> str:
        """Generate cache key from URL and parameters"""
        # Include URL and any additional parameters in key
        key_parts = [url]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()

    async def get(self, url: str, **kwargs) -> Optional[Any]:
        """
        Get cached value for URL.

        Args:
            url: Request URL
            **kwargs: Additional parameters for cache key (e.g., method, headers)

        Returns:
            Cached value if found and not expired, None otherwise
        """
        key = self._generate_key(url, **kwargs)

        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._misses += 1
                return None

            # Check expiration
            if entry.is_expired():
                del self._cache[key]
                del self._access_times[key]
                self._misses += 1
                logger.debug("[Cache] Expired entry for %s", url[:100])
                return None

            # Update access time
            self._access_times[key] = time.time()
            self._hits += 1

            logger.debug(
                "[Cache] HIT for %s (age: %.1fs, hit_rate: %.1f%%)",
                url[:100],
                time.time() - entry.created_at,
                self.hit_rate * 100
            )

            return entry.value

    async def set(
        self,
        url: str,
        value: Any,
        ttl: Optional[float] = None,
        **kwargs
    ) -> None:
        """
        Store value in cache.

        Args:
            url: Request URL
            value: Value to cache
            ttl: Time-to-live in seconds (None = use default)
            **kwargs: Additional parameters for cache key
        """
        key = self._generate_key(url, **kwargs)
        ttl_seconds = ttl if ttl is not None else self._default_ttl

        async with self._lock:
            # Evict LRU entry if cache is full
            if len(self._cache) >= self._max_size and key not in self._cache:
                await self._evict_lru()

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                ttl_seconds=ttl_seconds
            )

            self._cache[key] = entry
            self._access_times[key] = time.time()

            logger.debug(
                "[Cache] SET %s (ttl: %.0fs, size: %d/%d)",
                url[:100],
                ttl_seconds,
                len(self._cache),
                self._max_size
            )

    async def _evict_lru(self) -> None:
        """Evict least recently used entry"""
        if not self._access_times:
            return

        # Find LRU key
        lru_key = min(self._access_times.items(), key=lambda x: x[1])[0]

        # Remove from cache
        del self._cache[lru_key]
        del self._access_times[lru_key]
        self._evictions += 1

        logger.debug("[Cache] Evicted LRU entry (total evictions: %d)", self._evictions)

    async def clear(self) -> None:
        """Clear all cache entries"""
        async with self._lock:
            self._cache.clear()
            self._access_times.clear()
            logger.info("[Cache] Cleared all entries")

    async def invalidate(self, url: str, **kwargs) -> None:
        """Invalidate specific cache entry"""
        key = self._generate_key(url, **kwargs)

        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                del self._access_times[key]
                logger.debug("[Cache] Invalidated %s", url[:100])

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0

    @property
    def stats(self) -> dict[str, Any]:
        """Get cache statistics"""
        return {
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
            "hit_rate": self.hit_rate,
            "size": len(self._cache),
            "max_size": self._max_size,
        }


# Global cache instances for different content types
# Different TTLs optimize for content change frequency
# Read from environment variables for configurability

import os

_CACHE_SIZE_GENRE = int(os.getenv("CACHE_SIZE_GENRE", "500"))
_CACHE_SIZE_STORY = int(os.getenv("CACHE_SIZE_STORY", "2000"))
_CACHE_SIZE_CHAPTER = int(os.getenv("CACHE_SIZE_CHAPTER", "1000"))

_CACHE_TTL_GENRE = int(os.getenv("CACHE_TTL_GENRE", "0")) or 3600  # 1 hour default
_CACHE_TTL_STORY = int(os.getenv("CACHE_TTL_STORY", "0")) or 1800  # 30 min default
_CACHE_TTL_CHAPTER = int(os.getenv("CACHE_TTL_CHAPTER", "0")) or 900  # 15 min default

# Genre pages change infrequently (new stories added hourly)
genre_cache = ResponseCache(max_size=_CACHE_SIZE_GENRE, default_ttl=_CACHE_TTL_GENRE)

# Story metadata relatively static
story_cache = ResponseCache(max_size=_CACHE_SIZE_STORY, default_ttl=_CACHE_TTL_STORY)

# Chapter lists updated more frequently
chapter_cache = ResponseCache(max_size=_CACHE_SIZE_CHAPTER, default_ttl=_CACHE_TTL_CHAPTER)

logger.info(
    "[Cache] Initialized caches - genre: %d/%ds, story: %d/%ds, chapter: %d/%ds",
    _CACHE_SIZE_GENRE, _CACHE_TTL_GENRE,
    _CACHE_SIZE_STORY, _CACHE_TTL_STORY,
    _CACHE_SIZE_CHAPTER, _CACHE_TTL_CHAPTER,
)


async def get_cached_response(
    url: str,
    cache_type: str = "genre",
    **kwargs
) -> Optional[Any]:
    """
    Get cached response for URL.

    Args:
        url: Request URL
        cache_type: Type of cache to use ("genre", "story", "chapter")
        **kwargs: Additional cache key parameters

    Returns:
        Cached value if available, None otherwise
    """
    cache_map = {
        "genre": genre_cache,
        "story": story_cache,
        "chapter": chapter_cache,
    }

    cache = cache_map.get(cache_type, genre_cache)
    return await cache.get(url, **kwargs)


async def cache_response(
    url: str,
    value: Any,
    cache_type: str = "genre",
    ttl: Optional[float] = None,
    **kwargs
) -> None:
    """
    Cache response for URL.

    Args:
        url: Request URL
        value: Value to cache
        cache_type: Type of cache to use ("genre", "story", "chapter")
        ttl: Custom TTL (None = use cache default)
        **kwargs: Additional cache key parameters
    """
    cache_map = {
        "genre": genre_cache,
        "story": story_cache,
        "chapter": chapter_cache,
    }

    cache = cache_map.get(cache_type, genre_cache)
    await cache.set(url, value, ttl=ttl, **kwargs)


async def get_cache_stats() -> dict[str, dict[str, Any]]:
    """Get statistics for all caches"""
    return {
        "genre": genre_cache.stats,
        "story": story_cache.stats,
        "chapter": chapter_cache.stats,
    }

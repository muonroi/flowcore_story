import time
from typing import Any

# Simple in-memory TTL cache for async functions

def _build_key(prefix: str, args: tuple[Any, ...]) -> tuple[str, tuple[Any, ...]]:
    return (prefix, args)

class AsyncTTLCache:
    def __init__(self, ttl: float = 300.0):
        self.ttl = ttl
        # Store (created_at, ttl, value)
        self.cache: dict[tuple[str, tuple[Any, ...]], tuple[float, float, Any]] = {}

    def get(self, key: tuple[str, tuple[Any, ...]], ttl: float | None = None):
        now = time.time()
        record = self.cache.get(key)
        if not record:
            return None

        created_at, stored_ttl, value = record
        effective_ttl = stored_ttl if ttl is None else min(stored_ttl, ttl)
        if effective_ttl <= 0 or now - created_at >= effective_ttl:
            self.cache.pop(key, None)
            return None
        return value

    def set(self, key: tuple[str, tuple[Any, ...]], value: Any, ttl: float | None = None):
        entry_ttl = self.ttl if ttl is None else ttl
        self.cache[key] = (time.time(), entry_ttl, value)

cache = AsyncTTLCache()

async def cached_get_story_details(adapter, url: str, title: str, ttl: float = 300.0):
    key = _build_key("story", (getattr(adapter, "SITE_KEY", str(id(adapter))), url))
    val = cache.get(key, ttl=ttl)
    if val is not None:
        return val
    result = await adapter.get_story_details(url, title)
    cache.set(key, result, ttl=ttl)
    return result

async def cached_get_chapter_list(
    adapter,
    url: str,
    title: str,
    site_key: str,
    total_chapters: int | None = None,
    ttl: float = 300.0,
):
    key = _build_key("chapters", (site_key, url))
    val = cache.get(key, ttl=ttl)
    if val is not None:
        return val
    result = await adapter.get_chapter_list(url, title, site_key, total_chapters=total_chapters)
    cache.set(key, result, ttl=ttl)
    return result

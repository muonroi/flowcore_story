import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from typing import Any

from bs4 import BeautifulSoup

from flowcore_story.config.config import (
    AI_METRICS_PATH,
    AI_MODEL,
    AI_PROFILE_TTL_HOURS,
    AI_PROFILES_PATH,
    AI_TRIM_MAX_BYTES,
    OPENAI_API_KEY,
    OPENAI_BASE,
)
from flowcore_story.utils.httpx_compat import create_async_client
from flowcore_story.utils.logger import logger


def _now_ts() -> int:
    return int(time.time())


def _load_profiles() -> dict[str, Any]:
    try:
        if not os.path.exists(AI_PROFILES_PATH):
            return {}
        with open(AI_PROFILES_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"[AI] Failed to load AI profiles: {e}")
        return {}


def _save_profiles(data: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(AI_PROFILES_PATH), exist_ok=True)
    tmp = AI_PROFILES_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, AI_PROFILES_PATH)


def _domain_key(url: str) -> str:
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return p.netloc
    except Exception:
        return url


def _trim_html(html: str, max_bytes: int = AI_TRIM_MAX_BYTES) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()
        body = soup.body or soup
        out = str(body)
        if len(out.encode("utf-8")) > max_bytes:
            # Hard trim while keeping tag balance minimal (string slice is acceptable for AI context)
            enc = out.encode("utf-8")[:max_bytes]
            out = enc.decode("utf-8", errors="ignore")
        return out
    except Exception:
        return html[:max_bytes]


@dataclass
class CategorySelectors:
    story_item_selector: str
    story_link_selector: str
    title_selector: str | None = None
    author_selector: str | None = None
    pagination_next_selector: str | None = None
    pagination_url_pattern: str | None = None  # e.g. "/trang-{n}/" or "?page={n}"


@dataclass
class ChapterListSelectors:
    chapter_item_selector: str
    chapter_link_selector: str
    pagination_next_selector: str | None = None
    pagination_url_pattern: str | None = None


@dataclass
class AIProfile:
    domain: str
    detected_at: int
    ttl_hours: int
    category: CategorySelectors | None = None
    chapter_list: ChapterListSelectors | None = None
    content_selector: str | None = None

    def expired(self) -> bool:
        age = _now_ts() - self.detected_at
        return age > self.ttl_hours * 3600


def _get_cached_profile(domain: str) -> AIProfile | None:
    allp = _load_profiles()
    data = allp.get(domain)
    if not data:
        return None
    try:
        cat = data.get("category")
        ch = data.get("chapter_list")
        profile = AIProfile(
            domain=domain,
            detected_at=data.get("detected_at", 0),
            ttl_hours=data.get("ttl_hours", AI_PROFILE_TTL_HOURS),
            category=CategorySelectors(**cat) if cat else None,
            chapter_list=ChapterListSelectors(**ch) if ch else None,
        )
        if profile.expired():
            return None
        return profile
    except Exception:
        return None


def _set_cached_profile(profile: AIProfile) -> None:
    allp = _load_profiles()
    data = asdict(profile)
    allp[profile.domain] = data
    _save_profiles(allp)


def _load_metrics() -> dict[str, Any]:
    try:
        if not os.path.exists(AI_METRICS_PATH):
            return {}
        with open(AI_METRICS_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_metrics(data: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(AI_METRICS_PATH), exist_ok=True)
    tmp = AI_METRICS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, AI_METRICS_PATH)


def _record_metric(domain: str, key: str) -> None:
    try:
        data = _load_metrics()
        rec = data.get(domain) or {}
        rec[key] = rec.get(key, 0) + 1
        rec["last_used_at"] = _now_ts()
        data[domain] = rec
        _save_metrics(data)
    except Exception:
        pass

_METRICS_PRINTED = False

def print_ai_metrics_summary() -> None:
    """Print a concise summary of AI metrics per domain once per run."""
    global _METRICS_PRINTED
    if _METRICS_PRINTED:
        return
    try:
        data = _load_metrics()
        if not data:
            logger.info("[AI][metrics] no data")
            _METRICS_PRINTED = True
            return
        logger.info("[AI][metrics] domain summary:")
        for domain, rec in data.items():
            cat_c = rec.get("category_calls", 0)
            cat_s = rec.get("category_success", 0)
            cat_f = rec.get("category_fail", 0)
            cat_hit = rec.get("category_cache_hit", 0)
            ch_c = rec.get("chapters_calls", 0)
            ch_s = rec.get("chapters_success", 0)
            ch_f = rec.get("chapters_fail", 0)
            ch_hit = rec.get("chapters_cache_hit", 0)
            ct_c = rec.get("content_calls", 0)
            ct_s = rec.get("content_success", 0)
            ct_f = rec.get("content_fail", 0)
            ct_hit = rec.get("content_cache_hit", 0)
            last = rec.get("last_used_at")
            logger.info(
                f"[AI][{domain}] cat {cat_s}/{cat_c} fail={cat_f} cache={cat_hit}; ch {ch_s}/{ch_c} fail={ch_f} cache={ch_hit}; content {ct_s}/{ct_c} fail={ct_f} cache={ct_hit}; last={last}"
            )
    except Exception as e:
        logger.warning(f"[AI][metrics] summary error: {e}")
    _METRICS_PRINTED = True


async def _call_openai_json(prompts: list[dict[str, str]]) -> dict[str, Any] | None:
    if not OPENAI_API_KEY:
        logger.warning("[AI] OPENAI_API_KEY not set; skip AI detection.")
        return None
    url = f"{OPENAI_BASE}/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": AI_MODEL,
        "messages": prompts,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    try:
        async with create_async_client(timeout=30) as client:
            r = await client.post(url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()
            content = data["choices"][0]["message"]["content"]
            return json.loads(content)
    except Exception as e:
        logger.error(f"[AI] OpenAI call failed: {e}")
        return None


async def detect_category_selectors(html: str, url: str) -> CategorySelectors | None:
    trimmed = _trim_html(html)
    domain = _domain_key(url)
    _record_metric(domain, "category_calls")
    sys = {
        "role": "system",
        "content": (
            "You extract robust CSS selectors and a pagination pattern from HTML of a category/list page. "
            "Return strict JSON with keys: story_item_selector, story_link_selector, title_selector (optional), author_selector (optional), "
            "pagination_next_selector (optional), pagination_url_pattern (optional using {n}, e.g. '/trang-{n}/' or '?page={n}')."
        ),
    }
    user = {
        "role": "user",
        "content": json.dumps({
            "page_url": url,
            "html": trimmed,
        }),
    }
    data = await _call_openai_json([sys, user])
    if not data:
        _record_metric(domain, "category_fail")
        return None
    try:
        sel = CategorySelectors(
            story_item_selector=data["story_item_selector"],
            story_link_selector=data.get("story_link_selector", "a[href]"),
            title_selector=data.get("title_selector"),
            author_selector=data.get("author_selector"),
            pagination_next_selector=data.get("pagination_next_selector"),
            pagination_url_pattern=data.get("pagination_url_pattern"),
        )
        _record_metric(domain, "category_success")
        logger.info(f"[AI][{domain}][CATEGORY] learned selectors")
        return sel
    except Exception as e:
        logger.error(f"[AI] Invalid category selectors JSON: {e}")
        _record_metric(domain, "category_fail")
        return None


async def detect_chapter_list_selectors(html: str, url: str) -> ChapterListSelectors | None:
    trimmed = _trim_html(html)
    domain = _domain_key(url)
    _record_metric(domain, "chapters_calls")
    sys = {
        "role": "system",
        "content": (
            "You extract robust CSS selectors for chapter list page and pagination pattern. "
            "Return strict JSON with keys: chapter_item_selector, chapter_link_selector, "
            "pagination_next_selector (optional), pagination_url_pattern (optional using {n})."
        ),
    }
    user = {
        "role": "user",
        "content": json.dumps({
            "page_url": url,
            "html": trimmed,
        }),
    }
    data = await _call_openai_json([sys, user])
    if not data:
        _record_metric(domain, "chapters_fail")
        return None
    try:
        sel = ChapterListSelectors(
            chapter_item_selector=data["chapter_item_selector"],
            chapter_link_selector=data.get("chapter_link_selector", "a[href]"),
            pagination_next_selector=data.get("pagination_next_selector"),
            pagination_url_pattern=data.get("pagination_url_pattern"),
        )
        _record_metric(domain, "chapters_success")
        logger.info(f"[AI][{domain}][CHAPTERS] learned selectors")
        return sel
    except Exception as e:
        logger.error(f"[AI] Invalid chapter selectors JSON: {e}")
        _record_metric(domain, "chapters_fail")
        return None


async def detect_content_selector(html: str, url: str) -> str | None:
    trimmed = _trim_html(html)
    domain = _domain_key(url)
    _record_metric(domain, "content_calls")
    sys = {
        "role": "system",
        "content": (
            "Return a robust CSS selector string that targets ONLY the main chapter content container (no ads, no menu). "
            "Reply strict JSON: {\"content_selector\": ""css""}."
        ),
    }
    user = {"role": "user", "content": json.dumps({"page_url": url, "html": trimmed})}
    data = await _call_openai_json([sys, user])
    if not data:
        _record_metric(domain, "content_fail")
        return None
    sel = data.get("content_selector")
    if isinstance(sel, str) and sel.strip():
        _record_metric(domain, "content_success")
        logger.info(f"[AI][{domain}][CONTENT] learned selector")
        return sel.strip()
    return None


async def get_or_create_profile_for_category(url: str, html: str) -> AIProfile | None:
    domain = _domain_key(url)
    prof = _get_cached_profile(domain)
    if prof and prof.category and not prof.expired():
        _record_metric(domain, "category_cache_hit")
        return prof
    async with _domain_lock(domain, "category"):
        prof2 = _get_cached_profile(domain)
        if prof2 and prof2.category and not prof2.expired():
            _record_metric(domain, "category_cache_hit")
            return prof2
        sel = await detect_category_selectors(html, url)
        if not sel:
            return prof2  # maybe still usable for chapters if exists
        prof2 = prof2 or AIProfile(domain=domain, detected_at=_now_ts(), ttl_hours=AI_PROFILE_TTL_HOURS)
        prof2.detected_at = _now_ts()
        prof2.category = sel
        _set_cached_profile(prof2)
        return prof2


async def get_or_create_profile_for_chapter_list(url: str, html: str) -> AIProfile | None:
    domain = _domain_key(url)
    prof = _get_cached_profile(domain)
    if prof and prof.chapter_list and not prof.expired():
        _record_metric(domain, "chapters_cache_hit")
        return prof
    async with _domain_lock(domain, "chapters"):
        prof2 = _get_cached_profile(domain)
        if prof2 and prof2.chapter_list and not prof2.expired():
            _record_metric(domain, "chapters_cache_hit")
            return prof2
        sel = await detect_chapter_list_selectors(html, url)
        if not sel:
            return prof2
        prof2 = prof2 or AIProfile(domain=domain, detected_at=_now_ts(), ttl_hours=AI_PROFILE_TTL_HOURS)
        prof2.detected_at = _now_ts()
        prof2.chapter_list = sel
        _set_cached_profile(prof2)
        return prof2


async def get_or_create_profile_for_content(url: str, html: str) -> AIProfile | None:
    domain = _domain_key(url)
    prof = _get_cached_profile(domain)
    if prof and prof.content_selector and not prof.expired():
        _record_metric(domain, "content_cache_hit")
        return prof
    async with _domain_lock(domain, "content"):
        prof2 = _get_cached_profile(domain)
        if prof2 and prof2.content_selector and not prof2.expired():
            _record_metric(domain, "content_cache_hit")
            return prof2
        sel = await detect_content_selector(html, url)
        if not sel:
            return prof2
        prof2 = prof2 or AIProfile(domain=domain, detected_at=_now_ts(), ttl_hours=AI_PROFILE_TTL_HOURS)
        prof2.detected_at = _now_ts()
        prof2.content_selector = sel
        _set_cached_profile(prof2)
        return prof2


def _apply_category_selectors(html: str, base_url: str, sel: CategorySelectors) -> tuple[list[dict[str, Any]], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    stories: list[dict[str, Any]] = []
    for item in soup.select(sel.story_item_selector):
        link = item.select_one(sel.story_link_selector)
        if not link or not link.get("href"):
            continue
        title = None
        if sel.title_selector:
            tt = item.select_one(sel.title_selector)
            title = tt.get_text(strip=True) if tt else None
        if not title:
            title = link.get("title") or link.get_text(strip=True)
        author = None
        if sel.author_selector:
            au = item.select_one(sel.author_selector)
            author = au.get_text(strip=True) if au else None
        stories.append({
            "title": title or "untitled",
            "url": link["href"],
            "author": author,
        })
    next_url = None
    if sel.pagination_url_pattern:
        # Assume caller expects page-1 -> page-2
        pat = sel.pagination_url_pattern
        if "{n}" in pat:
            if pat.startswith("?"):
                # append query to base_url
                sep = '&' if '?' in base_url else '?'
                next_url = f"{base_url.rstrip('/')}{sep}{pat.format(n=2).lstrip('?')}"
            else:
                next_url = base_url.rstrip('/') + '/' + pat.format(n=2).lstrip('/')
    elif sel.pagination_next_selector:
        a = soup.select_one(sel.pagination_next_selector)
        if a and a.get("href"):
            next_url = a["href"]
    return stories, next_url


def _apply_chapter_list_selectors(html: str, sel: ChapterListSelectors) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, str]] = []
    seen = set()
    for item in soup.select(sel.chapter_item_selector):
        a = item.select_one(sel.chapter_link_selector)
        if a and a.get("href") and a["href"] not in seen:
            seen.add(a["href"])  # type: ignore
            out.append({
                "title": a.get("title") or a.get_text(strip=True),
                "url": a["href"],
            })
    return out


async def ai_parse_category_fallback(page_url: str, html: str) -> tuple[list[dict[str, Any]], str | None]:
    prof = await get_or_create_profile_for_category(page_url, html)
    if not prof or not prof.category:
        return [], None
    return _apply_category_selectors(html, page_url, prof.category)


async def ai_parse_chapter_list_fallback(page_url: str, html: str) -> list[dict[str, str]]:
    prof = await get_or_create_profile_for_chapter_list(page_url, html)
    if not prof or not prof.chapter_list:
        return []
    return _apply_chapter_list_selectors(html, prof.chapter_list)


async def ai_get_content_selector(page_url: str, html: str) -> str | None:
    prof = await get_or_create_profile_for_content(page_url, html)
    if not prof:
        return None
    return prof.content_selector

# --------------------- Domain Locks ---------------------
_LOCKS: dict[str, asyncio.Lock] = {}

def _lock_key(domain: str, kind: str) -> str:
    return f"{domain}:{kind}"

@asynccontextmanager
async def _domain_lock(domain: str, kind: str):
    key = _lock_key(domain, kind)
    lock = _LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _LOCKS[key] = lock
    await lock.acquire()
    try:
        yield
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass

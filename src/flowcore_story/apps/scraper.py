import asyncio
import atexit
import importlib
import importlib.util
import os
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

try:
    _PLAYWRIGHT_SPEC = importlib.util.find_spec("playwright.async_api")
except (ModuleNotFoundError, ValueError):  # pragma: no cover - optional dependency missing
    _PLAYWRIGHT_SPEC = None
if _PLAYWRIGHT_SPEC:
    from playwright.async_api import Browser, BrowserContext, async_playwright  # type: ignore
else:  # pragma: no cover - fallback when Playwright is unavailable
    async_playwright = None  # type: ignore
    Browser = BrowserContext = None  # type: ignore
try:  # pragma: no cover - optional dependency
    from playwright_stealth import stealth_async  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - plugin not installed
    stealth_async = None  # type: ignore
except Exception:  # pragma: no cover - defensive guard
    stealth_async = None  # type: ignore
from flowcore_story.config.config import (
    GLOBAL_PROXY_PASSWORD,
    GLOBAL_PROXY_USERNAME,
    IMPERSONATE_PROFILE,
    IMPERSONATE_SITES,
    LOADED_PROXIES,
    PLAYWRIGHT_MAX_RETRIES,
    PLAYWRIGHT_REQUEST_TIMEOUT,
    PLAYWRIGHT_WAIT_FOR_NETWORKIDLE,
    SITE_IMPERSONATE_PROFILES,
    USE_PROXY,
    get_random_headers,
)
from flowcore_story.config.proxy_provider import (
    get_proxy_url,
    mark_bad_proxy,
    remove_bad_proxy,
    should_blacklist_proxy,
)
from flowcore_story.utils.anti_bot import is_anti_bot_content
from flowcore_story.utils.async_primitives import LoopBoundLock
from flowcore_story.utils.challenge_harvester_client import (
    ChallengeHarvesterError,
    get_challenge_harvester_client,
)
from flowcore_story.utils.cookie_manager import (
    CookieEntryInfo,
    apply_cookies_to_context,
    derive_tls_profile_from_headers,
    disable_cookie_entry,
    fingerprint_headers,
    get_cookie_header,
    select_cookie,
    set_cookies,
    store_context_cookies,
)
from flowcore_story.utils.crawler_session import CrawlerSession
from flowcore_story.utils.http_client import fetch
from flowcore_story.utils.logger import logger
from flowcore_story.utils.telemetry import record_telemetry

# Advanced Stealth Imports
try:
    from flowcore_story.utils.advanced_stealth import (
        STEALTH_JS_SCRIPTS,
        inject_canvas_noise,
    )
    _ADVANCED_STEALTH_AVAILABLE = True
except ImportError:
    _ADVANCED_STEALTH_AVAILABLE = False
    logger.warning("[Scraper] Advanced stealth module not available")

# Import fingerprinting modules
try:
    from flowcore_story.utils.browser_fingerprint import (
        apply_fingerprint_to_context_options,
        get_playwright_injection_script,
    )
    from flowcore_story.utils.fingerprint_pool import get_fingerprint_for_request
    _FINGERPRINT_AVAILABLE = True
except ImportError:
    _FINGERPRINT_AVAILABLE = False
    logger.warning("[Fingerprint] Advanced fingerprinting modules not available")

browser: Browser | None = None
playwright_obj = None
_init_lock = LoopBoundLock()
_context_pool: dict[str, BrowserContext] = {}
_context_last_used: dict[str, float] = {}
_shutdown_registered = False

_MAX_CONTEXT_POOL_SIZE = 20
_MAX_CONTEXT_IDLE_SECONDS = 60
fallback_stats = {
    "httpx_success": {},
    "fallback_count": {},
}

_BLOCK_STATUS_CODES = {401, 403, 406, 407, 429, 451, 503}


async def _apply_stealth(page, fingerprint_script: str | None = None) -> None:
    """Apply stealth evasions and fingerprint injection when available.

    Args:
        page: Playwright page object
        fingerprint_script: Optional JavaScript to inject for fingerprinting
    """
    # Apply playwright-stealth first
    if stealth_async is not None:
        try:
            await stealth_async(page)
        except Exception:
            logger.debug("[playwright] Failed to apply stealth evasions", exc_info=True)

    # Inject advanced fingerprint script
    if fingerprint_script and _FINGERPRINT_AVAILABLE:
        try:
            await page.add_init_script(fingerprint_script)
            logger.debug("[playwright] Applied advanced fingerprint injection")
        except Exception:
            logger.debug("[playwright] Failed to inject fingerprint script", exc_info=True)


@dataclass
class ScraperResponse:
    """Uniform response object returned by :func:`make_request`."""

    text: str
    status_code: int | None = None
    headers: Any | None = None
    url: Any | None = None
    raw_response: Any | None = None

    def __bool__(self) -> bool:  # pragma: no cover - bool semantics are simple
        return bool(self.text) or self.status_code is not None

    def json(self, *args, **kwargs):  # pragma: no cover - passthrough helper
        if self.raw_response and hasattr(self.raw_response, "json"):
            return self.raw_response.json(*args, **kwargs)
        import json

        return json.loads(self.text)


def _wrap_response(response: Any, text: str | None = None) -> ScraperResponse | None:
    if response is None:
        return None

    resolved_text = text if text is not None else getattr(response, "text", "")
    status_code = getattr(response, "status_code", None)
    headers = getattr(response, "headers", None)
    url = getattr(response, "url", None)

    return ScraperResponse(
        text=resolved_text or "",
        status_code=status_code,
        headers=headers,
        url=url,
        raw_response=response,
    )


async def _retry_httpx_with_cleared_cookie(
    url: str,
    site_key: str,
    *,
    timeout: int,
    headers: Mapping[str, str],
    extra_headers: dict[str, str] | None,
    proxy_url: str | None,
    entry_info: CookieEntryInfo | None,
) -> ScraperResponse | None:
    """Re-run the original HTTP request after Playwright solved a challenge."""

    if not entry_info or not entry_info.entry_id:
        return None

    combined_headers: dict[str, str] = dict(headers)
    if extra_headers:
        combined_headers.update(extra_headers)

    user_agent = combined_headers.get("User-Agent")
    cookie_header = get_cookie_header(
        site_key,
        proxy_url=proxy_url,
        proxy_id=entry_info.proxy_id,
        user_agent=user_agent,
        preferred_entry_id=entry_info.entry_id,
    )
    if not cookie_header:
        return None

    combined_headers["Cookie"] = cookie_header

    verification_resp = await fetch(
        url,
        site_key,
        timeout,
        extra_headers=combined_headers,
        force_proxy_url=proxy_url,
        preferred_entry_id=entry_info.entry_id,
        retry_attempts=1,
    )

    if verification_resp is None:
        return None

    text = getattr(verification_resp, "text", "")
    status_code = getattr(verification_resp, "status_code", None)

    if status_code == 200 and text and not is_anti_bot_content(text):
        logger.info("[fallback] HTTP verification succeeded after Playwright challenge")
        return _wrap_response(verification_resp, text)

    if status_code == 429:
        logger.warning(
            "[fallback] HTTP verification still hit rate-limit (429); disabling cookie entry %s",
            entry_info.entry_id,
        )
        disable_cookie_entry(
            site_key,
            entry_info.entry_id,
            proxy_url=proxy_url,
            proxy_id=entry_info.proxy_id,
        )

    return None


def _should_use_playwright(
    response,
    response_text: str | None = None,
    anti_bot_detected: bool | None = None,
) -> bool:
    if response is None:
        return True

    status = getattr(response, "status_code", None)
    text = response_text if response_text is not None else getattr(response, "text", "")

    if status in _BLOCK_STATUS_CODES:
        return True

    if status == 200:
        if not text:
            return True
        if anti_bot_detected is None:
            anti_bot_detected = is_anti_bot_content(text)
        if anti_bot_detected:
            return True

    return False


async def initialize_scraper(
    site_key, override_headers: dict[str, str] | None = None
) -> None:
    """Khởi tạo browser Playwright một lần và tái sử dụng."""
    global browser, playwright_obj
    if async_playwright is None:
        logger.warning("Playwright not installed; initialize_scraper skipped")
        return

    async with _init_lock:
        headless_mode = os.environ.get("PLAYWRIGHT_HEADLESS", "true").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        use_xvfb = os.environ.get("CHALLENGE_HARVESTER_USE_XVFB", "true").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if not os.environ.get("DISPLAY") and use_xvfb:
            display = os.environ.get("CHALLENGE_HARVESTER_XVFB_DISPLAY", ":99")
            os.environ["DISPLAY"] = display
            logger.info("[playwright] DISPLAY not set; using Xvfb display %s", display)

        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-first-run",
        ]
        max_attempts = 2

        for attempt in range(1, max_attempts + 1):
            logger.info(
                "[playwright] init attempt %d/%d (headless=%s, display=%s, args=%s)",
                attempt,
                max_attempts,
                headless_mode,
                os.environ.get("DISPLAY"),
                launch_args,
            )
            try:
                if playwright_obj is None:
                    logger.info("[playwright] Starting Playwright runtime")
                    playwright_obj = await async_playwright().start()
                    logger.info("[playwright] Playwright runtime started")

                if browser is None:
                    logger.info("[playwright] Launching Chromium browser")
                    browser = await playwright_obj.chromium.launch(
                        headless=headless_mode,
                        args=launch_args,
                    )
                    logger.info(
                        "Playwright browser initialized (connected=%s)",
                        browser.is_connected(),
                    )
                _register_shutdown_hook()
                break
            except Exception as e:
                error_msg = str(e).lower()
                is_connect_error = any(
                    pattern in error_msg
                    for pattern in (
                        "failed to connect to browser",
                        "browser has been closed",
                        "browser has disconnected",
                        "connection closed",
                        "target closed",
                    )
                )
                logger.error(
                    "[playwright] Init failed on attempt %d/%d (connect_error=%s): %s",
                    attempt,
                    max_attempts,
                    is_connect_error,
                    e,
                )
                if is_connect_error and attempt < max_attempts:
                    if browser:
                        try:
                            await browser.close()
                        except Exception:
                            pass
                        browser = None
                    if playwright_obj:
                        try:
                            await playwright_obj.stop()
                        except Exception:
                            pass
                        playwright_obj = None
                    await asyncio.sleep(1)
                    continue

                logger.error(f"Lỗi khi khởi tạo Playwright: {e}")
                browser = None
                break

def _context_key(proxy_settings, site_key: str | None) -> str:
    return f"{site_key or '__default__'}::{proxy_settings}"


async def get_context(
    proxy_settings,
    headers,
    site_key: str | None = None,
    proxy_url: str | None = None
) -> BrowserContext:
    if async_playwright is None:
        raise RuntimeError("Playwright support is not available")

    key = _context_key(proxy_settings, site_key)
    ctx = _context_pool.get(key)
    if ctx:
        _context_last_used[key] = asyncio.get_event_loop().time()
        return ctx
    assert browser, "Browser not initialized"

    # Build context options with fingerprinting
    context_options = {
        "user_agent": headers.get("User-Agent"),
        "proxy": proxy_settings,
        # Enhanced Stealth Options
        "locale": "vi-VN",
        "timezone_id": "Asia/Ho_Chi_Minh",
        "permissions": ["geolocation", "notifications"],
        "geolocation": {
            "latitude": 10.7769 + random.uniform(-0.05, 0.05),  # Ho Chi Minh City
            "longitude": 106.7009 + random.uniform(-0.05, 0.05),
        },
        "device_scale_factor": random.choice([1, 1.25, 1.5, 2]),
        "has_touch": random.choice([True, False]),
        "is_mobile": False,
    }

    # Apply advanced fingerprinting if available
    browser_fp = None
    if _FINGERPRINT_AVAILABLE:
        try:
            browser_fp, _ = get_fingerprint_for_request(
                proxy_url=proxy_url,
                user_agent=headers.get("User-Agent"),
                site_key=site_key,
            )
            context_options = apply_fingerprint_to_context_options(browser_fp, context_options)
            logger.debug(
                "[playwright] Applied fingerprint %s for context %s",
                browser_fp.fingerprint_id[:8],
                key[:50]
            )
        except Exception as e:
            logger.debug("[playwright] Failed to apply fingerprint to context: %s", e)

    ctx = await browser.new_context(**context_options)
    
    # Apply Advanced Stealth if available
    if _ADVANCED_STEALTH_AVAILABLE:
        try:
            # 1. Inject Stealth JS Patches
            for script in STEALTH_JS_SCRIPTS:
                await ctx.add_init_script(script)

            # 2. Inject Canvas/WebGL Noise
            await inject_canvas_noise(ctx, fingerprint=browser_fp.browser_fingerprint)
            
            logger.debug("[playwright] Applied Advanced Stealth patches to context %s", key[:50])
        except Exception as e:
            logger.warning("[playwright] Failed to apply Advanced Stealth patches: %s", e)

    _context_pool[key] = ctx
    _context_last_used[key] = asyncio.get_event_loop().time()
    await _evict_contexts_if_needed()
    return ctx

async def release_context(proxy_settings, site_key: str | None = None) -> None:
    key = _context_key(proxy_settings, site_key)
    ctx = _context_pool.pop(key, None)
    _context_last_used.pop(key, None)
    if ctx:
        try:
            await ctx.close()
        except Exception:
            pass

async def close_playwright():
    global browser, playwright_obj
    for ctx in list(_context_pool.values()):
        try:
            await ctx.close()
        except Exception:
            pass
    _context_pool.clear()
    _context_last_used.clear()
    if browser:
        try:
            await browser.close()
        except Exception:
            pass
        browser = None
    if playwright_obj:
        try:
            await playwright_obj.stop()
        except Exception:
            pass
        playwright_obj = None

async def recycle_idle_contexts(max_idle_seconds: float | None = None):
    if max_idle_seconds is None:
        max_idle_seconds = _MAX_CONTEXT_IDLE_SECONDS
    now = asyncio.get_event_loop().time()
    for key, last in list(_context_last_used.items()):
        if now - last > max_idle_seconds:
            ctx = _context_pool.pop(key, None)
            _context_last_used.pop(key, None)
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass
    await _evict_contexts_if_needed()


async def _evict_contexts_if_needed(max_contexts: int | None = None) -> None:
    if max_contexts is None:
        max_contexts = _MAX_CONTEXT_POOL_SIZE
    excess = len(_context_pool) - max_contexts
    if excess <= 0:
        return
    for key, _ in sorted(
        _context_last_used.items(), key=lambda item: item[1]
    ):
        if excess <= 0:
            break
        ctx = _context_pool.pop(key, None)
        _context_last_used.pop(key, None)
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        excess -= 1


async def _make_request_playwright(
    url,
    site_key,
    timeout: int = 30,
    max_retries: int = 5,
    wait_for_selector: str | None = None,
    extra_headers: dict[str, str] | None = None,
    impersonate: str | None = None,
):
    """Load trang bằng Playwright."""
    global browser
    headers = await get_random_headers(site_key)
    if extra_headers:
        headers.update(extra_headers)

    max_retries = max(1, min(max_retries, PLAYWRIGHT_MAX_RETRIES))
    page_timeout = min(timeout, PLAYWRIGHT_REQUEST_TIMEOUT) if timeout else PLAYWRIGHT_REQUEST_TIMEOUT
    from flowcore_story.utils.site_config import load_site_config
    site_config = load_site_config(site_key)
    skip_proxy = bool(site_config.get("skip_proxy"))
    use_proxy = USE_PROXY and not skip_proxy

    harvester = get_challenge_harvester_client()
    if harvester.is_enabled:
        # Use curl_cffi impersonate mode for configured sites (faster, no browser crash)
        # FIX 2025-12-10: Use site-specific profile if available
        # tangthuvien needs safari15_5 (chrome profiles get TLS errors with rotating proxy)
        site_key_lower = site_key.lower()
        if impersonate:
            impersonate_mode = impersonate
        elif site_key_lower in IMPERSONATE_SITES:
            # Check for site-specific profile first, then fall back to default
            impersonate_mode = SITE_IMPERSONATE_PROFILES.get(site_key_lower, IMPERSONATE_PROFILE)
        else:
            impersonate_mode = None

        if impersonate_mode:
            logger.debug(f"[{site_key}] Using impersonate mode: {impersonate_mode}")

        # FIX 2025-12-19: Get proxy URL and pass it to challenge harvester
        # This ensures curl_cffi uses proxy to bypass Cloudflare IP blocks
        harvester_proxy_url = None
        if use_proxy:
            harvester_proxy_url = get_proxy_url(
                GLOBAL_PROXY_USERNAME,
                GLOBAL_PROXY_PASSWORD,
                site_key=site_key,
            )
            if harvester_proxy_url:
                logger.debug(f"[{site_key}] Passing proxy to challenge harvester: {harvester_proxy_url[:30]}...")
            else:
                logger.warning(f"[{site_key}] get_proxy_url() returned None - challenge harvester will receive no proxy!")

        try:
            clearance = await harvester.request_clearance(
                url,
                site_key,
                headers=dict(headers),
                wait_for_selector=wait_for_selector,
                extra_headers=extra_headers,
                referer=(extra_headers or {}).get("Referer"),
                impersonate=impersonate_mode,
                proxy=harvester_proxy_url,  # FIX: Pass proxy to harvester
            )
        except ChallengeHarvesterError as exc:
            logger.warning(
                "[challenge-harvester] Failed to obtain clearance for %s: %s",
                url,
                exc,
            )
        else:
            if clearance:
                metadata = {"source": "challenge_harvester"}
                if clearance.metadata:
                    metadata.update(clearance.metadata)
                if clearance.cf_turnstile_token:
                    metadata["cf_turnstile_token"] = clearance.cf_turnstile_token

                if clearance.user_agent:
                    headers["User-Agent"] = clearance.user_agent

                if clearance.cookies:
                    fingerprint = fingerprint_headers(headers)
                    tls_profile = derive_tls_profile_from_headers(headers)
                    set_cookies(
                        site_key,
                        clearance.cookies,
                        headers=headers,
                        fingerprint=fingerprint,
                        tls_profile=tls_profile,
                    )

                if clearance.has_body and not is_anti_bot_content(clearance.body or ""):
                    return ScraperResponse(
                        text=clearance.body or "",
                        status_code=clearance.status,
                        headers=clearance.headers,
                        raw_response=metadata,
                    )

                if clearance.cookies:
                    refreshed_headers = dict(extra_headers or {})
                    cookie_header = get_cookie_header(site_key)
                    if cookie_header:
                        refreshed_headers["Cookie"] = cookie_header
                    refreshed = await fetch(
                        url,
                        site_key,
                        timeout,
                        extra_headers=refreshed_headers or None,
                    )
                    if refreshed is not None:
                        text = getattr(refreshed, "text", "")
                        if text and not is_anti_bot_content(text):
                            wrapped = _wrap_response(refreshed, text)
                            if wrapped is not None:
                                wrapped.raw_response = metadata
                            return wrapped

                headers = await get_random_headers(site_key)
                if extra_headers:
                    headers.update(extra_headers)

    last_exception = None

    if async_playwright is None:
        logger.warning("Playwright not installed; cannot fetch %s", url)
        return None

    for attempt in range(1, max_retries + 1):
        proxy_url = None
        proxy_settings = None
        try:
            if not browser:
                await initialize_scraper(site_key)
                if not browser:
                    raise RuntimeError("Playwright browser not initialized")

            if use_proxy:
                proxy_url = get_proxy_url(
                    GLOBAL_PROXY_USERNAME,
                    GLOBAL_PROXY_PASSWORD,
                    site_key=site_key,
                )
                if proxy_url:
                    from urllib.parse import urlparse
                    p = urlparse(proxy_url)
                    proxy_settings = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
                    if p.username:
                        proxy_settings["username"] = p.username
                    if p.password:
                        proxy_settings["password"] = p.password

            fingerprint = fingerprint_headers(headers)
            tls_profile = derive_tls_profile_from_headers(headers)
            selection = select_cookie(
                site_key,
                proxy_url=proxy_url if use_proxy else None,
                fingerprint=fingerprint,
                user_agent=headers.get("User-Agent"),
                tls_profile=tls_profile,
            )
            if selection and selection.user_agent:
                headers["User-Agent"] = selection.user_agent

            context = await get_context(
                proxy_settings,
                headers,
                site_key=site_key,
                proxy_url=proxy_url if use_proxy else None
            )
            await apply_cookies_to_context(
                context,
                site_key,
                url=url,
                proxy_url=proxy_url if use_proxy else None,
                headers=headers,
                selection=selection,
            )
            page = await context.new_page()

            # Get fingerprint injection script if available
            fingerprint_script = None
            if _FINGERPRINT_AVAILABLE:
                try:
                    browser_fp, _ = get_fingerprint_for_request(
                        proxy_url=proxy_url if USE_PROXY else None,
                        user_agent=headers.get("User-Agent"),
                        site_key=site_key,
                    )
                    fingerprint_script = get_playwright_injection_script(browser_fp)
                except Exception as e:
                    logger.debug("[playwright] Failed to get fingerprint script: %s", e)

            await _apply_stealth(page, fingerprint_script=fingerprint_script)
            logger.debug(f"[make_request] {attempt}/{max_retries} -> {url}")
            content = None
            try:
                await page.goto(url, timeout=page_timeout * 1000)
                
                # Check for Cloudflare/Anti-bot and wait for it to resolve
                cf_check_attempts = 0
                while cf_check_attempts < 5: # Try up to 5 times (max 25s wait)
                    current_title = await page.title()
                    current_content = await page.content()
                    
                    if "Just a moment" in current_title or "Attention Required" in current_title or "Please turn JavaScript on" in current_content:
                        logger.warning(f"[{site_key}] Cloudflare/Anti-bot detected for {url}. Attempt {cf_check_attempts + 1}. Waiting...")
                        await page.wait_for_timeout(random.uniform(3000, 5000)) # Wait 3-5 seconds
                        cf_check_attempts += 1
                    else:
                        break # Challenge resolved
                
                if cf_check_attempts >= 5:
                    logger.warning(f"[{site_key}] Cloudflare/Anti-bot persisted after multiple waits for {url}. This request might fail.")
                    # Optionally, trigger proxy blacklisting or context release here if persistent.
                    if USE_PROXY and proxy_settings and proxy_url:
                        await mark_bad_proxy(proxy_url, site_key=site_key)
                    await asyncio.sleep(random.uniform(1.2, 2.0))
                    if proxy_settings is not None:
                        await release_context(proxy_settings, site_key=site_key)
                    continue # Try next retry attempt with a new context/proxy if available

                try:
                    if PLAYWRIGHT_WAIT_FOR_NETWORKIDLE:
                        await page.wait_for_load_state("networkidle", timeout=page_timeout * 1000)
                except Exception:
                    pass
                if wait_for_selector:
                    try:
                        await page.wait_for_selector(wait_for_selector, timeout=page_timeout * 1000)
                    except Exception:
                        pass
                max_challenge_checks = 2
                for _ in range(max_challenge_checks):
                    content = await page.content()
                    if not is_anti_bot_content(content):
                        break
                    logger.warning("[playwright] Anti-bot detected, waiting for challenge to resolve")
                    await page.wait_for_timeout(2000)
                else:
                    logger.warning("[playwright] Anti-bot persisted after waits")
                    if USE_PROXY and proxy_settings and proxy_url:
                        await mark_bad_proxy(proxy_url, site_key=site_key)
                    await asyncio.sleep(random.uniform(1.2, 2.0))
                    if proxy_settings is not None:
                        await release_context(proxy_settings, site_key=site_key)
                    continue
            finally:
                try:
                    await page.close()
                except Exception:
                    pass
                await recycle_idle_contexts()

            entry_info = await store_context_cookies(
                context,
                site_key,
                proxy_url=proxy_url if use_proxy else None,
                headers=headers,
                fingerprint=fingerprint_headers(headers),
            )
            if content and not is_anti_bot_content(content):
                verification = await _retry_httpx_with_cleared_cookie(
                    url,
                    site_key,
                    timeout=timeout,
                    headers=headers,
                    extra_headers=extra_headers,
                    proxy_url=proxy_url if use_proxy else None,
                    entry_info=entry_info,
                )
                if verification is not None:
                    return verification
            return content
        except Exception as ex:
            last_exception = ex
            logger.warning(f"[make_request] Lỗi lần {attempt} khi truy cập {url}: {ex}")
            if (
                USE_PROXY
                and proxy_settings
                and proxy_url
                and should_blacklist_proxy(proxy_url, LOADED_PROXIES)
            ):
                remove_bad_proxy(proxy_url)
        await asyncio.sleep(random.uniform(0.6, 1.2))

    logger.error(f"[playwright] Đã thử {max_retries} lần nhưng thất bại: {last_exception}")
    return None


async def _make_request_playwright_post(
    url: str,
    site_key: str,
    timeout: int = 30,
    max_retries: int = 5,
    data: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
):
    global browser
    headers = await get_random_headers(site_key)
    if extra_headers:
        headers.update(extra_headers)

    payload = {str(k): str(v) for k, v in (data or {}).items()}
    last_exception: Exception | None = None
    from flowcore_story.utils.site_config import load_site_config
    site_config = load_site_config(site_key)
    skip_proxy = bool(site_config.get("skip_proxy"))
    use_proxy = USE_PROXY and not skip_proxy

    if async_playwright is None:
        logger.warning("Playwright not installed; cannot POST %s", url)
        return None

    max_retries = max(1, min(max_retries, PLAYWRIGHT_MAX_RETRIES))
    page_timeout = min(timeout, PLAYWRIGHT_REQUEST_TIMEOUT) if timeout else PLAYWRIGHT_REQUEST_TIMEOUT

    for attempt in range(1, max_retries + 1):
        proxy_url = None
        proxy_settings = None
        try:
            if not browser:
                await initialize_scraper(site_key)
                if not browser:
                    raise RuntimeError("Playwright browser not initialized")

            if use_proxy:
                proxy_url = get_proxy_url(
                    GLOBAL_PROXY_USERNAME,
                    GLOBAL_PROXY_PASSWORD,
                    site_key=site_key,
                )
                if proxy_url:
                    from urllib.parse import urlparse

                    p = urlparse(proxy_url)
                    proxy_settings = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
                    if p.username:
                        proxy_settings["username"] = p.username
                    if p.password:
                        proxy_settings["password"] = p.password

            fingerprint = fingerprint_headers(headers)
            tls_profile = derive_tls_profile_from_headers(headers)
            selection = select_cookie(
                site_key,
                proxy_url=proxy_url if use_proxy else None,
                fingerprint=fingerprint,
                user_agent=headers.get("User-Agent"),
                tls_profile=tls_profile,
            )
            if selection and selection.user_agent:
                headers["User-Agent"] = selection.user_agent

            context = await get_context(proxy_settings, headers, site_key=site_key)
            await apply_cookies_to_context(
                context,
                site_key,
                url=url,
                proxy_url=proxy_url if use_proxy else None,
                headers=headers,
                selection=selection,
            )

            referer = (extra_headers or {}).get("Referer")
            if referer:
                page = await context.new_page()
                await _apply_stealth(page)
                try:
                    await page.goto(referer, timeout=page_timeout * 1000)
                    try:
                        if PLAYWRIGHT_WAIT_FOR_NETWORKIDLE:
                            await page.wait_for_load_state("networkidle", timeout=page_timeout * 1000)
                    except Exception:
                        pass
                finally:
                    try:
                        await page.close()
                    except Exception:
                        pass

            request_headers = dict(headers)
            request_headers.setdefault("X-Requested-With", "XMLHttpRequest")
            request_headers.setdefault("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

            response = await context.request.post(
                url,
                data=payload,
                headers=request_headers,
                timeout=page_timeout * 1000,
            )
            text = await response.text()
            status = response.status
            await store_context_cookies(
                context,
                site_key,
                proxy_url=proxy_url if use_proxy else None,
                headers=headers,
                fingerprint=fingerprint_headers(headers),
            )

            if status == 200 and text and not is_anti_bot_content(text):
                return text

            logger.warning(
                f"[playwright][POST] Potential anti-bot or bad status for {url} (status: {status})"
            )
            if use_proxy and proxy_settings and proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                await mark_bad_proxy(proxy_url, site_key=site_key)
                await release_context(proxy_settings, site_key=site_key)
            await asyncio.sleep(random.uniform(0.8, 1.6))
        except Exception as ex:
            last_exception = ex
            logger.warning(f"[make_request][POST] Lỗi lần {attempt} khi truy cập {url}: {ex}")
            if use_proxy and proxy_settings and proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                remove_bad_proxy(proxy_url)
                await release_context(proxy_settings, site_key=site_key)
            await asyncio.sleep(random.uniform(0.8, 1.6))
        finally:
            await recycle_idle_contexts()

    if last_exception:
        logger.error(f"[make_request][POST] Playwright exhausted retries for {url}: {last_exception}")

    return None


async def refresh_site_cookie(
    site_key: str,
    url: str,
    *,
    wait_for_selector: str | None = None,
    close_after: bool = False,
) -> str | None:
    """Fetch ``url`` via Playwright to refresh and persist cookies for ``site_key``."""

    await initialize_scraper(site_key)
    _ = await _make_request_playwright(
        url,
        site_key,
        wait_for_selector=wait_for_selector,
    )
    if close_after:
        await close_playwright()
    return get_cookie_header(site_key)


def _register_shutdown_hook() -> None:
    global _shutdown_registered
    if _shutdown_registered:
        return
    if async_playwright is None:
        return
    atexit.register(_close_playwright_sync)
    _shutdown_registered = True


def _close_playwright_sync() -> None:
    if not _context_pool and browser is None and playwright_obj is None:
        return
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        try:
            fut = asyncio.run_coroutine_threadsafe(close_playwright(), loop)
        except RuntimeError:
            return
        try:
            fut.result(timeout=10)
        except Exception:
            pass
    else:
        try:
            asyncio.run(close_playwright())
        except RuntimeError:
            new_loop = asyncio.new_event_loop()
            try:
                new_loop.run_until_complete(close_playwright())
            finally:
                new_loop.close()


async def make_request(
    url: str,
    site_key: str,
    timeout: int = 30,
    max_retries: int = 5,
    wait_for_selector: str | None = None,
    method: str = 'GET',
    data: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
    impersonate: str | None = None,
    crawler_session: CrawlerSession | None = None,
):
    """Try httpx first then fallback to Playwright when blocked.

    Returns:
        Optional[ScraperResponse]: ``None`` when every attempt failed, otherwise a
        response object with unified ``text`` and ``status_code`` attributes
        regardless of whether the data came from httpx or Playwright.
    """
    fallback_stats["httpx_success"].setdefault(site_key, 0)
    fallback_stats["fallback_count"].setdefault(site_key, 0)
    
    start_time = asyncio.get_event_loop().time()
    final_status = None
    final_success = False
    engine_used = "httpx"
    response_size = 0
    error_type = None
    
    # Helper to record telemetry on exit
    async def _log_telemetry():
        duration = asyncio.get_event_loop().time() - start_time
        
        # Try to extract proxy/ua info if possible, or pass None
        # We rely on what was configured globally or passed in
        proxy = None
        ua = (extra_headers or {}).get("User-Agent")
        if crawler_session:
            ua = crawler_session.user_agent
            proxy = crawler_session.proxy_url
            
        await record_telemetry(
            site_key=site_key,
            url=url,
            method=method,
            status_code=final_status,
            duration=duration,
            proxy_url=proxy,
            user_agent=ua,
            success=final_success,
            engine=engine_used,
            error_type=error_type,
            response_size=response_size
        )

    if method.upper() == 'POST':
        resp = await fetch(
            url,
            site_key,
            timeout,
            method=method,
            data=data,
            extra_headers=extra_headers,
            impersonate_profile=impersonate,
        )
        if resp is not None:
            text = getattr(resp, "text", "")
            status_code = getattr(resp, "status_code", None)
            response_size = len(text)
            final_status = status_code
            
            anti_bot_detected = is_anti_bot_content(text) if text else False
            if status_code == 404:
                logger.warning(
                    f"[{site_key}] Received HTTP 404 for {url}; skipping Playwright fallback."
                )
                final_success = True # 404 is a "successful" fetch in networking terms
                await _log_telemetry()
                return _wrap_response(resp, text)
            if status_code == 200 and text and not anti_bot_detected:
                fallback_stats["httpx_success"][site_key] += 1
                final_success = True
                await _log_telemetry()
                return _wrap_response(resp, text)
            
            if anti_bot_detected:
                error_type = "anti_bot"
            elif status_code != 200:
                error_type = f"http_{status_code}"
                
            if not _should_use_playwright(resp, text, anti_bot_detected=anti_bot_detected):
                final_success = True # Accepted despite weirdness
                await _log_telemetry()
                return _wrap_response(resp, text)
        else:
            error_type = "no_response"
            logger.info(f"[{site_key}] HTTP client returned no response for {url}; considering Playwright fallback.")

        fallback_stats["fallback_count"][site_key] += 1
        engine_used = "playwright" # or nodriver via internal fallback
        
        text = await _make_request_playwright_post(
            url,
            site_key,
            timeout=timeout,
            max_retries=max_retries,
            data=data or {},
            extra_headers=extra_headers or {},
        )
        if text is None:
            logger.error(f"[{site_key}] POST request to {url} failed with httpx and Playwright fallback.")
            final_success = False
            await _log_telemetry()
            return None

        final_success = True
        final_status = 200 # Assumed
        response_size = len(text)
        await _log_telemetry()
        return ScraperResponse(text=text or "")

    resp = await fetch(
        url,
        site_key,
        timeout,
        extra_headers=extra_headers,
        impersonate_profile=impersonate,
    )
    if resp is not None:
        text = getattr(resp, "text", "")
        status_code = getattr(resp, "status_code", None)
        response_size = len(text)
        final_status = status_code
        
        anti_bot_detected = is_anti_bot_content(text) if text else False
        if status_code == 200 and text and not anti_bot_detected:
            fallback_stats["httpx_success"][site_key] += 1
            final_success = True
            await _log_telemetry()
            return _wrap_response(resp, text)
        if status_code == 404:
            logger.warning(f"[{site_key}] Received HTTP 404 for {url}; skipping Playwright fallback.")
            final_success = True
            await _log_telemetry()
            return _wrap_response(resp, text)
            
        if anti_bot_detected:
            error_type = "anti_bot"
        elif status_code != 200:
            error_type = f"http_{status_code}"
            
        if not _should_use_playwright(resp, text, anti_bot_detected=anti_bot_detected):
            final_success = True
            await _log_telemetry()
            return _wrap_response(resp, text)
    else:
        error_type = "no_response"
        logger.debug(f"[{site_key}] HTTP client returned no response for {url}; evaluating Playwright fallback.")

    fallback_stats["fallback_count"][site_key] += 1
    engine_used = "playwright"
    logger.info("[request] Fallback to Playwright due to block or anti-bot detection")
    playwright_response = await _make_request_playwright(
        url,
        site_key,
        timeout,
        max_retries,
        wait_for_selector=wait_for_selector,
        extra_headers=extra_headers,
        impersonate=impersonate,
    )
    
    if playwright_response is None:
        final_success = False
        await _log_telemetry()
        return None

    final_success = True
    
    if isinstance(playwright_response, ScraperResponse):
        final_status = playwright_response.status_code
        response_size = len(playwright_response.text)
        
        # Check if Harvester/Nodriver/Curl was actually used inside
        if hasattr(playwright_response, "raw_response") and isinstance(playwright_response.raw_response, dict):
             meta = playwright_response.raw_response
             if meta.get("engine"):
                 engine_used = meta.get("engine")
                 
        await _log_telemetry()
        return playwright_response

    if isinstance(playwright_response, str):
        response_size = len(playwright_response)
        final_status = 200
        await _log_telemetry()
        return ScraperResponse(text=playwright_response or "")

    text_value = getattr(playwright_response, "text", None)
    if text_value is not None:
        response_size = len(text_value)
        final_status = 200
        await _log_telemetry()
        return ScraperResponse(text=text_value or "")

    await _log_telemetry()
    return ScraperResponse(text=str(playwright_response))
async def discover_madara_chapter_ranges_via_playwright(
    story_url: str,
    site_key: str,
    *,
    ajax_endpoint: str = "/wp-admin/admin-ajax.php",
    timeout: int = 30,
    max_clicks: int | None = None,
) -> list[dict]:
    """Open ``story_url`` and capture real AJAX chapter list ranges.

    This uses Playwright only to observe requests that the page itself makes
    when expanding chapter groups. It returns a list of (from, to) pairs that
    were actually posted to the server so the caller can re-fetch via HTTP.
    """

    if async_playwright is None:
        return []

    global browser
    headers = await get_random_headers(site_key)

    collected: set[tuple[str, str]] = set()

    def _try_parse_range(post_data: str | None) -> tuple[str, str] | None:
        if not post_data:
            return None
        try:
            from urllib.parse import parse_qs

            q = parse_qs(post_data)
            action = (q.get("action") or [None])[0]
            if action != "load_chapter_list_from_to":
                return None
            f_raw = (q.get("from") or [None])[0]
            t_raw = (q.get("to") or [None])[0]
            if f_raw is None or t_raw is None:
                return None
            # Keep raw strings exactly as posted (e.g., '1634m')
            return str(f_raw), str(t_raw)
        except Exception:
            return None
        return None

    try:
        if not browser:
            await initialize_scraper(site_key)
            if not browser:
                return []

        fingerprint = fingerprint_headers(headers)
        tls_profile = derive_tls_profile_from_headers(headers)
        selection = select_cookie(
            site_key,
            proxy_url=None,
            fingerprint=fingerprint,
            user_agent=headers.get("User-Agent"),
            tls_profile=tls_profile,
        )
        if selection and selection.user_agent:
            headers["User-Agent"] = selection.user_agent

        context = await get_context(None, headers, site_key=site_key)
        await apply_cookies_to_context(
            context,
            site_key,
            url=story_url,
            proxy_url=None,
            headers=headers,
            selection=selection,
        )
        page = await context.new_page()
        await _apply_stealth(page)

        target_sub = ajax_endpoint if ajax_endpoint.startswith("/") else "/" + ajax_endpoint

        async def _on_request(req):
            try:
                if req.method.lower() != "post":
                    return
                url = req.url or ""
                if target_sub not in url:
                    return
                rng = _try_parse_range(req.post_data)
                if rng:
                    collected.add(rng)
            except Exception:
                pass

        page.on("request", _on_request)

        await page.goto(story_url, timeout=timeout * 1000)
        try:
            await page.wait_for_load_state("networkidle", timeout=timeout * 1000)
        except Exception:
            pass

        # Click all chapter group expanders to trigger their AJAX calls
        group_selectors = [
            "ul.version-chap > li.has-child a.has-child",
            "ul.main.version-chap > li.has-child a.has-child",
        ]
        anchors = []
        for sel in group_selectors:
            try:
                anchors = await page.query_selector_all(sel)
            except Exception:
                anchors = []
            if anchors:
                break

        limit = max_clicks if (isinstance(max_clicks, int) and max_clicks > 0) else None
        clicked = 0
        for a in anchors:
            try:
                await a.scroll_into_view_if_needed()
                async with page.expect_load_state("networkidle", timeout=timeout * 1000):
                    await a.click()
            except Exception:
                try:
                    await a.click()
                except Exception:
                    pass
            await page.wait_for_timeout(300)
            clicked += 1
            if limit and clicked >= limit:
                break

        # Small grace period to allow late requests to fire
        await page.wait_for_timeout(800)

        try:
            await page.close()
        except Exception:
            pass
        await store_context_cookies(
            context,
            site_key,
            proxy_url=None,
            headers=headers,
            fingerprint=fingerprint_headers(headers),
        )

        if not collected:
            return []
        # Convert set of tuples into list of dicts preserving order by from/to digits when possible
        import re
        def _key(item: tuple[str, str]) -> tuple[int, int]:
            def _to_int(s: str) -> int:
                m = re.search(r"(\d+)", s)
                return int(m.group(1)) if m else 0
            return (_to_int(item[0]), _to_int(item[1]))
        ordered = sorted(collected, key=_key)
        return [{"from": f, "to": t} for f, t in ordered]
    except Exception:
        return []

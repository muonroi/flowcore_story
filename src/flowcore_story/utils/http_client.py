import asyncio
import atexit
import importlib
import importlib.util
import logging
import random
from http.cookies import SimpleCookie
from typing import Any

from flowcore_story.utils.httpx_compat import USING_HTTPX_IMPERSONATE, create_async_client, httpx

try:
    _PLAYWRIGHT_SPEC = importlib.util.find_spec("playwright.async_api")
except (ModuleNotFoundError, ValueError):  # pragma: no cover
    _PLAYWRIGHT_SPEC = None
if _PLAYWRIGHT_SPEC:
    from playwright.async_api import async_playwright  # type: ignore
else:  # pragma: no cover
    async_playwright = None  # type: ignore
try:  # pragma: no cover - optional dependency
    from playwright_stealth import stealth_async  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    stealth_async = None  # type: ignore
except Exception:  # pragma: no cover - defensive guard
    stealth_async = None  # type: ignore

from flowcore_story.config.config import (
    DELAY_ON_RETRY,
    GLOBAL_PROXY_PASSWORD,
    GLOBAL_PROXY_USERNAME,
    LOADED_PROXIES,
    PLAYWRIGHT_SKIP_PROXY,
    PROXIES_FILE,
    RATE_LIMIT_BACKOFF_MAX_SECONDS,
    RATE_LIMIT_BACKOFF_SECONDS,
    REQUEST_DELAY,
    RETRY_ATTEMPTS,
    TIMEOUT_REQUEST,
    USE_PROXY,
    get_random_headers,
)
from flowcore_story.config.proxy_provider import (
    get_proxy_url,
    mark_bad_proxy,
    record_proxy_result,
    reload_proxies_if_changed,
    should_blacklist_proxy,
)
from flowcore_story.utils.anti_bot import is_anti_bot_content
from flowcore_story.utils.cookie_manager import (
    derive_tls_profile_from_user_agent,
    fingerprint_headers,
    record_cookie_feedback,
    select_cookie,
    set_cookies,
    store_context_cookies,
)
from flowcore_story.utils.logger import logger
from flowcore_story.utils.rate_limit_monitor import rate_limit_monitor
from flowcore_story.utils.structured_logging import log_http_event

# PHASE 2 OPTIMIZATION: Adaptive rate limiting
try:
    from flowcore_story.utils.adaptive_rate_limiter import (
        adaptive_rate_limiter,
        get_site_limiter,
    )
    _ADAPTIVE_RATE_LIMITER_AVAILABLE = True
except ImportError:
    _ADAPTIVE_RATE_LIMITER_AVAILABLE = False
    logger.debug("[HTTP] Adaptive rate limiter not available")

# PHASE 1 OPTIMIZATION: Response caching
try:
    from flowcore_story.utils.response_cache import (
        cache_response,
        get_cached_response,
    )
    _RESPONSE_CACHE_AVAILABLE = True
except ImportError:
    _RESPONSE_CACHE_AVAILABLE = False
    logger.debug("[HTTP] Response cache not available")

# Advanced Stealth Import
try:
    from flowcore_story.utils.advanced_stealth import (
        AdvancedStealthContext,
        inject_canvas_noise,
    )
    _ADVANCED_STEALTH_AVAILABLE = True
except ImportError:
    _ADVANCED_STEALTH_AVAILABLE = False
    logger.warning("[HTTP] Advanced stealth module not available")

# ZenRows has been removed - no longer needed

# Import new profile management system
_PROFILE_MANAGER_AVAILABLE = False
_PROFILE_MANAGER_ENABLED = False
try:
    from flowcore_story.config import config as app_config
    from flowcore_story.utils.profile_manager import (
        get_profile_for_request,
        record_profile_result,
    )
    from flowcore_story.utils.tls_fingerprint import get_impersonate_profile_for_httpx

    _PROFILE_MANAGER_AVAILABLE = True
    _PROFILE_MANAGER_ENABLED = getattr(app_config, "ENABLE_FINGERPRINT_POOL", False)
    if _PROFILE_MANAGER_ENABLED:
        logger.info("[Profile] Profile management ENABLED")
        logger.info(f"[Profile] Pool size per proxy: {getattr(app_config, 'FINGERPRINT_POOL_SIZE_PER_PROXY', 5)}")
        logger.info(f"[Profile] Rotation interval: {getattr(app_config, 'FINGERPRINT_ROTATION_INTERVAL', 3600)}s")
    else:
        logger.info("[Profile] Profile management DISABLED (set ENABLE_FINGERPRINT_POOL=true to enable)")
except ImportError as e:
    _PROFILE_MANAGER_AVAILABLE = False
    _PROFILE_MANAGER_ENABLED = False
    logger.debug(f"[HTTP] Profile manager not available: {e}")

# Import TLS fingerprinting for better impersonation (legacy fallback)
_TLS_FINGERPRINT_AVAILABLE = False
try:
    from flowcore_story.utils.fingerprint_pool import get_fingerprint_for_request
    from flowcore_story.utils.tls_fingerprint import (
        get_impersonate_profile_for_httpx,
    )
    _TLS_FINGERPRINT_AVAILABLE = True
except ImportError as e:
    _TLS_FINGERPRINT_AVAILABLE = False
    logger.debug(f"[HTTP] TLS fingerprinting modules not available: {e}")


# ==============================================================================
# FIX: Event loop cleanup for curl_cffi
# Issue: RuntimeError: Event loop is closed
# Solution: Proper cleanup of async resources before event loop closes
# Date: 2025-12-07
# ==============================================================================
import weakref
from contextlib import suppress

# Track active HTTP clients for cleanup
_active_http_clients: weakref.WeakSet = weakref.WeakSet()
_cleanup_registered = False


def _register_cleanup_handler():
    """Register atexit cleanup handler (call once)"""
    global _cleanup_registered
    if _cleanup_registered:
        return
    _cleanup_registered = True

    def _sync_cleanup():
        """Cleanup all active HTTP clients before exit"""
        try:
            # Get event loop
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                try:
                    loop = asyncio.get_event_loop()
                except Exception:
                    return  # No loop available

            # Skip if loop is closed
            if loop.is_closed():
                return

            # Cleanup all tracked clients
            clients = list(_active_http_clients)
            if clients:
                logger.debug(f"[Cleanup] Closing {len(clients)} HTTP clients before exit")

                # Create cleanup tasks
                async def _async_cleanup():
                    for client in clients:
                        with suppress(Exception):
                            if hasattr(client, 'aclose'):
                                await client.aclose()
                            # Force cleanup any pending curl sessions
                            if hasattr(client, '_transport') and hasattr(client._transport, 'aclose'):
                                with suppress(Exception):
                                    await client._transport.aclose()

                # Run cleanup
                with suppress(Exception):
                    if not loop.is_closed():
                        loop.run_until_complete(_async_cleanup())

        except Exception as e:
            # Don't raise during cleanup, just log
            logger.debug(f"[Cleanup] Error during HTTP client cleanup: {e}")

    atexit.register(_sync_cleanup)


# Register cleanup on module import
_register_cleanup_handler()
# ==============================================================================


def _determine_cache_type(url: str) -> str | None:
    """
    PHASE 1 OPTIMIZATION: Determine cache type from URL.

    Returns:
        "genre" for genre/category pages
        "story" for story detail pages
        "chapter" for chapter pages
        None for no caching
    """
    url_lower = url.lower()

    # Genre/category pages (change infrequently, long TTL)
    if any(pattern in url_lower for pattern in ['/danh-sach/', '/the-loai/', '/category/', '/genre/']):
        return "genre"

    # Chapter pages (update periodically, medium TTL)
    if any(pattern in url_lower for pattern in ['/chuong-', '/chapter-', '/chap-']):
        return "chapter"

    # Story detail pages (relatively static, medium TTL)
    if '/truyen/' in url_lower or '/story/' in url_lower:
        return "story"

    # Don't cache other types (homepage, search, etc.)
    return None


def _extract_cf_metadata(resp: httpx.Response) -> dict[str, str | None]:
    """Extract Cloudflare-related telemetry headers from ``resp``."""

    headers = getattr(resp, "headers", None)
    if not headers:
        return {}

    cf_cache = None
    cf_ray = None
    cf_server = None
    try:
        cf_cache = headers.get("cf-cache-status")
    except Exception:  # pragma: no cover - defensive guard
        cf_cache = None
    try:
        cf_ray = headers.get("cf-ray")
    except Exception:  # pragma: no cover - defensive guard
        cf_ray = None
    try:
        server_header = headers.get("server")
    except Exception:  # pragma: no cover - defensive guard
        server_header = None
    if server_header and "cloudflare" in server_header.lower():
        cf_server = server_header

    return {
        "cf_cache_status": cf_cache,
        "cf_ray": cf_ray,
        "cf_server": cf_server,
    }


async def _apply_playwright_stealth(page) -> None:
    """Apply stealth anti-detection patches when available."""
    if stealth_async is None:
        return
    try:
        await stealth_async(page)
    except Exception:
        logger.debug("[Playwright] Failed to apply stealth evasions", exc_info=True)


_client_cache: dict[tuple[str, str, str], httpx.AsyncClient] = {}
_shutdown_registered = False


def _parse_set_cookie_headers(resp: httpx.Response) -> list[dict[str, Any]]:
    """Convert Set-Cookie headers from ``resp`` into cookie dictionaries."""

    headers = getattr(resp, "headers", None)
    if headers is None:
        return []

    header_values: list[str] = []
    try:
        if hasattr(headers, "get_list"):
            header_values = [value for value in headers.get_list("set-cookie") if value]
        else:
            header_value = headers.get("set-cookie")
            if header_value:
                header_values = [header_value]
    except Exception:
        return []

    cookies: list[dict[str, Any]] = []
    for header_value in header_values:
        parser = SimpleCookie()
        try:
            parser.load(header_value)
        except Exception:
            continue
        for morsel in parser.values():
            if not morsel.key:
                continue
            cookie_dict: dict[str, Any] = {
                "name": morsel.key,
                "value": morsel.value,
            }
            if morsel["path"]:
                cookie_dict["path"] = morsel["path"]
            if morsel["domain"]:
                cookie_dict["domain"] = morsel["domain"]
            if morsel["expires"]:
                cookie_dict["expires"] = morsel["expires"]
            if "secure" in morsel and morsel["secure"]:
                cookie_dict["secure"] = True
            if "httponly" in morsel and morsel["httponly"]:
                cookie_dict["httpOnly"] = True
            if "samesite" in morsel and morsel["samesite"]:
                cookie_dict["sameSite"] = morsel["samesite"]
            cookies.append(cookie_dict)
    return cookies


def _persist_response_cookies(
    site_key: str,
    resp: httpx.Response,
    *,
    headers: dict[str, str],
    proxy_url: str | None,
    fingerprint: str | None,
    tls_profile: str | None,
) -> None:
    """Persist cookies from the response when the server sets new values."""

    if not site_key:
        return

    try:
        cookies_payload = _parse_set_cookie_headers(resp)
    except Exception:
        cookies_payload = []

    if not cookies_payload:
        return

    try:
        set_cookies(
            site_key,
            cookies_payload,
            proxy_url=proxy_url,
            headers=headers,
            fingerprint=fingerprint,
            tls_profile=tls_profile,
        )
    except Exception:
        logger.debug("[httpx] Failed to persist cookies from response", exc_info=True)


def _client_cache_key(
    site_key: str | None,
    proxy_url: str | None,
    impersonate_profile: str | None = None,
) -> tuple[str, str, str]:
    """Return the cache key for a given site/proxy/impersonation combination."""

    normalized_site = site_key or "__default__"
    normalized_proxy = proxy_url or "__no_proxy__"
    normalized_impersonate = impersonate_profile or "__default__"
    return normalized_site, normalized_proxy, normalized_impersonate


async def _aclose_clients() -> None:
    global _client_cache
    clients = list(_client_cache.values())
    _client_cache.clear()
    for client in clients:
        try:
            await client.aclose()
        except Exception:
            pass


def _close_clients_sync() -> None:
    if not _client_cache:
        return

    async def _cleanup() -> None:
        await _aclose_clients()

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        fut = asyncio.run_coroutine_threadsafe(_cleanup(), loop)
        try:
            fut.result(timeout=10)
        except Exception:
            pass
    else:
        try:
            asyncio.run(_cleanup())
        except RuntimeError:
            new_loop = asyncio.new_event_loop()
            try:
                new_loop.run_until_complete(_cleanup())
            finally:
                new_loop.close()


def _register_shutdown_hook() -> None:
    global _shutdown_registered
    if _shutdown_registered:
        return
    atexit.register(_close_clients_sync)
    _shutdown_registered = True


def _get_async_client(
    site_key: str,
    proxy_url: str | None,
    impersonate_profile: str | None = None,
) -> httpx.AsyncClient:
    _register_shutdown_hook()
    global _client_cache

    effective_proxy = proxy_url if USE_PROXY and proxy_url else None
    key = _client_cache_key(
        site_key,
        effective_proxy,
        impersonate_profile if USING_HTTPX_IMPERSONATE else None,
    )
    client = _client_cache.get(key)
    if client is None:
        # POOL TIMEOUT FIX: Add explicit timeout and limits config
        # Issue: PoolTimeout errors when concurrent requests > pool capacity
        # Solution: Increase pool timeout and connection limits

        # Connection pool limits
        # FIX 2025-12-11: Further optimize for rotating proxy to prevent PoolTimeout
        # Problem: tangthuvien and other sites getting PoolTimeout because:
        # 1. Too many concurrent connections exhaust the proxy server
        # 2. Pool timeout too short when many requests compete for connections
        # Solution: Drastically reduce connections and increase timeouts
        # POOL TIMEOUT FIX V3 (2026-01-09)
        # Root cause analysis from Codex + Gemini:
        # 1. Challenge harvester overload causes connection queue backup
        # 2. Too many concurrent requests competing for pool
        # 3. Proxy rotation (120s) + TLS errors cause connection failures
        # Solution: Configurable limits via environment variables
        import os
        http_max_connections = int(os.environ.get("HTTP_MAX_CONNECTIONS", "30"))
        http_pool_timeout = float(os.environ.get("HTTP_POOL_TIMEOUT", "60"))

        limits = httpx.Limits(
            max_connections=http_max_connections,  # Configurable, default 30
            max_keepalive_connections=http_max_connections // 2,
            keepalive_expiry=30.0,
        )

        timeout_config = httpx.Timeout(
            connect=15.0,              # Fail fast on bad proxies
            read=TIMEOUT_REQUEST,      # Default: 30s
            write=15.0,
            pool=http_pool_timeout,    # Configurable, default 60s
        )

        client_kwargs: dict[str, Any] = {
            "follow_redirects": True,
            "limits": limits,           # ADD: Connection pool limits
            "timeout": timeout_config,  # ADD: Timeout configuration
        }
        if effective_proxy:
            # httpx AsyncClient uses 'proxy' (singular), not 'proxies' (plural)
            client_kwargs["proxy"] = effective_proxy
        if USING_HTTPX_IMPERSONATE and impersonate_profile:
            client_kwargs["impersonate"] = impersonate_profile
        client = create_async_client(**client_kwargs)
        _client_cache[key] = client
        # Track client for cleanup (FIX: Event loop cleanup)
        _active_http_clients.add(client)
    return client


async def _invalidate_client(site_key: str, proxy_url: str | None) -> None:
    if not proxy_url:
        return

    normalized_site, normalized_proxy, _ = _client_cache_key(
        site_key,
        proxy_url if USE_PROXY else None,
    )

    keys_to_remove = [
        key for key in list(_client_cache.keys()) if key[0] == normalized_site and key[1] == normalized_proxy
    ]

    for key in keys_to_remove:
        client = _client_cache.pop(key, None)
        if client is None:
            continue
        # Remove from tracking (FIX: Event loop cleanup)
        _active_http_clients.discard(client)
        try:
            await client.aclose()
        except Exception:
            pass


async def fetch(
    url: str,
    site_key: str,
    timeout: int | None = None,
    *,
    method: str = 'GET',
    data: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
    retry_attempts: int | None = None,
    force_proxy_url: str | None = None,
    preferred_entry_id: str | None = None,
    use_cache: bool = True,  # PHASE 1 OPTIMIZATION: Enable caching by default
    impersonate_profile: str | None = None,
) -> httpx.Response | None:
    timeout = timeout or TIMEOUT_REQUEST

    # PHASE 1 OPTIMIZATION: Check cache before making request
    cache_type = _determine_cache_type(url) if use_cache and _RESPONSE_CACHE_AVAILABLE else None
    if cache_type and method.upper() == 'GET':
        try:
            cached_resp = await get_cached_response(url, cache_type=cache_type, site_key=site_key)
            if cached_resp:
                logger.info(
                    "[HTTP] Cache HIT for %s (type: %s, saved challenge harvester call)",
                    url[:100],
                    cache_type
                )
                return cached_resp
        except Exception as e:
            logger.debug("[HTTP] Cache check failed: %s", e)

    await reload_proxies_if_changed(PROXIES_FILE)

    # [DB OPTIMIZATION] Check site health before making requests
    from flowcore_story.storage.db_tracking import site_health
    health_info = await site_health.get_site_health(site_key)
    if health_info:
        failure_rate = health_info.get("failure_rate", 0.0)
        challenge_count_1h = health_info.get("challenge_count_1h", 0)

        # Apply backoff if site is unhealthy
        if failure_rate > 0.5:  # More than 50% failure rate
            backoff_seconds = min(60, int(failure_rate * 120))  # Max 60s backoff
            logger.warning(
                f"[DB_HEALTH] Site {site_key} has high failure rate ({failure_rate:.1%}). "
                f"Applying {backoff_seconds}s backoff."
            )
            await asyncio.sleep(backoff_seconds)

        # Apply backoff if too many recent challenges
        if challenge_count_1h > 20:
            challenge_backoff = min(30, challenge_count_1h - 20)
            logger.warning(
                f"[DB_HEALTH] Site {site_key} has {challenge_count_1h} challenges in last hour. "
                f"Applying {challenge_backoff}s backoff."
            )
            await asyncio.sleep(challenge_backoff)

    used_cookie_ids: set[str] = set()

    # Load site config for debug logging context
    from flowcore_story.utils.site_config import load_site_config
    from urllib.parse import urlparse
    
    site_config = load_site_config(site_key)
    skip_proxy = bool(site_config.get("skip_proxy"))
    debug_site_key = site_key
    if not debug_site_key:
        try:
            debug_site_key = urlparse(url).netloc
        except Exception:
            pass

    attempt_limit = retry_attempts or RETRY_ATTEMPTS
    consecutive_rate_limits = 0
    consecutive_failures = 0
    attempted_proxies: set[str] = set()  # Track proxies already tried

    for attempt in range(1, attempt_limit + 1):
        headers = await get_random_headers(site_key)
        if extra_headers:
            headers.update(extra_headers)

        retry_immediately = False
        rate_limit_backoff = 0.0

        # PHASE 2 OPTIMIZATION: Apply adaptive rate limiting delay before request
        if _ADAPTIVE_RATE_LIMITER_AVAILABLE and site_key:
            try:
                site_limiter = get_site_limiter(site_key)
                if site_limiter.is_circuit_open(site_key):
                    logger.warning(
                        f"[HTTP] Circuit breaker OPEN for {site_key}. "
                        f"Waiting for recovery..."
                    )
                adaptive_delay = site_limiter.get_delay(site_key)
                if adaptive_delay > 0.5:  # Only apply if significant delay
                    logger.info(
                        f"[HTTP] Applying adaptive delay of {adaptive_delay:.2f}s for {site_key}"
                    )
                    await asyncio.sleep(adaptive_delay)
            except Exception as e:
                logger.debug(f"[HTTP] Adaptive rate limiter error: {e}")

        if USE_PROXY and skip_proxy and attempt == 1:
            logger.info(f"[HTTP] Skipping proxy for {site_key} (site config skip_proxy=true)")

        use_proxy = USE_PROXY and not skip_proxy
        proxy_url = None
        if use_proxy:
            if force_proxy_url:
                proxy_url = force_proxy_url
            else:
                proxy_url = get_proxy_url(
                    GLOBAL_PROXY_USERNAME,
                    GLOBAL_PROXY_PASSWORD,
                    site_key=site_key,
                    exclude_proxies=attempted_proxies,  # Exclude already tried proxies
                )

        effective_proxy = proxy_url if use_proxy else None

        # Track this proxy to avoid reusing in next attempts
        if effective_proxy:
            attempted_proxies.add(effective_proxy)

        # Use new profile manager if available
        complete_profile = None
        if _PROFILE_MANAGER_AVAILABLE and _PROFILE_MANAGER_ENABLED:
            try:
                complete_profile = get_profile_for_request(
                    site_key=site_key,
                    proxy_url=effective_proxy,
                    extra_headers=headers,
                    exclude_cookie_ids=used_cookie_ids,
                )

                # Use profile's user agent and cookies
                headers["User-Agent"] = complete_profile.get_user_agent()
                cookie_header = complete_profile.get_cookie_header()
                if cookie_header and "Cookie" not in headers:
                    headers["Cookie"] = cookie_header

                # Get TLS profile from fingerprint profile
                tls_profile = get_impersonate_profile_for_httpx(
                    complete_profile.fingerprint_profile.tls_fingerprint
                )
                if impersonate_profile:
                    tls_profile = impersonate_profile

                # Apply header randomization from profile
                headers = complete_profile.fingerprint_profile.apply_headers(headers)

                selected_entry_id = complete_profile.cookie_selection.entry_id if complete_profile.cookie_selection else None

                logger.debug(
                    "[HTTP] Using profile manager - profile_id: %s, has_cookies: %s",
                    complete_profile.fingerprint_profile.profile_id,
                    bool(complete_profile.cookie_selection)
                )
            except Exception as e:
                logger.warning(
                    "[HTTP] Failed to use profile manager: %s, falling back to legacy mode",
                    e
                )
                complete_profile = None

        # Legacy fallback if profile manager not available or failed
        if complete_profile is None:
            initial_user_agent = headers.get("User-Agent")
            fingerprint = fingerprint_headers(headers)

            # Use advanced TLS fingerprinting if available
            tls_profile = None
            if _TLS_FINGERPRINT_AVAILABLE:
                try:
                    _, tls_fp = get_fingerprint_for_request(
                        proxy_url=effective_proxy,
                        user_agent=initial_user_agent,
                        site_key=site_key,
                    )
                    tls_profile = get_impersonate_profile_for_httpx(tls_fp)
                except Exception as e:
                    logger.debug(f"[HTTP] Failed to get TLS fingerprint: {e}")
                    tls_profile = derive_tls_profile_from_user_agent(initial_user_agent)
            else:
                tls_profile = derive_tls_profile_from_user_agent(initial_user_agent)
            if impersonate_profile:
                tls_profile = impersonate_profile

            selection = select_cookie(
                site_key,
                proxy_url=effective_proxy,
                fingerprint=fingerprint,
                user_agent=initial_user_agent,
                tls_profile=tls_profile,
                exclude_ids=used_cookie_ids,
                preferred_entry_id=preferred_entry_id,
            )
            selected_entry_id = selection.entry_id if selection else None

            if selection:
                if selection.user_agent:
                    headers["User-Agent"] = selection.user_agent
                if selection.header and "Cookie" not in headers:
                    headers["Cookie"] = selection.header

        log_http_event(
            logging.DEBUG,
            logger=logger,
            phase="attempt",
            site_key=site_key,
            url=url,
            proxy=effective_proxy,
            cookie_id=selected_entry_id,
            attempt=attempt,
            message="fetch_start",
        )

        # Prepare request metadata based on which mode we're using
        if complete_profile:
            request_fingerprint = fingerprint_headers(headers)
            request_tls_profile = tls_profile
        else:
            # Legacy mode - handle selection
            request_fingerprint = fingerprint
            request_tls_profile = tls_profile

            if selection:
                request_fingerprint = selection.fingerprint or fingerprint_headers(headers)

                # Derive TLS profile from selected user agent
                if _TLS_FINGERPRINT_AVAILABLE and selection.user_agent:
                    try:
                        _, tls_fp = get_fingerprint_for_request(
                            proxy_url=effective_proxy,
                            user_agent=selection.user_agent,
                            site_key=site_key,
                        )
                        request_tls_profile = get_impersonate_profile_for_httpx(tls_fp)
                    except Exception:
                        request_tls_profile = selection.tls_profile or derive_tls_profile_from_user_agent(
                            headers.get("User-Agent")
                        )
                else:
                    request_tls_profile = selection.tls_profile or derive_tls_profile_from_user_agent(
                        headers.get("User-Agent")
                    )
            else:
                request_fingerprint = fingerprint_headers(headers)
                if not request_tls_profile:
                    request_tls_profile = derive_tls_profile_from_user_agent(headers.get("User-Agent"))

        if impersonate_profile:
            request_tls_profile = impersonate_profile

        try:
            request_kwargs = {
                'headers': headers,
                'timeout': httpx.Timeout(timeout) if timeout else None,
                'follow_redirects': True,
            }
            client = _get_async_client(
                site_key,
                effective_proxy,
                request_tls_profile if USING_HTTPX_IMPERSONATE else None,
            )
            if method.upper() == 'POST':
                resp = await client.post(url, data=data, **request_kwargs)
            else:
                resp = await client.get(url, **request_kwargs)

            await asyncio.sleep(random.uniform(0, REQUEST_DELAY))
            rate_limit_monitor.record_response(site_key, resp.status_code)
            is_success = resp.status_code == 200 and resp.text and not is_anti_bot_content(resp.text)
            limited = resp.status_code == 429

            # PHASE 2 OPTIMIZATION: Record result with adaptive rate limiter
            if _ADAPTIVE_RATE_LIMITER_AVAILABLE and site_key:
                try:
                    site_limiter = get_site_limiter(site_key)
                    if limited:
                        site_limiter.on_rate_limit(site_key)
                    elif is_success:
                        site_limiter.on_success(site_key, resp.status_code)
                except Exception as e:
                    logger.debug(f"[HTTP] Adaptive rate limiter record error: {e}")

            # PHASE 2 OPTIMIZATION: Record proxy result for smart selection
            if effective_proxy and site_key:
                try:
                    record_proxy_result(
                        site_key,
                        effective_proxy,
                        success=is_success,
                        rate_limited=limited
                    )
                except Exception as e:
                    logger.debug(f"[HTTP] Proxy result recording error: {e}")
            if limited:
                consecutive_rate_limits += 1
                if RATE_LIMIT_BACKOFF_SECONDS > 0:
                    exponent = max(consecutive_rate_limits - 1, 0)
                    computed_backoff = RATE_LIMIT_BACKOFF_SECONDS * (2 ** exponent)
                    jitter = random.uniform(0.75, 1.25)
                    rate_limit_backoff = min(
                        RATE_LIMIT_BACKOFF_MAX_SECONDS,
                        max(RATE_LIMIT_BACKOFF_SECONDS, computed_backoff) * jitter,
                    )
                retry_immediately = False
                if rate_limit_backoff > 0:
                    logger.debug(
                        "[httpx] Applying %.2fs backoff after rate limit for %s (attempt=%d, consecutive=%d)",
                        rate_limit_backoff,
                        site_key,
                        attempt,
                        consecutive_rate_limits,
                    )
            else:
                consecutive_rate_limits = 0
            anti_bot_detected = bool(resp.status_code == 200 and not is_success)
            log_level = (
                logging.INFO
                if is_success
                else logging.ERROR
                if resp.status_code and resp.status_code >= 500
                else logging.WARNING
            )
            cf_meta = _extract_cf_metadata(resp)
            extra_fields: dict[str, Any] = {"rate_limited": limited}
            if rate_limit_backoff > 0:
                extra_fields["rate_limit_backoff_seconds"] = round(rate_limit_backoff, 3)
                extra_fields["consecutive_rate_limits"] = consecutive_rate_limits
            log_http_event(
                log_level,
                logger=logger,
                phase="response",
                site_key=site_key,
                url=url,
                status_code=resp.status_code,
                proxy=effective_proxy,
                cookie_id=selected_entry_id,
                attempt=attempt,
                retry_immediately=retry_immediately,
                is_success=is_success,
                anti_bot=anti_bot_detected,
                cf_cache_status=cf_meta.get("cf_cache_status"),
                cf_ray=cf_meta.get("cf_ray"),
                cf_server=cf_meta.get("cf_server"),
                message="fetch_response",
                extra=extra_fields,
            )
            try:
                _persist_response_cookies(
                    site_key,
                    resp,
                    headers=headers,
                    proxy_url=effective_proxy,
                    fingerprint=request_fingerprint,
                    tls_profile=request_tls_profile,
                )
            except Exception:
                logger.debug("[httpx] Unable to refresh cookies from response", exc_info=True)

            # Record results with profile manager or legacy mode
            if complete_profile:
                try:
                    record_profile_result(
                        profile=complete_profile,
                        status_code=resp.status_code,
                        success=is_success,
                    )
                except Exception as e:
                    logger.debug(f"[HTTP] Failed to record profile result: {e}")
            elif selected_entry_id:
                record_cookie_feedback(
                    site_key,
                    selected_entry_id,
                    status_code=resp.status_code,
                    successful=is_success,
                    proxy_url=effective_proxy,
                )

            if is_success:
                # PHASE 1 OPTIMIZATION: Cache successful response
                if cache_type and _RESPONSE_CACHE_AVAILABLE:
                    try:
                        await cache_response(
                            url,
                            resp,
                            cache_type=cache_type,
                            site_key=site_key
                        )
                        logger.debug(
                            "[HTTP] Cached response for %s (type: %s)",
                            url[:100],
                            cache_type
                        )
                    except Exception as e:
                        logger.debug("[HTTP] Failed to cache response: %s", e)

                return resp
            if resp.status_code == 404:
                logger.warning(f"[httpx] Not found (404) for {url}")
                return resp
            if selected_entry_id:
                used_cookie_ids.add(selected_entry_id)
                logger.debug(
                    "[httpx] Rotating cookie entry %s after blocked response for %s",
                    selected_entry_id,
                    url,
                )
            if resp.status_code == 429:
                body_preview = (resp.text or "").strip()
                if body_preview:
                    logger.error(
                        "[httpx] Rate limited (429) for %s; response preview: %s",
                        url,
                        body_preview[:1000],
                    )
                else:
                    logger.error("[httpx] Rate limited (429) for %s with empty body", url)
                if selected_entry_id:
                    used_cookie_ids.add(selected_entry_id)
            
            # Re-check anti-bot with debug key to trigger detailed logging
            if is_anti_bot_content(resp.text, debug_site_key=debug_site_key):
                logger.warning(f"[httpx] Potential anti-bot detected for {url} (status: {resp.status_code}). See warning logs for reason.")
            else:
                logger.warning(f"[httpx] Bad status code for {url} (status: {resp.status_code})")
                
            logger.debug(f"[httpx] Anti-bot response body: {resp.text[:500]}")

            # Track failure
            consecutive_failures += 1

            if proxy_url and should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                await _invalidate_client(site_key, proxy_url)
                await mark_bad_proxy(proxy_url, site_key=site_key)
        except Exception as e:
            logger.warning(f"[httpx] request error ({type(e).__name__}: {e}) for {url}")

            # PHASE 2 OPTIMIZATION: Record error with adaptive rate limiter
            if _ADAPTIVE_RATE_LIMITER_AVAILABLE and site_key:
                try:
                    site_limiter = get_site_limiter(site_key)
                    site_limiter.on_error(site_key, e)
                except Exception as limiter_error:
                    logger.debug(f"[HTTP] Adaptive rate limiter error recording failed: {limiter_error}")

            log_http_event(
                logging.WARNING,
                logger=logger,
                phase="exception",
                site_key=site_key,
                url=url,
                proxy=effective_proxy,
                cookie_id=selected_entry_id,
                attempt=attempt,
                message="fetch_exception",
                extra={"error": str(e)},
            )
            # ADDED: Record proxy failure for health tracking
            if effective_proxy and site_key:
                record_proxy_result(site_key, effective_proxy, success=False, rate_limited=False)
            if selected_entry_id:
                used_cookie_ids.add(selected_entry_id)
                logger.debug(
                    "[httpx] Rotating cookie entry %s after request error for %s",
                    selected_entry_id,
                    url,
                )

            # Track failure
            consecutive_failures += 1

            tls_error = False
            if proxy_url:
                error_text = str(e).lower()
                tls_error = any(
                    token in error_text
                    for token in (
                        "tls connect error",
                        "wrong_version_number",
                        "packet length too long",
                    )
                )

            if proxy_url:
                if tls_error:
                    logger.warning(
                        "[httpx] Detected TLS handshake failure via proxy %s; forcing blacklist.",
                        proxy_url,
                    )
                    await _invalidate_client(site_key, proxy_url)
                    await mark_bad_proxy(proxy_url, site_key=site_key, permanent=True)
                elif should_blacklist_proxy(proxy_url, LOADED_PROXIES):
                    await _invalidate_client(site_key, proxy_url)
                    await mark_bad_proxy(proxy_url, site_key=site_key)
            consecutive_rate_limits = 0
        if not retry_immediately and attempt < attempt_limit:
            delay = DELAY_ON_RETRY * attempt
            if rate_limit_backoff > 0:
                delay = max(delay, rate_limit_backoff)
            if delay > 0:
                await asyncio.sleep(delay)

    # All attempts failed
    if consecutive_failures > 0:
        logger.error(
            "[HTTP] All %d attempts failed for %s",
            attempt_limit,
            site_key,
        )

    return None


async def fetch_with_playwright(url: str) -> str | None:
    """Fetches page content using Playwright routed through the project's proxy config with advanced stealth."""
    if async_playwright is None:
        logger.warning("Playwright not installed; fetch_with_playwright skipped for %s", url)
        return None

    headers = await get_random_headers('xtruyen')
    user_agent = headers.get('User-Agent')

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        proxy_config = None
        # Skip proxy for Playwright if PLAYWRIGHT_SKIP_PROXY is enabled
        if not PLAYWRIGHT_SKIP_PROXY:
            proxy_url_str = get_proxy_url(GLOBAL_PROXY_USERNAME, GLOBAL_PROXY_PASSWORD)
            if USE_PROXY and proxy_url_str:
                try:
                    # Parse proxy string like 'http://user:pass@host:port'
                    parsed_url = httpx.URL(proxy_url_str)
                    proxy_config = {
                        "server": f"{parsed_url.scheme}://{parsed_url.host}:{parsed_url.port}",
                        "username": parsed_url.username,
                        "password": parsed_url.password
                    }
                    logger.info(f"[Playwright] Using proxy: {proxy_config['server']}")
                except Exception as e:
                    logger.error(f"[Playwright] Failed to parse proxy URL: {e}")
        else:
            logger.debug("[Playwright] Skipping proxy (PLAYWRIGHT_SKIP_PROXY=true)")

        try:
            async with async_playwright() as p:
                # Modern anti-detection flags
                browser_args = [
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-site-isolation-trials",
                    "--disable-setuid-sandbox",
                    "--disable-infobars",
                    "--window-position=0,0",
                    "--ignore-certificate-errors",
                    "--ignore-certificate-errors-spki-list",
                ]
                
                browser = await p.chromium.launch(
                    headless=True,
                    args=browser_args,
                    proxy=proxy_config
                )
                
                # Use AdvancedStealthContext for multi-layered protection
                if _ADVANCED_STEALTH_AVAILABLE:
                    from flowcore_story.utils.advanced_stealth import AdvancedStealthContext, stealth_navigate
                    
                    # Randomize viewport
                    viewport_w = random.choice([1280, 1366, 1440, 1536, 1920])
                    viewport_h = random.choice([720, 768, 864, 900, 1080])
                    
                    async with AdvancedStealthContext(
                        browser, 
                        user_agent=user_agent,
                        viewport={"width": viewport_w, "height": viewport_h},
                        proxy=proxy_config
                    ) as context:
                        page = await context.new_page()
                        
                        # Apply playwright-stealth as second layer
                        await _apply_playwright_stealth(page)
                        
                        # Use stealth_navigate for human-like behavior
                        logger.info(f"[Playwright] Navigating with advanced stealth to {url}")
                        await stealth_navigate(page, url, simulate_behavior=True, reading_time=random.uniform(1.0, 3.0))
                        
                        content = await page.content()
                else:
                    # Fallback to basic stealth if module missing
                    context = await browser.new_context(user_agent=user_agent)
                    page = await context.new_page()
                    await _apply_playwright_stealth(page)
                    await page.goto(url, timeout=60000, wait_until='domcontentloaded')
                    
                    try:
                        await page.wait_for_selector('body', timeout=45000)
                    except Exception:
                        logger.warning(f"[Playwright] Timed out waiting for selector on {url}.")
                    
                    content = await page.content()

                await browser.close()

                if "Just a moment..." not in content and not is_anti_bot_content(content, debug_site_key="xtruyen"):
                    logger.info(f"[Playwright] Successfully fetched content for {url}")
                    return content
                else:
                    logger.warning(f"[Playwright] Anti-bot content detected on {url} on attempt {attempt}")

        except Exception as e:
            logger.error(f"[Playwright] Error fetching {url} on attempt {attempt}: {e}")

        await asyncio.sleep(DELAY_ON_RETRY * attempt)

    logger.error(f"[Playwright] Failed to fetch {url} after {RETRY_ATTEMPTS} attempts.")
    return None


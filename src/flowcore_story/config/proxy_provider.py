import asyncio
import os
import random
import time
from collections import defaultdict, deque
from urllib.parse import urlparse
from enum import Enum, auto
import aiofiles

from flowcore_story.config.config import (
    BANNED_PROXIES_LOG,
    LOADED_PROXIES,
    PROXIES_FILE,
    PROXY_API_URL,
    USE_PROXY,
)
from flowcore_story.utils.httpx_compat import create_async_client
from flowcore_story.utils.logger import logger

class ProxyType(Enum):
    DATACENTER = auto()
    ISP = auto()        # Static residential
    RESIDENTIAL = auto() # Rotating residential
    MOBILE = auto()     # 4G/5G
    UNKNOWN = auto()

def classify_proxy_type(proxy_url: str) -> ProxyType:
    """Classify proxy type based on URL keywords or port patterns."""
    url_lower = proxy_url.lower()

    # Check for keyword-based classification first
    if any(kw in url_lower for kw in ["mobile", "4g", "5g", "lte"]):
        return ProxyType.MOBILE
    elif any(kw in url_lower for kw in ["residential", "resi", "home", "rotating"]):
        return ProxyType.RESIDENTIAL
    elif any(kw in url_lower for kw in ["isp", "static", "dedi"]):
        return ProxyType.ISP

    # IP-based classification for known proxy providers
    # 103.183.119.19 = 4G Mobile Rotating proxy
    if "103.183.119.19" in proxy_url:
        return ProxyType.MOBILE

    # 103.117.198.60 = Datacenter Static (mProxy)
    if "103.117.198.60" in proxy_url:
        return ProxyType.DATACENTER

    # Default assumption if not marked
    return ProxyType.DATACENTER

# Config: Site preferences for proxy types
SITE_PROXY_TYPE_PREFERENCE = {
    "tangthuvien": [ProxyType.ISP, ProxyType.MOBILE, ProxyType.RESIDENTIAL], # TTV chặn DC gắt
    "truyencom": [ProxyType.RESIDENTIAL, ProxyType.MOBILE],
    "xtruyen": [ProxyType.MOBILE, ProxyType.RESIDENTIAL, ProxyType.ISP],
    "truyenfull": [ProxyType.DATACENTER, ProxyType.ISP], # Dễ tính
}

MPROXY_RESET_URL = "https://mproxy.vn/capi/osddzY0KFwft0316--KM3Jq2UcCaRDDviC9d8IH4tPQ/key/DC-o3rVQf99O6g/resetIp"
MPROXY_RESET_ENABLED = os.getenv("MPROXY_RESET_ENABLED", "true").lower() in ("1", "true", "yes", "on")
_MPROXY_RESET_BACKOFF_SECONDS = int(os.getenv("MPROXY_RESET_BACKOFF_SECONDS", "600"))
_MPROXY_RESET_COOLDOWN_SECONDS = 120
_MPROXY_TARGET_HOST = "103.117.198.60"
_last_mproxy_reset_at = 0.0
_mproxy_consecutive_failures = 0
_mproxy_reset_disabled_until = 0.0
_NO_PROXY_PREF_WARNED: set[str] = set()
_BLACKLIST_DISABLED_LOGGED = False


# ==============================================================================
# PHASE 2 OPTIMIZATION: Smart Proxy Selection
# Track proxy health per site and select best performing proxies
# ==============================================================================
class ProxyHealthTracker:
    """Track proxy health metrics for smart selection."""

    def __init__(self, window_seconds: float = 600.0):
        self._window = window_seconds
        # Structure: {site_key: {proxy_key: ProxyHealth}}
        self._health: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {
            'total': 0,
            'success': 0,
            'rate_limited': 0,
            'last_success': 0.0,
            'consecutive_failures': 0,
            'timestamps': deque(maxlen=100),  # Recent request timestamps
        }))

    def record(self, site_key: str, proxy_key: str, success: bool, rate_limited: bool = False) -> None:
        """Record a request result."""
        if not proxy_key:
            return
        health = self._health[site_key][proxy_key]
        health['total'] += 1
        health['timestamps'].append(time.time())

        if success:
            health['success'] += 1
            health['last_success'] = time.time()
            health['consecutive_failures'] = 0
        else:
            health['consecutive_failures'] += 1

        if rate_limited:
            health['rate_limited'] += 1

    def get_success_rate(self, site_key: str, proxy_key: str) -> float:
        """Get success rate for a proxy on a site."""
        if not proxy_key:
            return 1.0
        health = self._health.get(site_key, {}).get(proxy_key)
        if not health or health['total'] == 0:
            return 1.0  # New proxy, assume good
        return health['success'] / health['total']

    def get_best_proxy(self, site_key: str, available_proxies: list[str]) -> str | None:
        """Select the best proxy for a site based on health metrics."""
        if not available_proxies:
            return None

        # Score each proxy
        scores: list[tuple[float, str]] = []
        now = time.time()

        for proxy in available_proxies:
            proxy_key = _normalize_proxy_key(proxy)
            health = self._health.get(site_key, {}).get(proxy_key)

            if not health or health['total'] < 3:
                # New proxy, give it a chance with neutral score
                scores.append((0.5, proxy))
                continue

            # Calculate score based on multiple factors
            success_rate = health['success'] / max(1, health['total'])
            rate_limit_penalty = health['rate_limited'] / max(1, health['total']) * 0.5

            # Penalize proxies with consecutive failures
            if health['consecutive_failures'] >= 3:
                score = 0.1
            else:
                score = success_rate - rate_limit_penalty

                # Bonus for recent success
                if health['last_success'] > 0 and (now - health['last_success']) < 60:
                    score *= 1.1

            scores.append((max(0, min(1, score)), proxy))

        if not scores:
            return random.choice(available_proxies) if available_proxies else None

        # Sort by score descending
        scores.sort(key=lambda x: x[0], reverse=True)

        # Select from top performers with some randomness
        top_threshold = max(scores[0][0] * 0.8, 0.3)
        top_proxies = [proxy for score, proxy in scores if score >= top_threshold]

        if top_proxies:
            return random.choice(top_proxies[:3])  # Random from top 3

        return scores[0][1]  # Best available

    def cleanup_old_data(self, max_age: float = 3600.0) -> None:
        """Remove old health data."""
        cutoff = time.time() - max_age
        for site_key in list(self._health.keys()):
            site_health = self._health[site_key]
            for proxy_key in list(site_health.keys()):
                health = site_health[proxy_key]
                if health['timestamps']:
                    # Remove if all timestamps are old
                    if max(health['timestamps']) < cutoff:
                        del site_health[proxy_key]
            if not site_health:
                del self._health[site_key]


# Global proxy health tracker
_proxy_health_tracker = ProxyHealthTracker()


def record_proxy_result(site_key: str, proxy_url: str, success: bool, rate_limited: bool = False) -> None:
    """Record a proxy request result for smart selection."""
    if proxy_url:
        _proxy_health_tracker.record(
            site_key,
            _normalize_proxy_key(proxy_url),
            success,
            rate_limited
        )
        if success and _is_mproxy_proxy(proxy_url):
            _reset_mproxy_failures()


def get_smart_proxy(site_key: str, available_proxies: list[str]) -> str | None:
    """Get the best proxy for a site based on health metrics."""
    return _proxy_health_tracker.get_best_proxy(site_key, available_proxies)
# ==============================================================================


# Import PROXY_URL lazily to avoid circular import
def _get_proxy_url():
    from flowcore_story.config import config
    return getattr(config, 'PROXY_URL', None)

proxy_mode = "random"
current_proxy_index: int = 0
bad_proxy_counts: dict[str, int] = {}
COOLDOWN_PROXIES: dict[str, float] = {}
PROXY_COOLDOWN_SECONDS = 60
PROXY_SOFT_COOLDOWN_SECONDS = int(
    os.getenv("PROXY_SOFT_COOLDOWN_SECONDS", str(PROXY_COOLDOWN_SECONDS))
)
PROXY_NOTIFIED = False
MAX_FAIL_RATE = 10
FAILED_PROXY_TIMES: list[float] = []
_last_proxy_mtime = 0
SITE_PROXY_BLACKLIST_TTL = 300
WEAK_PROXY_THRESHOLD = 5
WEAK_PROXY_RETENTION_SECONDS = 1800
SITE_PROXY_BLACKLIST: dict[str, dict[str, float]] = {}
_proxy_fail_history: dict[str, deque[float]] = defaultdict(deque)
_site_block_history: dict[str, deque[float]] = defaultdict(deque)
_site_last_adaptation: dict[str, float] = {}
SITE_BLOCK_HISTORY_WINDOW = 900
SITE_ADAPTATION_COOLDOWN = 120

def _normalize_proxy_key(proxy: str) -> str:
    if not proxy:
        return ""
    candidate = proxy if "://" in proxy else f"http://{proxy}"
    try:
        parsed = urlparse(candidate)
        host = parsed.hostname or candidate
        port = parsed.port
        if host and port:
            return f"{host}:{port}".lower()
        if host:
            return host.lower()
    except Exception:
        pass
    return candidate.lower()


def _extract_proxy_host(proxy: str) -> str:
    if not proxy:
        return ""
    candidate = proxy if "://" in proxy else f"http://{proxy}"
    try:
        parsed = urlparse(candidate)
        if parsed.hostname:
            return parsed.hostname
    except Exception:
        return ""
    host_port = proxy.split("@")[-1]
    return host_port.split(":")[0]


def parse_proxy_url(proxy_str: str) -> tuple[str, str | None]:
    """Parse proxy URL, support http://, https://, socks5://, socks4://
    
    Returns:
        tuple[str, str | None]: (scheme, normalized_url)
        scheme: 'http', 'socks5', 'socks4'
    """
    if not proxy_str:
        return ("http", None)
        
    proxy_str = proxy_str.strip()
    
    if proxy_str.startswith("socks5://"):
        return ("socks5", proxy_str)
    elif proxy_str.startswith("socks4://"):
        return ("socks4", proxy_str)
    elif proxy_str.startswith("http://") or proxy_str.startswith("https://"):
        # Chuẩn hóa về http:// theo khuyến nghị để tránh lỗi TLS handshake một số tool
        if proxy_str.startswith("https://"):
             normalized = "http://" + proxy_str[8:]
             return ("http", normalized)
        return ("http", proxy_str)
    else:
        # Không có scheme mặc định là http
        return ("http", f"http://{proxy_str}")


def _is_mproxy_proxy(proxy: str) -> bool:
    return _extract_proxy_host(proxy) == _MPROXY_TARGET_HOST


def _reset_mproxy_failures() -> None:
    global _mproxy_consecutive_failures
    _mproxy_consecutive_failures = 0


async def trigger_mproxy_reset() -> bool:
    global _last_mproxy_reset_at
    global _mproxy_reset_disabled_until
    if not MPROXY_RESET_ENABLED or not MPROXY_RESET_URL:
        return False
    now = time.time()
    if now < _mproxy_reset_disabled_until:
        return False
    if now - _last_mproxy_reset_at < _MPROXY_RESET_COOLDOWN_SECONDS:
        return False
    _last_mproxy_reset_at = now
    try:
        async with create_async_client(timeout=10) as client:
            resp = await client.get(MPROXY_RESET_URL)
        if resp.status_code >= 400:
            _mproxy_reset_disabled_until = now + _MPROXY_RESET_BACKOFF_SECONDS
            logger.warning(
                "[Proxy] mProxy reset returned status %s; backoff %ss",
                resp.status_code,
                _MPROXY_RESET_BACKOFF_SECONDS,
            )
            return False
        return True
    except Exception as exc:
        _mproxy_reset_disabled_until = now + _MPROXY_RESET_BACKOFF_SECONDS
        logger.warning(
            "[Proxy] mProxy reset request failed; backoff %ss: %s",
            _MPROXY_RESET_BACKOFF_SECONDS,
            exc,
        )
        return False


def _prune_site_blacklist(site_key: str, now: float | None = None) -> set[str]:
    if site_key not in SITE_PROXY_BLACKLIST:
        return set()

    now = now or time.time()
    site_map = SITE_PROXY_BLACKLIST[site_key]
    expired = [proxy for proxy, expiry in site_map.items() if expiry <= now]
    for proxy in expired:
        site_map.pop(proxy, None)

    if not site_map:
        SITE_PROXY_BLACKLIST.pop(site_key, None)
        return set()

    return set(site_map.keys())


def _remove_proxy_by_normalized(normalized_key: str) -> bool:
    removed = False
    for proxy in LOADED_PROXIES[:]:
        if _normalize_proxy_key(proxy) == normalized_key:
            LOADED_PROXIES.remove(proxy)
            COOLDOWN_PROXIES.pop(proxy, None)
            removed = True
    if removed:
        for site_map in SITE_PROXY_BLACKLIST.values():
            site_map.pop(normalized_key, None)
        bad_proxy_counts.pop(normalized_key, None)
        _proxy_fail_history.pop(normalized_key, None)
    return removed


def _record_proxy_failure(normalized_key: str) -> None:
    if not normalized_key:
        return

    now = time.time()
    history = _proxy_fail_history[normalized_key]
    history.append(now)
    cutoff = now - WEAK_PROXY_RETENTION_SECONDS
    while history and history[0] < cutoff:
        history.popleft()


def _apply_proxy_cooldown(proxy: str, until_ts: float) -> None:
    if not proxy:
        return
    normalized = _normalize_proxy_key(proxy)
    COOLDOWN_PROXIES[proxy] = until_ts
    if not normalized:
        return
    for loaded in LOADED_PROXIES:
        if _normalize_proxy_key(loaded) == normalized:
            COOLDOWN_PROXIES[loaded] = until_ts


async def cleanup_weak_proxies(
    retention_seconds: int = WEAK_PROXY_RETENTION_SECONDS,
    threshold: int = WEAK_PROXY_THRESHOLD,
) -> None:
    now = time.time()
    for proxy_key, history in list(_proxy_fail_history.items()):
        while history and history[0] < now - retention_seconds:
            history.popleft()
        if not history:
            _proxy_fail_history.pop(proxy_key, None)
            continue
        if len(history) >= threshold:
            if _remove_proxy_by_normalized(proxy_key):
                logger.warning(
                    "[Proxy] Proxy %s removed after %d failures in the last %d seconds.",
                    proxy_key,
                    len(history),
                    retention_seconds,
                )
            _proxy_fail_history.pop(proxy_key, None)


def _get_available_proxies(site_key: str | None = None) -> list[str]:
    now = time.time()
    for p, t in list(COOLDOWN_PROXIES.items()):
        if t <= now:
            COOLDOWN_PROXIES.pop(p, None)

    proxies = [p for p in LOADED_PROXIES if COOLDOWN_PROXIES.get(p, 0) <= now]

    # DEBUG: Track proxy filtering
    original_count = len(LOADED_PROXIES)
    after_cooldown_count = len(proxies)
    in_cooldown_count = original_count - after_cooldown_count

    if not site_key:
        if not proxies and original_count > 0:
            logger.warning(f"[Proxy] No proxies available: {in_cooldown_count}/{original_count} in cooldown")
        return proxies

    # FILTER BY PROXY TYPE (Priority 2 Task 6)
    preferences = SITE_PROXY_TYPE_PREFERENCE.get(site_key)
    if preferences and proxies:
        preferred_proxies = []
        # First pass: Get strictly matching proxies
        for p in proxies:
            p_type = classify_proxy_type(p)
            if p_type in preferences:
                preferred_proxies.append(p)

        # If we have preferred proxies, use them. Otherwise fallback to all available.
        if preferred_proxies:
            proxies = preferred_proxies
            # Optional: Sort by preference order?
            # Not strictly necessary if we just want "good enough"
        else:
            # DEBUG: No proxies match the preference
            if site_key not in _NO_PROXY_PREF_WARNED:
                logger.warning(
                    "[%s] No proxies match type preference %s, using all %d available",
                    site_key,
                    preferences,
                    len(proxies),
                )
                _NO_PROXY_PREF_WARNED.add(site_key)
            else:
                logger.debug(
                    "[%s] No proxies match type preference %s, using all %d available",
                    site_key,
                    preferences,
                    len(proxies),
                )

    blacklist = _prune_site_blacklist(site_key, now)
    after_type_filter_count = len(proxies)

    if not blacklist:
        if not proxies and original_count > 0:
            logger.warning(
                f"[{site_key}] No proxies available after filtering: "
                f"{original_count} total, {in_cooldown_count} in cooldown, "
                f"{after_type_filter_count} after type filter"
            )
        return proxies

    final_proxies = [p for p in proxies if _normalize_proxy_key(p) not in blacklist]
    blacklisted_count = len(proxies) - len(final_proxies)

    # DEBUG: Log when all proxies are filtered out
    if not final_proxies and original_count > 0:
        logger.warning(
            f"[{site_key}] No proxies available after filtering: "
            f"{original_count} total, {in_cooldown_count} in cooldown, "
            f"{after_type_filter_count} after type filter, {blacklisted_count} blacklisted, "
            f"0 remaining"
        )

    return final_proxies

def should_blacklist_proxy(proxy_url, loaded_proxies):
    """Decide whether a proxy can be blacklisted/removal-safe."""

    proxy_domain = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url

    # Treat pools with a single unique endpoint as non-removable even if file
    # keeps duplicates for weighting, otherwise we end up clearing the list and
    # all requests fall back to direct IP.
    unique_proxy_count = len(
        {_normalize_proxy_key(p) for p in loaded_proxies if p}
    )
    if unique_proxy_count <= 1:
        return False
    if any(key in proxy_domain for key in [
        "proxy-cheap.com"
    ]):
        return False
    return True

async def load_proxies(filename: str | None = None) -> list[str]:
    """Load proxies từ file txt, API, hoặc single PROXY_URL.

    Priority order:
    1. PROXY_URL (single proxy from env) - simplest setup
    2. PROXY_API_URL (dynamic proxy list from API)
    3. PROXIES_FILE (static proxy list from file)
    """
    global LOADED_PROXIES

    if not USE_PROXY:
        # Khi không sử dụng proxy, đảm bảo danh sách rỗng và không log nhầm số lượng proxy.
        if LOADED_PROXIES:
            LOADED_PROXIES.clear()
        logger.info("[Proxy] USE_PROXY=false, bỏ qua việc load proxy.")
        return LOADED_PROXIES

    # Priority 1: Single PROXY_URL from env (simplest setup)
    proxy_url = _get_proxy_url()
    if proxy_url:
        LOADED_PROXIES.clear()
        LOADED_PROXIES.append(proxy_url)
        logger.info("[Proxy] Loaded single proxy from PROXY_URL/PROXY_HOST+PORT")
        return LOADED_PROXIES

    # Priority 2: API-based proxy list
    if PROXY_API_URL:
        try:
            async with create_async_client(timeout=10) as client:
                resp = await client.get(PROXY_API_URL)
            # Đọc data, kiểm tra phải list
            data = resp.json()
            proxies_data = data.get("data")
            if not isinstance(proxies_data, list):
                proxies_data = []
            LOADED_PROXIES.clear()
            LOADED_PROXIES.extend([
                f"http://{item['ip']}:{item['port']}"
                for item in proxies_data
                if isinstance(item, dict) and 'ip' in item and 'port' in item
            ])
            if not LOADED_PROXIES:
                logger.info("[Crawl Notify] Không có proxy nào từ API!")
        except Exception as e:
            print(f"Proxy API load error: {e}")
    else:
        filename = filename or PROXIES_FILE
        try:
            async with aiofiles.open(filename) as f:
                lines = await f.readlines()
            raw_entries = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
            LOADED_PROXIES.clear()
            LOADED_PROXIES.extend(raw_entries)
            if not LOADED_PROXIES:
                asyncio.create_task(send_telegram_notify("[Crawl Notify] Không có proxy nào được load vào hệ thống!", status="warning"))
        except Exception as e:
            print(f"Proxy load error: {e}")
    return LOADED_PROXIES


async def reload_proxies_if_changed(filename: str | None = None) -> None:
    """Reload proxies nếu file đổi (hoặc luôn reload nếu dùng API)"""
    global _last_proxy_mtime

    if not USE_PROXY:
        return

    # Priority 1: Single PROXY_URL - no need to reload from file
    proxy_url = _get_proxy_url()
    if proxy_url:
        # PROXY_URL is static, no need to reload
        return

    # Priority 2: API-based proxy list
    if PROXY_API_URL:
        # Luôn reload mỗi lần gọi
        await load_proxies()
        logger.info("[Proxy] Reloaded proxy list from API")
        return

    # Priority 3: File-based proxy list
    filename = filename or PROXIES_FILE
    try:
        mtime = os.path.getmtime(filename)
    except FileNotFoundError:
        return
    if mtime > _last_proxy_mtime:
        await load_proxies(filename)
        _last_proxy_mtime = mtime
        logger.info("[Proxy] Reloaded proxy list from disk")

async def mark_bad_proxy(proxy: str, site_key: str | None = None, *, permanent: bool = False):
    global FAILED_PROXY_TIMES, _mproxy_consecutive_failures
    global _BLACKLIST_DISABLED_LOGGED

    normalized = _normalize_proxy_key(proxy)
    now = time.time()

    if _is_mproxy_proxy(proxy):
        _mproxy_consecutive_failures += 1
        if _mproxy_consecutive_failures >= 3:
            if await trigger_mproxy_reset():
                logger.warning(
                    "[Proxy] 4G/DC Proxy blocked, triggering IP reset..."
                )
                _reset_mproxy_failures()

    # [CONFIG] Check if blacklist is disabled via env var
    if os.environ.get("DISABLE_PROXY_BLACKLIST", "").lower() == "true":
        if not _BLACKLIST_DISABLED_LOGGED:
            logger.warning("[Proxy] Blacklist DISABLED. Applying soft cooldown for failures.")
            _BLACKLIST_DISABLED_LOGGED = True
        else:
            logger.debug("[Proxy] Blacklist disabled; soft cooldown for %s.", proxy)
        _record_proxy_failure(normalized)
        _apply_proxy_cooldown(proxy, now + PROXY_SOFT_COOLDOWN_SECONDS)
        return

    if site_key:
        SITE_PROXY_BLACKLIST.setdefault(site_key, {})[normalized] = now + SITE_PROXY_BLACKLIST_TTL
    else:
        _apply_proxy_cooldown(proxy, now + PROXY_COOLDOWN_SECONDS)

    _record_proxy_failure(normalized)

    # ALWAYS check proxy count first - never blacklist single/rotating proxy
    if not should_blacklist_proxy(proxy, LOADED_PROXIES):
        logger.warning(
            f"[Proxy] Proxy xoay hoặc pool chỉ có 1 proxy, sẽ không blacklist: {proxy}. Chỉ sleep & retry."
        )
        await asyncio.sleep(10)
        await cleanup_weak_proxies(
            retention_seconds=WEAK_PROXY_RETENTION_SECONDS,
            threshold=WEAK_PROXY_THRESHOLD,
        )
        return

    # Only blacklist if we have multiple proxies available
    bad_proxy_counts.setdefault(normalized, 0)
    bad_proxy_counts[normalized] += 1

    if permanent or (bad_proxy_counts[normalized] >= 3 and LOADED_PROXIES):
        if _remove_proxy_by_normalized(normalized):
            logger.warning(f"Proxy '{proxy}' auto-banned do quá nhiều lỗi.")
            FAILED_PROXY_TIMES.append(time.time())
            FAILED_PROXY_TIMES[:] = [t for t in FAILED_PROXY_TIMES if time.time() - t < 60]
            if len(FAILED_PROXY_TIMES) >= MAX_FAIL_RATE:
                asyncio.create_task(
                    send_telegram_notify(
                        "[Crawl Notify] Cảnh báo: Số lượng proxy bị ban liên tục vượt ngưỡng, hãy kiểm tra lại hệ thống/proxy!",
                        status="error",
                    )
                )
                FAILED_PROXY_TIMES.clear()

    await cleanup_weak_proxies(
        retention_seconds=WEAK_PROXY_RETENTION_SECONDS,
        threshold=WEAK_PROXY_THRESHOLD,
    )

def get_random_proxy_url(
    username: str = None, password: str = None, site_key: str | None = None
) -> str | None:
    available = _get_available_proxies(site_key=site_key)
    if not available:
        return None
    proxy = random.choice(available)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_round_robin_proxy_url(
    username: str = None, password: str = None, site_key: str | None = None
) -> str | None:
    global current_proxy_index
    available = _get_available_proxies(site_key=site_key)
    if not available:
        return None
    proxy = available[current_proxy_index % len(available)]
    current_proxy_index = (current_proxy_index + 1) % len(available)
    if "://" in proxy:
        return proxy
    if username and password:
        return f"http://{username}:{password}@{proxy}"
    return f"http://{proxy}"

def get_proxy_url(
    username: str = None,
    password: str = None,
    site_key: str | None = None,
    exclude_proxies: set[str] | None = None,
) -> str | None:
    if not USE_PROXY:
        return None
    global PROXY_NOTIFIED
    available = _get_available_proxies(site_key=site_key)
    if not available:
        if not PROXY_NOTIFIED:
            PROXY_NOTIFIED = True
        return None
    PROXY_NOTIFIED = False

    # Filter out already attempted proxies
    if exclude_proxies:
        # Normalize exclude_proxies to extract IP:PORT only (strip scheme and credentials)
        normalized_excludes = set()
        for proxy in exclude_proxies:
            # Extract IP:PORT from http://user:pass@IP:PORT or IP:PORT
            if "@" in proxy:
                # Format: http://user:pass@IP:PORT
                normalized_excludes.add(proxy.split("@")[1])
            elif "://" in proxy:
                # Format: http://IP:PORT
                normalized_excludes.add(proxy.split("://")[1])
            else:
                # Format: IP:PORT
                normalized_excludes.add(proxy)

        # Filter available proxies
        available = [p for p in available if p not in normalized_excludes]
        # If all proxies have been tried, allow retry with all proxies
        if not available:
            available = _get_available_proxies(site_key=site_key)

    # PHASE 2 OPTIMIZATION: Use smart proxy selection when site_key is provided
    if site_key and len(available) > 1:
        smart_proxy = get_smart_proxy(site_key, available)
        if smart_proxy:
            if "://" in smart_proxy:
                return smart_proxy
            if username and password:
                return f"http://{username}:{password}@{smart_proxy}"
            return f"http://{smart_proxy}"

    if proxy_mode == "round_robin":
        return get_round_robin_proxy_url(username, password, site_key=site_key)
    return get_random_proxy_url(username, password, site_key=site_key)


async def _append_banned_proxy_log(line: str) -> None:
    try:
        async with aiofiles.open(BANNED_PROXIES_LOG, "a", encoding="utf-8") as f:
            await f.write(line)
    except Exception:
        logger.exception(f"Không ghi được {BANNED_PROXIES_LOG}")


def _log_banned_proxy_removal(normalized_proxy: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} - {normalized_proxy} bị remove vì lỗi nhiều lần\n"

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            with open(BANNED_PROXIES_LOG, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            logger.exception(f"Không ghi được {BANNED_PROXIES_LOG}")
        return

    try:
        loop.create_task(_append_banned_proxy_log(line))
    except RuntimeError:
        # Nếu loop đã đóng, fallback sang ghi đồng bộ
        try:
            with open(BANNED_PROXIES_LOG, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            logger.exception(f"Không ghi được {BANNED_PROXIES_LOG}")


def remove_bad_proxy(bad_proxy_url):
    if not bad_proxy_url:
        return
    normalized = _normalize_proxy_key(bad_proxy_url)
    if not normalized:
        logger.warning(f"Không tách được IP:PORT từ {bad_proxy_url}")
        return
    if _remove_proxy_by_normalized(normalized):
        logger.warning(f"Đã loại proxy lỗi: {bad_proxy_url}")
        _log_banned_proxy_removal(normalized)
    else:
        logger.warning(f"Không tìm thấy proxy {bad_proxy_url} để remove.")

def shuffle_proxies():
    random.shuffle(LOADED_PROXIES)
    logger.info("[Proxy] Đã shuffle lại proxy pool!")


def _schedule_cleanup_task(
    retention_seconds: int = WEAK_PROXY_RETENTION_SECONDS,
    threshold: int = WEAK_PROXY_THRESHOLD,
) -> None:
    async def _runner() -> None:
        await cleanup_weak_proxies(
            retention_seconds=retention_seconds, threshold=threshold
        )

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(_runner())
    else:
        loop.create_task(_runner())


def handle_site_block_event(
    site_key: str,
    *,
    reason: str | None = None,
    severity: str = "severe",
) -> None:
    """React to a circuit breaker block to keep the proxy pool healthy."""

    if not site_key:
        return

    now = time.time()
    history = _site_block_history[site_key]
    history.append(now)
    cutoff = now - SITE_BLOCK_HISTORY_WINDOW
    while history and history[0] < cutoff:
        history.popleft()

    last_action = _site_last_adaptation.get(site_key, 0.0)
    if now - last_action < SITE_ADAPTATION_COOLDOWN:
        return

    _site_last_adaptation[site_key] = now

    normalized_reason = reason or "unknown"
    logger.warning(
        "[Proxy] Phát hiện domain %s bị block (%s) - điều chỉnh proxy pool.",
        site_key,
        normalized_reason,
    )

    removed = 0
    suspect_threshold = max(1, WEAK_PROXY_THRESHOLD // 2)
    retention_seconds = max(WEAK_PROXY_RETENTION_SECONDS // 2, 300)
    for proxy_key, history in list(_proxy_fail_history.items()):
        if len(history) >= suspect_threshold:
            if _remove_proxy_by_normalized(proxy_key):
                removed += 1

    if removed:
        logger.warning(
            "[Proxy] Đã loại %d proxy yếu sau cảnh báo của %s.", removed, site_key
        )
    else:
        shuffle_proxies()

    _schedule_cleanup_task(
        retention_seconds=retention_seconds, threshold=max(suspect_threshold, 2)
    )

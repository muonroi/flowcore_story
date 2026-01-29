"""Standalone microservice for harvesting anti-bot challenges."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import shutil
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiohttp
from aiohttp import web

from flowcore_story.config.proxy_provider import (
    ProxyType,
    classify_proxy_type,
    parse_proxy_url,
)

logger = logging.getLogger("storyflow.challenge_harvester")

try:  # pragma: no cover - optional dependency loaded dynamically
    from playwright.async_api import Browser, BrowserContext, Page, async_playwright
except Exception:  # pragma: no cover - simplified fallback when Playwright missing
    Browser = BrowserContext = Page = None  # type: ignore[misc,assignment]
    async_playwright = None  # type: ignore[assignment]

try:  # pragma: no cover - optional plugin
    from playwright_stealth import stealth_async
except Exception:  # pragma: no cover - fallback when plugin unavailable
    stealth_async = None  # type: ignore[assignment]

try:
    from curl_cffi.requests import AsyncSession
except ImportError:
    AsyncSession = None

try:  # pragma: no cover - advanced stealth module
    from flowcore_story.utils.advanced_stealth import (
        STEALTH_JS_SCRIPTS,
        AdvancedStealthContext,
        HumanBehaviorSimulator,
        inject_canvas_noise,
        warmup_browser_session,
    )
    ADVANCED_STEALTH_AVAILABLE = True
except Exception:  # pragma: no cover - fallback when module unavailable
    AdvancedStealthContext = None  # type: ignore[assignment,misc]
    HumanBehaviorSimulator = None  # type: ignore[assignment]
    inject_canvas_noise = None  # type: ignore[assignment]
    warmup_browser_session = None  # type: ignore[assignment]
    STEALTH_JS_SCRIPTS = []
    ADVANCED_STEALTH_AVAILABLE = False

try:
    from flowcore_story.utils.nodriver_harvester import harvest_with_nodriver
    NODRIVER_AVAILABLE = True
except ImportError:
    harvest_with_nodriver = None  # type: ignore
    NODRIVER_AVAILABLE = False

# PHASE 1 OPTIMIZATION: Cookie reuse system
try:
    from flowcore_story.utils.cookie_manager import (
        fingerprint_headers,
        select_cookie,
        set_cookies,
    )
    COOKIE_MANAGER_AVAILABLE = True
except Exception:
    COOKIE_MANAGER_AVAILABLE = False
    logger.warning("[ChallengeHarvester] Cookie manager not available - operating without cookie reuse")


TWOCAPTCHA_API_KEY = os.environ.get("TWOCAPTCHA_API_KEY", "").strip()
try:
    TWOCAPTCHA_MAX_DAILY = int(os.environ.get("TWOCAPTCHA_MAX_DAILY", "20"))
except (TypeError, ValueError):
    TWOCAPTCHA_MAX_DAILY = 20
TWOCAPTCHA_MAX_DAILY = max(0, TWOCAPTCHA_MAX_DAILY)
TWOCAPTCHA_COOLDOWN_SECONDS = 300
TWOCAPTCHA_POLL_INTERVAL = 5.0
TWOCAPTCHA_POLL_TIMEOUT = 120.0

_TWOCAPTCHA_USAGE_DATE: datetime.date | None = None
_TWOCAPTCHA_USAGE_COUNT = 0
_TWOCAPTCHA_LAST_CALL_TS = 0.0
_TWOCAPTCHA_LOCK = asyncio.Lock()
_TTV_SITEKEY_RE = re.compile(r"0x4[0-9a-zA-Z_-]{20,}")

# Universal TLS profile fallback chain
# Logic: Start with Strongest/Most Common -> Fallback to Different Engine/OS -> Fallback to Legacy
UNIVERSAL_FALLBACK_CHAIN = {
    "chrome120": "safari15_5",  # Primary: Modern Chrome (Windows) -> Backup: Safari (MacOS)
    "safari15_5": "edge99",     # Backup 2: Legacy Edge
    "edge99": "chrome99",       # Backup 3: Legacy Chrome
    "chrome99": None,           # End of chain
}

# Import memory monitoring utilities
try:
    from flowcore_story.utils.memory_monitor import (
        ContextPoolCleaner,
        ResourceMonitor,  # Updated from MemoryMonitor
    )
    MEMORY_MONITOR_AVAILABLE = True
except Exception:
    MEMORY_MONITOR_AVAILABLE = False
    logger.debug("[ChallengeHarvester] Memory monitor not available")


def _is_ttv_discovery_url(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if not path:
        return False
    if "/doc-truyen/" in path:
        return False
    if "/chuong" in path or "/chapter" in path:
        return False
    query = parse_qs(parsed.query)
    if "/the-loai/" in path or path.endswith("/the-loai"):
        return True
    if "/tong-hop" in path and "ctg" in query:
        return True
    if "/tim-kiem" in path or "/search" in path:
        return True
    if any(key in query for key in ("keyword", "tukhoa", "q")):
        return True
    return False


def _extract_ttv_sitekey(html: str | None) -> str | None:
    if not html:
        return None
    match = _TTV_SITEKEY_RE.search(html)
    if not match:
        return None
    return match.group(0)


async def _fetch_html_for_sitekey(url: str) -> str | None:
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                return await response.text()
    except Exception as exc:
        logger.debug("[ChallengeHarvester] Failed to fetch HTML for sitekey: %s", exc)
        return None


async def _reserve_twocaptcha_call() -> bool:
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()

    async with _TWOCAPTCHA_LOCK:
        global _TWOCAPTCHA_USAGE_DATE, _TWOCAPTCHA_USAGE_COUNT, _TWOCAPTCHA_LAST_CALL_TS

        if _TWOCAPTCHA_USAGE_DATE != now.date():
            _TWOCAPTCHA_USAGE_DATE = now.date()
            _TWOCAPTCHA_USAGE_COUNT = 0

        if _TWOCAPTCHA_USAGE_COUNT >= TWOCAPTCHA_MAX_DAILY:
            logger.warning(
                "[ChallengeHarvester] 2Captcha daily limit reached (%d/%d)",
                _TWOCAPTCHA_USAGE_COUNT,
                TWOCAPTCHA_MAX_DAILY,
            )
            return False

        if _TWOCAPTCHA_LAST_CALL_TS and now_ts - _TWOCAPTCHA_LAST_CALL_TS < TWOCAPTCHA_COOLDOWN_SECONDS:
            remaining = TWOCAPTCHA_COOLDOWN_SECONDS - (now_ts - _TWOCAPTCHA_LAST_CALL_TS)
            logger.info(
                "[ChallengeHarvester] 2Captcha cooldown active (%.0fs remaining)",
                max(0.0, remaining),
            )
            return False

        _TWOCAPTCHA_LAST_CALL_TS = now_ts
        return True


async def _record_twocaptcha_usage() -> None:
    now = datetime.now(timezone.utc)
    async with _TWOCAPTCHA_LOCK:
        global _TWOCAPTCHA_USAGE_DATE, _TWOCAPTCHA_USAGE_COUNT

        if _TWOCAPTCHA_USAGE_DATE != now.date():
            _TWOCAPTCHA_USAGE_DATE = now.date()
            _TWOCAPTCHA_USAGE_COUNT = 0

        _TWOCAPTCHA_USAGE_COUNT += 1


async def _parse_2captcha_payload(response: aiohttp.ClientResponse) -> dict[str, Any] | None:
    text = await response.text()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, dict):
        return parsed

    text = text.strip()
    if text.startswith("OK|"):
        return {"status": 1, "request": text.split("|", 1)[1]}
    if text:
        return {"status": 0, "request": text}
    return None


async def _solve_with_2captcha(url: str, sitekey: str | None) -> str | None:
    if not TWOCAPTCHA_API_KEY:
        logger.info("[ChallengeHarvester] 2Captcha disabled (missing TWOCAPTCHA_API_KEY)")
        return None
    if TWOCAPTCHA_MAX_DAILY <= 0:
        logger.info("[ChallengeHarvester] 2Captcha disabled (TWOCAPTCHA_MAX_DAILY=0)")
        return None

    resolved_sitekey = sitekey
    if not resolved_sitekey:
        html = await _fetch_html_for_sitekey(url)
        resolved_sitekey = _extract_ttv_sitekey(html)
        if not resolved_sitekey:
            logger.warning("[ChallengeHarvester] 2Captcha sitekey not found for %s", url)
            return None

    if not await _reserve_twocaptcha_call():
        return None

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        submit_payload = {
            "key": TWOCAPTCHA_API_KEY,
            "method": "turnstile",
            "sitekey": resolved_sitekey,
            "pageurl": url,
            "json": 1,
        }
        try:
            async with session.post("https://2captcha.com/in.php", data=submit_payload) as resp:
                submit_data = await _parse_2captcha_payload(resp)
        except Exception as exc:
            logger.warning("[ChallengeHarvester] 2Captcha submit failed: %s", exc)
            return None

        if not submit_data or submit_data.get("status") != 1:
            error_message = submit_data.get("request") if isinstance(submit_data, dict) else "unknown"
            logger.warning("[ChallengeHarvester] 2Captcha submit rejected: %s", error_message)
            return None

        task_id = str(submit_data.get("request"))
        await _record_twocaptcha_usage()

        deadline = time.monotonic() + TWOCAPTCHA_POLL_TIMEOUT
        while time.monotonic() < deadline:
            await asyncio.sleep(TWOCAPTCHA_POLL_INTERVAL)
            try:
                async with session.get(
                    "https://2captcha.com/res.php",
                    params={
                        "key": TWOCAPTCHA_API_KEY,
                        "action": "get",
                        "id": task_id,
                        "json": 1,
                    },
                ) as resp:
                    result_data = await _parse_2captcha_payload(resp)
            except Exception as exc:
                logger.warning("[ChallengeHarvester] 2Captcha poll failed: %s", exc)
                return None

            if not result_data:
                return None

            if result_data.get("status") == 1:
                return str(result_data.get("request"))

            if result_data.get("request") != "CAPCHA_NOT_READY":
                logger.warning(
                    "[ChallengeHarvester] 2Captcha solve error: %s",
                    result_data.get("request"),
                )
                return None

    logger.warning("[ChallengeHarvester] 2Captcha solve timed out for %s", url)
    return None


@dataclass(slots=True)
class ServiceSettings:
    host: str = os.environ.get("CHALLENGE_HARVESTER_HOST", "0.0.0.0")
    port: int = int(os.environ.get("CHALLENGE_HARVESTER_PORT", "8099"))
    headful: bool = os.environ.get("CHALLENGE_HARVESTER_HEADFUL", "true").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    # FIX ISSUE #1: Increase timeout from 60s to 120s to handle slow challenges
    navigation_timeout: float = float(os.environ.get("CHALLENGE_HARVESTER_NAVIGATION_TIMEOUT", "120"))
    delay_min: float = float(os.environ.get("CHALLENGE_HARVESTER_DELAY_MIN", "0.3"))
    delay_max: float = float(os.environ.get("CHALLENGE_HARVESTER_DELAY_MAX", "0.9"))
    wait_for_networkidle: bool = os.environ.get(
        "CHALLENGE_HARVESTER_WAIT_FOR_NETWORKIDLE", "false"
    ).lower() in ("1", "true", "yes", "on")
    # Increase retries from 2 to 3 for better reliability
    max_retries: int = int(os.environ.get("CHALLENGE_HARVESTER_MAX_RETRIES", "3"))
    retry_delay: float = float(os.environ.get("CHALLENGE_HARVESTER_RETRY_DELAY", "2.0"))
    # Advanced stealth settings
    use_advanced_stealth: bool = os.environ.get("CHALLENGE_HARVESTER_ADVANCED_STEALTH", "true").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    simulate_human_behavior: bool = os.environ.get("CHALLENGE_HARVESTER_HUMAN_BEHAVIOR", "true").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    reading_time: float = float(os.environ.get("CHALLENGE_HARVESTER_READING_TIME", "2.0"))
    enable_nodriver: bool = os.environ.get("CHALLENGE_HARVESTER_ENABLE_NODRIVER", "true").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    def normalized_delay_range(self) -> tuple[float, float]:
        low = max(0.0, min(self.delay_min, self.delay_max))
        high = max(self.delay_min, self.delay_max, low + 0.1)
        return low, high


class ChallengeHarvesterService:
    """Encapsulates the Playwright browser used to clear challenges."""

    # Error patterns that indicate browser/context is dead and needs recovery
    BROWSER_CRASH_PATTERNS = (
        "crashed",
        "closed",
        "disconnected",
        "destroyed",
        "disposed",
        "target page",
        "browser has been closed",
        "context has been closed",
        "connection closed",
    )

    def __init__(self, settings: ServiceSettings) -> None:
        self._settings = settings
        self._browser: Browser | None = None
        self._playwright = None
        self._lock = asyncio.Lock()
        # PHASE 1 OPTIMIZATION: Context pooling to reuse browser contexts
        # Key: (site_key, proxy_url, user_agent), Value: BrowserContext
        self._context_pool: dict[tuple[str, str, str | None], BrowserContext] = {}
        self._context_lock = asyncio.Lock()
        # Track browser health for auto-recovery
        self._browser_crash_count = 0
        self._max_crash_before_restart = 3
        self._request_count = 0
        self._max_requests_before_refresh = 50

        # MEMORY OPTIMIZATION: Add resource monitor and context pool cleaner
        self._memory_monitor: ResourceMonitor | None = None
        self._context_cleaner: ContextPoolCleaner | None = None
        self._xvfb_process: subprocess.Popen | None = None

        # Resource settings from environment
        self._max_memory_percent = float(os.environ.get("CHALLENGE_HARVESTER_MAX_MEMORY_PERCENT", "85"))
        self._max_cpu_percent = float(os.environ.get("CHALLENGE_HARVESTER_MAX_CPU_PERCENT", "90"))
        self._enable_auto_cleanup = os.environ.get("CHALLENGE_HARVESTER_ENABLE_AUTO_CLEANUP", "true").lower() in ("1", "true", "yes", "on")
        self._cleanup_interval = int(os.environ.get("CHALLENGE_HARVESTER_CLEANUP_INTERVAL", "300"))
        self._context_idle_timeout = int(os.environ.get("CHALLENGE_HARVESTER_CONTEXT_IDLE_TIMEOUT", "300"))
        self._context_pool_max_size = int(os.environ.get("CHALLENGE_HARVESTER_CONTEXT_POOL_MAX_SIZE", "5"))
        max_concurrency = int(os.environ.get("CHALLENGE_HARVESTER_MAX_CONCURRENCY", "2"))
        self._browser_semaphore = asyncio.Semaphore(max(1, max_concurrency))

    def _maybe_enable_headed_mode(self) -> None:
        """Enable headed mode with Xvfb when a display is available."""
        use_xvfb = os.environ.get("CHALLENGE_HARVESTER_USE_XVFB", "true").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if not use_xvfb:
            return

        if os.environ.get("DISPLAY"):
            if not self._settings.headful:
                self._settings.headful = True
                logger.info("[ChallengeHarvester] DISPLAY detected, enabling headed mode")
            return

        xvfb_path = shutil.which("Xvfb")
        if not xvfb_path:
            if self._settings.headful:
                logger.warning(
                    "[ChallengeHarvester] Headed mode requested but no DISPLAY/Xvfb found; falling back to headless"
                )
                self._settings.headful = False
            return

        display = os.environ.get("CHALLENGE_HARVESTER_XVFB_DISPLAY", ":99")
        if self._xvfb_process and self._xvfb_process.poll() is None:
            os.environ["DISPLAY"] = display
            if not self._settings.headful:
                self._settings.headful = True
            return

        try:
            self._xvfb_process = subprocess.Popen(
                [
                    xvfb_path,
                    display,
                    "-screen",
                    "0",
                    "1920x1080x24",
                    "-nolisten",
                    "tcp",
                    "-ac",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            os.environ["DISPLAY"] = display
            if not self._settings.headful:
                self._settings.headful = True
            
            # Give Xvfb some time to start before launching browser
            time.sleep(2.0)
            
            logger.info("[ChallengeHarvester] Xvfb started on %s (headed mode enabled)", display)
        except Exception as e:
            logger.warning("[ChallengeHarvester] Failed to start Xvfb: %s", e)

    def _is_rotating_proxy(self, proxy: str | None) -> bool:
        if not proxy:
            return False
        try:
            proxy_type = classify_proxy_type(proxy)
        except Exception:
            return False
        return proxy_type in (ProxyType.MOBILE, ProxyType.RESIDENTIAL)

    def _is_browser_crash_error(self, error: Exception) -> bool:
        """Check if error indicates browser/context crash that needs recovery."""
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        for pattern in self.BROWSER_CRASH_PATTERNS:
            if pattern in error_str or pattern in error_type:
                return True
        return False

    async def _restart_browser(self, reason: str = "unknown") -> None:
        """Force restart browser and clear all contexts.

        Args:
            reason: Reason for restart (e.g., "periodic_refresh", "crash_detected", "manual")
        """
        logger.warning("[ChallengeHarvester] Initiating browser restart (reason: %s)...", reason)

        # Clear context pool first
        async with self._context_lock:
            for pool_key, context in list(self._context_pool.items()):
                try:
                    # Close pages first
                    pages = context.pages
                    for page in pages:
                        try:
                            await page.close()
                        except Exception:
                            pass
                    await context.close()
                except Exception:
                    pass  # Best effort cleanup
            self._context_pool.clear()
            logger.info("[ChallengeHarvester] Cleared contexts from pool")

        # Close and restart browser
        async with self._lock:
            if self._browser:
                try:
                    await self._browser.close()
                except Exception:
                    pass  # Best effort
            self._browser = None

            # CRITICAL: OS-level cleanup to prevent zombie processes
            try:
                # Get the current process group or parent PID to target only our own children
                ppid = os.getpid()
                logger.info("[ChallengeHarvester] Cleaning up orphan Chrome processes for PPID %d", ppid)
                # Use pkill with -P to kill only children of this process
                subprocess.run(["pkill", "-9", "-P", str(ppid), "-f", "chrome|chromium"], stderr=subprocess.DEVNULL)
            except Exception as e:
                logger.debug("[ChallengeHarvester] OS-level cleanup failed: %s", e)

            # Restart
            if self._playwright:
                try:
                    self._maybe_enable_headed_mode()
                    launch_kwargs = {
                        "headless": not self._settings.headful,
                        "args": [
                            "--disable-blink-features=AutomationControlled",
                            "--disable-dev-shm-usage",
                            "--no-sandbox",
                            "--disable-gpu",
                            "--disable-extensions",
                            "--disable-setuid-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-accelerated-2d-canvas",
                            "--no-first-run",
                        ],
                    }
                    self._browser = await self._playwright.chromium.launch(**launch_kwargs)
                    self._browser_crash_count = 0
                    logger.info("[ChallengeHarvester] Browser restarted successfully")
                except Exception as e:
                    logger.error("[ChallengeHarvester] Failed to restart browser: %s", e)
                    raise

    async def _validate_context(self, context: BrowserContext) -> bool:
        """Validate if a context is still healthy and usable."""
        try:
            # Try to access pages - this will fail if context is dead
            _ = context.pages
            # Try to get cookies - another lightweight check
            await asyncio.wait_for(context.cookies(), timeout=2.0)
            return True
        except Exception:
            return False

    async def startup(self) -> None:
        if async_playwright is None:  # pragma: no cover - defensive guard
            logger.warning("Playwright is not installed. Only 'impersonate' mode will work.")
        else:
            async with self._lock:
                if self._browser is not None:
                    return
                self._maybe_enable_headed_mode()
                self._playwright = await async_playwright().start()
                launch_kwargs = {
                    "headless": not self._settings.headful,
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                        "--disable-gpu",
                        "--disable-extensions",
                        "--disable-setuid-sandbox",
                        "--no-first-run",
                    ],
                }
                if self._settings.headful:
                    launch_kwargs.setdefault("args", []).append("--start-maximized")
                self._browser = await self._playwright.chromium.launch(**launch_kwargs)
                logger.info(
                    "Challenge harvester browser started (headful=%s)",
                    self._settings.headful,
                )

        if AsyncSession is None:
            logger.warning("curl_cffi is not installed. 'impersonate' mode will fail.")

        # MEMORY OPTIMIZATION: Start resource monitoring and context cleanup
        if MEMORY_MONITOR_AVAILABLE and self._enable_auto_cleanup:
            # Initialize resource monitor
            self._memory_monitor = ResourceMonitor(
                max_memory_percent=self._max_memory_percent,
                max_cpu_percent=self._max_cpu_percent,
                check_interval=60,  # Check every minute
                cleanup_callback=self._resource_cleanup_callback,
            )
            await self._memory_monitor.start()
            logger.info(
                "[ChallengeHarvester] Resource monitor started (mem_max: %.1f%%, cpu_max: %.1f%%)",
                self._max_memory_percent,
                self._max_cpu_percent
            )

            # Initialize context pool cleaner
            self._context_cleaner = ContextPoolCleaner(
                context_pool=self._context_pool,
                max_idle_time=self._context_idle_timeout,
                cleanup_interval=self._cleanup_interval,
                max_pool_size=self._context_pool_max_size,
            )
            await self._context_cleaner.start()
            logger.info(
                "[ChallengeHarvester] Context pool cleaner started (max_size: %d, idle_timeout: %ds)",
                self._context_pool_max_size,
                self._context_idle_timeout
            )

    async def _resource_cleanup_callback(self) -> None:
        """Custom cleanup callback for resource monitor (CPU/Memory)."""
        logger.warning("[ChallengeHarvester] Resource cleanup triggered! Performing emergency cleanup...")

        # 1. Cleanup idle contexts first (lightweight)
        if self._context_cleaner:
            try:
                result = await self._context_cleaner.cleanup_idle_contexts()
                logger.info("[ChallengeHarvester] Cleaned up contexts: %s", result)
            except Exception as e:
                logger.error("[ChallengeHarvester] Context cleanup failed: %s", e)

        # 2. Check system load - if critical, restart browser (heavyweight)
        # We rely on ResourceMonitor to call this when thresholds are met.
        # If we are here, it means something is wrong.
        # To be safe, we restart the browser to clear zombie processes.
        logger.warning("[ChallengeHarvester] Restarting browser to clear zombie processes/high load...")
        try:
            await self._restart_browser(reason="resource_cleanup")
        except Exception as e:
            logger.error("[ChallengeHarvester] Browser restart failed during cleanup: %s", e)

    async def shutdown(self) -> None:
        # MEMORY OPTIMIZATION: Stop monitoring first
        if self._memory_monitor:
            await self._memory_monitor.stop()
            logger.info("[ChallengeHarvester] Memory monitor stopped")

        if self._context_cleaner:
            await self._context_cleaner.stop()
            logger.info("[ChallengeHarvester] Context pool cleaner stopped")

        # PHASE 1 OPTIMIZATION: Close all pooled contexts first
        async with self._context_lock:
            for (site_key, proxy, _user_agent), context in self._context_pool.items():
                try:
                    await context.close()
                    logger.debug("[ChallengeHarvester] Closed pooled context for %s", site_key)
                except Exception:  # pragma: no cover - best effort cleanup
                    logger.debug("Failed to close pooled context for %s", site_key, exc_info=True)
            self._context_pool.clear()

        async with self._lock:
            if self._browser:
                try:
                    await self._browser.close()
                except Exception:  # pragma: no cover - best effort shutdown
                    logger.exception("Failed to close challenge harvester browser")
            self._browser = None
            if self._playwright:
                try:
                    await self._playwright.stop()
                except Exception:  # pragma: no cover - best effort shutdown
                    logger.exception("Failed to stop Playwright instance")
            self._playwright = None

        if self._xvfb_process:
            try:
                self._xvfb_process.terminate()
                self._xvfb_process.wait(timeout=3)
            except Exception:
                pass
            self._xvfb_process = None

    async def _apply_stealth(self, page: Page) -> None:
        """Apply stealth techniques to page. Uses advanced stealth if available, falls back to playwright-stealth."""
        # Try advanced stealth first (modern approach)
        if self._settings.use_advanced_stealth and ADVANCED_STEALTH_AVAILABLE:
            try:
                # Advanced stealth scripts are already injected at context level
                # But we can add page-level scripts here if needed
                logger.debug("Using advanced stealth techniques")
                return
            except Exception:  # pragma: no cover - stealth best effort
                logger.debug("Advanced stealth failed, falling back to playwright-stealth", exc_info=True)

        # Fallback to playwright-stealth (legacy)
        if stealth_async is None:
            return
        try:
            await stealth_async(page)
        except Exception:  # pragma: no cover - stealth best effort
            logger.debug("Failed to apply stealth evasions", exc_info=True)

    async def _warmup_navigation(self, page: Page, proxy: str | None = None) -> None:
        """Warm-up navigation to reduce bot detection before the target URL."""
        if proxy and self._is_rotating_proxy(proxy):
            logger.debug("[ChallengeHarvester] Skipping warm-up for rotating proxy")
            return
        if self._settings.use_advanced_stealth and warmup_browser_session:
            try:
                await warmup_browser_session(page)
            except Exception as e:
                logger.debug("[ChallengeHarvester] Advanced warm-up visit failed: %s", e)
        else:
            warmup_url = "https://www.google.com"
            try:
                await page.goto(
                    warmup_url,
                    timeout=self._settings.navigation_timeout * 1000,
                    wait_until="domcontentloaded",
                )
                await page.wait_for_timeout(2000)
                logger.debug("[ChallengeHarvester] Basic warm-up visit completed")
            except Exception as e:
                logger.debug("[ChallengeHarvester] Basic warm-up visit failed: %s", e)

    async def _collect_turnstile_token(self, page: Page) -> str | None:
        script = """
            () => {
                try {
                    if (typeof window.turnstile !== 'undefined' && window.turnstile.getResponse) {
                        const resp = window.turnstile.getResponse();
                        if (resp) {
                            return resp;
                        }
                    }
                } catch (err) {}
                const candidates = document.querySelectorAll('input[name="cf-turnstile-response"], input[name="cf-turnstile-token"]');
                for (const el of candidates) {
                    if (el && el.value) {
                        return el.value;
                    }
                }
                return null;
            }
        """
        try:
            return await page.evaluate(script)
        except Exception:
            return None

    async def _ensure_browser(self) -> Browser:
        if self._browser is None:
            await self.startup()
        if self._browser is None:
             raise RuntimeError("Browser failed to initialize")
        return self._browser

    async def _get_or_create_context(
        self,
        site_key: str | None,
        proxy: str | None,
        user_agent: str | None,
        context_options: dict[str, Any],
        use_advanced: bool,
    ) -> tuple[BrowserContext, bool]:
        """PHASE 1 OPTIMIZATION: Get existing context from pool or create new one."""
        # FIX: Detect rotating proxy - do NOT pool contexts for rotating proxies
        # Rotating proxies change IP frequently, so reusing context causes IP/fingerprint mismatch
        is_rotating_proxy = self._is_rotating_proxy(proxy)
        should_pool = not is_rotating_proxy

        if is_rotating_proxy:
            logger.info(
                "[ChallengeHarvester] Rotating proxy detected for %s - creating fresh context (no pooling)",
                site_key or "default"
            )

        # Generate pool key
        pool_key = (site_key or "__default__", proxy or "__no_proxy__", user_agent)

        # Try to get existing context from pool (skip for rotating proxies)
        if should_pool:
            async with self._context_lock:
                existing_context = self._context_pool.get(pool_key)
                if existing_context:
                    # IMPROVED: Use robust validation instead of simple page check
                    if await self._validate_context(existing_context):
                        logger.debug(
                            "[ChallengeHarvester] Reusing pooled context for %s (proxy: %s)",
                            site_key,
                            "enabled" if proxy else "disabled"
                        )
                        # MEMORY OPTIMIZATION: Mark as recently used
                        if self._context_cleaner:
                            self._context_cleaner.mark_used(pool_key)
                        return existing_context, True
                    else:
                        # Context is closed/invalid, remove from pool
                        logger.warning(
                            "[ChallengeHarvester] Context for %s failed validation, creating new one",
                            site_key,
                        )
                        self._context_pool.pop(pool_key, None)
                        # Try to close gracefully
                        try:
                            await existing_context.close()
                        except Exception:
                            pass

        # Create new context
        browser = await self._ensure_browser()

        # Generate deterministic seed for fingerprinting based on proxy and site
        # This ensures consistent fingerprint (Task 7) for the same proxy/site pair
        fingerprint_seed = None
        if proxy or site_key:
            seed_source = f"{proxy or ''}|{site_key or ''}"
            fingerprint_seed = hashlib.md5(seed_source.encode()).hexdigest()

        if use_advanced and AdvancedStealthContext:
            # Use AdvancedStealthContext manager which handles everything
            stealth_ctx_mgr = AdvancedStealthContext(
                browser,
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent,
                proxy=context_options.get("proxy"), # Use the parsed config
                seed=fingerprint_seed,
            )
            # Manually enter context (we keep it open for pooling)
            context = await stealth_ctx_mgr.__aenter__()
        else:
            # Legacy/Fallback creation
            if use_advanced:
                # Basic manual stealth setup if AdvancedStealthContext class not available
                viewport_size = {"width": 1920, "height": 1080}
                advanced_options = {
                    "viewport": viewport_size,
                    "locale": "vi-VN",
                    "timezone_id": "Asia/Ho_Chi_Minh",
                    "permissions": ["geolocation", "notifications"],
                    "geolocation": {
                        "latitude": 10.7769 + random.uniform(-0.1, 0.1),
                        "longitude": 106.7009 + random.uniform(-0.1, 0.1),
                    },
                    "device_scale_factor": random.choice([1, 1.25, 1.5, 2]),
                    "is_mobile": False,
                    "has_touch": random.choice([True, False]),
                }
                context_options.update(advanced_options)

            context = await browser.new_context(**context_options)

            # Inject stealth scripts if advanced mode (legacy injection)
            if use_advanced:
                for script in STEALTH_JS_SCRIPTS:
                    try:
                        await context.add_init_script(script)
                    except Exception as e:
                        logger.debug("Failed to inject stealth script: %s", e)

                try:
                    await inject_canvas_noise(context)
                except Exception as e:
                    logger.debug("Failed to inject canvas noise: %s", e)

        # Store in pool unless proxy is rotating
        if should_pool:
            async with self._context_lock:
                self._context_pool[pool_key] = context
                logger.info(
                    "[ChallengeHarvester] Created new pooled context for %s (pool size: %d)",
                    site_key,
                    len(self._context_pool)
                )
        else:
            logger.info(
                "[ChallengeHarvester] Created ephemeral context for %s (no pooling)",
                site_key or "default",
            )

        # MEMORY OPTIMIZATION: Mark context as used in cleaner
        if self._context_cleaner and should_pool:
            self._context_cleaner.mark_used(pool_key)

        return context, should_pool

    async def harvest_with_impersonate(
        self,
        url: str,
        impersonate: str,
        proxy: str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Use curl_cffi to impersonate a browser for TLS fingerprinting."""
        if AsyncSession is None:
            raise RuntimeError("curl_cffi not installed, cannot use impersonate mode")

        logger.info(f"[ChallengeHarvester] Using curl_cffi impersonate='{impersonate}' for {url}")
        logger.info(f"[ChallengeHarvester] Proxy parameter received: {proxy}")

        # Prepare proxies with scheme normalization
        proxies = None
        proxy_url = proxy
        if proxy_url:
            # Normalize proxy URL - ensure it has scheme
            if "://" not in proxy_url:
                proxy_url = f"http://{proxy_url}"
            # Force HTTP proxy for curl_cffi (BoringSSL issue with HTTPS proxy)
            if proxy_url.startswith("https://"):
                # Only allow HTTPS proxy if explicitly enabled
                if os.environ.get("CURL_CFFI_PROXY_TLS", "false") != "true":
                    proxy_url = "http://" + proxy_url[len("https://"):]
            proxies = {"http": proxy_url, "https": proxy_url}

        # Use universal fallback chain for all sites
        fallback_chain = UNIVERSAL_FALLBACK_CHAIN

        # WORKAROUND: Inject advanced headers for ANY site if using chrome impersonation
        # This ensures TLS Fingerprint (Chrome) matches HTTP Headers (Sec-CH-UA) consistently
        request_headers = dict(headers) if headers else {}
        if "chrome" in impersonate:
            # Add/Overwrite critical headers for Chrome 120+ impersonation
            advanced_headers = {
                "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-site": "none",
                "sec-fetch-mode": "navigate", 
                "sec-fetch-user": "?1",
                "sec-fetch-dest": "document",
                "upgrade-insecure-requests": "1",
                "accept-language": "en-US,en;q=0.9,vi;q=0.8",
            }
            # Only apply if not already present (preserve caller intent if specific headers passed)
            for k, v in advanced_headers.items():
                if k.lower() not in [h.lower() for h in request_headers]:
                    request_headers[k] = v

        # Try with fallback chain (recursive fallback)
        current_profile = impersonate
        attempted_profiles = []

        response = None
        cookies_list = []

        while True:
            attempted_profiles.append(current_profile)
            
            # IMPROVEMENT: Use default auto-negotiation for HTTP version.
            # Only force HTTP/1.1 if we explicitly detect persistent TLS/Proxy issues.
            session_kwargs = {"impersonate": current_profile}
            
            # If we've already tried and failed, try forcing HTTP/1.1 for stability
            if len(attempted_profiles) > 1:
                session_kwargs["http_version"] = 1

            try:
                async with AsyncSession(**session_kwargs) as s:
                    response = await s.get(
                        url,
                        headers=request_headers, # Use the enhanced headers
                        proxies=proxies,
                        timeout=timeout,
                        allow_redirects=True,
                        verify=False,  # Disable SSL verification to support rotating proxies
                    )

                    # Convert cookies to Playwright format (list of dicts)
                    # Do this BEFORE breaking out of context manager
                    try:
                        # curl_cffi cookies handling
                        # First try to get full cookie objects
                        for cookie in s.cookies:
                            if hasattr(cookie, "name"):
                                cookies_list.append({
                                    "name": cookie.name,
                                    "value": cookie.value,
                                    "domain": getattr(cookie, "domain", ""),
                                    "path": getattr(cookie, "path", "/"),
                                    "secure": getattr(cookie, "secure", False),
                                    "expires": getattr(cookie, "expires", None)
                                })
                            elif isinstance(cookie, str):
                                # It's a key string
                                cookies_list.append({
                                    "name": cookie,
                                    "value": s.cookies.get(cookie),
                                    "domain": "",
                                    "path": "/"
                                })
                    except Exception as e:
                        logger.warning(f"Complex cookie parsing failed: {e}. Falling back to simple items.")
                        for k, v in s.cookies.items():
                            cookies_list.append({
                                "name": k,
                                "value": v,
                                "domain": "",
                                "path": "/"
                            })

                    # Success! Break out of retry loop
                    break

            except Exception as e:
                error_msg = str(e)
                # Check for TLS connect error (35) - common with curl_cffi + proxy
                if ("tls connect error" in error_msg.lower() or "code 35" in error_msg) and proxy_url:
                    # Get next fallback profile
                    next_profile = fallback_chain.get(current_profile)

                    if next_profile is None or next_profile in attempted_profiles:
                        # No more fallback options or circular reference
                        logger.warning(
                            f"[ChallengeHarvester] TLS error with {current_profile}, no more fallback profiles available (tried: {attempted_profiles})"
                        )
                        raise

                    logger.warning(
                        f"[ChallengeHarvester] TLS error with {current_profile}, retrying with fallback profile: {next_profile}"
                    )
                    
                    # WORKAROUND: Add random sleep to avoid rapid TLS handshake fingerprinting
                    # Cloudflare might flag rapid sequential handshakes from same IP
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                    
                    current_profile = next_profile
                    # Loop continues with next profile
                else:
                    # Not a TLS error, re-raise
                    raise

        # After breaking out of loop, return result
        return {
            "url": str(response.url),
            "status": response.status_code,
            "body": response.text,  # Text content
            "cookies": cookies_list,
            "headers": dict(response.headers),
            "cf_turnstile_token": None,  # curl_cffi doesn't run JS
            "metadata": {
                "engine": "curl_cffi",
                "impersonate": current_profile,  # Use actual profile that succeeded
                "proxy_used": bool(proxy)
            }
        }

    async def _harvest_with_playwright(
        self,
        url: str,
        *,
        site_key: str | None = None,
        headers: Mapping[str, str] | None = None,
        wait_for_selector: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        referer: str | None = None,
        human_delay_range: Sequence[float] | None = None,
        proxy: str | None = None,
        actions: Sequence[Mapping[str, Any]] | None = None,
        sniff_network: bool | None = None,
    ) -> dict[str, Any]:

        # OPTION 2: FULL BROWSER (Playwright)
        # Use advanced stealth context if available and enabled
        use_advanced = self._settings.use_advanced_stealth and ADVANCED_STEALTH_AVAILABLE
        logger.info(f"[ChallengeHarvester] Using Playwright (no impersonate) for {url}, proxy={proxy}")

        # PHASE 1 OPTIMIZATION: Load existing cookies if available
        existing_cookies = None
        cookie_fingerprint = None
        user_agent_from_headers = (headers or {}).get("User-Agent")

        if COOKIE_MANAGER_AVAILABLE and site_key:
            try:
                # Try to select existing cookie entry for this site
                header_dict = dict(headers) if headers else {}
                cookie_fingerprint = fingerprint_headers(header_dict)
                selection = select_cookie(
                    site_key=site_key,
                    proxy_url=proxy,
                    fingerprint=cookie_fingerprint,
                    user_agent=user_agent_from_headers,
                )

                if selection and selection.cookies:
                    existing_cookies = selection.cookies
                    # Use same user-agent as stored cookies for consistency
                    if selection.user_agent:
                        user_agent_from_headers = selection.user_agent
                    logger.info(
                        "[ChallengeHarvester] Reusing %d cookies for %s (entry: %s)",
                        len(existing_cookies),
                        site_key,
                        selection.entry_id
                    )
            except Exception as e:
                logger.debug("[ChallengeHarvester] Failed to load existing cookies: %s", e)

        # Prepare Context Options
        context_options = {
            "user_agent": user_agent_from_headers,
        }

        # FIX: Parse proxy credentials explicitly for Playwright
        if proxy:
            proxy_type, proxy_url = parse_proxy_url(proxy)
            proxy_config = {"server": proxy_url}

            # Extract credentials if present in URL
            try:
                parsed = urlparse(proxy_url)
                if parsed.username and parsed.password:
                    proxy_config["username"] = parsed.username
                    proxy_config["password"] = parsed.password
                    # Remove credentials from server URL to be safe, though Playwright handles both
                    # proxy_config["server"] = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
            except Exception:
                pass

            context_options["proxy"] = proxy_config

        # Prepare headers
        combined_headers: dict[str, str] = {}
        for source in (headers, extra_headers):
            if not source:
                continue
            for key, value in source.items():
                if key.lower() == "user-agent":
                    continue
                combined_headers[key] = value

        sniffed_urls: list[str] = []

        # Retry loop for context operations with auto-recovery
        context = None
        context_pooled = True
        page = None
        max_context_retries = 3  # Increased from 2 for better resilience

        for attempt in range(max_context_retries):
            try:
                # PHASE 1 OPTIMIZATION: Use context pooling instead of creating new context each time
                context, context_pooled = await self._get_or_create_context(
                    site_key=site_key,
                    proxy=proxy,
                    user_agent=user_agent_from_headers,
                    context_options=context_options,
                    use_advanced=use_advanced,
                )

                if combined_headers:
                    await context.set_extra_http_headers(combined_headers)

                # PHASE 1 OPTIMIZATION: Set existing cookies into context before creating page
                if existing_cookies:
                    try:
                        await context.add_cookies(existing_cookies)
                        logger.debug("[ChallengeHarvester] Set %d existing cookies into context", len(existing_cookies))
                    except Exception as e:
                        logger.debug("[ChallengeHarvester] Failed to set existing cookies: %s", e)

                page = await context.new_page()
                if sniff_network:
                    def _capture_request(request: Any) -> None:
                        url = getattr(request, "url", None)
                        if url:
                            sniffed_urls.append(url)

                    page.on("request", _capture_request)
                break  # Success, proceed
            except Exception as e:
                # IMPROVED: Use comprehensive crash detection
                is_crash = self._is_browser_crash_error(e)

                if is_crash and attempt < max_context_retries - 1:
                    logger.warning(
                        "[ChallengeHarvester] Browser/context crash detected on attempt %d/%d: %s",
                        attempt + 1, max_context_retries, str(e)[:100]
                    )

                    # Invalidate context from pool
                    pool_key = (site_key or "__default__", proxy or "__no_proxy__", user_agent_from_headers)
                    async with self._context_lock:
                        if self._context_pool.get(pool_key) == context:
                            self._context_pool.pop(pool_key, None)

                    # Track crash count and trigger browser restart if needed
                    self._browser_crash_count += 1
                    if self._browser_crash_count >= self._max_crash_before_restart:
                        logger.warning(
                            "[ChallengeHarvester] Crash threshold reached (%d), restarting browser...",
                            self._browser_crash_count
                        )
                        try:
                            await self._restart_browser(reason="crash_threshold")
                        except Exception as restart_err:
                            logger.error("[ChallengeHarvester] Browser restart failed: %s", restart_err)

                    # Small delay before retry
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue

                # Non-crash error or out of retries
                raise

        try:
            # Apply stealth (uses advanced or fallback to playwright-stealth)
            await self._apply_stealth(page)
            await self._warmup_navigation(page, proxy=proxy)

            delay_range = self._settings.normalized_delay_range()
            if human_delay_range and len(human_delay_range) >= 2:
                low = max(0.0, float(human_delay_range[0]))
                high = max(low, float(human_delay_range[1]))
                delay_range = (low, high)

            # Navigate to URL
            response = await page.goto(
                url,
                timeout=self._settings.navigation_timeout * 1000,
                referer=referer,
            )

            # Human behavior simulation if enabled
            if use_advanced and self._settings.simulate_human_behavior and HumanBehaviorSimulator:
                try:
                    # Random initial delay (user looking at page)
                    await HumanBehaviorSimulator.random_delay(500, 1500)

                    # Simulate mouse movement to random position
                    viewport = page.viewport_size
                    if viewport:
                        target_x = random.randint(100, viewport["width"] - 100)
                        target_y = random.randint(100, viewport["height"] - 100)
                        await HumanBehaviorSimulator.simulate_mouse_movement(page, target_x, target_y)

                    # Simulate reading behavior
                    if self._settings.reading_time > 0:
                        await HumanBehaviorSimulator.simulate_reading(page, self._settings.reading_time)

                    logger.debug("[ChallengeHarvester] Completed human behavior simulation")
                except Exception as e:
                    logger.debug("Human behavior simulation failed: %s", e)

            try:
                if self._settings.wait_for_networkidle:
                    await page.wait_for_load_state("networkidle", timeout=self._settings.navigation_timeout * 1000)
            except Exception:
                pass
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=self._settings.navigation_timeout * 1000)
                except Exception:
                    logger.debug("Selector %s not found for %s", wait_for_selector, url)

            # --- OVERLAY BREAKER (Task 1) ---
            # Remove common ad overlays that block interaction with the player
            try:
                await page.evaluate("""
                    () => {
                        const overlaySelectors = [
                            '[data-cl-overlay]',
                            'div[class*="overlay" i]',
                            'div[id*="overlay" i]',
                            'div[class*="popup" i]',
                            'div[style*="z-index: 9999" i]',
                            'div[style*="z-index: 1001" i]'
                        ];
                        let count = 0;
                        overlaySelectors.forEach(selector => {
                            const elements = document.querySelectorAll(selector);
                            elements.forEach(el => {
                                // Check if it covers a large part of the screen
                                const rect = el.getBoundingClientRect();
                                if (rect.width > window.innerWidth * 0.5 && rect.height > window.innerHeight * 0.5) {
                                    el.remove();
                                    count++;
                                }
                            });
                        });
                        if (count > 0) console.log(`[OverlayBreaker] Removed ${count} overlays`);
                    }
                """)
            except Exception as e:
                logger.debug(f"[OverlayBreaker] Failed to clean overlays: {e}")

            if actions:
                for action in actions:
                    if not isinstance(action, Mapping):
                        logger.debug("[ChallengeHarvester] Skipping invalid action: %r", action)
                        continue
                    action_type = str(action.get("type") or "").lower()
                    if action_type == "click":
                        selector = action.get("selector")
                        if not selector:
                            logger.debug("[ChallengeHarvester] Click action missing selector")
                            continue
                        try:
                            await page.locator(selector).first.click()
                            if self._settings.wait_for_networkidle:
                                try:
                                    await page.wait_for_load_state(
                                        "networkidle",
                                        timeout=self._settings.navigation_timeout * 1000,
                                    )
                                except Exception:
                                    pass
                        except Exception as exc:
                            logger.debug(
                                "[ChallengeHarvester] Click action failed for %s: %s",
                                selector,
                                exc,
                            )
                    else:
                        logger.debug("[ChallengeHarvester] Unsupported action type: %s", action_type)

            # Legacy delay (only if no human behavior simulation)
            if not (use_advanced and self._settings.simulate_human_behavior):
                post_delay = random.uniform(*delay_range)
                if post_delay >= 0.05:
                    await page.wait_for_timeout(post_delay * 1000)

            content = await page.content()
            cookies = await context.cookies()
            token = await self._collect_turnstile_token(page)

            status = response.status if response else None
            headers_snapshot = dict(response.headers) if response else {}

            # PHASE 1 OPTIMIZATION: Persist cookies after successful harvest
            if COOKIE_MANAGER_AVAILABLE and site_key and cookies and status == 200:
                try:
                    # Use headers from request for fingerprinting
                    request_headers = combined_headers.copy() if combined_headers else {}
                    request_headers["User-Agent"] = user_agent_from_headers or ""

                    set_cookies(
                        site_key=site_key,
                        cookies=cookies,
                        url=url,
                        proxy_url=proxy,
                        headers=request_headers,
                        fingerprint=cookie_fingerprint,
                    )
                    logger.info(
                        "[ChallengeHarvester] Persisted %d cookies for %s (reusable for future requests)",
                        len(cookies),
                        site_key
                    )
                except Exception as e:
                    logger.debug("[ChallengeHarvester] Failed to persist cookies: %s", e)

            return {
                "url": url,
                "site_key": site_key,
                "status": status,
                "body": content,
                "cookies": cookies,
                "cf_turnstile_token": token,
                "headers": headers_snapshot,
                "sniffed_urls": sniffed_urls,
                "metadata": {
                    "delay_range": delay_range,
                    "advanced_stealth": use_advanced,
                    "human_behavior": use_advanced and self._settings.simulate_human_behavior,
                    "engine": "playwright",
                    "cookie_reused": existing_cookies is not None,
                    "cookie_count_persisted": len(cookies) if cookies else 0,
                    "context_pooled": context_pooled,
                },
            }
        finally:
            if page:
                try:
                    await page.close()
                except Exception as e:
                    logger.debug("[ChallengeHarvester] Failed to close page: %s", e)
            if context and not context_pooled:
                try:
                    await context.close()
                except Exception as e:
                    logger.debug("[ChallengeHarvester] Failed to close context: %s", e)

    async def harvest(
        self,
        url: str,
        *,
        site_key: str | None = None,
        headers: Mapping[str, str] | None = None,
        wait_for_selector: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        referer: str | None = None,
        human_delay_range: Sequence[float] | None = None,
        actions: Sequence[Mapping[str, Any]] | None = None,
        sniff_network: bool | None = None,
        # New params
        proxy: str | None = None,
        impersonate: str | None = None,
    ) -> dict[str, Any]:

        self._request_count += 1
        if self._request_count >= self._max_requests_before_refresh:
            logger.info(
                "[ChallengeHarvester] Periodic browser refresh triggered (limit %s reached)",
                self._max_requests_before_refresh,
            )
            await self._restart_browser(reason="periodic_refresh")
            self._request_count = 0

        # OPTION 1: FAST LANE (curl_cffi)
        if impersonate:
            result = await self.harvest_with_impersonate(
                url=url,
                impersonate=impersonate,
                proxy=proxy,
                headers=headers,
                timeout=self._settings.navigation_timeout
            )
            if sniff_network:
                result["sniffed_urls"] = []
            return result

        # OPTION 2: FULL BROWSER (Playwright)
        async with self._browser_semaphore:
            return await self._harvest_with_playwright(
                url,
                site_key=site_key,
                headers=headers,
                wait_for_selector=wait_for_selector,
                extra_headers=extra_headers,
                referer=referer,
                human_delay_range=human_delay_range,
                proxy=proxy,
                actions=actions,
                sniff_network=sniff_network,
            )


async def create_app(settings: ServiceSettings | None = None) -> web.Application:
    settings = settings or ServiceSettings()
    service = ChallengeHarvesterService(settings)

    routes = web.RouteTableDef()

    @routes.get("/health")
    async def health_handler(request: web.Request) -> web.Response:
        """Health check endpoint with detailed browser status."""
        browser_healthy = False
        context_pool_size = 0

        # Check browser health
        if service._browser:
            try:
                # Verify browser is responsive
                contexts = service._browser.contexts
                browser_healthy = True
                context_pool_size = len(service._context_pool)
            except Exception:
                browser_healthy = False

        return web.json_response({
            "status": "ok" if browser_healthy or AsyncSession is not None else "degraded",
            "service": "challenge-harvester",
            "browser_ready": service._browser is not None,
            "browser_healthy": browser_healthy,
            "curl_cffi_ready": AsyncSession is not None,
            "context_pool_size": context_pool_size,
            "crash_count": service._browser_crash_count,
            "max_crash_threshold": service._max_crash_before_restart,
        })

    @routes.post("/restart-browser")
    async def restart_browser_handler(request: web.Request) -> web.Response:
        """Manual browser restart endpoint for recovery."""
        try:
            await service._restart_browser(reason="manual_api")
            return web.json_response({
                "status": "ok",
                "message": "Browser restarted successfully",
            })
        except Exception as e:
            return web.json_response({
                "status": "error",
                "message": str(e),
            }, status=500)

    @routes.post("/harvest")
    async def harvest_handler(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON payload"}, status=400)

        url = payload.get("url")
        if not url:
            return web.json_response({"error": "Missing url"}, status=400)

        action = payload.get("action", "harvest") # Default to harvest (solve captcha)
        site_key = payload.get("site_key")
        site_key_lower = site_key.lower() if isinstance(site_key, str) else None
        impersonate = payload.get("impersonate")
        turnstile_sitekey = payload.get("turnstile_sitekey")
        actions = payload.get("actions")
        sniff_network = payload.get("sniff_network")
        
        # ... (Rest of logic) ...
        
        # Special handling for Render Mode
        if action == "render":
            # Force Playwright or Nodriver, skip curl_cffi unless explicitly requested
            pass 

        should_use_2captcha = site_key_lower == "tangthuvien" and _is_ttv_discovery_url(url)
        impersonate_sites_raw = os.environ.get("IMPERSONATE_SITES", "")
        impersonate_sites = {
            entry.strip().lower()
            for entry in re.split(r"[,\s]+", impersonate_sites_raw)
            if entry.strip()
        }
        should_impersonate_first = bool(site_key_lower and site_key_lower in impersonate_sites)

        # Retry logic with comprehensive error handling
        last_error = None
        browser_restart_attempted = False
        playwright_failures = 0
        twocaptcha_attempted = False

        if should_impersonate_first and not impersonate:
            if AsyncSession is None:
                logger.info(
                    "[ChallengeHarvester] curl_cffi unavailable, escalating to Playwright for %s",
                    url,
                )
            else:
                initial_profile = random.choice(("chrome120", "safari15_5"))
                try:
                    preflight_result = await service.harvest_with_impersonate(
                        url=url,
                        impersonate=initial_profile,
                        proxy=payload.get("proxy"),
                        headers=payload.get("headers"),
                        timeout=service._settings.navigation_timeout,
                    )
                    body = (preflight_result.get("body") or "")
                    body_lower = body.lower()
                    blocked_tokens = ("just a moment", "cf-challenge")
                    if preflight_result.get("status") == 200 and not any(
                        token in body_lower for token in blocked_tokens
                    ):
                        return web.json_response(preflight_result)
                    logger.info(
                        "[ChallengeHarvester] Impersonate blocked for %s, escalating to Playwright",
                        url,
                    )
                except Exception as exc:
                    logger.warning(
                        "[ChallengeHarvester] Impersonate preflight failed for %s, escalating to Playwright: %s",
                        url,
                        exc,
                    )

        for attempt in range(service._settings.max_retries + 1):
            try:
                result = await service.harvest(
                    url,
                    site_key=site_key,
                    headers=payload.get("headers"),
                    wait_for_selector=payload.get("wait_for_selector"),
                    extra_headers=payload.get("extra_headers"),
                    referer=payload.get("referer"),
                    human_delay_range=payload.get("human_delay_range"),
                    # Pass new params
                    proxy=payload.get("proxy"),
                    impersonate=impersonate,
                    actions=actions,
                    sniff_network=sniff_network,
                )
                if attempt > 0:
                    logger.info("Challenge harvest succeeded on attempt %d for %s", attempt + 1, url)
                return web.json_response(result)
            except Exception as exc:  # pragma: no cover - network and Playwright errors
                last_error = exc
                error_msg = str(exc)

                if not impersonate:
                    playwright_failures += 1

                # Check error type for retry strategy
                is_timeout = "timeout" in error_msg.lower() or "TimeoutError" in type(exc).__name__
                is_crash = service._is_browser_crash_error(exc)

                should_retry = (is_timeout or is_crash) and attempt < service._settings.max_retries

                if (
                    should_use_2captcha
                    and not impersonate
                    and not twocaptcha_attempted
                    and playwright_failures >= 2
                ):
                    twocaptcha_attempted = True
                    token = await _solve_with_2captcha(url, turnstile_sitekey)
                    if token:
                        return web.json_response({
                            "url": url,
                            "site_key": site_key,
                            "status": None,
                            "body": None,
                            "cookies": None,
                            "cf_turnstile_token": token,
                            "headers": {},
                            "metadata": {
                                "engine": "2captcha",
                                "twocaptcha": True,
                                "playwright_failures": playwright_failures,
                            },
                        })

                if should_retry:
                    # If crash and haven't tried restart yet, restart browser
                    if is_crash and not browser_restart_attempted:
                        logger.warning(
                            "[ChallengeHarvester] Browser crash in handler for %s, attempting restart...",
                            url
                        )
                        try:
                            await service._restart_browser(reason="crash_in_handler")
                            browser_restart_attempted = True
                        except Exception as restart_err:
                            logger.error("Browser restart in handler failed: %s", restart_err)

                    retry_delay = service._settings.retry_delay * (attempt + 1)  # exponential backoff
                    logger.warning(
                        "Challenge harvest %s for %s (attempt %d/%d), retrying in %.1fs...",
                        "crash" if is_crash else "timeout",
                        url, attempt + 1, service._settings.max_retries + 1, retry_delay
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    # Final attempt failed - try Nodriver fallback first (CDP-less)
                    if service._settings.enable_nodriver and NODRIVER_AVAILABLE and not impersonate:
                        logger.info(
                            "[ChallengeHarvester] Playwright failed, trying Nodriver fallback for %s",
                            url
                        )
                        try:
                            fallback_result = await harvest_with_nodriver(
                                url=url,
                                proxy=payload.get("proxy"),
                                headers=payload.get("headers"),
                                timeout=service._settings.navigation_timeout,
                                wait_for_selector=payload.get("wait_for_selector"),
                            )
                            fallback_result["metadata"]["fallback"] = "nodriver"
                            logger.info("[ChallengeHarvester] Nodriver fallback succeeded for %s", url)
                            return web.json_response(fallback_result)
                        except Exception as nd_err:
                            logger.warning(
                                "[ChallengeHarvester] Nodriver fallback failed for %s: %s",
                                url, nd_err
                            )

                    # Try curl_cffi fallback if available and Nodriver didn't work/wasn't used
                    if AsyncSession is not None and not payload.get("impersonate"):
                        logger.info(
                            "[ChallengeHarvester] Trying curl_cffi fallback for %s",
                            url
                        )
                        try:
                            fallback_result = await service.harvest_with_impersonate(
                                url=url,
                                impersonate="chrome120",  # Use modern Chrome fingerprint
                                proxy=payload.get("proxy"),
                                headers=payload.get("headers"),
                                timeout=service._settings.navigation_timeout,
                            )
                            fallback_result["metadata"]["fallback"] = True
                            logger.info("[ChallengeHarvester] curl_cffi fallback succeeded for %s", url)
                            return web.json_response(fallback_result)
                        except Exception as fallback_err:
                            logger.warning(
                                "[ChallengeHarvester] curl_cffi fallback also failed for %s: %s",
                                url, fallback_err
                            )

                    # All options exhausted
                    logger.error(
                        "Challenge harvest failed after %d attempts for %s: %s",
                        attempt + 1, url, error_msg
                    )
                    return web.json_response({"error": error_msg}, status=500)

        # Should never reach here, but for safety
        return web.json_response({"error": str(last_error)}, status=500)

    app = web.Application()
    app.add_routes(routes)

    async def on_startup(_: web.Application) -> None:
        await service.startup()

    async def on_cleanup(_: web.Application) -> None:
        await service.shutdown()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = ServiceSettings()
    app = asyncio.run(create_app(settings))
    web.run_app(app, host=settings.host, port=settings.port)


if __name__ == "__main__":  # pragma: no cover - manual execution entrypoint
    main()

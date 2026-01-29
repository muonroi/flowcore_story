"""Advanced Stealth Browser Implementation - Modern anti-detection for Playwright.

This module implements advanced anti-bot bypass techniques including:
- Undetected Chromium driver patches
- Human behavior simulation (mouse movements, scrolling, typing delays)
- Advanced fingerprint randomization
- TLS/HTTP2 fingerprinting
- Canvas/WebGL/Audio noise injection

References:
- https://scrapfly.io/blog/web-scraping-cloudflare-2025/
- https://github.com/ultrafunkamsterdam/undetected-chromedriver
- https://github.com/SeleniumBase/SeleniumBase UC Mode
"""

from __future__ import annotations

import asyncio
import math
import platform as sys_platform
import random
from typing import Any

from flowcore_story.utils.browser_fingerprint import (
    apply_fingerprint_to_context_options,
    generate_fingerprint,
    get_playwright_injection_script,
)
from flowcore_story.utils.logger import logger

try:
    from playwright.async_api import Browser, BrowserContext, Page
except ImportError:
    Page = BrowserContext = Browser = None  # type: ignore


# ============================================================================
# Advanced Stealth Patches for Playwright
# ============================================================================

STEALTH_JS_SCRIPTS = [
    # 1. Remove webdriver flag
    """
    Object.defineProperty(navigator, 'webdriver', {
        get: () => false,
    });
    """,

    # 2. Mock languages properly
    """
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en', 'vi'],
    });
    """,

    # 3. Mock plugins
    """
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            {
                0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                description: "Portable Document Format",
                filename: "internal-pdf-viewer",
                length: 1,
                name: "Chrome PDF Plugin"
            },
            {
                0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                description: "Portable Document Format",
                filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                length: 1,
                name: "Chrome PDF Viewer"
            }
        ],
    });
    """,

    # 4. Mock permissions
    """
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    """,

    # 5. Add chrome object (Chromium-specific)
    """
    window.chrome = {
        runtime: {},
    };
    """,

    # 6. WebRTC Leak Protection (Graceful Mocking)
    """
    (() => {
        'use strict';
        const emptyFunction = () => {};
        const mockRTCPeerConnection = function() {
            return {
                createOffer: () => Promise.resolve({ sdp: '', type: 'offer' }),
                createAnswer: () => Promise.resolve({ sdp: '', type: 'answer' }),
                setLocalDescription: () => Promise.resolve(),
                setRemoteDescription: () => Promise.resolve(),
                addIceCandidate: () => Promise.resolve(),
                close: emptyFunction,
                onicecandidate: null,
                ontrack: null,
                oniceconnectionstatechange: null,
                onnegotiationneeded: null
            };
        };

        if (typeof window.RTCPeerConnection !== 'undefined') {
            window.RTCPeerConnection = mockRTCPeerConnection;
        }
        if (typeof window.webkitRTCPeerConnection !== 'undefined') {
            window.webkitRTCPeerConnection = mockRTCPeerConnection;
        }
        if (typeof window.RTCSessionDescription !== 'undefined') {
            window.RTCSessionDescription = emptyFunction;
        }
        if (typeof window.RTCIceCandidate !== 'undefined') {
            window.RTCIceCandidate = emptyFunction;
        }
    })();
    """,

    # 7. Mock battery API
    """
    Object.defineProperty(navigator, 'getBattery', {
        get: () => async () => ({
            charging: true,
            chargingTime: 0,
            dischargingTime: Infinity,
            level: 1.0,
        }),
    });
    """,

    # 8. Override toString methods to hide proxy
    """
    const originalToString = Function.prototype.toString;
    Function.prototype.toString = function() {
        if (this === navigator.permissions.query) {
            return 'function query() { [native code] }';
        }
        return originalToString.call(this);
    };
    """,
]

WEBGL_SPOOF_SCRIPT = """
(() => {
    'use strict';
    const vendor = 'Intel Inc.';
    const renderer = 'ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11)';
    const paramVendor = 37445;
    const paramRenderer = 37446;

    const overrideGetParameter = (proto) => {
        const original = proto.getParameter;
        proto.getParameter = function(parameter) {
            if (parameter === paramVendor) {
                return vendor;
            }
            if (parameter === paramRenderer) {
                return renderer;
            }
            return original.apply(this, arguments);
        };
    };

    if (typeof WebGLRenderingContext !== 'undefined') {
        overrideGetParameter(WebGLRenderingContext.prototype);
    }
    if (typeof WebGL2RenderingContext !== 'undefined') {
        overrideGetParameter(WebGL2RenderingContext.prototype);
    }
})();
"""


# ============================================================================
# Human Behavior Simulation
# ============================================================================

class HumanBehaviorSimulator:
    """Simulates human-like behavior to avoid bot detection."""

    @staticmethod
    async def random_delay(min_ms: float = 100, max_ms: float = 500) -> None:
        """Random delay to simulate human thinking time."""
        delay = random.uniform(min_ms, max_ms) / 1000.0
        await asyncio.sleep(delay)

    @staticmethod
    async def simulate_mouse_movement(page: Page, target_x: int, target_y: int, steps: int = 10) -> None:
        """Simulate gradual mouse movement to a target position.

        Args:
            page: Playwright page
            target_x: Target X coordinate
            target_y: Target Y coordinate
            steps: Number of intermediate steps
        """
        try:
            # Get current mouse position (start from random position if first time)
            start_x = random.randint(0, 100)
            start_y = random.randint(0, 100)
            end_x = float(target_x)
            end_y = float(target_y)

            def bezier_point(p0, p1, p2, p3, t):
                u = 1 - t
                x = (u ** 3) * p0[0] + 3 * (u ** 2) * t * p1[0] + 3 * u * (t ** 2) * p2[0] + (t ** 3) * p3[0]
                y = (u ** 3) * p0[1] + 3 * (u ** 2) * t * p1[1] + 3 * u * (t ** 2) * p2[1] + (t ** 3) * p3[1]
                return x, y

            def ease_in_out(t):
                # Smoothstep curve for acceleration/deceleration
                return t * t * (3 - 2 * t)

            def control_points(p0, p3, spread):
                return (
                    (p0[0] + (p3[0] - p0[0]) * 0.3 + random.uniform(-spread, spread),
                     p0[1] + (p3[1] - p0[1]) * 0.3 + random.uniform(-spread, spread)),
                    (p0[0] + (p3[0] - p0[0]) * 0.7 + random.uniform(-spread, spread),
                     p0[1] + (p3[1] - p0[1]) * 0.7 + random.uniform(-spread, spread)),
                )

            start = (float(start_x), float(start_y))
            end = (end_x, end_y)
            distance = math.hypot(end_x - start_x, end_y - start_y)
            spread = max(10.0, distance * 0.18)

            # Use a mid anchor to add a second Bezier segment (multiple control points).
            mid = (
                start_x + (end_x - start_x) * 0.5 + random.uniform(-spread * 0.35, spread * 0.35),
                start_y + (end_y - start_y) * 0.5 + random.uniform(-spread * 0.35, spread * 0.35),
            )
            c1, c2 = control_points(start, mid, spread)
            c3, c4 = control_points(mid, end, spread)

            step_count = max(12, steps + random.randint(-2, 6))
            jitter = max(0.4, distance * 0.004)

            for i in range(step_count):
                progress = (i + 1) / step_count
                eased = ease_in_out(progress)
                if eased < 0.5:
                    t = eased * 2
                    current_x, current_y = bezier_point(start, c1, c2, mid, t)
                else:
                    t = (eased - 0.5) * 2
                    current_x, current_y = bezier_point(mid, c3, c4, end, t)

                current_x += random.uniform(-jitter, jitter)
                current_y += random.uniform(-jitter, jitter)

                await page.mouse.move(current_x, current_y)

                # Variable speed: slower at the ends, faster in the middle.
                speed_factor = 1 - abs(0.5 - progress) * 2
                delay = random.uniform(0.006, 0.012) + (1 - speed_factor) * random.uniform(0.01, 0.025)
                await asyncio.sleep(delay)

            # Final move to exact target
            await page.mouse.move(target_x, target_y)

        except Exception as e:
            logger.debug(f"[HumanBehavior] Mouse movement failed: {e}")

    @staticmethod
    async def simulate_reading(page: Page, duration_seconds: float = 2.0) -> None:
        """Simulate reading behavior with scrolling and pauses.

        Args:
            page: Playwright page
            duration_seconds: How long to simulate reading
        """
        try:
            end_time = asyncio.get_event_loop().time() + duration_seconds

            while asyncio.get_event_loop().time() < end_time:
                # Random scroll down
                scroll_amount = random.randint(100, 300)
                await page.evaluate(f'window.scrollBy(0, {scroll_amount})')

                # Pause (simulating reading)
                await asyncio.sleep(random.uniform(0.5, 1.5))

                # Sometimes scroll up a bit (user going back)
                if random.random() < 0.2:
                    scroll_up = random.randint(50, 100)
                    await page.evaluate(f'window.scrollBy(0, -{scroll_up})')
                    await asyncio.sleep(random.uniform(0.3, 0.8))

        except Exception as e:
            logger.debug(f"[HumanBehavior] Reading simulation failed: {e}")

    @staticmethod
    async def simulate_typing(page: Page, selector: str, text: str) -> None:
        """Simulate human-like typing with delays between keystrokes.

        Args:
            page: Playwright page
            selector: CSS selector of input element
            text: Text to type
        """
        try:
            await page.click(selector)
            await asyncio.sleep(random.uniform(0.1, 0.3))

            for char in text:
                await page.type(selector, char, delay=random.uniform(50, 150))

                # Random pause (thinking/looking at screen)
                if random.random() < 0.1:
                    await asyncio.sleep(random.uniform(0.3, 0.8))

        except Exception as e:
            logger.debug(f"[HumanBehavior] Typing simulation failed: {e}")


# ============================================================================
# Advanced Stealth Context Manager
# ============================================================================

class AdvancedStealthContext:
    """Context manager for creating stealth browser contexts."""

    def __init__(
        self,
        browser: Browser,
        viewport: dict[str, int] | None = None,
        user_agent: str | None = None,
        locale: str = "en-US",
        timezone: str = "Asia/Ho_Chi_Minh",
        proxy: dict[str, str] | None = None,
        seed: str | None = None,
    ):
        self.browser = browser
        self.viewport = viewport or {"width": 1920, "height": 1080}
        self.user_agent = user_agent
        self.locale = locale
        self.timezone = timezone
        self.proxy = proxy
        self.seed = seed
        self._context: BrowserContext | None = None
        self._fingerprint = None

    async def __aenter__(self) -> BrowserContext:
        """Create and configure stealth context."""
        platform_hint = None
        if not self.user_agent:
            system = sys_platform.system().lower()
            if system.startswith("win"):
                platform_hint = "windows"
            elif system == "darwin":
                platform_hint = "macos"
            elif system == "linux":
                platform_hint = "linux"

        self._fingerprint = generate_fingerprint(
            seed=self.seed,
            user_agent=self.user_agent,
            platform=platform_hint,
        )
        profile_rng = random.Random(self._fingerprint.fingerprint_id)
        if self.viewport:
            width = self.viewport.get("width")
            height = self.viewport.get("height")
            if width and height:
                chrome_ui = profile_rng.choice([0, 24, 40, 60])
                avail_height = max(0, height - chrome_ui)
                self._fingerprint.screen_resolution = (
                    width,
                    height,
                    width,
                    avail_height,
                    self._fingerprint.screen_resolution[4],
                )

        if self.locale:
            primary = self.locale.split("-")[0]
            self._fingerprint.language = self.locale
            self._fingerprint.languages = [self.locale, primary] if primary else [self.locale]

        if self.timezone:
            timezone_offsets = {
                "Asia/Ho_Chi_Minh": 420,
                "Asia/Bangkok": 420,
                "Asia/Shanghai": 480,
                "Asia/Tokyo": 540,
                "Europe/London": 0,
                "America/New_York": -360,
            }
            offset = timezone_offsets.get(self.timezone)
            if offset is not None:
                self._fingerprint.timezone_offset = offset

        # Create context with anti-detection options
        context_options: dict[str, Any] = {
            "permissions": ["geolocation", "notifications"],
            "geolocation": {
                "latitude": 10.7769 + profile_rng.uniform(-0.1, 0.1),  # Ho Chi Minh City area
                "longitude": 106.7009 + profile_rng.uniform(-0.1, 0.1),
            },
            "is_mobile": False,
            "has_touch": profile_rng.choice([True, False]),
            "proxy": self.proxy if self.proxy else None,
        }

        if self.user_agent:
            context_options["user_agent"] = self.user_agent

        # Keep viewport/screen/locale/timezone aligned with the fingerprint profile.
        context_options = apply_fingerprint_to_context_options(self._fingerprint, context_options)
        self._context = await self.browser.new_context(**context_options)

        try:
            fingerprint_script = get_playwright_injection_script(self._fingerprint)
            await self._context.add_init_script(fingerprint_script)
        except Exception as e:
            logger.warning(f"[Stealth] Failed to inject fingerprint script: {e}")

        # Apply stealth scripts
        for script in STEALTH_JS_SCRIPTS:
            try:
                await self._context.add_init_script(script)
            except Exception as e:
                logger.warning(f"[Stealth] Failed to inject script: {e}")

        try:
            await self._context.add_init_script(WEBGL_SPOOF_SCRIPT)
        except Exception as e:
            logger.warning(f"[Stealth] Failed to inject WebGL spoof script: {e}")

        logger.debug("[Stealth] Created advanced stealth context")
        return self._context

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup context."""
        if self._context:
            try:
                await self._context.close()
            except Exception as e:
                logger.debug(f"[Stealth] Context close failed: {e}")


# ============================================================================
# Stealth Page Navigation with Behavior Simulation
# ============================================================================

async def stealth_navigate(
    page: Page,
    url: str,
    wait_until: str = "domcontentloaded",
    simulate_behavior: bool = True,
    reading_time: float = 2.0,
) -> None:
    """Navigate to URL with stealth behavior simulation.

    Args:
        page: Playwright page
        url: Target URL
        wait_until: Wait condition (domcontentloaded, load, networkidle)
        simulate_behavior: Whether to simulate human behavior
        reading_time: How long to simulate reading (seconds)
    """
    try:
        # Navigate
        await page.goto(url, wait_until=wait_until, timeout=60000)

        if simulate_behavior:
            # Random initial delay (user looking at page)
            await HumanBehaviorSimulator.random_delay(500, 1500)

            # Simulate mouse movement to random position
            viewport = page.viewport_size
            if viewport:
                target_x = random.randint(100, viewport["width"] - 100)
                target_y = random.randint(100, viewport["height"] - 100)
                await HumanBehaviorSimulator.simulate_mouse_movement(page, target_x, target_y)

            # Simulate reading behavior
            if reading_time > 0:
                await HumanBehaviorSimulator.simulate_reading(page, reading_time)

        logger.debug(f"[Stealth] Successfully navigated to {url}")

    except Exception as e:
        logger.error(f"[Stealth] Navigation failed for {url}: {e}")
        raise


# ============================================================================
# Canvas/WebGL Noise Injection
# ============================================================================

def _get_canvas_webgl_script(fingerprint) -> str:
    """Build a deterministic canvas/WebGL script tied to a fingerprint."""
    webgl = fingerprint.webgl_config
    return f"""
(function() {{
    'use strict';

    const noisePattern = '{fingerprint.canvas_noise_pattern}';
    const noiseSeed = '{fingerprint.fingerprint_id}';
    const seedBase = (() => {{
        let hash = 2166136261;
        for (let i = 0; i < noiseSeed.length; i++) {{
            hash ^= noiseSeed.charCodeAt(i);
            hash = Math.imul(hash, 16777619);
        }}
        return hash >>> 0;
    }})();

    function seededRandom(index) {{
        let x = (seedBase + index) >>> 0;
        x ^= x << 13;
        x ^= x >>> 17;
        x ^= x << 5;
        return (x >>> 0) / 4294967296;
    }}

    const noiseScale = noisePattern === 'micro_noise' ? 0.03
        : (noisePattern === 'slight_blur' ? 0.08 : 0.05);

    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(...args) {{
        const context = this.getContext('2d');
        if (context) {{
            const imageData = context.getImageData(0, 0, this.width, this.height);
            for (let i = 0; i < imageData.data.length; i += 4) {{
                const offset = (seededRandom(i) * 2 - 1) * noiseScale;
                imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + offset));
            }}
            context.putImageData(imageData, 0, 0);
        }}
        return originalToDataURL.apply(this, args);
    }};

    const getParameterOriginal = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {{
        if (parameter === 37445) {{
            return '{webgl["unmasked_vendor"]}';
        }}
        if (parameter === 37446) {{
            return '{webgl["unmasked_renderer"]}';
        }}
        return getParameterOriginal.call(this, parameter);
    }};

    const getParameter2Original = WebGL2RenderingContext.prototype.getParameter;
    WebGL2RenderingContext.prototype.getParameter = function(parameter) {{
        if (parameter === 37445) {{
            return '{webgl["unmasked_vendor"]}';
        }}
        if (parameter === 37446) {{
            return '{webgl["unmasked_renderer"]}';
        }}
        return getParameter2Original.call(this, parameter);
    }};
}})();
"""


async def inject_canvas_noise(
    context: BrowserContext,
    fingerprint=None,
    user_agent: str | None = None,
    platform: str | None = None,
) -> None:
    """Inject canvas/WebGL noise to prevent fingerprinting."""
    try:
        if fingerprint is None:
            fingerprint = generate_fingerprint(user_agent=user_agent, platform=platform)
        script = _get_canvas_webgl_script(fingerprint)
        await context.add_init_script(script)
        logger.debug("[Stealth] Injected canvas noise protection")
    except Exception as e:
        logger.warning(f"[Stealth] Canvas noise injection failed: {e}")


# ============================================================================
# Main API
# ============================================================================

async def create_advanced_stealth_page(
    browser: Browser,
    url: str,
    user_agent: str | None = None,
    simulate_behavior: bool = True,
    reading_time: float = 2.0,
) -> tuple[BrowserContext, Page]:
    """Create a stealth page and navigate to URL.

    Returns:
        Tuple of (context, page)
    """
    async with AdvancedStealthContext(browser, user_agent=user_agent) as context:
        # Create page
        page = await context.new_page()

        # Navigate with behavior simulation
        await stealth_navigate(page, url, simulate_behavior=simulate_behavior, reading_time=reading_time)

        return context, page


async def warmup_browser_session(
    page: Page,
    warmup_sites: list[str] | None = None,
    duration_seconds: float = 10.0,
) -> None:
    """Warm-up browser session by visiting reputable sites to build trust score.
    
    Args:
        page: Playwright page
        warmup_sites: List of sites to visit
        duration_seconds: Total target duration
    """
    default_sites = [
        "https://www.google.com",
        "https://www.youtube.com",
        "https://www.reddit.com",
        "https://www.wikipedia.org",
    ]
    sites = warmup_sites or default_sites
    
    # Shuffle and pick 2 sites to keep it efficient but effective
    random_sites = list(sites)
    random.shuffle(random_sites)
    
    for site in random_sites[:2]:
        try:
            logger.debug(f"[Stealth] Warming up with {site}...")
            # Use shorter timeout for warmup
            await page.goto(site, wait_until="domcontentloaded", timeout=15000)
            
            # Simulate brief interaction
            await HumanBehaviorSimulator.random_delay(800, 2000)
            
            # Simple scroll
            scroll_amount = random.randint(200, 500)
            await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            
            await asyncio.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            logger.debug(f"[Stealth] Warmup failed for {site}: {e}")
            continue
    
    logger.info("[Stealth] Browser warmup completed")

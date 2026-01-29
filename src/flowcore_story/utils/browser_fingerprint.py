"""Advanced browser fingerprinting với randomization cho canvas, WebGL, audio, fonts.

Module này cung cấp các kỹ thuật chống phát hiện bot tiên tiến bằng cách randomize
các dấu vân tay trình duyệt một cách tự nhiên và phù hợp.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass
from typing import Any

# Canvas fingerprint noise patterns
CANVAS_NOISE_PATTERNS = [
    "subtle_shift",  # Dịch chuyển nhẹ pixel values
    "micro_noise",   # Thêm noise rất nhỏ
    "slight_blur",   # Làm mờ nhẹ
]

# Consistency rule: platform string, UA patterns, and WebGL config must describe the same OS family.
# We select a single profile first, then randomize only within its bounds to avoid cross-OS mismatches.
CONSISTENT_PROFILES = [
    {
        "name": "windows_desktop",
        "platform": "Win32",
        "platform_key": "windows",
        "user_agent_patterns": [
            "Windows NT 10.0; Win64; x64",
            "Windows NT 10.0; WOW64",
            "Windows NT 6.1; Win64; x64",
        ],
        "webgl_configs": [
            {
                "vendor": "Google Inc. (NVIDIA)",
                "renderer": "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
                "unmasked_vendor": "Google Inc. (NVIDIA)",
                "unmasked_renderer": "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)"
            },
            {
                "vendor": "Google Inc. (NVIDIA)",
                "renderer": "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)",
                "unmasked_vendor": "Google Inc. (NVIDIA)",
                "unmasked_renderer": "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)"
            },
            {
                "vendor": "Google Inc. (AMD)",
                "renderer": "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)",
                "unmasked_vendor": "Google Inc. (AMD)",
                "unmasked_renderer": "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)"
            },
            {
                "vendor": "Google Inc. (Intel)",
                "renderer": "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)",
                "unmasked_vendor": "Google Inc. (Intel)",
                "unmasked_renderer": "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)"
            },
        ],
    },
    {
        "name": "macos_apple_silicon",
        "platform": "MacIntel",
        "platform_key": "macos",
        "user_agent_patterns": [
            "Macintosh; Intel Mac OS X 10_15",
            "Macintosh; Intel Mac OS X 11_",
            "Macintosh; Intel Mac OS X 12_",
            "Macintosh; Intel Mac OS X 13_",
        ],
        "webgl_configs": [
            {
                "vendor": "Apple Inc.",
                "renderer": "Apple M1 Pro",
                "unmasked_vendor": "Apple Inc.",
                "unmasked_renderer": "Apple M1 Pro"
            },
            {
                "vendor": "Apple Inc.",
                "renderer": "Apple M1",
                "unmasked_vendor": "Apple Inc.",
                "unmasked_renderer": "Apple M1"
            },
            {
                "vendor": "Apple Inc.",
                "renderer": "Apple M2",
                "unmasked_vendor": "Apple Inc.",
                "unmasked_renderer": "Apple M2"
            },
        ],
    },
    {
        "name": "linux_x11",
        "platform": "Linux",
        "platform_key": "linux",
        "user_agent_patterns": [
            "X11; Linux x86_64",
            "Linux x86_64",
            "Ubuntu; Linux x86_64",
        ],
        "webgl_configs": [
            {
                "vendor": "Intel Inc.",
                "renderer": "Mesa Intel(R) UHD Graphics 620 (KBL GT2)",
                "unmasked_vendor": "Intel Open Source Technology Center",
                "unmasked_renderer": "Mesa DRI Intel(R) UHD Graphics 620 (Kabylake GT2)"
            },
            {
                "vendor": "Intel Inc.",
                "renderer": "Mesa Intel(R) UHD Graphics 630 (CFL GT2)",
                "unmasked_vendor": "Intel Open Source Technology Center",
                "unmasked_renderer": "Mesa DRI Intel(R) UHD Graphics 630 (Coffee Lake GT2)"
            },
        ],
    },
]

# Audio context fingerprint variations
AUDIO_CONTEXT_CONFIGS = [
    {"sampleRate": 44100, "channelCount": 2, "channelCountMode": "max", "latencyHint": "interactive"},
    {"sampleRate": 48000, "channelCount": 2, "channelCountMode": "max", "latencyHint": "interactive"},
    {"sampleRate": 44100, "channelCount": 2, "channelCountMode": "explicit", "latencyHint": "balanced"},
    {"sampleRate": 48000, "channelCount": 2, "channelCountMode": "explicit", "latencyHint": "balanced"},
]

# Font sets thường thấy trên các hệ điều hành khác nhau
FONT_SETS = {
    "windows": [
        "Arial", "Arial Black", "Calibri", "Cambria", "Cambria Math", "Comic Sans MS",
        "Consolas", "Courier New", "Georgia", "Impact", "Lucida Console", "Lucida Sans Unicode",
        "Microsoft Sans Serif", "Palatino Linotype", "Segoe UI", "Tahoma", "Times New Roman",
        "Trebuchet MS", "Verdana"
    ],
    "macos": [
        "American Typewriter", "Andale Mono", "Arial", "Arial Black", "Arial Narrow", "Arial Rounded MT Bold",
        "Arial Unicode MS", "Avenir", "Avenir Next", "Avenir Next Condensed", "Baskerville", "Big Caslon",
        "Bodoni 72", "Bradley Hand", "Brush Script MT", "Chalkboard", "Chalkduster", "Comic Sans MS",
        "Copperplate", "Courier", "Courier New", "Didot", "Futura", "Geneva", "Georgia", "Gill Sans",
        "Helvetica", "Helvetica Neue", "Herculanum", "Hoefler Text", "Impact", "Lucida Grande",
        "Luminari", "Marker Felt", "Menlo", "Monaco", "Noteworthy", "Optima", "Palatino", "Papyrus",
        "Phosphate", "Rockwell", "Savoye LET", "SignPainter", "Skia", "Snell Roundhand", "Tahoma",
        "Times", "Times New Roman", "Trattatello", "Trebuchet MS", "Verdana", "Zapfino"
    ],
    "linux": [
        "Arial", "Courier New", "DejaVu Sans", "DejaVu Sans Mono", "DejaVu Serif", "FreeMono",
        "FreeSans", "FreeSerif", "Georgia", "Liberation Mono", "Liberation Sans", "Liberation Serif",
        "Nimbus Mono L", "Nimbus Roman No9 L", "Nimbus Sans L", "Tahoma", "Times New Roman",
        "Ubuntu", "Ubuntu Condensed", "Ubuntu Mono", "Verdana"
    ]
}

# Screen resolutions thường gặp (width, height, availWidth, availHeight, colorDepth)
SCREEN_RESOLUTIONS = [
    (1920, 1080, 1920, 1040, 24),
    (1920, 1080, 1920, 1080, 24),
    (2560, 1440, 2560, 1400, 24),
    (2560, 1440, 2560, 1440, 24),
    (3840, 2160, 3840, 2120, 24),
    (1536, 864, 1536, 824, 24),
    (1440, 900, 1440, 877, 24),
    (1680, 1050, 1680, 1010, 24),
    (1366, 768, 1366, 728, 24),
    (1280, 720, 1280, 680, 24),
    (1280, 800, 1280, 760, 24),
]

# Navigator properties variations
NAVIGATOR_PROPS = {
    "hardwareConcurrency": [2, 4, 6, 8, 12, 16],
    "deviceMemory": [2, 4, 8, 16, 32],
    "maxTouchPoints": [0, 5, 10],
}


@dataclass
class BrowserFingerprint:
    """Đại diện cho một bộ dấu vân tay trình duyệt hoàn chỉnh."""

    fingerprint_id: str
    canvas_noise_pattern: str
    webgl_config: dict[str, str]
    audio_context_config: dict[str, Any]
    fonts: list[str]
    screen_resolution: tuple[int, int, int, int, int]
    navigator_props: dict[str, Any]
    timezone_offset: int
    language: str
    languages: list[str]
    platform: str

    def to_dict(self) -> dict[str, Any]:
        """Convert fingerprint to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert fingerprint to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BrowserFingerprint:
        """Create fingerprint from dictionary."""
        return cls(**data)


def _normalize_platform_key(platform: str | None) -> str | None:
    """Normalize platform hints to internal keys."""
    if not platform:
        return None
    platform_lower = platform.lower()
    if platform_lower in {"windows", "win32", "win64"}:
        return "windows"
    if "mac" in platform_lower:
        return "macos"
    if "linux" in platform_lower or "x11" in platform_lower:
        return "linux"
    return None


def _infer_platform_key_from_user_agent(user_agent: str | None) -> str | None:
    """Infer platform key from user agent string."""
    if not user_agent:
        return None
    ua_lower = user_agent.lower()
    if "windows" in ua_lower or "win64" in ua_lower or "win32" in ua_lower:
        return "windows"
    if "macintosh" in ua_lower or "mac os" in ua_lower:
        return "macos"
    if "linux" in ua_lower or "x11" in ua_lower:
        return "linux"
    return None


def _match_profiles_for_user_agent(user_agent: str) -> list[dict[str, Any]]:
    """Match profiles whose UA patterns appear in the user agent string."""
    ua_lower = user_agent.lower()
    return [
        profile for profile in CONSISTENT_PROFILES
        if any(pattern.lower() in ua_lower for pattern in profile["user_agent_patterns"])
    ]


def _select_consistent_profile(
    rng: random.Random,
    platform_key: str | None,
    user_agent: str | None
) -> dict[str, Any]:
    """Pick a profile that keeps platform, UA, and WebGL consistent."""
    candidates = CONSISTENT_PROFILES

    if user_agent:
        ua_matches = _match_profiles_for_user_agent(user_agent)
        if ua_matches:
            candidates = ua_matches

    if platform_key:
        platform_matches = [p for p in candidates if p["platform_key"] == platform_key]
        if platform_matches:
            candidates = platform_matches

    return rng.choice(candidates)


def _deterministic_choice(values: list[str], fingerprint_id: str) -> str:
    """Deterministic choice based on fingerprint ID to keep noise stable per session."""
    seeded_rng = random.Random(fingerprint_id)
    return seeded_rng.choice(values)


def generate_fingerprint(
    seed: str | None = None,
    platform: str | None = None,
    user_agent: str | None = None
) -> BrowserFingerprint:
    """Tạo một bộ dấu vân tay trình duyệt ngẫu nhiên nhưng consistent.

    Args:
        seed: Optional seed để tạo fingerprint deterministic
        platform: Platform hint ("windows", "macos", "linux", or "Win32/MacIntel/Linux")
        user_agent: User agent string để infer platform nếu cần

    Returns:
        BrowserFingerprint object với các thuộc tính đã được randomize
    """
    # Use seed for deterministic randomization if provided
    rng = random.Random(seed) if seed else random.Random()

    platform_key = _normalize_platform_key(platform)
    if platform_key is None:
        platform_key = _infer_platform_key_from_user_agent(user_agent)

    # Select a profile first to keep OS, UA patterns, and WebGL aligned.
    profile = _select_consistent_profile(rng, platform_key, user_agent)

    # Generate fingerprint ID
    if seed:
        fingerprint_id = hashlib.sha256(seed.encode()).hexdigest()[:16]
    else:
        fingerprint_id = hashlib.sha256(str(rng.random()).encode()).hexdigest()[:16]

    # Select components based on platform
    canvas_noise = _deterministic_choice(CANVAS_NOISE_PATTERNS, fingerprint_id)
    webgl_config = rng.choice(profile["webgl_configs"])
    audio_config = rng.choice(AUDIO_CONTEXT_CONFIGS)

    # Select appropriate fonts for platform
    fonts = FONT_SETS.get(profile["platform_key"], FONT_SETS["windows"]).copy()
    rng.shuffle(fonts)

    # Select screen resolution
    screen_res = rng.choice(SCREEN_RESOLUTIONS)

    # Navigator properties
    navigator_props = {
        "hardwareConcurrency": rng.choice(NAVIGATOR_PROPS["hardwareConcurrency"]),
        "deviceMemory": rng.choice(NAVIGATOR_PROPS["deviceMemory"]),
        "maxTouchPoints": rng.choice(NAVIGATOR_PROPS["maxTouchPoints"]),
    }

    # Timezone offset (-12 to +14 hours in minutes)
    timezone_offset = rng.choice([-720, -660, -600, -540, -480, -420, -360, -300, -240, -180, -120, -60, 0, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 660, 720, 780, 840])

    # Language settings
    languages_map = {
        "en-US": ["en-US", "en"],
        "en-GB": ["en-GB", "en"],
        "de-DE": ["de-DE", "de"],
        "fr-FR": ["fr-FR", "fr"],
        "es-ES": ["es-ES", "es"],
        "it-IT": ["it-IT", "it"],
        "pt-BR": ["pt-BR", "pt"],
        "ja-JP": ["ja-JP", "ja"],
        "zh-CN": ["zh-CN", "zh"],
        "ko-KR": ["ko-KR", "ko"],
        "ru-RU": ["ru-RU", "ru"],
        "vi-VN": ["vi-VN", "vi"],
    }
    language = rng.choice(list(languages_map.keys()))
    languages = languages_map[language]

    # Platform string
    return BrowserFingerprint(
        fingerprint_id=fingerprint_id,
        canvas_noise_pattern=canvas_noise,
        webgl_config=webgl_config,
        audio_context_config=audio_config,
        fonts=fonts,
        screen_resolution=screen_res,
        navigator_props=navigator_props,
        timezone_offset=timezone_offset,
        language=language,
        languages=languages,
        platform=profile["platform"],
    )


def get_playwright_injection_script(fingerprint: BrowserFingerprint) -> str:
    """Tạo JavaScript injection script cho Playwright để apply fingerprint.

    Args:
        fingerprint: BrowserFingerprint object chứa các thuộc tính cần inject

    Returns:
        JavaScript code string để inject vào page
    """
    webgl = fingerprint.webgl_config
    # audio = fingerprint.audio_context_config  # Reserved for future use
    screen = fingerprint.screen_resolution
    nav_props = fingerprint.navigator_props

    script = f"""
(function() {{
    'use strict';

    // Canvas fingerprinting protection
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    const originalToBlob = HTMLCanvasElement.prototype.toBlob;
    const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;

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

    // Add subtle noise to canvas operations
    HTMLCanvasElement.prototype.toDataURL = function(...args) {{
        const context = this.getContext('2d');
        if (context) {{
            const imageData = context.getImageData(0, 0, this.width, this.height);
            for (let i = 0; i < imageData.data.length; i += 4) {{
                // Deterministic noise per fingerprint to keep canvas stable per session
                const offset = (seededRandom(i) * 2 - 1) * noiseScale;
                imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + offset));
            }}
            context.putImageData(imageData, 0, 0);
        }}
        return originalToDataURL.apply(this, args);
    }};

    // WebGL fingerprinting override
    const getParameterOriginal = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {{
        if (parameter === 37445) {{ // UNMASKED_VENDOR_WEBGL
            return '{webgl["unmasked_vendor"]}';
        }}
        if (parameter === 37446) {{ // UNMASKED_RENDERER_WEBGL
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

    // Audio context fingerprinting protection
    const AudioContextOriginal = window.AudioContext || window.webkitAudioContext;
    if (AudioContextOriginal) {{
        const audioNoise = (seededRandom(1048576) * 2 - 1) * 0.000001;
        const OriginalAnalyser = AudioContextOriginal.prototype.createAnalyser;
        AudioContextOriginal.prototype.createAnalyser = function() {{
            const analyser = OriginalAnalyser.apply(this, arguments);
            const originalGetFloatFrequencyData = analyser.getFloatFrequencyData;
            analyser.getFloatFrequencyData = function(array) {{
                originalGetFloatFrequencyData.apply(this, arguments);
                for (let i = 0; i < array.length; i++) {{
                    array[i] += audioNoise;
                }}
                return array;
            }};
            return analyser;
        }};
    }}

    // Screen resolution override
    Object.defineProperty(window.screen, 'width', {{
        get: () => {screen[0]}
    }});
    Object.defineProperty(window.screen, 'height', {{
        get: () => {screen[1]}
    }});
    Object.defineProperty(window.screen, 'availWidth', {{
        get: () => {screen[2]}
    }});
    Object.defineProperty(window.screen, 'availHeight', {{
        get: () => {screen[3]}
    }});
    Object.defineProperty(window.screen, 'colorDepth', {{
        get: () => {screen[4]}
    }});
    Object.defineProperty(window.screen, 'pixelDepth', {{
        get: () => {screen[4]}
    }});

    // Navigator properties override
    Object.defineProperty(navigator, 'hardwareConcurrency', {{
        get: () => {nav_props["hardwareConcurrency"]}
    }});
    Object.defineProperty(navigator, 'deviceMemory', {{
        get: () => {nav_props["deviceMemory"]}
    }});
    Object.defineProperty(navigator, 'maxTouchPoints', {{
        get: () => {nav_props["maxTouchPoints"]}
    }});

    // Language override
    Object.defineProperty(navigator, 'language', {{
        get: () => '{fingerprint.language}'
    }});
    Object.defineProperty(navigator, 'languages', {{
        get: () => {json.dumps(fingerprint.languages)}
    }});

    // Platform override
    Object.defineProperty(navigator, 'platform', {{
        get: () => '{fingerprint.platform}'
    }});

    // Timezone offset
    const originalGetTimezoneOffset = Date.prototype.getTimezoneOffset;
    Date.prototype.getTimezoneOffset = function() {{
        return {fingerprint.timezone_offset};
    }};

    // Font detection protection (basic)
    const originalGetComputedStyle = window.getComputedStyle;
    window.getComputedStyle = function(element, pseudoElt) {{
        const style = originalGetComputedStyle(element, pseudoElt);
        const originalGetPropertyValue = style.getPropertyValue;
        style.getPropertyValue = function(property) {{
            if (property === 'font-family') {{
                // Return one of our fonts randomly
                const fonts = {json.dumps(fingerprint.fonts[:10])};
                const fontIndex = Math.floor(seededRandom(8192) * fonts.length);
                return fonts[fontIndex];
            }}
            return originalGetPropertyValue.call(this, property);
        }};
        return style;
    }};

    console.log('[Fingerprint] Applied fingerprint ID:', '{fingerprint.fingerprint_id}');
}})();
"""
    return script


def apply_fingerprint_to_context_options(
    fingerprint: BrowserFingerprint,
    base_options: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Apply fingerprint parameters to Playwright context options.

    Args:
        fingerprint: BrowserFingerprint object
        base_options: Existing context options to merge with

    Returns:
        Dict with context options including fingerprint parameters
    """
    options = base_options.copy() if base_options else {}

    screen = fingerprint.screen_resolution

    # Update viewport
    options["viewport"] = {
        "width": screen[0],
        "height": screen[1]
    }

    # Update screen size
    options["screen"] = {
        "width": screen[0],
        "height": screen[1]
    }

    # Update locale and timezone
    options["locale"] = fingerprint.language
    options["timezone_id"] = _get_timezone_id(fingerprint.timezone_offset)

    # Keep UI hints stable per fingerprint to avoid session inconsistencies.
    stable_rng = random.Random(fingerprint.fingerprint_id)
    options["color_scheme"] = stable_rng.choice(["light", "dark", "no-preference"])
    options["device_scale_factor"] = stable_rng.choice([1.0, 1.25, 1.5, 2.0])

    return options


def _get_timezone_id(offset_minutes: int) -> str:
    """Convert timezone offset in minutes to timezone ID."""
    timezone_map = {
        -720: "Pacific/Midway",
        -660: "Pacific/Honolulu",
        -600: "America/Anchorage",
        -540: "America/Los_Angeles",
        -480: "America/Denver",
        -420: "America/Chicago",
        -360: "America/New_York",
        -300: "America/Caracas",
        -240: "America/Halifax",
        -180: "America/Sao_Paulo",
        -120: "Atlantic/South_Georgia",
        -60: "Atlantic/Azores",
        0: "Europe/London",
        60: "Europe/Berlin",
        120: "Europe/Athens",
        180: "Europe/Moscow",
        240: "Asia/Dubai",
        300: "Asia/Karachi",
        360: "Asia/Dhaka",
        420: "Asia/Ho_Chi_Minh",
        480: "Asia/Shanghai",
        540: "Asia/Tokyo",
        600: "Australia/Brisbane",
        660: "Pacific/Guadalcanal",
        720: "Pacific/Auckland",
        780: "Pacific/Tongatapu",
        840: "Pacific/Kiritimati",
    }
    return timezone_map.get(offset_minutes, "Europe/London")


__all__ = [
    "CONSISTENT_PROFILES",
    "BrowserFingerprint",
    "generate_fingerprint",
    "get_playwright_injection_script",
    "apply_fingerprint_to_context_options",
]

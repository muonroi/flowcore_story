"""Headers Randomization - Order, casing, spacing control.

Module này cung cấp khả năng randomize HTTP headers một cách có kiểm soát
để tạo diversity và tránh pattern detection.
"""

from __future__ import annotations

import random
from collections import OrderedDict

# Standard header names with their common casings
HEADER_CASING_VARIANTS = {
    "user-agent": ["User-Agent", "user-agent", "USER-AGENT"],
    "accept": ["Accept", "accept"],
    "accept-encoding": ["Accept-Encoding", "accept-encoding"],
    "accept-language": ["Accept-Language", "accept-language"],
    "cache-control": ["Cache-Control", "cache-control"],
    "connection": ["Connection", "connection"],
    "content-type": ["Content-Type", "content-type"],
    "cookie": ["Cookie", "cookie"],
    "host": ["Host", "host"],
    "referer": ["Referer", "referer"],
    "upgrade-insecure-requests": ["Upgrade-Insecure-Requests", "upgrade-insecure-requests"],
    "sec-fetch-site": ["Sec-Fetch-Site", "sec-fetch-site"],
    "sec-fetch-mode": ["Sec-Fetch-Mode", "sec-fetch-mode"],
    "sec-fetch-user": ["Sec-Fetch-User", "sec-fetch-user"],
    "sec-fetch-dest": ["Sec-Fetch-Dest", "sec-fetch-dest"],
    "sec-ch-ua": ["Sec-CH-UA", "sec-ch-ua"],
    "sec-ch-ua-mobile": ["Sec-CH-UA-Mobile", "sec-ch-ua-mobile"],
    "sec-ch-ua-platform": ["Sec-CH-UA-Platform", "sec-ch-ua-platform"],
}

# Browser-specific header orders
HEADER_ORDERS = {
    "chrome": [
        "host",
        "connection",
        "cache-control",
        "sec-ch-ua",
        "sec-ch-ua-mobile",
        "sec-ch-ua-platform",
        "upgrade-insecure-requests",
        "user-agent",
        "accept",
        "sec-fetch-site",
        "sec-fetch-mode",
        "sec-fetch-user",
        "sec-fetch-dest",
        "accept-encoding",
        "accept-language",
        "cookie",
    ],
    "firefox": [
        "host",
        "user-agent",
        "accept",
        "accept-language",
        "accept-encoding",
        "connection",
        "upgrade-insecure-requests",
        "sec-fetch-dest",
        "sec-fetch-mode",
        "sec-fetch-site",
        "cache-control",
        "cookie",
    ],
    "safari": [
        "host",
        "connection",
        "upgrade-insecure-requests",
        "user-agent",
        "accept",
        "accept-language",
        "accept-encoding",
        "cookie",
    ],
    "edge": [
        "host",
        "connection",
        "cache-control",
        "sec-ch-ua",
        "sec-ch-ua-mobile",
        "sec-ch-ua-platform",
        "upgrade-insecure-requests",
        "user-agent",
        "accept",
        "sec-fetch-site",
        "sec-fetch-mode",
        "sec-fetch-user",
        "sec-fetch-dest",
        "accept-encoding",
        "accept-language",
        "cookie",
    ],
}

# Value spacing patterns
SPACING_PATTERNS = {
    "standard": lambda v: v,  # No extra spacing
    "extra_space": lambda v: v.replace(",", ", "),  # Add space after commas
    "no_space": lambda v: v.replace(", ", ","),  # Remove spaces after commas
}


class HeadersRandomizer:
    """Randomize HTTP headers with control over order, casing, and spacing."""

    def __init__(
        self,
        browser_type: str = "chrome",
        randomize_order: bool = True,
        randomize_casing: bool = True,
        randomize_spacing: bool = False,
    ):
        """Initialize headers randomizer.

        Args:
            browser_type: Browser type for header order ("chrome", "firefox", "safari", "edge")
            randomize_order: Whether to randomize header order slightly
            randomize_casing: Whether to randomize header name casing
            randomize_spacing: Whether to randomize header value spacing
        """
        self.browser_type = browser_type
        self.randomize_order = randomize_order
        self.randomize_casing = randomize_casing
        self.randomize_spacing = randomize_spacing
        self._base_order = HEADER_ORDERS.get(browser_type, HEADER_ORDERS["chrome"])

    def apply(self, headers: dict[str, str]) -> dict[str, str]:
        """Apply randomization to headers.

        Args:
            headers: Input headers dictionary

        Returns:
            Randomized headers dictionary with controlled order
        """
        # Normalize header names to lowercase for processing
        # IMPORTANT: list() creates snapshot to prevent "dictionary changed size" in concurrent async
        normalized = {k.lower(): v for k, v in tuple(headers.items())}

        # Get order for this request
        order = self._get_header_order(list(normalized.keys()))

        # Build result with proper order and randomization
        result = OrderedDict()

        for header_name in order:
            if header_name not in normalized:
                continue

            # Get value
            value = normalized[header_name]

            # Randomize spacing if enabled
            if self.randomize_spacing:
                value = self._randomize_spacing(value)

            # Randomize casing if enabled
            if self.randomize_casing:
                display_name = self._randomize_casing(header_name)
            else:
                display_name = self._get_standard_casing(header_name)

            result[display_name] = value

        # Add any headers not in base order (custom headers)
        # IMPORTANT: list() creates snapshot to prevent "dictionary changed size" in concurrent async
        for header_name, value in tuple(normalized.items()):
            if header_name not in order:
                display_name = self._get_standard_casing(header_name)
                result[display_name] = value

        return dict(result)

    def _get_header_order(self, present_headers: list[str]) -> list[str]:
        """Get header order for current request.

        Args:
            present_headers: List of header names that are present

        Returns:
            Ordered list of header names
        """
        # Start with base order
        order = [h for h in self._base_order if h in present_headers]

        # Slight randomization if enabled (swap adjacent pairs occasionally)
        if self.randomize_order and len(order) > 1:
            order = self._randomize_order_slightly(order)

        return order

    def _randomize_order_slightly(self, order: list[str]) -> list[str]:
        """Randomize order slightly by swapping adjacent pairs.

        Args:
            order: Base order

        Returns:
            Slightly randomized order
        """
        result = order.copy()

        # Swap adjacent pairs with 15% probability
        for i in range(len(result) - 1):
            if random.random() < 0.15:
                result[i], result[i + 1] = result[i + 1], result[i]

        return result

    def _randomize_casing(self, header_name: str) -> str:
        """Randomize header name casing.

        Args:
            header_name: Header name (lowercase)

        Returns:
            Header name with randomized casing
        """
        variants = HEADER_CASING_VARIANTS.get(header_name)
        if variants:
            return random.choice(variants)

        # Default to standard casing if no variants defined
        return self._get_standard_casing(header_name)

    def _get_standard_casing(self, header_name: str) -> str:
        """Get standard casing for header name.

        Args:
            header_name: Header name (lowercase)

        Returns:
            Header name with standard casing
        """
        # Use first variant if available, otherwise title case
        variants = HEADER_CASING_VARIANTS.get(header_name)
        if variants:
            return variants[0]

        # Default: Title-Case-With-Hyphens
        return "-".join(word.capitalize() for word in header_name.split("-"))

    def _randomize_spacing(self, value: str) -> str:
        """Randomize spacing in header value.

        Args:
            value: Header value

        Returns:
            Value with randomized spacing
        """
        # Randomly choose spacing pattern
        pattern_name = random.choice(["standard", "extra_space"])
        pattern_func = SPACING_PATTERNS[pattern_name]
        return pattern_func(value)


def infer_browser_from_user_agent(user_agent: str | None) -> str:
    """Infer browser type from User-Agent string.

    Args:
        user_agent: User-Agent string

    Returns:
        Browser type: "chrome", "firefox", "safari", or "edge"
    """
    if not user_agent:
        return "chrome"

    ua_lower = user_agent.lower()

    if "edg/" in ua_lower or "edge/" in ua_lower:
        return "edge"
    elif "firefox" in ua_lower:
        return "firefox"
    elif "safari" in ua_lower and "chrome" not in ua_lower:
        return "safari"
    else:
        # Default to Chrome (includes Chromium-based browsers)
        return "chrome"


def randomize_headers(
    headers: dict[str, str],
    user_agent: str | None = None,
    browser_type: str | None = None,
    randomize_order: bool = True,
    randomize_casing: bool = False,
    randomize_spacing: bool = False,
) -> dict[str, str]:
    """Convenience function to randomize headers.

    Args:
        headers: Input headers
        user_agent: User-Agent string (for inferring browser type)
        browser_type: Explicit browser type (overrides inference)
        randomize_order: Whether to randomize order
        randomize_casing: Whether to randomize casing
        randomize_spacing: Whether to randomize spacing

    Returns:
        Randomized headers dictionary
    """
    # Determine browser type
    if browser_type is None:
        if user_agent:
            browser_type = infer_browser_from_user_agent(user_agent)
        else:
            # Try to get from headers
            ua_from_headers = headers.get("User-Agent") or headers.get("user-agent")
            browser_type = infer_browser_from_user_agent(ua_from_headers)

    # Create randomizer and apply
    randomizer = HeadersRandomizer(
        browser_type=browser_type,
        randomize_order=randomize_order,
        randomize_casing=randomize_casing,
        randomize_spacing=randomize_spacing,
    )

    return randomizer.apply(headers)


def get_realistic_header_order(browser_type: str = "chrome") -> list[str]:
    """Get realistic header order for browser type.

    Args:
        browser_type: Browser type

    Returns:
        List of header names in order
    """
    return HEADER_ORDERS.get(browser_type, HEADER_ORDERS["chrome"]).copy()


def add_sec_ch_ua_headers(
    headers: dict[str, str],
    user_agent: str,
    mobile: bool = False,
) -> dict[str, str]:
    """Add Sec-CH-UA headers based on User-Agent.

    Args:
        headers: Existing headers
        user_agent: User-Agent string
        mobile: Whether this is mobile

    Returns:
        Headers with Sec-CH-UA headers added
    """
    result = headers.copy()

    # Only add for Chromium-based browsers
    if "chrome" not in user_agent.lower() and "edg" not in user_agent.lower():
        return result

    # Extract version from UA
    import re
    chrome_match = re.search(r'Chrome/(\d+)', user_agent)
    if chrome_match:
        version = chrome_match.group(1)
        result["Sec-CH-UA"] = f'"Chromium";v="{version}", "Google Chrome";v="{version}", "Not-A.Brand";v="24"'

    # Mobile flag
    result["Sec-CH-UA-Mobile"] = "?1" if mobile else "?0"

    # Platform
    if "Windows" in user_agent:
        result["Sec-CH-UA-Platform"] = '"Windows"'
    elif "Macintosh" in user_agent:
        result["Sec-CH-UA-Platform"] = '"macOS"'
    elif "Linux" in user_agent:
        result["Sec-CH-UA-Platform"] = '"Linux"'
    elif "Android" in user_agent:
        result["Sec-CH-UA-Platform"] = '"Android"'

    return result


__all__ = [
    "HeadersRandomizer",
    "randomize_headers",
    "infer_browser_from_user_agent",
    "get_realistic_header_order",
    "add_sec_ch_ua_headers",
    "HEADER_ORDERS",
]


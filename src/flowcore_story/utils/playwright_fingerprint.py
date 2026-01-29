"""Playwright Fingerprint Integration - Apply advanced browser fingerprints to Playwright contexts.

This module integrates browser fingerprinting (Canvas, WebGL, Audio, Font) with Playwright
contexts, allowing for consistent and diverse fingerprinting across requests.

Supports integration with FingerprintJS Pro and Multilogin-style APIs.
"""

from __future__ import annotations

from typing import Any

from flowcore_story.utils.browser_fingerprint import (
    apply_fingerprint_to_context_options,
    get_playwright_injection_script,
)
from flowcore_story.utils.fingerprint_pool import FingerprintProfile
from flowcore_story.utils.logger import logger


async def apply_profile_to_playwright_context(
    context,
    profile: FingerprintProfile,
) -> None:
    """Apply fingerprint profile to an existing Playwright context.

    Args:
        context: Playwright browser context
        profile: FingerprintProfile to apply
    """
    try:
        # Inject fingerprint scripts into all new pages
        browser_fp = profile.browser_fingerprint
        injection_script = get_playwright_injection_script(browser_fp)

        # Add init script to context (will be applied to all pages)
        await context.add_init_script(injection_script)

        logger.debug(
            "[Playwright] Applied fingerprint profile %s to context",
            profile.profile_id
        )
    except Exception as e:
        logger.warning(
            "[Playwright] Failed to apply fingerprint profile: %s",
            e,
            exc_info=True
        )


def create_playwright_context_options(
    profile: FingerprintProfile,
    base_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create Playwright context options with fingerprint applied.

    Args:
        profile: FingerprintProfile to apply
        base_options: Base context options to merge with

    Returns:
        Dict of context options with fingerprint parameters
    """
    browser_fp = profile.browser_fingerprint
    options = apply_fingerprint_to_context_options(browser_fp, base_options)

    # Add user agent from profile
    options["user_agent"] = profile.user_agent

    return options


async def create_fingerprinted_context(
    browser,
    profile: FingerprintProfile,
    base_options: dict[str, Any] | None = None,
):
    """Create a new Playwright context with fingerprint applied.

    Args:
        browser: Playwright browser instance
        profile: FingerprintProfile to apply
        base_options: Base context options to merge with

    Returns:
        Playwright browser context with fingerprint applied
    """
    # Create context with fingerprint options
    options = create_playwright_context_options(profile, base_options)
    context = await browser.new_context(**options)

    # Apply injection scripts
    await apply_profile_to_playwright_context(context, profile)

    return context


# FingerprintJS Pro / Multilogin API Integration
# These are optional integrations for commercial fingerprinting services

class FingerprintJSProIntegration:
    """Integration with FingerprintJS Pro for advanced fingerprinting.

    This is a placeholder for future integration. FingerprintJS Pro provides
    more sophisticated fingerprinting including:
    - Canvas fingerprinting with noise
    - WebGL parameter spoofing
    - Audio context fingerprinting
    - Font detection evasion
    - WebRTC leak protection

    Requires FingerprintJS Pro subscription and API key.
    """

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.enabled = bool(api_key)

    def get_fingerprint_config(self) -> dict[str, Any]:
        """Get fingerprint configuration from FingerprintJS Pro API.

        Returns:
            Fingerprint configuration dict
        """
        if not self.enabled:
            return {}

        # Placeholder for API integration
        # In production, this would call FingerprintJS Pro API
        logger.warning(
            "[FingerprintJS Pro] API integration not implemented - using local fingerprints"
        )
        return {}

    async def apply_to_context(self, context, config: dict[str, Any]) -> None:
        """Apply FingerprintJS Pro configuration to Playwright context.

        Args:
            context: Playwright context
            config: FingerprintJS Pro configuration
        """
        if not config:
            return

        # Placeholder for applying FingerprintJS Pro configuration
        # Would inject their JavaScript library and configuration
        logger.debug("[FingerprintJS Pro] Would apply config: %s", config)


class MultiloginAPIIntegration:
    """Integration with Multilogin API for professional fingerprinting.

    Multilogin provides browser profiles with comprehensive fingerprinting:
    - Complete browser profiles (Mimic, Stealthfox)
    - Canvas, WebGL, Audio fingerprinting
    - Geolocation spoofing
    - Timezone and language settings
    - Font fingerprinting
    - Hardware fingerprinting

    Requires Multilogin subscription and API access.
    """

    def __init__(
        self,
        api_url: str | None = None,
        api_key: str | None = None,
    ):
        self.api_url = api_url
        self.api_key = api_key
        self.enabled = bool(api_url and api_key)

    def get_profile(self, profile_id: str) -> dict[str, Any]:
        """Get browser profile from Multilogin API.

        Args:
            profile_id: Multilogin profile ID

        Returns:
            Profile configuration dict
        """
        if not self.enabled:
            return {}

        # Placeholder for Multilogin API integration
        # In production, this would call Multilogin API
        logger.warning(
            "[Multilogin] API integration not implemented (profile_id: %s) - using local fingerprints",
            profile_id
        )
        return {}

    async def create_context_from_profile(
        self,
        browser,
        profile_id: str,
    ):
        """Create Playwright context from Multilogin profile.

        Args:
            browser: Playwright browser
            profile_id: Multilogin profile ID

        Returns:
            Playwright context configured with Multilogin profile
        """
        if not self.enabled:
            return await browser.new_context()

        profile_config = self.get_profile(profile_id)

        # Placeholder for profile application
        # Would use Multilogin's profile configuration
        logger.debug("[Multilogin] Would use profile config: %s", profile_config)
        return await browser.new_context()


# Helper functions for common scenarios

async def create_context_with_cookies(
    browser,
    profile: FingerprintProfile,
    cookies: list | None = None,
    base_options: dict[str, Any] | None = None,
):
    """Create Playwright context with fingerprint and cookies.

    Args:
        browser: Playwright browser
        profile: FingerprintProfile to apply
        cookies: List of cookie dicts to set
        base_options: Base context options

    Returns:
        Playwright context with fingerprint and cookies
    """
    # Create context with fingerprint
    context = await create_fingerprinted_context(browser, profile, base_options)

    # Add cookies if provided
    if cookies:
        try:
            await context.add_cookies(cookies)
            logger.debug(
                "[Playwright] Added %d cookies to context",
                len(cookies)
            )
        except Exception as e:
            logger.warning(
                "[Playwright] Failed to add cookies: %s",
                e
            )

    return context


async def apply_stealth_and_fingerprint(
    context,
    profile: FingerprintProfile,
    use_stealth: bool = True,
) -> None:
    """Apply both stealth techniques and fingerprinting to context.

    Args:
        context: Playwright context
        profile: FingerprintProfile to apply
        use_stealth: Whether to apply playwright-stealth
    """
    # Apply fingerprint
    await apply_profile_to_playwright_context(context, profile)

    # Apply playwright-stealth if available and requested
    if use_stealth:
        try:
            from playwright_stealth import stealth_async
            await stealth_async(context)
            logger.debug("[Playwright] Applied stealth techniques")
        except ImportError:
            logger.debug("[Playwright] playwright-stealth not available")
        except Exception as e:
            logger.warning(
                "[Playwright] Failed to apply stealth: %s",
                e
            )


def get_fingerprint_summary(profile: FingerprintProfile) -> dict[str, Any]:
    """Get a summary of fingerprint configuration for debugging.

    Args:
        profile: FingerprintProfile to summarize

    Returns:
        Dict with fingerprint summary
    """
    browser_fp = profile.browser_fingerprint

    return {
        "profile_id": profile.profile_id,
        "user_agent": profile.user_agent[:80] + "..." if len(profile.user_agent) > 80 else profile.user_agent,
        "fingerprint_id": browser_fp.fingerprint_id,
        "browser_profile": {
            "platform": browser_fp.platform,
            "language": browser_fp.language,
            "screen_resolution": f"{browser_fp.screen_resolution[0]}x{browser_fp.screen_resolution[1]}",
            "webgl_vendor": browser_fp.webgl_config["vendor"],
            "canvas_noise": browser_fp.canvas_noise_pattern,
        },
        "tls_profile": profile.tls_fingerprint.profile_id,
        "http2_configured": profile.http2_profile is not None,
        "http3_configured": profile.http3_profile is not None,
    }


__all__ = [
    "apply_profile_to_playwright_context",
    "create_playwright_context_options",
    "create_fingerprinted_context",
    "create_context_with_cookies",
    "apply_stealth_and_fingerprint",
    "get_fingerprint_summary",
    "FingerprintJSProIntegration",
    "MultiloginAPIIntegration",
]


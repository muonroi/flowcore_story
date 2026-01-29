"""Profile Manager - Manages complete profile bundles with cookies + fingerprints.

This module combines cookie management with fingerprint management to create
complete profiles that persist across requests. It handles:
- Cookie storage with associated fingerprints
- Auto-rotation by site/proxy
- Quality tracking per site
- Profile lifecycle management
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from flowcore_story.utils.cookie_manager import (
    CookieSelection,
    fingerprint_headers,
    record_cookie_feedback,
    select_cookie,
)
from flowcore_story.utils.cookie_manager import (
    set_cookies as store_cookies,
)
from flowcore_story.utils.fingerprint_pool import (
    FingerprintProfile,
    get_global_pool,
    record_fingerprint_result,
)
from flowcore_story.utils.logger import logger


class ProfileRotationStrategy(Enum):
    """Strategy for rotating profiles."""
    QUALITY_WEIGHTED = "quality_weighted"
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_RECENTLY_USED = "least_recently_used"


@dataclass
class CompleteProfile:
    """Complete profile combining fingerprint and cookies."""

    fingerprint_profile: FingerprintProfile
    cookie_selection: CookieSelection | None = None

    # Metadata
    site_key: str = ""
    proxy_url: str | None = None
    created_at: float = field(default_factory=time.time)
    last_used_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "fingerprint_profile": self.fingerprint_profile.to_dict(),
            "cookie_selection": {
                "entry_id": self.cookie_selection.entry_id,
                "has_cookies": bool(self.cookie_selection.cookies),
                "user_agent": self.cookie_selection.user_agent,
            } if self.cookie_selection else None,
            "site_key": self.site_key,
            "proxy_url": self.proxy_url,
            "created_at": self.created_at,
            "last_used_at": self.last_used_at,
        }

    def get_user_agent(self) -> str:
        """Get the user agent to use for this profile."""
        # Prefer cookie's user agent if available, otherwise use fingerprint's
        if self.cookie_selection and self.cookie_selection.user_agent:
            return self.cookie_selection.user_agent
        return self.fingerprint_profile.user_agent

    def get_cookie_header(self) -> str | None:
        """Get cookie header string."""
        if self.cookie_selection:
            return self.cookie_selection.header
        return None

    def mark_used(self) -> None:
        """Mark profile as used."""
        self.last_used_at = time.time()
        self.fingerprint_profile.mark_used(self.site_key)


class ProfileManager:
    """Manages complete profiles (fingerprints + cookies) with rotation."""

    def __init__(self):
        """Initialize profile manager."""
        self.fingerprint_pool = get_global_pool()

        # Track active profiles per site/proxy
        self._active_profiles: dict[str, CompleteProfile] = {}

        # Profile rotation settings
        self._rotation_interval: float = 300.0  # 5 minutes
        self._force_rotation_on_failure: bool = True

    def get_profile(
        self,
        site_key: str,
        proxy_url: str | None = None,
        extra_headers: dict[str, str] | None = None,
        force_rotation: bool = False,
        exclude_cookie_ids: set[str] | None = None,
    ) -> CompleteProfile:
        """Get a complete profile for a request.

        Args:
            site_key: Site identifier
            proxy_url: Proxy URL
            extra_headers: Additional headers for cookie selection
            force_rotation: Force rotation instead of reusing cached profile
            exclude_cookie_ids: Cookie entry IDs to exclude

        Returns:
            CompleteProfile ready for use
        """
        cache_key = f"{site_key}:{proxy_url or 'no_proxy'}"

        # Check if we should rotate based on time
        should_rotate = force_rotation
        if not should_rotate and cache_key in self._active_profiles:
            profile = self._active_profiles[cache_key]
            time_since_creation = time.time() - profile.created_at
            if time_since_creation > self._rotation_interval:
                should_rotate = True
                logger.debug(
                    "[ProfileManager] Rotating profile for %s due to age (%.0fs)",
                    site_key,
                    time_since_creation
                )

        # Get or create profile
        if should_rotate or cache_key not in self._active_profiles:
            profile = self._create_profile(
                site_key=site_key,
                proxy_url=proxy_url,
                extra_headers=extra_headers,
                exclude_cookie_ids=exclude_cookie_ids,
            )
            self._active_profiles[cache_key] = profile
        else:
            profile = self._active_profiles[cache_key]

        profile.mark_used()
        return profile

    def _create_profile(
        self,
        site_key: str,
        proxy_url: str | None,
        extra_headers: dict[str, str] | None,
        exclude_cookie_ids: set[str] | None,
    ) -> CompleteProfile:
        """Create a new complete profile.

        Args:
            site_key: Site identifier
            proxy_url: Proxy URL
            extra_headers: Additional headers
            exclude_cookie_ids: Cookie IDs to exclude

        Returns:
            New CompleteProfile
        """
        # Get fingerprint profile
        fp_profile = self.fingerprint_pool.get_profile(
            site_key=site_key,
            proxy_url=proxy_url,
        )

        # Get matching cookies if available
        user_agent = fp_profile.user_agent
        tls_profile = fp_profile.tls_fingerprint.impersonate

        # Create headers for fingerprinting
        headers = extra_headers.copy() if extra_headers else {}
        headers["User-Agent"] = user_agent
        fingerprint = fingerprint_headers(headers)

        # Try to select matching cookies
        cookie_selection = select_cookie(
            site_key=site_key,
            proxy_url=proxy_url,
            fingerprint=fingerprint,
            user_agent=user_agent,
            tls_profile=tls_profile,
            exclude_ids=exclude_cookie_ids,
        )

        return CompleteProfile(
            fingerprint_profile=fp_profile,
            cookie_selection=cookie_selection,
            site_key=site_key,
            proxy_url=proxy_url,
        )

    def record_result(
        self,
        profile: CompleteProfile,
        status_code: int,
        success: bool,
    ) -> None:
        """Record request result for quality tracking.

        Args:
            profile: Profile that was used
            status_code: HTTP status code
            success: Whether request was successful
        """
        # Record fingerprint result
        record_fingerprint_result(
            profile_id=profile.fingerprint_profile.profile_id,
            site_key=profile.site_key,
            success=success,
        )

        # Record cookie result if cookies were used
        if profile.cookie_selection and profile.cookie_selection.entry_id:
            record_cookie_feedback(
                site_key=profile.site_key,
                entry_id=profile.cookie_selection.entry_id,
                status_code=status_code,
                successful=success,
                proxy_url=profile.proxy_url,
            )

        # Force rotation on failure if configured
        if not success and self._force_rotation_on_failure:
            cache_key = f"{profile.site_key}:{profile.proxy_url or 'no_proxy'}"
            # Use try-except to safely delete even if concurrent access
            try:
                if cache_key in self._active_profiles:
                    logger.debug(
                        "[ProfileManager] Removing failed profile for %s",
                        profile.site_key
                    )
                    del self._active_profiles[cache_key]
            except RuntimeError:
                # Dict changed during iteration - ignore and continue
                pass

    def store_response_cookies(
        self,
        profile: CompleteProfile,
        cookies: list[dict[str, Any]],
        headers: dict[str, str] | None = None,
    ) -> None:
        """Store cookies from response associated with profile.

        Args:
            profile: Profile that received the cookies
            cookies: Cookie dicts to store
            headers: Request headers for fingerprinting
        """
        if not cookies:
            return

        # Use profile's user agent and fingerprint
        user_agent = profile.get_user_agent()
        tls_profile = profile.fingerprint_profile.tls_fingerprint.impersonate

        # Create headers for fingerprinting
        req_headers = headers.copy() if headers else {}
        req_headers["User-Agent"] = user_agent
        fingerprint = fingerprint_headers(req_headers)

        # Store cookies
        try:
            store_cookies(
                site_key=profile.site_key,
                cookies=cookies,
                proxy_url=profile.proxy_url,
                headers=req_headers,
                fingerprint=fingerprint,
                tls_profile=tls_profile,
            )
            logger.debug(
                "[ProfileManager] Stored %d cookies for %s",
                len(cookies),
                profile.site_key
            )
        except Exception as e:
            logger.warning(
                "[ProfileManager] Failed to store cookies: %s",
                e,
                exc_info=True
            )

    def invalidate_profile(
        self,
        site_key: str | None = None,
        proxy_url: str | None = None,
    ) -> None:
        """Invalidate cached profiles.

        Args:
            site_key: If provided, only invalidate for this site
            proxy_url: If provided, only invalidate for this proxy
        """
        if site_key is None and proxy_url is None:
            # Clear all
            self._active_profiles.clear()
            logger.debug("[ProfileManager] Invalidated all profiles")
        else:
            # Clear matching profiles - snapshot keys to avoid iteration issues
            keys_to_remove = []
            try:
                for cache_key in list(self._active_profiles.keys()):  # Snapshot keys
                    key_site, key_proxy = cache_key.split(":", 1)
                    if key_proxy == "no_proxy":
                        key_proxy = None

                    if (site_key is None or key_site == site_key) and \
                       (proxy_url is None or key_proxy == proxy_url):
                        keys_to_remove.append(cache_key)

                for key in keys_to_remove:
                    if key in self._active_profiles:  # Check before delete
                        del self._active_profiles[key]
                        logger.debug(
                            "[ProfileManager] Invalidated profile %s",
                            key
                        )
            except RuntimeError:
                # Dict changed during iteration - ignore and continue
                pass

    def get_active_profiles_count(self) -> int:
        """Get count of active profiles."""
        return len(self._active_profiles)

    def get_profile_stats(self) -> dict[str, Any]:
        """Get statistics about profile manager."""
        return {
            "active_profiles": len(self._active_profiles),
            "fingerprint_pool_stats": self.fingerprint_pool.get_pool_stats(),
            "rotation_interval": self._rotation_interval,
            "force_rotation_on_failure": self._force_rotation_on_failure,
        }

    def set_rotation_interval(self, seconds: float) -> None:
        """Set rotation interval in seconds.

        Args:
            seconds: Rotation interval
        """
        self._rotation_interval = max(0.0, seconds)
        logger.debug(
            "[ProfileManager] Set rotation interval to %.0f seconds",
            self._rotation_interval
        )

    def set_force_rotation_on_failure(self, enabled: bool) -> None:
        """Set whether to force rotation on request failure.

        Args:
            enabled: Whether to enable
        """
        self._force_rotation_on_failure = enabled
        logger.debug(
            "[ProfileManager] Force rotation on failure: %s",
            enabled
        )


# Global instance
_global_manager: ProfileManager | None = None


def get_global_manager() -> ProfileManager:
    """Get or create global profile manager."""
    global _global_manager
    if _global_manager is None:
        _global_manager = ProfileManager()
    return _global_manager


def get_profile_for_request(
    site_key: str,
    proxy_url: str | None = None,
    extra_headers: dict[str, str] | None = None,
    force_rotation: bool = False,
    exclude_cookie_ids: set[str] | None = None,
) -> CompleteProfile:
    """Convenience function to get profile for request.

    Args:
        site_key: Site identifier
        proxy_url: Proxy URL
        extra_headers: Additional headers
        force_rotation: Force rotation
        exclude_cookie_ids: Cookie IDs to exclude

    Returns:
        CompleteProfile ready for use
    """
    manager = get_global_manager()
    return manager.get_profile(
        site_key=site_key,
        proxy_url=proxy_url,
        extra_headers=extra_headers,
        force_rotation=force_rotation,
        exclude_cookie_ids=exclude_cookie_ids,
    )


def record_profile_result(
    profile: CompleteProfile,
    status_code: int,
    success: bool,
) -> None:
    """Convenience function to record result."""
    manager = get_global_manager()
    manager.record_result(profile, status_code, success)


def store_profile_cookies(
    profile: CompleteProfile,
    cookies: list[dict[str, Any]],
    headers: dict[str, str] | None = None,
) -> None:
    """Convenience function to store cookies."""
    manager = get_global_manager()
    manager.store_response_cookies(profile, cookies, headers)


__all__ = [
    "CompleteProfile",
    "ProfileManager",
    "get_global_manager",
    "get_profile_for_request",
    "record_profile_result",
    "store_profile_cookies",
]


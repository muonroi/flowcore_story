"""Fingerprint Pool Management - Manages pools of complete fingerprint profiles.

This module provides a comprehensive fingerprint pool that combines:
- User-Agent strings
- TLS fingerprints (JA3)
- HTTP/2 or HTTP/3 settings
- Header order and formatting
- Browser fingerprints (Canvas, WebGL, Audio, Font)

Auto-rotation logic per site/proxy with quality tracking.
"""

from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass, field
from typing import Any

from flowcore_story.config.useragent_list import USER_AGENT_LIST
from flowcore_story.utils.browser_fingerprint import BrowserFingerprint, generate_fingerprint
from flowcore_story.utils.headers_randomizer import HeadersRandomizer, infer_browser_from_user_agent
from flowcore_story.utils.protocol_fingerprint import HTTP2Profile, HTTP3Profile, create_http2_profile_variant
from flowcore_story.utils.tls_fingerprint import (
    TLSFingerprint,
    select_tls_profile_for_user_agent,
)


@dataclass
class FingerprintProfile:
    """Complete fingerprint profile combining all fingerprinting techniques."""

    profile_id: str
    user_agent: str
    tls_fingerprint: TLSFingerprint
    http2_profile: HTTP2Profile | None
    http3_profile: HTTP3Profile | None
    browser_fingerprint: BrowserFingerprint
    header_randomizer: HeadersRandomizer

    # Metadata
    created_at: float = field(default_factory=time.time)
    last_used_at: float | None = None
    use_count: int = 0

    # Quality tracking per site
    site_quality_scores: dict[str, float] = field(default_factory=dict)
    site_success_counts: dict[str, int] = field(default_factory=dict)
    site_failure_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert profile to dictionary."""
        return {
            "profile_id": self.profile_id,
            "user_agent": self.user_agent,
            "tls_fingerprint": self.tls_fingerprint.to_dict(),
            "http2_profile": self.http2_profile.to_dict() if self.http2_profile else None,
            "http3_profile": self.http3_profile.to_dict() if self.http3_profile else None,
            "browser_fingerprint": self.browser_fingerprint.to_dict(),
            "created_at": self.created_at,
            "last_used_at": self.last_used_at,
            "use_count": self.use_count,
            "site_quality_scores": self.site_quality_scores,
            "site_success_counts": self.site_success_counts,
            "site_failure_counts": self.site_failure_counts,
        }

    def mark_used(self, site_key: str | None = None) -> None:
        """Mark this profile as used."""
        self.last_used_at = time.time()
        self.use_count += 1

    def record_success(self, site_key: str) -> None:
        """Record successful request for site."""
        if site_key not in self.site_success_counts:
            self.site_success_counts[site_key] = 0
        if site_key not in self.site_failure_counts:
            self.site_failure_counts[site_key] = 0

        self.site_success_counts[site_key] += 1
        self._update_quality_score(site_key)

    def record_failure(self, site_key: str) -> None:
        """Record failed request for site."""
        if site_key not in self.site_success_counts:
            self.site_success_counts[site_key] = 0
        if site_key not in self.site_failure_counts:
            self.site_failure_counts[site_key] = 0

        self.site_failure_counts[site_key] += 1
        self._update_quality_score(site_key)

    def _update_quality_score(self, site_key: str) -> None:
        """Update quality score for site based on success/failure ratio."""
        success = self.site_success_counts.get(site_key, 0)
        failure = self.site_failure_counts.get(site_key, 0)
        total = success + failure

        if total == 0:
            self.site_quality_scores[site_key] = 1.0
        else:
            # Quality score: success_rate with exponential decay for failures
            success_rate = success / total
            # Penalize consecutive failures
            recent_failures_penalty = min(0.3, failure * 0.05)
            self.site_quality_scores[site_key] = max(0.0, success_rate - recent_failures_penalty)

    def get_quality_score(self, site_key: str) -> float:
        """Get quality score for site (default 1.0 for new sites)."""
        return self.site_quality_scores.get(site_key, 1.0)

    def apply_headers(self, base_headers: dict[str, str]) -> dict[str, str]:
        """Apply header randomization to base headers."""
        return self.header_randomizer.apply(base_headers)


class FingerprintPool:
    """Manages a pool of fingerprint profiles with rotation and quality tracking."""

    def __init__(
        self,
        pool_size: int = 20,
        rotation_strategy: str = "quality_weighted",
        user_agents: list[str] | None = None,
    ):
        """Initialize fingerprint pool.

        Args:
            pool_size: Number of profiles to maintain in pool
            rotation_strategy: Strategy for selecting profiles:
                - "quality_weighted": Weight by quality score per site
                - "random": Random selection
                - "round_robin": Rotate through profiles
            user_agents: List of user agents to use (defaults to USER_AGENT_LIST)
        """
        self.pool_size = pool_size
        self.rotation_strategy = rotation_strategy
        self.user_agents = user_agents or USER_AGENT_LIST
        self.profiles: list[FingerprintProfile] = []
        self._rotation_index: int = 0

        # Site/proxy specific profile caching
        self._site_proxy_cache: dict[str, str] = {}  # (site, proxy) -> profile_id
        self._cache_ttl: float = 300.0  # 5 minutes
        self._cache_timestamps: dict[str, float] = {}

        # Initialize pool
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize the profile pool with diverse fingerprints."""
        # Create diverse user agent selection
        selected_user_agents = self._select_diverse_user_agents()

        for ua in selected_user_agents:
            profile = self._create_profile(ua)
            self.profiles.append(profile)

    def _select_diverse_user_agents(self) -> list[str]:
        """Select diverse user agents ensuring browser diversity."""
        if len(self.user_agents) <= self.pool_size:
            return self.user_agents.copy()

        # Group by browser type
        chrome_uas = [ua for ua in self.user_agents if "Chrome/" in ua and "Edg/" not in ua and not ("Safari/" in ua and "Chrome/" not in ua)]
        firefox_uas = [ua for ua in self.user_agents if "Firefox/" in ua]
        safari_uas = [ua for ua in self.user_agents if "Safari/" in ua and "Chrome/" not in ua]
        edge_uas = [ua for ua in self.user_agents if "Edg/" in ua]

        # Distribute pool size across browsers (60% Chrome, 20% Firefox, 10% Safari, 10% Edge)
        chrome_count = int(self.pool_size * 0.6)
        firefox_count = int(self.pool_size * 0.2)
        safari_count = int(self.pool_size * 0.1)
        edge_count = self.pool_size - chrome_count - firefox_count - safari_count

        selected = []
        if chrome_uas:
            selected.extend(random.sample(chrome_uas, min(chrome_count, len(chrome_uas))))
        if firefox_uas:
            selected.extend(random.sample(firefox_uas, min(firefox_count, len(firefox_uas))))
        if safari_uas:
            selected.extend(random.sample(safari_uas, min(safari_count, len(safari_uas))))
        if edge_uas:
            selected.extend(random.sample(edge_uas, min(edge_count, len(edge_uas))))

        # Fill remaining with random selection if needed
        while len(selected) < self.pool_size:
            remaining = [ua for ua in self.user_agents if ua not in selected]
            if not remaining:
                break
            selected.append(random.choice(remaining))

        return selected

    def _create_profile(self, user_agent: str) -> FingerprintProfile:
        """Create a complete fingerprint profile."""
        # Generate profile ID
        profile_id = hashlib.sha256(
            f"{user_agent}:{time.time()}:{random.random()}".encode()
        ).hexdigest()[:16]

        # Select TLS fingerprint matching user agent
        tls_fp = select_tls_profile_for_user_agent(user_agent, randomize=True)
        if not tls_fp:
            # Fallback to Chrome if no match
            from flowcore_story.utils.tls_fingerprint import TLSFingerprint
            tls_fp = TLSFingerprint.from_profile("chrome120")

        # Create HTTP/2 profile with slight variation
        browser_type = infer_browser_from_user_agent(user_agent)
        from flowcore_story.utils.protocol_fingerprint import HTTP2_PROFILES
        base_http2 = HTTP2_PROFILES.get(f"{browser_type}_default")
        http2_profile = create_http2_profile_variant(base_http2) if base_http2 else None

        # HTTP/3 profile (for H3 capable browsers)
        http3_profile = None  # Can be extended later

        # Generate browser fingerprint
        browser_fp = generate_fingerprint(
            seed=profile_id,
            user_agent=user_agent
        )

        # Create header randomizer
        header_randomizer = HeadersRandomizer(
            browser_type=browser_type,
            randomize_order=True,
            randomize_casing=False,  # Keep standard casing for compatibility
            randomize_spacing=False,
        )

        return FingerprintProfile(
            profile_id=profile_id,
            user_agent=user_agent,
            tls_fingerprint=tls_fp,
            http2_profile=http2_profile,
            http3_profile=http3_profile,
            browser_fingerprint=browser_fp,
            header_randomizer=header_randomizer,
        )

    def get_profile(
        self,
        site_key: str | None = None,
        proxy_url: str | None = None,
        force_rotation: bool = False,
    ) -> FingerprintProfile:
        """Get a fingerprint profile using configured rotation strategy.

        Args:
            site_key: Site identifier for quality-based selection
            proxy_url: Proxy URL for site/proxy pairing
            force_rotation: Force rotation instead of using cache

        Returns:
            FingerprintProfile to use for request
        """
        # Check cache if not forcing rotation
        if not force_rotation and site_key and proxy_url:
            cache_key = f"{site_key}:{proxy_url}"
            cached_profile_id = self._site_proxy_cache.get(cache_key)
            cache_timestamp = self._cache_timestamps.get(cache_key, 0)

            # Use cached profile if still valid
            if cached_profile_id and (time.time() - cache_timestamp) < self._cache_ttl:
                profile = self._get_profile_by_id(cached_profile_id)
                if profile:
                    profile.mark_used(site_key)
                    return profile

        # Select profile based on strategy
        if self.rotation_strategy == "quality_weighted" and site_key:
            profile = self._select_quality_weighted(site_key)
        elif self.rotation_strategy == "round_robin":
            profile = self._select_round_robin()
        else:  # random
            profile = self._select_random()

        # Cache the selection
        if site_key and proxy_url:
            cache_key = f"{site_key}:{proxy_url}"
            self._site_proxy_cache[cache_key] = profile.profile_id
            self._cache_timestamps[cache_key] = time.time()

        profile.mark_used(site_key)
        return profile

    def _get_profile_by_id(self, profile_id: str) -> FingerprintProfile | None:
        """Get profile by ID."""
        for profile in self.profiles:
            if profile.profile_id == profile_id:
                return profile
        return None

    def _select_quality_weighted(self, site_key: str) -> FingerprintProfile:
        """Select profile weighted by quality score for site."""
        # Calculate weights based on quality scores
        weights = [p.get_quality_score(site_key) for p in self.profiles]
        total_weight = sum(weights)

        if total_weight == 0:
            # All have zero quality, use random
            return random.choice(self.profiles)

        # Weighted random selection
        rand = random.uniform(0, total_weight)
        cumulative = 0.0
        for profile, weight in zip(self.profiles, weights, strict=False):
            cumulative += weight
            if rand <= cumulative:
                return profile

        # Fallback
        return self.profiles[-1]

    def _select_round_robin(self) -> FingerprintProfile:
        """Select profile in round-robin fashion."""
        profile = self.profiles[self._rotation_index]
        self._rotation_index = (self._rotation_index + 1) % len(self.profiles)
        return profile

    def _select_random(self) -> FingerprintProfile:
        """Select random profile."""
        return random.choice(self.profiles)

    def record_result(
        self,
        profile_id: str,
        site_key: str,
        success: bool,
    ) -> None:
        """Record request result for profile quality tracking.

        Args:
            profile_id: Profile ID that was used
            site_key: Site the request was made to
            success: Whether the request was successful
        """
        profile = self._get_profile_by_id(profile_id)
        if not profile:
            return

        if success:
            profile.record_success(site_key)
        else:
            profile.record_failure(site_key)

    def invalidate_cache(
        self,
        site_key: str | None = None,
        proxy_url: str | None = None,
    ) -> None:
        """Invalidate cached profile selections.

        Args:
            site_key: If provided, only invalidate for this site
            proxy_url: If provided, only invalidate for this proxy
        """
        if site_key is None and proxy_url is None:
            # Clear all cache
            self._site_proxy_cache.clear()
            self._cache_timestamps.clear()
        else:
            # Clear specific entries - snapshot keys to avoid iteration issues
            keys_to_remove = []
            try:
                # IMPORTANT: list() is required to create snapshot and prevent
                # "RuntimeError: dictionary changed size during iteration" in concurrent async context
                for cache_key in tuple(self._site_proxy_cache):
                    key_site, key_proxy = cache_key.split(":", 1)
                    if (site_key is None or key_site == site_key) and \
                       (proxy_url is None or key_proxy == proxy_url):
                        keys_to_remove.append(cache_key)

                for key in keys_to_remove:
                    self._site_proxy_cache.pop(key, None)
                    self._cache_timestamps.pop(key, None)
            except RuntimeError:
                # Dict changed during iteration - ignore and continue
                pass

    def get_pool_stats(self) -> dict[str, Any]:
        """Get statistics about the profile pool."""
        return {
            "pool_size": len(self.profiles),
            "rotation_strategy": self.rotation_strategy,
            "cache_size": len(self._site_proxy_cache),
            "profiles": [
                {
                    "profile_id": p.profile_id,
                    "user_agent": p.user_agent[:50] + "...",
                    "use_count": p.use_count,
                    "sites_tracked": len(p.site_quality_scores),
                    "avg_quality": sum(p.site_quality_scores.values()) / len(p.site_quality_scores)
                        if p.site_quality_scores else 1.0,
                }
                for p in self.profiles
            ]
        }


# Global pool instance
_global_pool: FingerprintPool | None = None


def get_global_pool() -> FingerprintPool:
    """Get or create global fingerprint pool."""
    global _global_pool
    if _global_pool is None:
        _global_pool = FingerprintPool(
            pool_size=20,
            rotation_strategy="quality_weighted",
        )
    return _global_pool


def get_fingerprint_for_request(
    site_key: str | None = None,
    proxy_url: str | None = None,
    user_agent: str | None = None,
    force_rotation: bool = False,
) -> tuple[FingerprintProfile, TLSFingerprint]:
    """Get fingerprint profile for a request.

    This is a convenience function that returns both the full profile
    and the TLS fingerprint for backward compatibility.

    Args:
        site_key: Site identifier
        proxy_url: Proxy URL
        user_agent: Optional user agent hint
        force_rotation: Force rotation instead of using cache

    Returns:
        Tuple of (FingerprintProfile, TLSFingerprint)
    """
    pool = get_global_pool()
    profile = pool.get_profile(
        site_key=site_key,
        proxy_url=proxy_url,
        force_rotation=force_rotation,
    )
    return profile, profile.tls_fingerprint


def record_fingerprint_result(
    profile_id: str,
    site_key: str,
    success: bool,
) -> None:
    """Record result for fingerprint profile quality tracking."""
    pool = get_global_pool()
    pool.record_result(profile_id, site_key, success)


__all__ = [
    "FingerprintProfile",
    "FingerprintPool",
    "get_global_pool",
    "get_fingerprint_for_request",
    "record_fingerprint_result",
]


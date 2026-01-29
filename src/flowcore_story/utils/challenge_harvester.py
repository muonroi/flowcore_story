"""Challenge Harvester - Advanced challenge solving and clearance management.

This module provides a comprehensive system for handling anti-bot challenges:
- Cloudflare Turnstile solving (via external solver or human workflow)
- Clearance cookie harvesting and storage
- TTL tracking and auto-refresh
- Distributed harvesting across multiple instances
- Pre-warming on schedule

Integrates with the profile management system for consistent fingerprinting.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from flowcore_story.utils.logger import logger


class ChallengeType(Enum):
    """Types of challenges that can be encountered."""
    CLOUDFLARE_TURNSTILE = "cloudflare_turnstile"
    CLOUDFLARE_UAM = "cloudflare_uam"
    RECAPTCHA_V2 = "recaptcha_v2"
    RECAPTCHA_V3 = "recaptcha_v3"
    HCAPTCHA = "hcaptcha"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


class SolveMethod(Enum):
    """Methods for solving challenges."""
    AUTOMATED_SOLVER = "automated_solver"  # Use external solver service
    HUMAN_WORKFLOW = "human_workflow"      # Human interaction in headful browser
    HYBRID = "hybrid"                      # Try automated first, fallback to human
    SKIP = "skip"                          # Skip challenge


@dataclass
class ChallengeResult:
    """Result of a challenge solving attempt."""

    success: bool
    challenge_type: ChallengeType
    solve_method: SolveMethod

    # Clearance data
    cookies: list[dict[str, Any]] = field(default_factory=list)
    tokens: dict[str, str] = field(default_factory=dict)

    # Metadata
    solve_time: float = 0.0
    attempts: int = 1
    ttl: float | None = None  # Time-to-live in seconds
    expires_at: float | None = None

    # Error info
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "challenge_type": self.challenge_type.value,
            "solve_method": self.solve_method.value,
            "cookies": self.cookies,
            "tokens": self.tokens,
            "solve_time": self.solve_time,
            "attempts": self.attempts,
            "ttl": self.ttl,
            "expires_at": self.expires_at,
            "error": self.error,
        }


@dataclass
class ClearanceEntry:
    """Stored clearance for a site."""

    site_key: str
    profile_id: str
    proxy_url: str | None

    # Clearance data
    cookies: list[dict[str, Any]]
    tokens: dict[str, str]

    # TTL tracking
    obtained_at: float
    ttl: float  # in seconds
    expires_at: float

    # Refresh settings
    refresh_before_seconds: float = 300.0  # Refresh 5 minutes before expiry
    last_validated_at: float | None = None
    validation_count: int = 0

    def should_refresh(self) -> bool:
        """Check if clearance should be refreshed."""
        now = time.time()
        refresh_threshold = self.expires_at - self.refresh_before_seconds
        return now >= refresh_threshold

    def is_expired(self) -> bool:
        """Check if clearance is expired."""
        return time.time() >= self.expires_at

    def time_until_expiry(self) -> float:
        """Get seconds until expiry."""
        return max(0.0, self.expires_at - time.time())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "site_key": self.site_key,
            "profile_id": self.profile_id,
            "proxy_url": self.proxy_url,
            "cookies": self.cookies,
            "tokens": self.tokens,
            "obtained_at": self.obtained_at,
            "ttl": self.ttl,
            "expires_at": self.expires_at,
            "refresh_before_seconds": self.refresh_before_seconds,
            "last_validated_at": self.last_validated_at,
            "validation_count": self.validation_count,
        }


class ChallengeDetector:
    """Detects challenges in page content or responses."""

    # Cloudflare Turnstile patterns
    TURNSTILE_PATTERNS = [
        "cf-turnstile",
        "cf_challenge_response",
        "turnstile.phtml",
        "challenges.cloudflare.com",
    ]

    # Cloudflare UAM patterns
    CLOUDFLARE_UAM_PATTERNS = [
        "Checking your browser",
        "DDoS protection by Cloudflare",
        "cf-browser-verification",
        "jschl-answer",
    ]

    # reCAPTCHA patterns
    RECAPTCHA_PATTERNS = [
        "g-recaptcha",
        "recaptcha/api",
        "recaptcha/enterprise",
    ]

    # hCaptcha patterns
    HCAPTCHA_PATTERNS = [
        "h-captcha",
        "hcaptcha.com",
    ]

    @classmethod
    def detect_challenge(cls, content: str, url: str = "") -> ChallengeType:
        """Detect challenge type from page content.

        Args:
            content: Page HTML content
            url: Page URL

        Returns:
            ChallengeType detected
        """
        content_lower = content.lower()

        # Check for Turnstile
        if any(pattern.lower() in content_lower for pattern in cls.TURNSTILE_PATTERNS):
            return ChallengeType.CLOUDFLARE_TURNSTILE

        # Check for Cloudflare UAM
        if any(pattern.lower() in content_lower for pattern in cls.CLOUDFLARE_UAM_PATTERNS):
            return ChallengeType.CLOUDFLARE_UAM

        # Check for reCAPTCHA
        if any(pattern.lower() in content_lower for pattern in cls.RECAPTCHA_PATTERNS):
            if "recaptcha/enterprise" in content_lower or "v3" in content_lower:
                return ChallengeType.RECAPTCHA_V3
            return ChallengeType.RECAPTCHA_V2

        # Check for hCaptcha
        if any(pattern.lower() in content_lower for pattern in cls.HCAPTCHA_PATTERNS):
            return ChallengeType.HCAPTCHA

        return ChallengeType.UNKNOWN

    @classmethod
    def has_challenge(cls, content: str, url: str = "") -> bool:
        """Check if content contains any challenge.

        Args:
            content: Page HTML content
            url: Page URL

        Returns:
            True if challenge detected
        """
        return cls.detect_challenge(content, url) != ChallengeType.UNKNOWN


class ChallengeHarvester:
    """Main challenge harvester orchestrator."""

    def __init__(
        self,
        default_solve_method: SolveMethod = SolveMethod.HYBRID,
        default_ttl: float = 3600.0,  # 1 hour default
        refresh_before_seconds: float = 300.0,  # 5 minutes
    ):
        """Initialize challenge harvester.

        Args:
            default_solve_method: Default method for solving challenges
            default_ttl: Default TTL for clearances (seconds)
            refresh_before_seconds: Refresh clearance this many seconds before expiry
        """
        self.default_solve_method = default_solve_method
        self.default_ttl = default_ttl
        self.refresh_before_seconds = refresh_before_seconds

        # Storage for active clearances
        self._clearances: dict[str, ClearanceEntry] = {}

        # Solver registry
        self._solvers: dict[ChallengeType, Callable] = {}

        # Statistics
        self._stats = {
            "total_challenges": 0,
            "successful_solves": 0,
            "failed_solves": 0,
            "automated_solves": 0,
            "human_solves": 0,
            "cache_hits": 0,
            "refreshes": 0,
        }

        logger.info("[Harvester] Initialized with method: %s", default_solve_method.value)

    def register_solver(
        self,
        challenge_type: ChallengeType,
        solver_func: Callable
    ) -> None:
        """Register a solver function for a challenge type.

        Args:
            challenge_type: Type of challenge
            solver_func: Async function that solves the challenge
        """
        self._solvers[challenge_type] = solver_func
        logger.debug("[Harvester] Registered solver for %s", challenge_type.value)

    def _make_cache_key(
        self,
        site_key: str,
        profile_id: str | None = None,
        proxy_url: str | None = None,
    ) -> str:
        """Make cache key for clearance storage."""
        parts = [site_key]
        if profile_id:
            parts.append(profile_id)
        if proxy_url:
            parts.append(proxy_url)
        return ":".join(parts)

    def get_clearance(
        self,
        site_key: str,
        profile_id: str | None = None,
        proxy_url: str | None = None,
    ) -> ClearanceEntry | None:
        """Get stored clearance if available and not expired.

        Args:
            site_key: Site identifier
            profile_id: Profile ID used
            proxy_url: Proxy URL used

        Returns:
            ClearanceEntry if available and valid, None otherwise
        """
        cache_key = self._make_cache_key(site_key, profile_id, proxy_url)
        entry = self._clearances.get(cache_key)

        if entry is None:
            return None

        if entry.is_expired():
            logger.debug(
                "[Harvester] Clearance expired for %s (expired %.0fs ago)",
                site_key,
                time.time() - entry.expires_at
            )
            del self._clearances[cache_key]
            return None

        self._stats["cache_hits"] += 1
        logger.debug(
            "[Harvester] Using cached clearance for %s (expires in %.0fs)",
            site_key,
            entry.time_until_expiry()
        )
        return entry

    def store_clearance(
        self,
        site_key: str,
        cookies: list[dict[str, Any]],
        tokens: dict[str, str],
        profile_id: str | None = None,
        proxy_url: str | None = None,
        ttl: float | None = None,
    ) -> ClearanceEntry:
        """Store clearance for reuse.

        Args:
            site_key: Site identifier
            cookies: Clearance cookies
            tokens: Challenge tokens
            profile_id: Profile ID used
            proxy_url: Proxy URL used
            ttl: Time-to-live in seconds

        Returns:
            Stored ClearanceEntry
        """
        now = time.time()
        ttl = ttl or self.default_ttl

        entry = ClearanceEntry(
            site_key=site_key,
            profile_id=profile_id or "default",
            proxy_url=proxy_url,
            cookies=cookies,
            tokens=tokens,
            obtained_at=now,
            ttl=ttl,
            expires_at=now + ttl,
            refresh_before_seconds=self.refresh_before_seconds,
        )

        cache_key = self._make_cache_key(site_key, profile_id, proxy_url)
        self._clearances[cache_key] = entry

        logger.info(
            "[Harvester] Stored clearance for %s (TTL: %.0fs, expires: %.0fs from now)",
            site_key,
            ttl,
            entry.time_until_expiry()
        )

        return entry

    async def solve_challenge(
        self,
        challenge_type: ChallengeType,
        site_key: str,
        url: str,
        context: Any,  # Playwright context
        solve_method: SolveMethod | None = None,
        profile_id: str | None = None,
        proxy_url: str | None = None,
        **kwargs
    ) -> ChallengeResult:
        """Solve a challenge.

        Args:
            challenge_type: Type of challenge to solve
            site_key: Site identifier
            url: Challenge URL
            context: Playwright browser context
            solve_method: Method to use (defaults to self.default_solve_method)
            profile_id: Profile ID being used
            proxy_url: Proxy URL being used
            **kwargs: Additional arguments for solver

        Returns:
            ChallengeResult with solve outcome
        """
        self._stats["total_challenges"] += 1
        solve_method = solve_method or self.default_solve_method

        start_time = time.time()

        logger.info(
            "[Harvester] Solving %s challenge for %s using %s",
            challenge_type.value,
            site_key,
            solve_method.value
        )

        try:
            # Check if we have a solver registered
            if challenge_type not in self._solvers:
                logger.warning(
                    "[Harvester] No solver registered for %s, falling back to human workflow",
                    challenge_type.value
                )
                solve_method = SolveMethod.HUMAN_WORKFLOW

            # Solve based on method
            if solve_method == SolveMethod.AUTOMATED_SOLVER:
                result = await self._solve_automated(
                    challenge_type, site_key, url, context, **kwargs
                )
                if result.success:
                    self._stats["automated_solves"] += 1

            elif solve_method == SolveMethod.HUMAN_WORKFLOW:
                result = await self._solve_human(
                    challenge_type, site_key, url, context, **kwargs
                )
                if result.success:
                    self._stats["human_solves"] += 1

            elif solve_method == SolveMethod.HYBRID:
                # Try automated first
                result = await self._solve_automated(
                    challenge_type, site_key, url, context, **kwargs
                )

                if not result.success:
                    logger.info("[Harvester] Automated solve failed, falling back to human")
                    result = await self._solve_human(
                        challenge_type, site_key, url, context, **kwargs
                    )
                    if result.success:
                        self._stats["human_solves"] += 1
                else:
                    self._stats["automated_solves"] += 1

            else:  # SKIP
                result = ChallengeResult(
                    success=False,
                    challenge_type=challenge_type,
                    solve_method=solve_method,
                    error="Challenge solving skipped"
                )

            # Record solve time
            result.solve_time = time.time() - start_time

            # Update stats
            if result.success:
                self._stats["successful_solves"] += 1

                # Store clearance
                if result.cookies or result.tokens:
                    self.store_clearance(
                        site_key=site_key,
                        cookies=result.cookies,
                        tokens=result.tokens,
                        profile_id=profile_id,
                        proxy_url=proxy_url,
                        ttl=result.ttl,
                    )
            else:
                self._stats["failed_solves"] += 1

            logger.info(
                "[Harvester] Challenge solve %s for %s (took %.1fs)",
                "succeeded" if result.success else "failed",
                site_key,
                result.solve_time
            )

            return result

        except Exception as e:
            logger.error(
                "[Harvester] Exception solving challenge for %s: %s",
                site_key,
                e,
                exc_info=True
            )

            self._stats["failed_solves"] += 1

            return ChallengeResult(
                success=False,
                challenge_type=challenge_type,
                solve_method=solve_method,
                solve_time=time.time() - start_time,
                error=str(e)
            )

    async def _solve_automated(
        self,
        challenge_type: ChallengeType,
        site_key: str,
        url: str,
        context: Any,
        **kwargs
    ) -> ChallengeResult:
        """Solve challenge using automated solver.

        Args:
            challenge_type: Type of challenge
            site_key: Site identifier
            url: Challenge URL
            context: Playwright context
            **kwargs: Additional solver arguments

        Returns:
            ChallengeResult
        """
        solver = self._solvers.get(challenge_type)

        if solver is None:
            return ChallengeResult(
                success=False,
                challenge_type=challenge_type,
                solve_method=SolveMethod.AUTOMATED_SOLVER,
                error=f"No automated solver for {challenge_type.value}"
            )

        try:
            result = await solver(
                site_key=site_key,
                url=url,
                context=context,
                **kwargs
            )
            return result
        except Exception as e:
            logger.warning(
                "[Harvester] Automated solver failed: %s",
                e
            )
            return ChallengeResult(
                success=False,
                challenge_type=challenge_type,
                solve_method=SolveMethod.AUTOMATED_SOLVER,
                error=str(e)
            )

    async def _solve_human(
        self,
        challenge_type: ChallengeType,
        site_key: str,
        url: str,
        context: Any,
        **kwargs
    ) -> ChallengeResult:
        """Solve challenge using human workflow (headful browser).

        Args:
            challenge_type: Type of challenge
            site_key: Site identifier
            url: Challenge URL
            context: Playwright context
            **kwargs: Additional arguments

        Returns:
            ChallengeResult
        """
        # Import human interaction module
        try:
            from flowcore_story.utils.human_like_interaction import wait_for_human_solve

            logger.info(
                "[Harvester] Waiting for human to solve %s challenge for %s",
                challenge_type.value,
                site_key
            )

            result = await wait_for_human_solve(
                challenge_type=challenge_type,
                site_key=site_key,
                url=url,
                context=context,
                **kwargs
            )

            return result

        except ImportError:
            logger.error("[Harvester] Human interaction module not available")
            return ChallengeResult(
                success=False,
                challenge_type=challenge_type,
                solve_method=SolveMethod.HUMAN_WORKFLOW,
                error="Human interaction module not available"
            )

    def get_clearances_needing_refresh(self) -> list[ClearanceEntry]:
        """Get list of clearances that need refreshing.

        Returns:
            List of ClearanceEntry objects that should be refreshed
        """
        entries = []
        for entry in self._clearances.values():
            if entry.should_refresh() and not entry.is_expired():
                entries.append(entry)
        return entries

    def get_stats(self) -> dict[str, Any]:
        """Get harvester statistics.

        Returns:
            Dict with statistics
        """
        return {
            **self._stats,
            "active_clearances": len(self._clearances),
            "needing_refresh": len(self.get_clearances_needing_refresh()),
        }

    def clear_expired(self) -> int:
        """Clear expired clearances from storage.

        Returns:
            Number of clearances removed
        """
        expired_keys = []
        for key, entry in self._clearances.items():
            if entry.is_expired():
                expired_keys.append(key)

        for key in expired_keys:
            del self._clearances[key]

        removed = len(expired_keys)
        if removed > 0:
            logger.info("[Harvester] Cleared %d expired clearances", removed)

        return removed


# Global instance
_global_harvester: ChallengeHarvester | None = None


def get_global_harvester() -> ChallengeHarvester:
    """Get or create global challenge harvester instance."""
    global _global_harvester
    if _global_harvester is None:
        _global_harvester = ChallengeHarvester()
    return _global_harvester


__all__ = [
    "ChallengeType",
    "SolveMethod",
    "ChallengeResult",
    "ClearanceEntry",
    "ChallengeDetector",
    "ChallengeHarvester",
    "get_global_harvester",
]


"""
Adaptive Rate Limiter - Automatically adjust request delays based on response patterns.

This module provides intelligent rate limiting that:
- Increases delay exponentially when rate limited (429)
- Decreases delay gradually on successful requests
- Tracks per-site rate limit state
- Supports circuit breaker pattern for severe rate limiting

Created: 2025-12-15
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Optional

from flowcore_story.config.env_loader import get_float
from flowcore_story.utils.logger import logger


@dataclass
class SiteRateLimitState:
    """Track rate limit state for a single site."""

    current_delay: float = 1.0
    consecutive_429s: int = 0
    consecutive_successes: int = 0
    last_request_time: float = 0.0
    last_429_time: float = 0.0
    total_requests: int = 0
    total_429s: int = 0
    circuit_open: bool = False
    circuit_open_until: float = 0.0


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts delays based on server responses.

    Features:
    - Exponential backoff on 429 responses
    - Gradual cooldown on successful requests
    - Per-site tracking
    - Circuit breaker for severe rate limiting

    Usage:
        limiter = AdaptiveRateLimiter()

        # Before request
        delay = limiter.get_delay("xtruyen")
        await asyncio.sleep(delay)

        # After response
        if status_code == 429:
            limiter.on_rate_limit("xtruyen")
        else:
            limiter.on_success("xtruyen")
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        min_delay: float = 0.5,
        backoff_factor: float = 2.0,
        cooldown_factor: float = 0.9,
        circuit_threshold: int = 5,
        circuit_recovery_time: float = 120.0,
    ):
        """
        Initialize the adaptive rate limiter.

        Args:
            base_delay: Starting delay in seconds
            max_delay: Maximum delay cap in seconds
            min_delay: Minimum delay floor in seconds
            backoff_factor: Multiplier for delay on rate limit
            cooldown_factor: Multiplier for delay reduction on success
            circuit_threshold: Number of consecutive 429s to open circuit
            circuit_recovery_time: Seconds to wait before retrying after circuit opens
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.backoff_factor = backoff_factor
        self.cooldown_factor = cooldown_factor
        self.circuit_threshold = circuit_threshold
        self.circuit_recovery_time = circuit_recovery_time

        self._states: dict[str, SiteRateLimitState] = {}
        self._lock = Lock()

    def _get_state(self, site_key: str) -> SiteRateLimitState:
        """Get or create state for a site."""
        with self._lock:
            if site_key not in self._states:
                self._states[site_key] = SiteRateLimitState(
                    current_delay=self.base_delay
                )
            return self._states[site_key]

    def get_delay(self, site_key: str) -> float:
        """
        Get the recommended delay before making a request.

        Args:
            site_key: Site identifier

        Returns:
            Delay in seconds
        """
        state = self._get_state(site_key)

        # Check circuit breaker
        if state.circuit_open:
            if time.time() < state.circuit_open_until:
                remaining = state.circuit_open_until - time.time()
                logger.warning(
                    f"[AdaptiveRateLimiter] Circuit OPEN for {site_key}. "
                    f"Wait {remaining:.1f}s before retry."
                )
                return remaining
            else:
                # Circuit recovery - try with half the max delay
                state.circuit_open = False
                state.current_delay = self.max_delay / 2
                logger.info(
                    f"[AdaptiveRateLimiter] Circuit HALF-OPEN for {site_key}. "
                    f"Testing with {state.current_delay:.1f}s delay."
                )

        return state.current_delay

    def on_success(self, site_key: str, status_code: int = 200) -> None:
        """
        Record a successful request. Gradually reduces delay.

        Args:
            site_key: Site identifier
            status_code: HTTP status code
        """
        state = self._get_state(site_key)

        with self._lock:
            state.total_requests += 1
            state.consecutive_successes += 1
            state.consecutive_429s = 0
            state.last_request_time = time.time()

            # Gradually reduce delay on consecutive successes
            if state.consecutive_successes >= 3:
                old_delay = state.current_delay
                state.current_delay = max(
                    self.min_delay,
                    state.current_delay * self.cooldown_factor
                )
                if old_delay != state.current_delay:
                    logger.debug(
                        f"[AdaptiveRateLimiter] {site_key}: Reduced delay "
                        f"{old_delay:.2f}s -> {state.current_delay:.2f}s "
                        f"(consecutive successes: {state.consecutive_successes})"
                    )

            # Close circuit on successful recovery
            if state.circuit_open:
                state.circuit_open = False
                logger.info(
                    f"[AdaptiveRateLimiter] Circuit CLOSED for {site_key} "
                    f"after successful request."
                )

    def on_rate_limit(self, site_key: str) -> float:
        """
        Record a rate limit (429) response. Increases delay exponentially.

        Args:
            site_key: Site identifier

        Returns:
            New recommended delay in seconds
        """
        state = self._get_state(site_key)

        with self._lock:
            state.total_requests += 1
            state.total_429s += 1
            state.consecutive_429s += 1
            state.consecutive_successes = 0
            state.last_429_time = time.time()
            state.last_request_time = time.time()

            # Exponential backoff
            old_delay = state.current_delay
            state.current_delay = min(
                self.max_delay,
                state.current_delay * self.backoff_factor
            )

            logger.warning(
                f"[AdaptiveRateLimiter] {site_key}: Rate limited! "
                f"Increased delay {old_delay:.2f}s -> {state.current_delay:.2f}s "
                f"(consecutive 429s: {state.consecutive_429s})"
            )

            # Open circuit if too many consecutive rate limits
            if state.consecutive_429s >= self.circuit_threshold:
                state.circuit_open = True
                state.circuit_open_until = time.time() + self.circuit_recovery_time
                logger.error(
                    f"[AdaptiveRateLimiter] Circuit OPEN for {site_key}! "
                    f"Too many rate limits ({state.consecutive_429s}). "
                    f"Blocking requests for {self.circuit_recovery_time}s."
                )

            return state.current_delay

    def on_error(self, site_key: str, error: Exception) -> None:
        """
        Record a request error (connection failed, timeout, etc.)

        Args:
            site_key: Site identifier
            error: The exception that occurred
        """
        state = self._get_state(site_key)

        with self._lock:
            state.total_requests += 1
            state.last_request_time = time.time()

            # Slight delay increase on errors (could be server overload)
            if "timeout" in str(error).lower() or "pool" in str(error).lower():
                old_delay = state.current_delay
                state.current_delay = min(
                    self.max_delay,
                    state.current_delay * 1.5  # Gentler backoff for timeouts
                )
                logger.debug(
                    f"[AdaptiveRateLimiter] {site_key}: Timeout/pool error, "
                    f"increased delay {old_delay:.2f}s -> {state.current_delay:.2f}s"
                )

    def is_circuit_open(self, site_key: str) -> bool:
        """Check if circuit breaker is open for a site."""
        state = self._get_state(site_key)
        if not state.circuit_open:
            return False
        if time.time() >= state.circuit_open_until:
            return False
        return True

    def get_stats(self, site_key: str) -> dict:
        """Get rate limit statistics for a site."""
        state = self._get_state(site_key)
        return {
            "site_key": site_key,
            "current_delay": state.current_delay,
            "consecutive_429s": state.consecutive_429s,
            "consecutive_successes": state.consecutive_successes,
            "total_requests": state.total_requests,
            "total_429s": state.total_429s,
            "rate_limit_ratio": state.total_429s / max(1, state.total_requests),
            "circuit_open": state.circuit_open,
            "circuit_open_until": state.circuit_open_until if state.circuit_open else None,
        }

    def reset(self, site_key: str) -> None:
        """Reset rate limit state for a site."""
        with self._lock:
            if site_key in self._states:
                del self._states[site_key]
        logger.info(f"[AdaptiveRateLimiter] Reset state for {site_key}")


# Global instance
adaptive_rate_limiter = AdaptiveRateLimiter()

# Site-specific instances with tuned parameters
_site_limiters: dict[str, AdaptiveRateLimiter] = {}


def get_site_limiter(site_key: str) -> AdaptiveRateLimiter:
    """
    Get a rate limiter configured for a specific site.

    Different sites may need different rate limit parameters.
    """
    if site_key not in _site_limiters:
        env_var_name = f"{site_key.upper()}_REQUEST_DELAY"
        env_base_delay = get_float(env_var_name)
        
        if env_base_delay is not None:
             logger.info(f"[AdaptiveRateLimiter] Loaded configuration for {site_key} with ENV override: {env_var_name}={env_base_delay}")

        # Site-specific configurations
        if site_key == "xtruyen":
            # XTruyen is very aggressive with rate limiting
            _site_limiters[site_key] = AdaptiveRateLimiter(
                base_delay=env_base_delay if env_base_delay is not None else 3.0,
                max_delay=120.0,
                min_delay=2.0,
                backoff_factor=2.5,
                cooldown_factor=0.85,
                circuit_threshold=3,
                circuit_recovery_time=180.0,
            )
        elif site_key == "truyencom":
            # TruyenCom is moderate
            _site_limiters[site_key] = AdaptiveRateLimiter(
                base_delay=env_base_delay if env_base_delay is not None else 1.5,
                max_delay=60.0,
                min_delay=1.0,
                backoff_factor=2.0,
                cooldown_factor=0.9,
                circuit_threshold=5,
                circuit_recovery_time=120.0,
            )
        elif site_key == "tangthuvien":
            # TangThuVien is more lenient
            _site_limiters[site_key] = AdaptiveRateLimiter(
                base_delay=env_base_delay if env_base_delay is not None else 1.0,
                max_delay=30.0,
                min_delay=0.5,
                backoff_factor=2.0,
                cooldown_factor=0.95,
                circuit_threshold=7,
                circuit_recovery_time=60.0,
            )
        else:
            # Default configuration
            _site_limiters[site_key] = AdaptiveRateLimiter(
                base_delay=env_base_delay if env_base_delay is not None else 1.0
            )

    return _site_limiters[site_key]


__all__ = [
    "AdaptiveRateLimiter",
    "SiteRateLimitState",
    "adaptive_rate_limiter",
    "get_site_limiter",
]

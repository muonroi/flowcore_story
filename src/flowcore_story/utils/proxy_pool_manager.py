"""Proxy Pool Manager - Advanced proxy management with reputation scoring.

This module provides sophisticated proxy pool management:
- Auto reputation scoring based on success/failure
- Priority system (residential > ISP > datacenter)
- Health checking and auto-removal of bad proxies
- Load balancing across proxy providers
- Integration with distributed infrastructure

Features:
- Real-time reputation updates
- Exponential decay for old scores
- Automatic proxy rotation
- Provider diversity
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum

from flowcore_story.utils.logger import logger


class ProxyType(Enum):
    """Proxy types in priority order."""
    RESIDENTIAL = "residential"  # Highest priority
    ISP = "isp"  # Medium-high priority
    MOBILE = "mobile"  # Medium priority
    DATACENTER = "datacenter"  # Lowest priority
    UNKNOWN = "unknown"


@dataclass
class ProxyReputation:
    """Reputation scoring for a proxy."""

    # Counters
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limited_requests: int = 0  # 429
    blocked_requests: int = 0  # 403

    # Time tracking
    first_used: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    last_success: float = field(default_factory=time.time)
    last_failure: float = 0.0

    # Derived metrics
    _reputation_score: float = 100.0
    _last_score_update: float = field(default_factory=time.time)

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests

    @property
    def block_rate(self) -> float:
        """Calculate block rate."""
        if self.total_requests == 0:
            return 0.0
        return (self.blocked_requests + self.rate_limited_requests) / self.total_requests

    @property
    def reputation_score(self) -> float:
        """Get current reputation score with decay."""
        # Apply time-based decay
        now = time.time()
        time_since_update = now - self._last_score_update

        # Decay factor: lose 10% per hour of inactivity
        decay_per_hour = 0.10
        hours_inactive = time_since_update / 3600
        decay = decay_per_hour * hours_inactive

        decayed_score = self._reputation_score * (1 - min(decay, 0.5))  # Max 50% decay

        return max(0.0, decayed_score)

    def update_score(self, success: bool, status_code: int | None = None) -> None:
        """Update reputation score based on request result.

        Args:
            success: Whether request was successful
            status_code: HTTP status code
        """
        now = time.time()

        self.total_requests += 1
        self.last_used = now

        if success:
            self.successful_requests += 1
            self.last_success = now
            # Increase score on success
            self._reputation_score = min(100.0, self._reputation_score + 1.0)
        else:
            self.failed_requests += 1
            self.last_failure = now

            # Different penalties based on failure type
            if status_code == 429:
                self.rate_limited_requests += 1
                self._reputation_score -= 5.0  # Moderate penalty
            elif status_code in [403, 451]:
                self.blocked_requests += 1
                self._reputation_score -= 10.0  # Heavy penalty
            else:
                self._reputation_score -= 3.0  # Light penalty for other failures

        self._reputation_score = max(0.0, self._reputation_score)
        self._last_score_update = now


@dataclass
class Proxy:
    """Proxy with metadata and reputation."""

    url: str  # Full proxy URL (e.g., http://user:pass@host:port)
    proxy_type: ProxyType
    provider: str  # Provider name

    # Network info
    asn: int | None = None
    region: str | None = None
    country: str | None = None

    # Reputation
    reputation: ProxyReputation = field(default_factory=ProxyReputation)

    # Status
    is_active: bool = True
    cooldown_until: float = 0.0
    quarantined: bool = False

    # Performance
    avg_response_time: float = 0.0
    response_times: list[float] = field(default_factory=list)

    def is_available(self) -> bool:
        """Check if proxy is available for use."""
        if not self.is_active or self.quarantined:
            return False
        if time.time() < self.cooldown_until:
            return False
        return True

    def record_response_time(self, response_time: float) -> None:
        """Record a response time.

        Args:
            response_time: Response time in seconds
        """
        self.response_times.append(response_time)

        # Keep only last 100 measurements
        if len(self.response_times) > 100:
            self.response_times = self.response_times[-100:]

        # Update average
        self.avg_response_time = sum(self.response_times) / len(self.response_times)


class ProxyPoolManager:
    """Advanced proxy pool management with reputation scoring."""

    def __init__(
        self,
        min_reputation_score: float = 30.0,
        cooldown_on_failure: float = 60.0,
        health_check_interval: float = 300.0,
    ):
        """Initialize proxy pool manager.

        Args:
            min_reputation_score: Minimum score to be eligible
            cooldown_on_failure: Cooldown after failure (seconds)
            health_check_interval: Health check interval (seconds)
        """
        self.min_reputation_score = min_reputation_score
        self.cooldown_on_failure = cooldown_on_failure
        self.health_check_interval = health_check_interval

        self.logger = logger.bind(component="ProxyPoolManager")

        # Proxy pool
        self._proxies: dict[str, Proxy] = {}

        # Indexes
        self._by_type: dict[ProxyType, list[str]] = {pt: [] for pt in ProxyType}
        self._by_provider: dict[str, list[str]] = {}

        # Health check task
        self._health_check_task: asyncio.Task | None = None

    def add_proxy(self, proxy: Proxy) -> None:
        """Add a proxy to the pool.

        Args:
            proxy: Proxy to add
        """
        if proxy.url in self._proxies:
            self.logger.warning(f"Proxy {proxy.url} already exists")
            return

        self._proxies[proxy.url] = proxy

        # Index by type
        self._by_type[proxy.proxy_type].append(proxy.url)

        # Index by provider
        if proxy.provider not in self._by_provider:
            self._by_provider[proxy.provider] = []
        self._by_provider[proxy.provider].append(proxy.url)

        self.logger.info(
            f"Added proxy {proxy.url} (type: {proxy.proxy_type.value}, provider: {proxy.provider})"
        )

    def remove_proxy(self, proxy_url: str) -> bool:
        """Remove a proxy from the pool.

        Args:
            proxy_url: Proxy URL

        Returns:
            True if removed
        """
        if proxy_url not in self._proxies:
            return False

        proxy = self._proxies.pop(proxy_url)

        # Remove from indexes
        self._by_type[proxy.proxy_type].remove(proxy_url)
        self._by_provider[proxy.provider].remove(proxy_url)

        self.logger.info(f"Removed proxy {proxy_url}")
        return True

    def select_proxy(
        self,
        prefer_residential: bool = True,
        required_provider: str | None = None,
        exclude_proxies: set[str] | None = None,
        site_key: str | None = None,
    ) -> Proxy | None:
        """Select a proxy with reputation-based selection.

        Args:
            prefer_residential: Prefer residential proxies
            required_provider: Required provider
            exclude_proxies: Proxy URLs to exclude
            site_key: Site key (for site-specific selection)

        Returns:
            Proxy or None
        """
        # Get available proxies
        candidates = [
            proxy for proxy in self._proxies.values()
            if proxy.is_available() and proxy.reputation.reputation_score >= self.min_reputation_score
        ]

        if not candidates:
            self.logger.warning("No available proxies")
            return None

        # Filter by provider
        if required_provider:
            candidates = [p for p in candidates if p.provider == required_provider]

        # Filter by exclusion
        if exclude_proxies:
            candidates = [p for p in candidates if p.url not in exclude_proxies]

        if not candidates:
            self.logger.warning("No candidates after filtering")
            return None

        # Prioritize by type if requested
        if prefer_residential:
            # Try residential first
            residential = [p for p in candidates if p.proxy_type == ProxyType.RESIDENTIAL]
            if residential:
                candidates = residential
            else:
                # Try ISP next
                isp = [p for p in candidates if p.proxy_type == ProxyType.ISP]
                if isp:
                    candidates = isp

        # Score candidates
        scored = []
        for proxy in candidates:
            score = self._score_proxy(proxy)
            scored.append((score, proxy))

        # Sort by score
        scored.sort(reverse=True, key=lambda x: x[0])

        # Select from top candidates with weighted randomness
        top_n = min(5, len(scored))
        weights = [s[0] for s in scored[:top_n]]
        selected = random.choices(scored[:top_n], weights=weights, k=1)[0][1]

        return selected

    def _score_proxy(self, proxy: Proxy) -> float:
        """Score a proxy for selection.

        Args:
            proxy: Proxy

        Returns:
            Score (higher is better)
        """
        score = 0.0

        # Base score from reputation
        score += proxy.reputation.reputation_score

        # Type priority bonus
        type_bonuses = {
            ProxyType.RESIDENTIAL: 50,
            ProxyType.ISP: 30,
            ProxyType.MOBILE: 20,
            ProxyType.DATACENTER: 0,
            ProxyType.UNKNOWN: -10,
        }
        score += type_bonuses.get(proxy.proxy_type, 0)

        # Success rate bonus
        score += proxy.reputation.success_rate * 20

        # Penalize slow proxies
        if proxy.avg_response_time > 5.0:
            score -= 10
        elif proxy.avg_response_time < 2.0:
            score += 5

        # Penalize recent failures
        time_since_failure = time.time() - proxy.reputation.last_failure
        if time_since_failure < 300:  # Last 5 minutes
            score -= 20

        # Bonus for recent success
        time_since_success = time.time() - proxy.reputation.last_success
        if time_since_success < 60:  # Last minute
            score += 10

        return score

    def record_request(
        self,
        proxy_url: str,
        success: bool,
        status_code: int | None = None,
        response_time: float | None = None,
    ) -> None:
        """Record a request result for a proxy.

        Args:
            proxy_url: Proxy URL
            success: Whether request was successful
            status_code: HTTP status code
            response_time: Response time in seconds
        """
        if proxy_url not in self._proxies:
            return

        proxy = self._proxies[proxy_url]

        # Update reputation
        proxy.reputation.update_score(success, status_code)

        # Record response time
        if response_time is not None:
            proxy.record_response_time(response_time)

        # Apply cooldown on failure
        if not success:
            proxy.cooldown_until = time.time() + self.cooldown_on_failure

            # Auto-deactivate if reputation too low
            if proxy.reputation.reputation_score < 10.0:
                proxy.is_active = False
                self.logger.warning(f"Auto-deactivated proxy {proxy_url} due to low reputation")

    async def start_health_check(self) -> None:
        """Start automatic health checking."""
        if self._health_check_task and not self._health_check_task.done():
            return

        async def health_check_loop():
            while True:
                await asyncio.sleep(self.health_check_interval)
                await self._perform_health_check()

        self._health_check_task = asyncio.create_task(health_check_loop())
        self.logger.info(f"Started health check (interval: {self.health_check_interval}s)")

    def stop_health_check(self) -> None:
        """Stop health checking."""
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            self.logger.info("Stopped health check")

    async def _perform_health_check(self) -> None:
        """Perform health check on all proxies."""
        self.logger.debug("Performing health check")

        # Check for proxies with very low reputation
        to_remove = []
        for url, proxy in self._proxies.items():
            if proxy.reputation.reputation_score < 5.0 and proxy.reputation.total_requests > 20:
                to_remove.append(url)

        for url in to_remove:
            self.logger.warning(f"Removing proxy {url} due to consistently poor performance")
            self.remove_proxy(url)

    def get_statistics(self) -> dict:
        """Get proxy pool statistics.

        Returns:
            Dict with statistics
        """
        total_proxies = len(self._proxies)
        active_proxies = sum(1 for p in self._proxies.values() if p.is_active)
        available_proxies = sum(1 for p in self._proxies.values() if p.is_available())

        # By type
        type_counts = {
            pt.value: len([url for url in urls if self._proxies[url].is_active])
            for pt, urls in self._by_type.items()
        }

        # Reputation distribution
        avg_reputation = sum(p.reputation.reputation_score for p in self._proxies.values()) / total_proxies if total_proxies > 0 else 0
        high_rep = sum(1 for p in self._proxies.values() if p.reputation.reputation_score >= 80)
        med_rep = sum(1 for p in self._proxies.values() if 50 <= p.reputation.reputation_score < 80)
        low_rep = sum(1 for p in self._proxies.values() if p.reputation.reputation_score < 50)

        # Success rates
        total_requests = sum(p.reputation.total_requests for p in self._proxies.values())
        total_success = sum(p.reputation.successful_requests for p in self._proxies.values())
        overall_success_rate = total_success / total_requests if total_requests > 0 else 0

        return {
            'total_proxies': total_proxies,
            'active_proxies': active_proxies,
            'available_proxies': available_proxies,
            'type_distribution': type_counts,
            'avg_reputation': avg_reputation,
            'high_reputation_count': high_rep,
            'medium_reputation_count': med_rep,
            'low_reputation_count': low_rep,
            'total_requests': total_requests,
            'overall_success_rate': overall_success_rate,
        }

    def print_status(self) -> None:
        """Print proxy pool status."""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("Proxy Pool Status")
        print("=" * 70)

        print("\nProxies:")
        print(f"  Total: {stats['total_proxies']}")
        print(f"  Active: {stats['active_proxies']}")
        print(f"  Available: {stats['available_proxies']}")

        print("\nBy Type:")
        for proxy_type, count in stats['type_distribution'].items():
            print(f"  {proxy_type}: {count}")

        print("\nReputation:")
        print(f"  Average: {stats['avg_reputation']:.1f}/100")
        print(f"  High (≥80): {stats['high_reputation_count']}")
        print(f"  Medium (50-79): {stats['medium_reputation_count']}")
        print(f"  Low (<50): {stats['low_reputation_count']}")

        print("\nPerformance:")
        print(f"  Total Requests: {stats['total_requests']}")
        print(f"  Success Rate: {stats['overall_success_rate']:.1%}")

        print("=" * 70)


# Global instance
_proxy_pool: ProxyPoolManager | None = None


def get_proxy_pool_manager() -> ProxyPoolManager:
    """Get global proxy pool manager.

    Returns:
        ProxyPoolManager
    """
    global _proxy_pool

    if _proxy_pool is None:
        import os

        _proxy_pool = ProxyPoolManager(
            min_reputation_score=float(os.environ.get("PROXY_MIN_REPUTATION", "30.0")),
            cooldown_on_failure=float(os.environ.get("PROXY_COOLDOWN", "60.0")),
            health_check_interval=float(os.environ.get("PROXY_HEALTH_CHECK_INTERVAL", "300.0")),
        )

    return _proxy_pool


if __name__ == "__main__":
    # Test proxy pool manager
    async def main():
        manager = get_proxy_pool_manager()

        # Add some test proxies
        for i in range(10):
            proxy_type = ProxyType.RESIDENTIAL if i < 3 else (ProxyType.ISP if i < 6 else ProxyType.DATACENTER)
            proxy = Proxy(
                url=f"http://proxy{i}:8080",
                proxy_type=proxy_type,
                provider=f"provider_{i % 3}",
            )
            manager.add_proxy(proxy)

        # Simulate some requests
        for i in range(50):
            proxy = manager.select_proxy(prefer_residential=True)
            if proxy:
                # Simulate result
                success = random.random() > 0.2  # 80% success
                status_code = 200 if success else random.choice([429, 403, 500])
                response_time = random.uniform(0.5, 3.0)

                manager.record_request(proxy.url, success, status_code, response_time)

                if i % 10 == 0:
                    print(f"\nAfter {i+1} requests:")
                    print(f"  Selected: {proxy.url}")
                    print(f"  Reputation: {proxy.reputation.reputation_score:.1f}")

        # Print status
        manager.print_status()

        # Start health check
        await manager.start_health_check()
        await asyncio.sleep(2)
        manager.stop_health_check()

    asyncio.run(main())


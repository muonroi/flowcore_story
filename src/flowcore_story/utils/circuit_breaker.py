"""Circuit Breaker - Fault tolerance for sites and fingerprints.

Implements circuit breaker pattern at two levels:
1. Site-level: Stop requests to problematic sites
2. Fingerprint-level: Stop using problematic fingerprints

States:
- CLOSED: Normal operation
- OPEN: Circuit tripped, requests blocked
- HALF_OPEN: Testing if service recovered

Prevents cascading failures and allows graceful degradation.
"""

import time
from dataclasses import dataclass
from enum import Enum

from flowcore_story.utils.logger import logger


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Circuit tripped, blocking requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""

    # Failure threshold
    failure_threshold: int = 5  # Number of failures to trip
    failure_rate_threshold: float = 0.5  # 50% failure rate
    min_requests: int = 10  # Minimum requests before calculating rate

    # Time windows
    timeout: float = 60.0  # Time to wait before trying again (seconds)
    rolling_window: float = 60.0  # Rolling window for failure tracking

    # Half-open testing
    half_open_max_requests: int = 3  # Max test requests in half-open state


@dataclass
class CircuitBreakerMetrics:
    """Metrics for a circuit breaker."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    # Recent history (timestamps)
    recent_successes: list = None
    recent_failures: list = None

    # State tracking
    last_failure_time: float = 0.0
    last_state_change: float = 0.0
    trip_count: int = 0

    def __post_init__(self):
        if self.recent_successes is None:
            self.recent_successes = []
        if self.recent_failures is None:
            self.recent_failures = []

    def add_success(self, timestamp: float) -> None:
        """Record a success."""
        self.total_requests += 1
        self.successful_requests += 1
        self.recent_successes.append(timestamp)

    def add_failure(self, timestamp: float) -> None:
        """Record a failure."""
        self.total_requests += 1
        self.failed_requests += 1
        self.recent_failures.append(timestamp)
        self.last_failure_time = timestamp

    def cleanup_old_records(self, cutoff_time: float) -> None:
        """Remove records older than cutoff."""
        self.recent_successes = [t for t in self.recent_successes if t >= cutoff_time]
        self.recent_failures = [t for t in self.recent_failures if t >= cutoff_time]

    def get_recent_failure_count(self) -> int:
        """Get count of recent failures."""
        return len(self.recent_failures)

    def get_recent_request_count(self) -> int:
        """Get count of recent requests."""
        return len(self.recent_successes) + len(self.recent_failures)

    def get_recent_failure_rate(self) -> float:
        """Get recent failure rate."""
        recent_total = self.get_recent_request_count()
        if recent_total == 0:
            return 0.0
        return len(self.recent_failures) / recent_total


class CircuitBreaker:
    """Circuit breaker for a specific entity (site or fingerprint)."""

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None
    ):
        """Initialize circuit breaker.

        Args:
            name: Circuit name (e.g., "site:tangthuvien" or "fingerprint:fp_123")
            config: CircuitBreakerConfig
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()

        self.state = CircuitState.CLOSED
        self.metrics = CircuitBreakerMetrics()

        self._half_open_requests = 0

        self.logger = logger.bind(circuit=name)

    def call(self) -> bool:
        """Check if a call is allowed.

        Returns:
            True if call is allowed, False if circuit is open
        """
        now = time.time()

        # Cleanup old metrics
        cutoff = now - self.config.rolling_window
        self.metrics.cleanup_old_records(cutoff)

        if self.state == CircuitState.CLOSED:
            return True

        elif self.state == CircuitState.OPEN:
            # Check if timeout has elapsed
            time_since_last_failure = now - self.metrics.last_failure_time

            if time_since_last_failure >= self.config.timeout:
                self.logger.info("Circuit transitioning to HALF_OPEN")
                self.state = CircuitState.HALF_OPEN
                self.metrics.last_state_change = now
                self._half_open_requests = 0
                return True

            return False

        elif self.state == CircuitState.HALF_OPEN:
            # Allow limited test requests
            if self._half_open_requests < self.config.half_open_max_requests:
                self._half_open_requests += 1
                return True

            return False

        return False

    def record_success(self) -> None:
        """Record a successful call."""
        now = time.time()
        self.metrics.add_success(now)

        if self.state == CircuitState.HALF_OPEN:
            # Check if we can close the circuit
            if self._half_open_requests >= self.config.half_open_max_requests:
                self.logger.info("Circuit recovery successful, closing circuit")
                self.state = CircuitState.CLOSED
                self.metrics.last_state_change = now
                self._half_open_requests = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        now = time.time()
        self.metrics.add_failure(now)

        if self.state == CircuitState.HALF_OPEN:
            # Failure during half-open, trip again
            self.logger.warning("Circuit failed during HALF_OPEN, reopening")
            self.state = CircuitState.OPEN
            self.metrics.last_state_change = now
            self.metrics.trip_count += 1
            self._half_open_requests = 0

        elif self.state == CircuitState.CLOSED:
            # Check if we should trip
            if self._should_trip():
                self.logger.error("Circuit breaker tripped!")
                self.state = CircuitState.OPEN
                self.metrics.last_state_change = now
                self.metrics.trip_count += 1

    def _should_trip(self) -> bool:
        """Check if circuit should trip.

        Returns:
            True if circuit should trip
        """
        recent_failures = self.metrics.get_recent_failure_count()
        recent_requests = self.metrics.get_recent_request_count()

        # Not enough data yet
        if recent_requests < self.config.min_requests:
            return False

        # Check absolute failure threshold
        if recent_failures >= self.config.failure_threshold:
            return True

        # Check failure rate threshold
        failure_rate = self.metrics.get_recent_failure_rate()
        if failure_rate >= self.config.failure_rate_threshold:
            return True

        return False

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self.logger.info("Circuit manually reset")
        self.state = CircuitState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        self._half_open_requests = 0

    def get_status(self) -> dict:
        """Get circuit breaker status.

        Returns:
            Dict with status information
        """
        return {
            'name': self.name,
            'state': self.state.value,
            'total_requests': self.metrics.total_requests,
            'successful_requests': self.metrics.successful_requests,
            'failed_requests': self.metrics.failed_requests,
            'recent_failure_count': self.metrics.get_recent_failure_count(),
            'recent_failure_rate': self.metrics.get_recent_failure_rate(),
            'trip_count': self.metrics.trip_count,
            'time_since_last_failure': time.time() - self.metrics.last_failure_time if self.metrics.last_failure_time > 0 else None,
        }


class CircuitBreakerManager:
    """Manages multiple circuit breakers."""

    def __init__(
        self,
        site_config: CircuitBreakerConfig | None = None,
        fingerprint_config: CircuitBreakerConfig | None = None,
    ):
        """Initialize circuit breaker manager.

        Args:
            site_config: Config for site-level breakers
            fingerprint_config: Config for fingerprint-level breakers
        """
        self.site_config = site_config or CircuitBreakerConfig(
            failure_threshold=10,
            failure_rate_threshold=0.6,
            timeout=300.0,  # 5 minutes
        )

        self.fingerprint_config = fingerprint_config or CircuitBreakerConfig(
            failure_threshold=5,
            failure_rate_threshold=0.5,
            timeout=600.0,  # 10 minutes
        )

        self.logger = logger.bind(component="CircuitBreakerManager")

        # Circuit breakers
        self._site_breakers: dict[str, CircuitBreaker] = {}
        self._fingerprint_breakers: dict[str, CircuitBreaker] = {}

    def check_site(self, site_key: str) -> bool:
        """Check if requests to a site are allowed.

        Args:
            site_key: Site key

        Returns:
            True if allowed
        """
        if site_key not in self._site_breakers:
            self._site_breakers[site_key] = CircuitBreaker(
                f"site:{site_key}",
                self.site_config
            )

        return self._site_breakers[site_key].call()

    def check_fingerprint(self, fingerprint_id: str) -> bool:
        """Check if a fingerprint can be used.

        Args:
            fingerprint_id: Fingerprint ID

        Returns:
            True if allowed
        """
        if fingerprint_id not in self._fingerprint_breakers:
            self._fingerprint_breakers[fingerprint_id] = CircuitBreaker(
                f"fingerprint:{fingerprint_id}",
                self.fingerprint_config
            )

        return self._fingerprint_breakers[fingerprint_id].call()

    def record_site_result(self, site_key: str, success: bool) -> None:
        """Record a site request result.

        Args:
            site_key: Site key
            success: Whether request was successful
        """
        if site_key not in self._site_breakers:
            return

        if success:
            self._site_breakers[site_key].record_success()
        else:
            self._site_breakers[site_key].record_failure()

    def record_fingerprint_result(self, fingerprint_id: str, success: bool) -> None:
        """Record a fingerprint usage result.

        Args:
            fingerprint_id: Fingerprint ID
            success: Whether request was successful
        """
        if fingerprint_id not in self._fingerprint_breakers:
            return

        if success:
            self._fingerprint_breakers[fingerprint_id].record_success()
        else:
            self._fingerprint_breakers[fingerprint_id].record_failure()

    def get_tripped_sites(self) -> list:
        """Get list of sites with tripped circuit breakers.

        Returns:
            List of site keys
        """
        return [
            site_key
            for site_key, breaker in self._site_breakers.items()
            if breaker.state == CircuitState.OPEN
        ]

    def get_tripped_fingerprints(self) -> list:
        """Get list of fingerprints with tripped circuit breakers.

        Returns:
            List of fingerprint IDs
        """
        return [
            fp_id
            for fp_id, breaker in self._fingerprint_breakers.items()
            if breaker.state == CircuitState.OPEN
        ]

    def get_statistics(self) -> dict:
        """Get circuit breaker statistics.

        Returns:
            Dict with statistics
        """
        site_breakers_total = len(self._site_breakers)
        site_breakers_tripped = len([b for b in self._site_breakers.values() if b.state == CircuitState.OPEN])

        fp_breakers_total = len(self._fingerprint_breakers)
        fp_breakers_tripped = len([b for b in self._fingerprint_breakers.values() if b.state == CircuitState.OPEN])

        return {
            'site_breakers': {
                'total': site_breakers_total,
                'tripped': site_breakers_tripped,
                'tripped_list': self.get_tripped_sites(),
            },
            'fingerprint_breakers': {
                'total': fp_breakers_total,
                'tripped': fp_breakers_tripped,
                'tripped_list': self.get_tripped_fingerprints(),
            }
        }

    def print_status(self) -> None:
        """Print circuit breaker status."""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("Circuit Breaker Status")
        print("=" * 70)

        print("\nSite-Level Breakers:")
        print(f"  Total: {stats['site_breakers']['total']}")
        print(f"  Tripped: {stats['site_breakers']['tripped']}")
        if stats['site_breakers']['tripped_list']:
            print(f"  Tripped sites: {', '.join(stats['site_breakers']['tripped_list'])}")

        print("\nFingerprint-Level Breakers:")
        print(f"  Total: {stats['fingerprint_breakers']['total']}")
        print(f"  Tripped: {stats['fingerprint_breakers']['tripped']}")
        if stats['fingerprint_breakers']['tripped_list']:
            print(f"  Tripped fingerprints: {', '.join(stats['fingerprint_breakers']['tripped_list'][:5])}")

        print("=" * 70)


# Global instance
_circuit_breaker_manager: CircuitBreakerManager | None = None


def get_circuit_breaker_manager() -> CircuitBreakerManager:
    """Get global circuit breaker manager.

    Returns:
        CircuitBreakerManager
    """
    global _circuit_breaker_manager

    if _circuit_breaker_manager is None:
        import os

        site_config = CircuitBreakerConfig(
            failure_threshold=int(os.environ.get("CB_SITE_FAILURE_THRESHOLD", "10")),
            failure_rate_threshold=float(os.environ.get("CB_SITE_FAILURE_RATE", "0.6")),
            timeout=float(os.environ.get("CB_SITE_TIMEOUT", "300")),
        )

        fp_config = CircuitBreakerConfig(
            failure_threshold=int(os.environ.get("CB_FP_FAILURE_THRESHOLD", "5")),
            failure_rate_threshold=float(os.environ.get("CB_FP_FAILURE_RATE", "0.5")),
            timeout=float(os.environ.get("CB_FP_TIMEOUT", "600")),
        )

        _circuit_breaker_manager = CircuitBreakerManager(
            site_config=site_config,
            fingerprint_config=fp_config,
        )

    return _circuit_breaker_manager


if __name__ == "__main__":
    # Test circuit breaker
    manager = get_circuit_breaker_manager()

    site = "tangthuvien"
    fingerprint = "fp_123"

    print(f"Testing circuit breakers for {site} and {fingerprint}")

    # Simulate successful requests
    for i in range(5):
        if manager.check_site(site):
            manager.record_site_result(site, True)
            print(f"  Request {i+1}: Success")

    # Simulate failures
    for i in range(12):
        if manager.check_site(site):
            success = i < 2  # First 2 succeed, rest fail
            manager.record_site_result(site, success)
            print(f"  Request {i+6}: {'Success' if success else 'Failure'}")
        else:
            print(f"  Request {i+6}: BLOCKED (circuit open)")

    # Print status
    manager.print_status()

    # Check if site is allowed
    print(f"\nCan access {site}? {manager.check_site(site)}")


"""
Circuit breaker pattern for database operations.
Prevents cascading failures when database is unavailable.
"""

import os
import time
from enum import Enum

from flowcore_story.utils.logger import logger


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if recovered


class DatabaseCircuitBreaker:
    """Circuit breaker for database operations."""

    def __init__(
        self,
        failure_threshold: int | None = None,
        recovery_timeout: float | None = None,
        success_threshold: int | None = None
    ):
        # Load from environment with defaults
        self.failure_threshold = failure_threshold or int(
            os.getenv("POSTGRES_STATE_CIRCUIT_FAILURE_THRESHOLD", "5")
        )
        self.recovery_timeout = recovery_timeout or float(
            os.getenv("POSTGRES_STATE_CIRCUIT_RECOVERY_TIMEOUT", "60.0")
        )
        self.success_threshold = success_threshold or int(
            os.getenv("POSTGRES_STATE_CIRCUIT_SUCCESS_THRESHOLD", "2")
        )

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0

        logger.info(
            f"[CircuitBreaker] Initialized: "
            f"failure_threshold={self.failure_threshold}, "
            f"recovery_timeout={self.recovery_timeout}s, "
            f"success_threshold={self.success_threshold}"
        )

    def is_available(self) -> bool:
        """Check if requests should be allowed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            # Check if recovery timeout elapsed
            if time.monotonic() - self.last_failure_time >= self.recovery_timeout:
                logger.info("[CircuitBreaker] Recovery timeout elapsed, transitioning to HALF_OPEN")
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                return True

            # Still in OPEN state
            return False

        # HALF_OPEN - allow requests to test recovery
        return True

    def record_success(self) -> None:
        """Record successful operation."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            logger.debug(
                f"[CircuitBreaker] Success in HALF_OPEN: "
                f"{self.success_count}/{self.success_threshold}"
            )

            if self.success_count >= self.success_threshold:
                logger.info("[CircuitBreaker] Recovery confirmed, transitioning to CLOSED")
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == CircuitState.CLOSED:
            # Reset failure count on success
            if self.failure_count > 0:
                logger.debug(f"[CircuitBreaker] Resetting failure count from {self.failure_count}")
                self.failure_count = 0

    def record_failure(self, error: Exception | None = None) -> None:
        """Record failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.monotonic()

        error_msg = f": {error}" if error else ""
        logger.warning(
            f"[CircuitBreaker] Failure recorded ({self.failure_count}/{self.failure_threshold}){error_msg}"
        )

        if self.state == CircuitState.HALF_OPEN:
            # Failed during recovery test, go back to OPEN
            logger.warning("[CircuitBreaker] Recovery test failed, transitioning back to OPEN")
            self.state = CircuitState.OPEN
            self.success_count = 0

        elif self.failure_count >= self.failure_threshold:
            # Too many failures, open circuit
            logger.error(
                f"[CircuitBreaker] Failure threshold reached ({self.failure_count}), "
                f"transitioning to OPEN (will retry after {self.recovery_timeout}s)"
            )
            self.state = CircuitState.OPEN

    def get_state(self) -> dict:
        """Get current circuit breaker state."""
        return {
            "state": self.state.value,
            "available": self.is_available(),
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time,
            "config": {
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "success_threshold": self.success_threshold,
            }
        }


# Global instance
_circuit_breaker: DatabaseCircuitBreaker | None = None


def get_circuit_breaker() -> DatabaseCircuitBreaker:
    """Get or create global circuit breaker instance."""
    global _circuit_breaker
    if _circuit_breaker is None:
        _circuit_breaker = DatabaseCircuitBreaker()
    return _circuit_breaker


def reset_circuit_breaker() -> None:
    """Reset circuit breaker (mainly for testing)."""
    global _circuit_breaker
    _circuit_breaker = None

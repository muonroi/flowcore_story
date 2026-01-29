"""
Metrics collection for state save operations.
Tracks performance and usage statistics.
"""

import time
from dataclasses import dataclass, field


@dataclass
class StateSaveMetrics:
    """Metrics for state save operations."""

    # Counters
    total_saves: int = 0
    db_saves: int = 0
    file_saves: int = 0
    batch_saves: int = 0
    skipped_debounce: int = 0
    skipped_unchanged: int = 0
    failed_saves: int = 0

    # Timing (in seconds)
    total_save_time: float = 0.0
    last_save_time: float = 0.0
    min_save_time: float = float('inf')
    max_save_time: float = 0.0

    # Per-site metrics
    per_site: dict[str, 'StateSaveMetrics'] = field(default_factory=dict)

    def record_save(
        self,
        duration: float,
        backend: str,
        site_key: str = None,
        success: bool = True
    ) -> None:
        """Record a save operation."""
        self.total_saves += 1
        self.total_save_time += duration
        self.last_save_time = time.monotonic()

        # Update min/max
        if duration < self.min_save_time:
            self.min_save_time = duration
        if duration > self.max_save_time:
            self.max_save_time = duration

        # Count by backend
        if backend == "db":
            self.db_saves += 1
        elif backend == "file":
            self.file_saves += 1
        elif backend == "batch":
            self.batch_saves += 1

        # Track failures
        if not success:
            self.failed_saves += 1

        # Per-site tracking
        if site_key:
            if site_key not in self.per_site:
                self.per_site[site_key] = StateSaveMetrics()

            self.per_site[site_key].record_save(
                duration=duration,
                backend=backend,
                success=success
            )

    def record_skip(self, reason: str, site_key: str = None) -> None:
        """Record a skipped save."""
        if reason == "debounce":
            self.skipped_debounce += 1
        elif reason == "unchanged":
            self.skipped_unchanged += 1

        if site_key and site_key in self.per_site:
            self.per_site[site_key].record_skip(reason)

    def get_summary(self) -> dict:
        """Get metrics summary."""
        avg_time = (
            self.total_save_time / self.total_saves
            if self.total_saves > 0
            else 0
        )

        min_time = self.min_save_time if self.min_save_time != float('inf') else 0
        max_time = self.max_save_time

        return {
            "total_saves": self.total_saves,
            "db_saves": self.db_saves,
            "file_saves": self.file_saves,
            "batch_saves": self.batch_saves,
            "skipped": self.skipped_debounce + self.skipped_unchanged,
            "skipped_debounce": self.skipped_debounce,
            "skipped_unchanged": self.skipped_unchanged,
            "failed": self.failed_saves,
            "timing_ms": {
                "avg": round(avg_time * 1000, 2),
                "min": round(min_time * 1000, 2),
                "max": round(max_time * 1000, 2),
                "total": round(self.total_save_time * 1000, 2),
            },
            "sites_tracked": len(self.per_site),
            "success_rate": round(
                (self.total_saves - self.failed_saves) / self.total_saves * 100, 2
            ) if self.total_saves > 0 else 100.0
        }

    def get_per_site_summary(self) -> dict[str, dict]:
        """Get per-site metrics summary."""
        return {
            site_key: metrics.get_summary()
            for site_key, metrics in self.per_site.items()
        }

    def reset(self) -> None:
        """Reset all metrics."""
        self.total_saves = 0
        self.db_saves = 0
        self.file_saves = 0
        self.batch_saves = 0
        self.skipped_debounce = 0
        self.skipped_unchanged = 0
        self.failed_saves = 0
        self.total_save_time = 0.0
        self.last_save_time = 0.0
        self.min_save_time = float('inf')
        self.max_save_time = 0.0
        self.per_site.clear()


# Global instance
_metrics = StateSaveMetrics()


def get_state_metrics() -> StateSaveMetrics:
    """Get global state metrics instance."""
    return _metrics


def reset_state_metrics() -> None:
    """Reset global state metrics."""
    _metrics.reset()

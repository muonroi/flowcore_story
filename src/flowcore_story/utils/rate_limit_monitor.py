"""Track rate limit responses and provide snapshots for monitoring."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from threading import Lock


@dataclass(frozen=True)
class RateLimitSnapshot:
    """Immutable view of rate limit statistics for a single site."""

    site_key: str
    total_requests: int
    rate_limited: int
    ratio: float
    last_event_at: float | None
    last_triggered_at: float | None


class _SiteRateLimitState:
    __slots__ = ("events", "total", "limited", "last_event", "last_triggered")

    def __init__(self) -> None:
        self.events: deque[tuple[float, bool]] = deque()
        self.total: int = 0
        self.limited: int = 0
        self.last_event: float = 0.0
        self.last_triggered: float = 0.0


class RateLimitMonitor:
    """Maintain a sliding window of HTTP responses to detect rate limiting."""

    def __init__(self, *, window_seconds: float = 600.0) -> None:
        self._window = max(float(window_seconds), 30.0)
        self._sites: dict[str, _SiteRateLimitState] = {}
        self._lock = Lock()

    # ------------------------------------------------------------------
    def configure_window(self, seconds: float) -> None:
        """Update the observation window for future events."""

        with self._lock:
            self._window = max(float(seconds), 30.0)

    def record_response(self, site_key: str, status_code: int | None) -> None:
        """Record an HTTP response status for ``site_key``."""

        if not site_key:
            return
        limited = bool(status_code == 429)
        now = time.time()
        with self._lock:
            state = self._sites.setdefault(site_key, _SiteRateLimitState())
            self._prune_locked(state, now)
            state.events.append((now, limited))
            state.total += 1
            if limited:
                state.limited += 1
            state.last_event = now

    # ------------------------------------------------------------------
    def mark_refreshed(self, site_key: str, *, reset_counters: bool = True) -> None:
        """Mark that mitigation (e.g. cookie refresh) ran for ``site_key``."""

        if not site_key:
            return
        now = time.time()
        with self._lock:
            state = self._sites.setdefault(site_key, _SiteRateLimitState())
            state.last_triggered = now
            if reset_counters:
                state.events.clear()
                state.total = 0
                state.limited = 0

    # ------------------------------------------------------------------
    def get_snapshot(self, site_key: str) -> RateLimitSnapshot:
        """Return the current statistics for ``site_key``."""

        now = time.time()
        with self._lock:
            state = self._sites.get(site_key)
            if not state:
                return RateLimitSnapshot(
                    site_key=site_key,
                    total_requests=0,
                    rate_limited=0,
                    ratio=0.0,
                    last_event_at=None,
                    last_triggered_at=None,
                )
            self._prune_locked(state, now)
            total = max(state.total, 0)
            limited = max(state.limited, 0)
            ratio = (limited / total) if total else 0.0
            last_event = state.last_event or None
            last_triggered = state.last_triggered or None

        return RateLimitSnapshot(
            site_key=site_key,
            total_requests=total,
            rate_limited=limited,
            ratio=ratio,
            last_event_at=last_event,
            last_triggered_at=last_triggered,
        )

    def collect_snapshots(
        self, site_keys: Iterable[str] | None = None
    ) -> Mapping[str, RateLimitSnapshot]:
        """Collect snapshots for the provided ``site_keys`` (or all sites)."""

        if site_keys is None:
            with self._lock:
                keys = list(self._sites.keys())
        else:
            keys = list(site_keys)
        return {key: self.get_snapshot(key) for key in keys}

    def get_hotspots(
        self,
        *,
        limit: int = 5,
        min_requests: int = 10,
    ) -> list[RateLimitSnapshot]:
        """Return the highest rate-limited sites within the sliding window."""

        if limit <= 0:
            return []

        snapshots: list[RateLimitSnapshot] = []
        now = time.time()
        with self._lock:
            for site_key, state in self._sites.items():
                self._prune_locked(state, now)
                total = max(state.total, 0)
                if total < max(int(min_requests), 0):
                    continue
                limited = max(state.limited, 0)
                ratio = (limited / total) if total else 0.0
                snapshots.append(
                    RateLimitSnapshot(
                        site_key=site_key,
                        total_requests=total,
                        rate_limited=limited,
                        ratio=ratio,
                        last_event_at=state.last_event or None,
                        last_triggered_at=state.last_triggered or None,
                    )
                )

        snapshots.sort(key=lambda snapshot: snapshot.ratio, reverse=True)
        return snapshots[:limit]

    # ------------------------------------------------------------------
    def _prune_locked(self, state: _SiteRateLimitState, now: float) -> None:
        cutoff = now - self._window
        while state.events and state.events[0][0] < cutoff:
            _, limited = state.events.popleft()
            state.total -= 1
            if limited:
                state.limited -= 1
        if state.total < 0:
            state.total = 0
        if state.limited < 0:
            state.limited = 0


rate_limit_monitor = RateLimitMonitor()

__all__ = ["RateLimitMonitor", "RateLimitSnapshot", "rate_limit_monitor"]


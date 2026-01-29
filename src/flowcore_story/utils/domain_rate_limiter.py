"""Adaptive per-domain limiter backed by crawl metrics."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from flowcore_story.config.config import ASYNC_SEMAPHORE_LIMIT
from flowcore_story.config.proxy_provider import handle_site_block_event
from flowcore_story.utils.logger import logger
from flowcore_story.utils.metrics_tracker import SiteHealthSnapshot, metrics_tracker
from flowcore_story.utils.notifier import send_telegram_notify


class DomainCircuitBreaker:
    """Coordinate concurrent requests per domain using health metrics."""

    def __init__(
        self,
        *,
        base_limit: int = ASYNC_SEMAPHORE_LIMIT,
        metrics=metrics_tracker,
        warmup_attempts: int = 12,
        degrade_threshold: float = 0.35,
        block_threshold: float = 0.6,
        degrade_ratio: float = 0.4,
        moderate_delay: float = 0.75,
        severe_delay: float = 1.5,
        fresh_window: float = 120.0,
        cooldown_seconds: float = 90.0,
        min_limit: int = 1,
        block_alert_threshold: int = 3,
        block_alert_window: float = 600.0,
        block_alert_cooldown: float = 900.0,
        notifier=send_telegram_notify,
    ) -> None:
        self._base_limit = max(int(base_limit), 1)
        self._metrics = metrics
        self._warmup_attempts = max(int(warmup_attempts), 0)
        self._degrade_threshold = max(float(degrade_threshold), 0.0)
        self._block_threshold = max(float(block_threshold), self._degrade_threshold)
        self._degrade_ratio = max(min(float(degrade_ratio), 1.0), 0.0)
        self._moderate_delay = max(float(moderate_delay), 0.0)
        self._severe_delay = max(float(severe_delay), 0.0)
        self._fresh_window = max(float(fresh_window), 0.0)
        self._cooldown_seconds = max(float(cooldown_seconds), 0.0)
        self._min_limit = max(int(min_limit), 1)

        self._condition = asyncio.Condition()
        self._inflight: dict[str, int] = defaultdict(int)
        self._cooldowns: dict[str, float] = {}
        self._last_limits: dict[str, int] = {}
        self._last_reasons: dict[str, str | None] = {}
        self._block_history: dict[str, deque[float]] = defaultdict(deque)
        self._block_alert_cooldowns: dict[str, float] = {}
        self._block_alert_threshold = max(int(block_alert_threshold), 1)
        self._block_alert_window = max(float(block_alert_window), 0.0)
        self._block_alert_cooldown = max(float(block_alert_cooldown), 0.0)
        self._notifier = notifier

    # ------------------------------------------------------------------
    @asynccontextmanager
    async def limit(self, site_key: str) -> AsyncIterator[None]:
        """Acquire a concurrency slot for ``site_key``."""

        snapshot = await self._acquire_slot(site_key)
        try:
            delay = self._compute_delay(site_key, snapshot)
            if delay > 0:
                await asyncio.sleep(delay)
            yield
        finally:
            await self._release_slot(site_key)

    # ------------------------------------------------------------------
    async def _acquire_slot(self, site_key: str) -> SiteHealthSnapshot | None:
        while True:
            snapshot = self._get_snapshot(site_key)
            limit = self._determine_limit(site_key, snapshot)
            async with self._condition:
                inflight = self._inflight[site_key]
                if inflight < limit:
                    self._inflight[site_key] = inflight + 1
                    return snapshot
                await self._condition.wait()

    async def _release_slot(self, site_key: str) -> None:
        async with self._condition:
            inflight = self._inflight.get(site_key, 0)
            if inflight <= 1:
                self._inflight.pop(site_key, None)
            else:
                self._inflight[site_key] = inflight - 1
            self._condition.notify_all()

    # ------------------------------------------------------------------
    def _get_snapshot(self, site_key: str) -> SiteHealthSnapshot | None:
        try:
            snapshot = self._metrics.get_site_health_snapshot(site_key)
        except AttributeError:  # pragma: no cover - defensive guard
            return None
        return snapshot

    def _determine_limit(
        self, site_key: str, snapshot: SiteHealthSnapshot | None
    ) -> int:
        limit = self._base_limit
        reason: str | None = None
        now = time.time()

        if snapshot is not None:
            attempts = max(int(snapshot.success + snapshot.failure), 0)
            recent_enough = (now - snapshot.updated_at) <= self._fresh_window
            if attempts >= self._warmup_attempts and recent_enough and attempts > 0:
                failure_rate = snapshot.failure / attempts
                if failure_rate >= self._block_threshold:
                    limit = self._min_limit
                    reason = f"failure rate {failure_rate:.0%}"
                    if self._cooldown_seconds > 0:
                        self._cooldowns[site_key] = now + self._cooldown_seconds
                elif failure_rate >= self._degrade_threshold:
                    degraded = int(self._base_limit * self._degrade_ratio)
                    limit = max(self._min_limit, degraded)
                    reason = f"elevated failure rate {failure_rate:.0%}"

        cooldown_until = self._cooldowns.get(site_key)
        if cooldown_until:
            if cooldown_until > now:
                limit = self._min_limit
                if not reason:
                    remaining = int(cooldown_until - now)
                    reason = f"cooldown {remaining}s"
            else:
                self._cooldowns.pop(site_key, None)

        previous_limit = self._last_limits.get(site_key)
        previous_reason = self._last_reasons.get(site_key)
        self._maybe_log_limit(site_key, limit, reason)
        if (
            limit <= self._min_limit
            and reason
            and (
                previous_limit is None
                or limit < previous_limit
                or reason != previous_reason
            )
        ):
            self._handle_block_event(site_key, reason, snapshot)
        return max(limit, self._min_limit)

    def _compute_delay(
        self, site_key: str, snapshot: SiteHealthSnapshot | None
    ) -> float:
        now = time.time()
        cooldown_until = self._cooldowns.get(site_key)
        if cooldown_until and cooldown_until > now:
            remaining = cooldown_until - now
            return min(self._severe_delay or remaining, remaining)

        if snapshot is None:
            return 0.0

        attempts = max(int(snapshot.success + snapshot.failure), 0)
        recent_enough = (now - snapshot.updated_at) <= self._fresh_window
        if attempts < self._warmup_attempts or not recent_enough or attempts <= 0:
            return 0.0

        failure_rate = snapshot.failure / attempts
        if failure_rate >= self._block_threshold:
            return self._severe_delay
        if failure_rate >= self._degrade_threshold:
            return self._moderate_delay
        return 0.0

    def _maybe_log_limit(
        self, site_key: str, limit: int, reason: str | None
    ) -> None:
        previous = self._last_limits.get(site_key)
        if previous is None:
            self._last_limits[site_key] = limit
            self._last_reasons[site_key] = reason
            return

        if limit == previous and reason == self._last_reasons.get(site_key):
            return

        self._last_limits[site_key] = limit
        self._last_reasons[site_key] = reason

        if limit < previous:
            detail = f" giảm còn {limit}" if reason is None else f" giảm còn {limit} ({reason})"
            logger.warning(f"[CIRCUIT] Giới hạn domain {site_key}{detail}")
        elif limit > previous:
            logger.info(
                f"[CIRCUIT] Phục hồi giới hạn domain {site_key} lên {limit}"
                + (f" sau {reason}" if reason else "")
            )

    # ------------------------------------------------------------------
    def _handle_block_event(
        self, site_key: str, reason: str, snapshot: SiteHealthSnapshot | None
    ) -> None:
        now = time.time()
        history = self._block_history[site_key]
        history.append(now)
        if self._block_alert_window:
            cutoff = now - self._block_alert_window
            while history and history[0] < cutoff:
                history.popleft()

        handle_site_block_event(site_key, reason=reason)

        alert_due = False
        if self._block_alert_threshold and self._block_alert_window:
            alert_due = len(history) >= self._block_alert_threshold

        cooldown_until = self._block_alert_cooldowns.get(site_key, 0.0)
        if not alert_due or (self._block_alert_cooldown and now < cooldown_until):
            return

        self._block_alert_cooldowns[site_key] = now + self._block_alert_cooldown

        attempts = 0
        failure_rate = 0.0
        success = failure = None
        if snapshot is not None:
            attempts = max(int(snapshot.success + snapshot.failure), 0)
            success = snapshot.success
            failure = snapshot.failure
            if attempts:
                failure_rate = snapshot.failure / attempts

        message = (
            f"[CIRCUIT][ALERT] Site {site_key} bị chặn {len(history)} lần"
            f" trong {int(self._block_alert_window // 60) or 1} phút."
            f" Lý do gần nhất: {reason}."
        )
        logger.error(message)
        extra = {
            "failure_rate": f"{failure_rate:.2%}",
            "attempts": attempts,
            "success": success,
            "failure": failure,
        }
        self._dispatch_notification(site_key, message, extra)

    def _dispatch_notification(
        self, site_key: str, message: str, extra: dict[str, object]
    ) -> None:
        if not self._notifier:
            return

        try:
            if inspect.iscoroutinefunction(self._notifier):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    asyncio.run(
                        self._notifier(message, status="error", extra=extra)
                    )
                else:
                    loop.create_task(
                        self._notifier(message, status="error", extra=extra)
                    )
            else:
                self._notifier(message, status="error", extra=extra)
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception(
                f"[CIRCUIT] Không gửi được cảnh báo block domain {site_key}: {exc}"
            )


domain_circuit_breaker = DomainCircuitBreaker()

__all__ = ["domain_circuit_breaker", "DomainCircuitBreaker"]


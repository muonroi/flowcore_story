"""Monitoring helpers for site health and automatic alerting."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from flowcore_story.utils.errors import CrawlError
from flowcore_story.utils.logger import logger
from flowcore_story.utils.metrics_tracker import metrics_tracker
from flowcore_story.utils.notifier import send_telegram_notify

Notifier = Callable[..., Any]


@dataclass
class SiteHealth:
    site_key: str
    success: int = 0
    failure: int = 0
    last_error: str | None = None
    last_alert_at: float = 0.0
    updated_at: float = field(default_factory=time.time)


class SiteHealthMonitor:
    """Track per-site success/failure counts and send alerts when needed."""

    def __init__(
        self,
        *,
        failure_threshold: float = 0.5,
        min_attempts: int = 10,
        alert_cooldown: int = 900,
        notifier: Notifier | None = None,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.min_attempts = min_attempts
        self.alert_cooldown = alert_cooldown
        self._notifier = notifier or send_telegram_notify
        self._sites: dict[str, SiteHealth] = {}

    # ------------------------------------------------------------------
    def record_success(self, site_key: str) -> None:
        state = self._sites.setdefault(site_key, SiteHealth(site_key=site_key))
        state.success += 1
        state.updated_at = time.time()
        metrics_tracker.update_site_health(site_key, success_delta=1)

    def record_failure(self, site_key: str, error: CrawlError) -> None:
        state = self._sites.setdefault(site_key, SiteHealth(site_key=site_key))
        state.failure += 1
        state.last_error = error.value
        state.updated_at = time.time()
        metrics_tracker.update_site_health(site_key, failure_delta=1, last_error=error.value)
        self._maybe_alert(site_key, state)

    # ------------------------------------------------------------------
    def _maybe_alert(self, site_key: str, state: SiteHealth) -> None:
        attempts = state.success + state.failure
        if attempts < self.min_attempts:
            return
        failure_rate = state.failure / attempts if attempts else 0.0
        now = time.time()
        if failure_rate < self.failure_threshold:
            return
        if (now - state.last_alert_at) < self.alert_cooldown:
            return

        state.last_alert_at = now
        metrics_tracker.update_site_health(site_key, last_alert_at=now)
        message = (
            f"[ALERT][SITE] Site {site_key} có tỷ lệ lỗi cao {failure_rate:.0%}"
        )
        logger.error(message)
        extra = {
            "failure_rate": f"{failure_rate:.2%}",
            "success": state.success,
            "failure": state.failure,
            "last_error": state.last_error,
        }
        self._dispatch_notification(message, extra)

    def _dispatch_notification(self, message: str, extra: dict[str, Any]) -> None:
        if not self._notifier:
            return

        try:
            if inspect.iscoroutinefunction(self._notifier):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    asyncio.run(self._notifier(message, status="warning", extra=extra))
                else:
                    loop.create_task(self._notifier(message, status="warning", extra=extra))
            else:
                self._notifier(message, status="warning", extra=extra)
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception(f"[ALERT] Không gửi được cảnh báo site {message}: {exc}")


site_health_monitor = SiteHealthMonitor()

__all__ = ["site_health_monitor", "SiteHealthMonitor"]

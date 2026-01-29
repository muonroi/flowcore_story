"""Alert Manager - Monitor metrics and trigger alerts.

This module monitors key metrics and triggers alerts based on thresholds:
- 429/403 rate thresholds
- CF-Ray pattern anomalies
- Fingerprint block rate
- Proxy block rate
- Bot score trends

Integrates with notification channels (email, Slack, webhook).
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import httpx

from flowcore_story.utils.logger import logger


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertType(Enum):
    """Alert types."""
    HIGH_BLOCK_RATE = "high_block_rate"
    HIGH_RATE_LIMIT = "high_rate_limit"
    FINGERPRINT_BLOCKED = "fingerprint_blocked"
    PROXY_BLOCKED = "proxy_blocked"
    CF_RAY_ANOMALY = "cf_ray_anomaly"
    BOT_SCORE_DEGRADATION = "bot_score_degradation"
    PROFILE_QUARANTINED = "profile_quarantined"


@dataclass
class Alert:
    """Alert data."""

    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    timestamp: float = field(default_factory=time.time)

    # Context
    site_key: str | None = None
    profile_id: str | None = None
    proxy_url: str | None = None
    fingerprint_id: str | None = None

    # Metrics
    metric_value: float | None = None
    threshold_value: float | None = None

    # Additional data
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict."""
        return {
            "alert_type": self.alert_type.value,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp,
            "site_key": self.site_key,
            "profile_id": self.profile_id,
            "proxy_url": self.proxy_url,
            "fingerprint_id": self.fingerprint_id,
            "metric_value": self.metric_value,
            "threshold_value": self.threshold_value,
            "extra": self.extra,
        }


@dataclass
class AlertThresholds:
    """Configurable alert thresholds."""

    # Rate thresholds
    rate_limit_threshold: float = 0.10  # 10% 429 rate
    block_rate_threshold: float = 0.20  # 20% 403 rate
    combined_block_threshold: float = 0.30  # 30% 429+403 rate

    # Fingerprint thresholds
    fingerprint_block_threshold: float = 0.50  # 50% block rate for fingerprint
    min_fingerprint_samples: int = 10  # Minimum requests to trigger alert

    # Proxy thresholds
    proxy_block_threshold: float = 0.60  # 60% block rate for proxy
    min_proxy_samples: int = 20

    # Bot score thresholds
    bot_score_threshold: int = 30  # Bot score below 30 is suspicious
    bot_score_alert_percentage: float = 0.20  # 20% of requests with low bot score

    # Time windows (seconds)
    time_window: int = 300  # 5 minutes

    # Alert cooldown (prevent alert spam)
    alert_cooldown: int = 600  # 10 minutes


class AlertNotifier:
    """Base class for alert notifiers."""

    async def notify(self, alert: Alert) -> bool:
        """Send alert notification."""
        raise NotImplementedError


class SlackNotifier(AlertNotifier):
    """Slack webhook notifier."""

    def __init__(self, webhook_url: str):
        """Initialize Slack notifier.

        Args:
            webhook_url: Slack webhook URL
        """
        self.webhook_url = webhook_url
        self.client = httpx.AsyncClient(timeout=10.0)
        self.logger = logger.bind(notifier="slack")

    async def notify(self, alert: Alert) -> bool:
        """Send alert to Slack.

        Args:
            alert: Alert

        Returns:
            True if successful
        """
        # Format Slack message
        color_map = {
            AlertSeverity.INFO: "#36a64f",  # green
            AlertSeverity.WARNING: "#ff9900",  # orange
            AlertSeverity.ERROR: "#ff0000",  # red
            AlertSeverity.CRITICAL: "#990000",  # dark red
        }

        payload = {
            "attachments": [
                {
                    "color": color_map.get(alert.severity, "#808080"),
                    "title": f"{alert.severity.value.upper()}: {alert.title}",
                    "text": alert.message,
                    "fields": [
                        {"title": "Type", "value": alert.alert_type.value, "short": True},
                        {"title": "Site", "value": alert.site_key or "N/A", "short": True},
                    ],
                    "ts": int(alert.timestamp),
                }
            ]
        }

        if alert.metric_value is not None:
            payload["attachments"][0]["fields"].append({
                "title": "Metric Value",
                "value": f"{alert.metric_value:.2%}" if alert.metric_value < 10 else f"{alert.metric_value:.2f}",
                "short": True,
            })

        if alert.threshold_value is not None:
            payload["attachments"][0]["fields"].append({
                "title": "Threshold",
                "value": f"{alert.threshold_value:.2%}" if alert.threshold_value < 10 else f"{alert.threshold_value:.2f}",
                "short": True,
            })

        try:
            response = await self.client.post(self.webhook_url, json=payload)
            response.raise_for_status()
            return True
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {e}")
            return False

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


class WebhookNotifier(AlertNotifier):
    """Generic webhook notifier."""

    def __init__(self, webhook_url: str, headers: dict[str, str] | None = None):
        """Initialize webhook notifier.

        Args:
            webhook_url: Webhook URL
            headers: Additional headers
        """
        self.webhook_url = webhook_url
        self.client = httpx.AsyncClient(
            headers=headers or {},
            timeout=10.0,
        )
        self.logger = logger.bind(notifier="webhook")

    async def notify(self, alert: Alert) -> bool:
        """Send alert to webhook.

        Args:
            alert: Alert

        Returns:
            True if successful
        """
        try:
            response = await self.client.post(self.webhook_url, json=alert.to_dict())
            response.raise_for_status()
            return True
        except Exception as e:
            self.logger.error(f"Failed to send webhook notification: {e}")
            return False

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


class AlertManager:
    """Manages alerts and thresholds."""

    def __init__(self, thresholds: AlertThresholds | None = None):
        """Initialize alert manager.

        Args:
            thresholds: Alert thresholds
        """
        self.thresholds = thresholds or AlertThresholds()
        self.notifiers: list[AlertNotifier] = []
        self.logger = logger.bind(component="AlertManager")

        # Alert history (for cooldown)
        self._alert_history: dict[str, float] = {}  # key -> last alert time

        # Metrics buffer
        self._metrics_buffer: list[dict[str, Any]] = []
        self._buffer_size = 1000

    def add_notifier(self, notifier: AlertNotifier) -> None:
        """Add an alert notifier.

        Args:
            notifier: AlertNotifier instance
        """
        self.notifiers.append(notifier)
        self.logger.info(f"Added notifier: {notifier.__class__.__name__}")

    async def send_alert(self, alert: Alert) -> None:
        """Send alert to all notifiers.

        Args:
            alert: Alert
        """
        # Check cooldown
        alert_key = f"{alert.alert_type.value}:{alert.site_key}:{alert.profile_id or alert.proxy_url or 'global'}"

        last_alert_time = self._alert_history.get(alert_key, 0)
        if time.time() - last_alert_time < self.thresholds.alert_cooldown:
            self.logger.debug(f"Alert {alert_key} in cooldown, skipping")
            return

        # Send to all notifiers
        self.logger.info(f"Sending alert: {alert.title} ({alert.severity.value})")

        tasks = [notifier.notify(alert) for notifier in self.notifiers]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Update history
        self._alert_history[alert_key] = time.time()

    def add_metric(self, metric: dict[str, Any]) -> None:
        """Add a metric to buffer for analysis.

        Args:
            metric: Metric data (timestamp, site_key, status_code, etc.)
        """
        self._metrics_buffer.append(metric)

        # Trim buffer if too large
        if len(self._metrics_buffer) > self._buffer_size:
            self._metrics_buffer = self._metrics_buffer[-self._buffer_size:]

    async def analyze_metrics(self) -> list[Alert]:
        """Analyze buffered metrics and generate alerts.

        Returns:
            List of alerts
        """
        alerts = []

        if not self._metrics_buffer:
            return alerts

        # Group metrics by time window
        now = time.time()
        window_start = now - self.thresholds.time_window

        recent_metrics = [
            m for m in self._metrics_buffer
            if m.get('timestamp', 0) >= window_start
        ]

        if not recent_metrics:
            return alerts

        # Overall rate limits
        rate_alert = self._check_rate_limit_threshold(recent_metrics)
        if rate_alert:
            alerts.append(rate_alert)

        # Overall block rate
        block_alert = self._check_block_rate_threshold(recent_metrics)
        if block_alert:
            alerts.append(block_alert)

        # Per-fingerprint analysis
        fingerprint_alerts = self._check_fingerprint_thresholds(recent_metrics)
        alerts.extend(fingerprint_alerts)

        # Per-proxy analysis
        proxy_alerts = self._check_proxy_thresholds(recent_metrics)
        alerts.extend(proxy_alerts)

        # Bot score analysis
        bot_score_alert = self._check_bot_score_threshold(recent_metrics)
        if bot_score_alert:
            alerts.append(bot_score_alert)

        return alerts

    def _check_rate_limit_threshold(self, metrics: list[dict[str, Any]]) -> Alert | None:
        """Check if rate limit threshold is exceeded.

        Args:
            metrics: Recent metrics

        Returns:
            Alert or None
        """
        total = len(metrics)
        rate_limited = sum(1 for m in metrics if m.get('status_code') == 429)

        if total < 10:  # Need minimum sample size
            return None

        rate_limit_rate = rate_limited / total

        if rate_limit_rate >= self.thresholds.rate_limit_threshold:
            return Alert(
                alert_type=AlertType.HIGH_RATE_LIMIT,
                severity=AlertSeverity.WARNING if rate_limit_rate < 0.3 else AlertSeverity.ERROR,
                title="High Rate Limiting Detected",
                message=f"Rate limiting rate is {rate_limit_rate:.1%} (threshold: {self.thresholds.rate_limit_threshold:.1%})",
                metric_value=rate_limit_rate,
                threshold_value=self.thresholds.rate_limit_threshold,
                extra={"total_requests": total, "rate_limited_requests": rate_limited},
            )

        return None

    def _check_block_rate_threshold(self, metrics: list[dict[str, Any]]) -> Alert | None:
        """Check if block rate threshold is exceeded.

        Args:
            metrics: Recent metrics

        Returns:
            Alert or None
        """
        total = len(metrics)
        blocked = sum(1 for m in metrics if m.get('status_code') in [403, 451])

        if total < 10:
            return None

        block_rate = blocked / total

        if block_rate >= self.thresholds.block_rate_threshold:
            return Alert(
                alert_type=AlertType.HIGH_BLOCK_RATE,
                severity=AlertSeverity.ERROR if block_rate < 0.5 else AlertSeverity.CRITICAL,
                title="High Block Rate Detected",
                message=f"Block rate is {block_rate:.1%} (threshold: {self.thresholds.block_rate_threshold:.1%})",
                metric_value=block_rate,
                threshold_value=self.thresholds.block_rate_threshold,
                extra={"total_requests": total, "blocked_requests": blocked},
            )

        return None

    def _check_fingerprint_thresholds(self, metrics: list[dict[str, Any]]) -> list[Alert]:
        """Check per-fingerprint block rates.

        Args:
            metrics: Recent metrics

        Returns:
            List of alerts
        """
        alerts = []

        # Group by fingerprint
        fingerprint_stats: dict[str, dict[str, int]] = {}

        for metric in metrics:
            fp_id = metric.get('fingerprint_id') or metric.get('user_agent', 'unknown')[:50]

            if fp_id not in fingerprint_stats:
                fingerprint_stats[fp_id] = {'total': 0, 'blocked': 0}

            fingerprint_stats[fp_id]['total'] += 1
            if metric.get('status_code') in [403, 429, 451]:
                fingerprint_stats[fp_id]['blocked'] += 1

        # Check each fingerprint
        for fp_id, stats in fingerprint_stats.items():
            if stats['total'] < self.thresholds.min_fingerprint_samples:
                continue

            block_rate = stats['blocked'] / stats['total']

            if block_rate >= self.thresholds.fingerprint_block_threshold:
                alerts.append(Alert(
                    alert_type=AlertType.FINGERPRINT_BLOCKED,
                    severity=AlertSeverity.WARNING,
                    title="Fingerprint Flagged",
                    message=f"Fingerprint {fp_id[:30]}... has {block_rate:.1%} block rate",
                    fingerprint_id=fp_id,
                    metric_value=block_rate,
                    threshold_value=self.thresholds.fingerprint_block_threshold,
                    extra=stats,
                ))

        return alerts

    def _check_proxy_thresholds(self, metrics: list[dict[str, Any]]) -> list[Alert]:
        """Check per-proxy block rates.

        Args:
            metrics: Recent metrics

        Returns:
            List of alerts
        """
        alerts = []

        # Group by proxy
        proxy_stats: dict[str, dict[str, int]] = {}

        for metric in metrics:
            proxy = metric.get('proxy_url') or 'direct'

            if proxy not in proxy_stats:
                proxy_stats[proxy] = {'total': 0, 'blocked': 0}

            proxy_stats[proxy]['total'] += 1
            if metric.get('status_code') in [403, 429, 451]:
                proxy_stats[proxy]['blocked'] += 1

        # Check each proxy
        for proxy, stats in proxy_stats.items():
            if stats['total'] < self.thresholds.min_proxy_samples:
                continue

            block_rate = stats['blocked'] / stats['total']

            if block_rate >= self.thresholds.proxy_block_threshold:
                alerts.append(Alert(
                    alert_type=AlertType.PROXY_BLOCKED,
                    severity=AlertSeverity.WARNING,
                    title="Proxy Flagged",
                    message=f"Proxy {proxy} has {block_rate:.1%} block rate",
                    proxy_url=proxy if proxy != 'direct' else None,
                    metric_value=block_rate,
                    threshold_value=self.thresholds.proxy_block_threshold,
                    extra=stats,
                ))

        return alerts

    def _check_bot_score_threshold(self, metrics: list[dict[str, Any]]) -> Alert | None:
        """Check bot score degradation.

        Args:
            metrics: Recent metrics

        Returns:
            Alert or None
        """
        # Filter metrics with bot scores
        scored_metrics = [m for m in metrics if m.get('cf_bot_score') is not None]

        if len(scored_metrics) < 10:
            return None

        low_score_count = sum(1 for m in scored_metrics if m.get('cf_bot_score', 100) < self.thresholds.bot_score_threshold)
        low_score_rate = low_score_count / len(scored_metrics)

        if low_score_rate >= self.thresholds.bot_score_alert_percentage:
            return Alert(
                alert_type=AlertType.BOT_SCORE_DEGRADATION,
                severity=AlertSeverity.ERROR,
                title="Bot Score Degradation",
                message=f"{low_score_rate:.1%} of requests have bot score < {self.thresholds.bot_score_threshold}",
                metric_value=low_score_rate,
                threshold_value=self.thresholds.bot_score_alert_percentage,
                extra={"scored_requests": len(scored_metrics), "low_score_requests": low_score_count},
            )

        return None

    async def close(self) -> None:
        """Close all notifiers."""
        for notifier in self.notifiers:
            if hasattr(notifier, 'close'):
                await notifier.close()


# Global instance
_alert_manager: AlertManager | None = None


def get_alert_manager() -> AlertManager:
    """Get global alert manager instance.

    Returns:
        AlertManager
    """
    global _alert_manager

    if _alert_manager is None:
        # Load thresholds from environment
        import os

        thresholds = AlertThresholds(
            rate_limit_threshold=float(os.environ.get("ALERT_RATE_LIMIT_THRESHOLD", "0.10")),
            block_rate_threshold=float(os.environ.get("ALERT_BLOCK_RATE_THRESHOLD", "0.20")),
            fingerprint_block_threshold=float(os.environ.get("ALERT_FINGERPRINT_THRESHOLD", "0.50")),
            proxy_block_threshold=float(os.environ.get("ALERT_PROXY_THRESHOLD", "0.60")),
            time_window=int(os.environ.get("ALERT_TIME_WINDOW", "300")),
            alert_cooldown=int(os.environ.get("ALERT_COOLDOWN", "600")),
        )

        _alert_manager = AlertManager(thresholds)

        # Add notifiers
        slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
        if slack_webhook:
            _alert_manager.add_notifier(SlackNotifier(slack_webhook))

        webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
        if webhook_url:
            _alert_manager.add_notifier(WebhookNotifier(webhook_url))

    return _alert_manager


if __name__ == "__main__":
    # Test alert manager
    async def main():
        manager = get_alert_manager()

        # Simulate metrics
        for i in range(100):
            manager.add_metric({
                'timestamp': time.time(),
                'status_code': 429 if i < 30 else (403 if i < 50 else 200),
                'fingerprint_id': f"fp_{i % 3}",
                'proxy_url': f"proxy_{i % 2}" if i % 5 != 0 else None,
                'cf_bot_score': 20 if i < 25 else 80,
            })

        # Analyze
        alerts = await manager.analyze_metrics()

        print(f"Generated {len(alerts)} alerts:")
        for alert in alerts:
            print(f"  [{alert.severity.value.upper()}] {alert.title}: {alert.message}")

        # Send alerts
        for alert in alerts:
            await manager.send_alert(alert)

        await manager.close()

    asyncio.run(main())


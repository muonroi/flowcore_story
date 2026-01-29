"""Expose Storyflow operational metrics via Prometheus."""

from __future__ import annotations

import socket
from collections.abc import Mapping

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from flowcore_story.utils.logger import logger

ALERT_LEVEL_TO_CODE = {"unknown": -1, "ok": 0, "warn": 1, "error": 2}


class PrometheusMetricsPublisher:
    """Push crawler metrics to a Prometheus Pushgateway instance."""

    def __init__(
        self,
        gateway_url: str,
        *,
        job_name: str,
        grouping: dict[str, str] | None = None,
    ) -> None:
        self._gateway_url = gateway_url
        self._job_name = job_name
        self._grouping = grouping or {"instance": socket.gethostname()}

    def publish(
        self,
        *,
        metrics: Mapping[str, float],
        per_site_error_rate: Mapping[str, float],
        per_site_rate_limit_ratio: Mapping[str, float],
        alert_levels: Mapping[str, str],
    ) -> None:
        """Push the latest metrics snapshot to the configured gateway."""

        registry = CollectorRegistry()

        Gauge("storyflow_kafka_backlog", "Kafka backlog size", registry=registry).set(
            float(metrics.get("kafka_queue_backlog", 0) or 0)
        )
        Gauge("storyflow_crawl_error_rate", "Overall crawl failure ratio", registry=registry).set(
            float(metrics.get("crawl_error_rate", 0.0) or 0.0)
        )
        Gauge("storyflow_dead_story_count", "Stories with dead chapters", registry=registry).set(
            float(metrics.get("dead_story_count", 0) or 0)
        )
        Gauge("storyflow_dead_chapter_total", "Total dead chapter entries", registry=registry).set(
            float(metrics.get("dead_chapter_total", 0) or 0)
        )

        alert_gauge = Gauge(
            "storyflow_alert_level",
            "Alert level (0=ok,1=warn,2=error)",
            labelnames=("type",),
            registry=registry,
        )
        for key, level in alert_levels.items():
            alert_gauge.labels(type=key).set(ALERT_LEVEL_TO_CODE.get(level, -1))

        site_gauge = Gauge(
            "storyflow_site_crawl_error_rate",
            "Crawl failure ratio by site",
            labelnames=("site",),
            registry=registry,
        )
        for site, rate in per_site_error_rate.items():
            site_gauge.labels(site=site).set(float(rate or 0.0))

        rate_limit_gauge = Gauge(
            "storyflow_site_rate_limit_ratio",
            "Rate limit (HTTP 429) ratio by site",
            labelnames=("site",),
            registry=registry,
        )
        for site, ratio in per_site_rate_limit_ratio.items():
            rate_limit_gauge.labels(site=site).set(float(ratio or 0.0))

        try:
            push_to_gateway(
                self._gateway_url,
                job=self._job_name,
                registry=registry,
                grouping_key=self._grouping,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.warning("[PROMETHEUS] Không push được metrics tới %s: %s", self._gateway_url, exc)


__all__ = ["PrometheusMetricsPublisher", "ALERT_LEVEL_TO_CODE"]


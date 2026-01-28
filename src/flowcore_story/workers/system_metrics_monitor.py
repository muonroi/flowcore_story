"""Background task that publishes system health metrics to the dashboard."""

from __future__ import annotations

import asyncio
import time

from flowcore_story.config import config as app_config
from flowcore.kafka.kafka_monitor import fetch_consumer_lag
from flowcore.utils.async_primitives import LoopBoundLock
from flowcore.utils.dead_story_monitor import DeadStoryMonitor, DeadStoryStats
from flowcore.utils.logger import logger
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.prometheus_exporter import ALERT_LEVEL_TO_CODE, PrometheusMetricsPublisher
from flowcore.utils.rate_limit_monitor import rate_limit_monitor

DEFAULT_INTERVAL_SECONDS = 30.0


def _build_base_metrics() -> dict[str, int]:
    metrics: dict[str, int] = {}
    try:
        metrics["proxies_in_use"] = len(app_config.LOADED_PROXIES)
    except Exception:  # pragma: no cover - defensive guard
        metrics["proxies_in_use"] = 0
    return metrics


def _ensure_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return None


async def _collect_kafka_metrics() -> dict[str, object]:
    topic = getattr(app_config, "KAFKA_TOPIC", None)
    group_id = getattr(app_config, "KAFKA_GROUP_ID", None)
    bootstrap = getattr(app_config, "KAFKA_BOOTSTRAP_SERVERS", None)

    if not topic or not group_id or not bootstrap:
        return {}

    lag, metadata = await fetch_consumer_lag(topic, group_id, bootstrap)
    payload: dict[str, object] = {}
    if lag is not None:
        payload["kafka_queue_backlog"] = max(int(lag), 0)
    status = metadata.get("status") if isinstance(metadata, dict) else None
    if status:
        payload["kafka_queue_status"] = status
    error = metadata.get("error") if isinstance(metadata, dict) else None
    if error:
        payload["kafka_queue_error"] = str(error)
    partitions = metadata.get("partitions") if isinstance(metadata, dict) else None
    if isinstance(partitions, int) and partitions >= 0:
        payload["kafka_queue_partitions"] = partitions
    return payload


class SystemMetricsMonitor:
    """Periodically refresh operational metrics for the realtime dashboard."""

    def __init__(self, *, interval_seconds: float = DEFAULT_INTERVAL_SECONDS) -> None:
        self._interval = max(5.0, float(interval_seconds))
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._alert_levels: dict[str, str] = {
            "kafka": "unknown",
            "crawl_error_rate": "unknown",
            "dead_story": "unknown",
        }

        refresh_seconds = getattr(app_config, "DEAD_STORY_REFRESH_SECONDS", 300.0)
        self._dead_monitor = DeadStoryMonitor(
            [
                getattr(app_config, "DATA_FOLDER", ""),
                getattr(app_config, "COMPLETED_FOLDER", ""),
            ],
            refresh_interval=max(float(refresh_seconds), 30.0),
        )

        push_url = getattr(app_config, "PROMETHEUS_PUSHGATEWAY_URL", None)
        job_name = getattr(app_config, "PROMETHEUS_JOB_NAME", "storyflow_crawler")
        self._prometheus: PrometheusMetricsPublisher | None = None
        if push_url:
            self._prometheus = PrometheusMetricsPublisher(push_url, job_name=job_name)
        self._prometheus_push_interval = max(
            float(getattr(app_config, "PROMETHEUS_PUSH_INTERVAL", max(self._interval, 30.0))),
            15.0,
        )
        self._last_prometheus_push = 0.0

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:  # pragma: no cover - defensive guard
            raise RuntimeError(
                "SystemMetricsMonitor.start() requires an active asyncio event loop"
            ) from exc
        logger.info(
            "[SYSTEM][MONITOR] Bắt đầu cập nhật chỉ số hệ thống mỗi %.0f giây",
            self._interval,
        )
        self._stop_event.clear()
        self._task = loop.create_task(self._run(), name="system-metrics-monitor")

    async def stop(self) -> None:
        task = self._task
        if not task:
            return
        logger.info("[SYSTEM][MONITOR] Đang dừng theo dõi chỉ số hệ thống...")
        self._stop_event.set()
        try:
            await task
        finally:
            self._task = None
            logger.info("[SYSTEM][MONITOR] Đã dừng theo dõi chỉ số hệ thống.")

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await self._refresh_metrics()
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.exception(
                        "[SYSTEM][MONITOR] Lỗi khi cập nhật chỉ số hệ thống: %s",
                        exc,
                    )
                if self._stop_event.is_set():
                    break
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
                except TimeoutError:
                    continue
        except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
            logger.debug("[SYSTEM][MONITOR] Task bị huỷ")
        finally:
            self._stop_event.clear()

    async def _refresh_metrics(self) -> None:
        metrics: dict[str, object] = _build_base_metrics()
        kafka_metrics = await _collect_kafka_metrics()
        metrics.update(kafka_metrics)
        (
            crawl_metrics,
            overall_error_rate,
            per_site_error_rate,
            worst_site,
            worst_rate,
        ) = self._collect_site_health()
        metrics.update(crawl_metrics)

        dead_stats = await asyncio.to_thread(self._dead_monitor.refresh)
        metrics["dead_story_count"] = dead_stats.story_count
        metrics["dead_chapter_total"] = dead_stats.dead_chapter_total
        metrics["dead_story_last_scan_ts"] = int(dead_stats.refreshed_at)

        alert_levels = self._evaluate_alerts(
            metrics,
            overall_error_rate=overall_error_rate,
            worst_site=worst_site,
            worst_rate=worst_rate,
            dead_stats=dead_stats,
        )

        rate_limit_metrics, per_site_rate_limit_ratio = self._collect_rate_limit_metrics()
        metrics.update(rate_limit_metrics)

        await self._push_prometheus(
            metrics,
            per_site_error_rate,
            per_site_rate_limit_ratio,
            alert_levels,
        )

        metrics_tracker.update_system_metrics(**metrics)

        # Track system metrics in database
        try:
            from flowcore.storage.db_tracking_helpers import track_system_metrics
            await track_system_metrics(metrics)
        except Exception as e:
            logger.debug(f"[SystemMetrics] Failed to track metrics in DB: {e}")

    # ------------------------------------------------------------------
    # Metric collectors
    # ------------------------------------------------------------------
    def _collect_site_health(
        self,
    ) -> tuple[dict[str, object], float, dict[str, float], str | None, float]:
        metrics: dict[str, object] = {}
        per_site_error_rate: dict[str, float] = {}
        total_success = 0
        total_failure = 0
        worst_site: str | None = None
        worst_rate = 0.0

        enabled_sites = getattr(app_config, "ENABLED_SITE_KEYS", []) or []
        for site_key in enabled_sites:
            snapshot = metrics_tracker.get_site_health_snapshot(site_key)
            if not snapshot:
                continue
            success = max(int(snapshot.success), 0)
            failure = max(int(snapshot.failure), 0)
            total_success += success
            total_failure += failure
            total = success + failure
            failure_rate = (failure / total) if total else 0.0
            safe_key = site_key.replace("-", "_")
            metrics[f"crawl_success_total_{safe_key}"] = success
            metrics[f"crawl_failure_total_{safe_key}"] = failure
            metrics[f"crawl_error_rate_{safe_key}"] = round(failure_rate, 4)
            per_site_error_rate[site_key] = failure_rate
            if failure_rate >= worst_rate and total > 0:
                worst_rate = failure_rate
                worst_site = site_key

        total_events = total_success + total_failure
        overall_error_rate = (total_failure / total_events) if total_events else 0.0
        metrics["crawl_success_total"] = total_success
        metrics["crawl_failure_total"] = total_failure
        metrics["crawl_error_rate"] = round(overall_error_rate, 4)
        if worst_site is not None:
            metrics["crawl_error_rate_worst_site"] = worst_site
            metrics["crawl_error_rate_worst_value"] = round(worst_rate, 4)

        return metrics, overall_error_rate, per_site_error_rate, worst_site, worst_rate

    def _collect_rate_limit_metrics(self) -> tuple[dict[str, object], dict[str, float]]:
        enabled_sites = getattr(app_config, "ENABLED_SITE_KEYS", []) or []
        snapshots = rate_limit_monitor.collect_snapshots(enabled_sites)
        metrics: dict[str, object] = {}
        per_site_ratio: dict[str, float] = {}
        for site_key, snapshot in snapshots.items():
            safe_key = site_key.replace("-", "_")
            metrics[f"rate_limit_ratio_{safe_key}"] = round(snapshot.ratio, 4)
            metrics[f"rate_limit_total_{safe_key}"] = snapshot.total_requests
            metrics[f"rate_limit_limited_{safe_key}"] = snapshot.rate_limited
            if snapshot.last_event_at:
                metrics[f"rate_limit_last_event_ts_{safe_key}"] = int(snapshot.last_event_at)
            per_site_ratio[site_key] = snapshot.ratio

        hotspot_limit = int(getattr(app_config, "RATE_LIMIT_HOTSPOT_LIMIT", 5))
        hotspot_min_requests = int(
            getattr(app_config, "RATE_LIMIT_HOTSPOT_MIN_REQUESTS", 20)
        )
        hotspots = rate_limit_monitor.get_hotspots(
            limit=max(hotspot_limit, 1),
            min_requests=max(hotspot_min_requests, 1),
        )
        metrics["rate_limit_hotspots"] = [
            {
                "site": snapshot.site_key,
                "ratio": round(snapshot.ratio, 4),
                "rate_limited": snapshot.rate_limited,
                "total": snapshot.total_requests,
                "last_event_ts": int(snapshot.last_event_at)
                if snapshot.last_event_at
                else None,
            }
            for snapshot in hotspots
        ]
        if hotspots:
            metrics["rate_limit_hotspots_display"] = ", ".join(
                f"{snapshot.site_key}:{snapshot.ratio:.0%}"
                f" ({snapshot.rate_limited}/{snapshot.total_requests})"
                for snapshot in hotspots
            )
        else:
            metrics["rate_limit_hotspots_display"] = ""
        return metrics, per_site_ratio

    # ------------------------------------------------------------------
    # Alerts & reporting
    # ------------------------------------------------------------------
    def _evaluate_alerts(
        self,
        metrics: dict[str, object],
        *,
        overall_error_rate: float,
        worst_site: str | None,
        worst_rate: float,
        dead_stats: DeadStoryStats,
    ) -> dict[str, str]:
        alerts: dict[str, str] = {}

        backlog_value = _ensure_float(metrics.get("kafka_queue_backlog"))
        backlog_level = self._classify_threshold(
            backlog_value,
            getattr(app_config, "KAFKA_BACKLOG_WARN", 100),
            getattr(app_config, "KAFKA_BACKLOG_ERROR", 1000),
        )
        alerts["kafka"] = backlog_level
        metrics["kafka_alert_level"] = backlog_level
        metrics["kafka_alert_level_code"] = ALERT_LEVEL_TO_CODE.get(backlog_level, -1)
        backlog_int = int(backlog_value) if backlog_value is not None else 0
        if backlog_value is None:
            ok_backlog_msg = "Chưa có dữ liệu backlog Kafka"
            warn_backlog_msg = ok_backlog_msg
            error_backlog_msg = ok_backlog_msg
        else:
            ok_backlog_msg = f"Backlog Kafka ổn định ({backlog_int})"
            warn_backlog_msg = (
                f"Backlog Kafka tăng cao: {backlog_int} (cảnh báo >= "
                f"{getattr(app_config, 'KAFKA_BACKLOG_WARN', 100)})"
            )
            error_backlog_msg = (
                f"Backlog Kafka quá cao: {backlog_int} (lỗi >= "
                f"{getattr(app_config, 'KAFKA_BACKLOG_ERROR', 1000)})"
            )
        self._log_alert_transition(
            "kafka",
            backlog_level,
            ok_message=ok_backlog_msg,
            warn_message=warn_backlog_msg,
            error_message=error_backlog_msg,
        )

        error_level = self._classify_threshold(
            overall_error_rate,
            getattr(app_config, "CRAWL_ERROR_RATE_WARN", 0.15),
            getattr(app_config, "CRAWL_ERROR_RATE_ERROR", 0.3),
        )
        alerts["crawl_error_rate"] = error_level
        metrics["crawl_error_rate_level"] = error_level
        metrics["crawl_error_rate_level_code"] = ALERT_LEVEL_TO_CODE.get(error_level, -1)

        if worst_site is not None:
            worst_detail = f" (site {worst_site}: {worst_rate:.1%})"
        else:
            worst_detail = ""
        self._log_alert_transition(
            "crawl_error_rate",
            error_level,
            ok_message=f"Tỷ lệ lỗi crawl ổn định ({overall_error_rate:.1%})",
            warn_message=(
                f"Tỷ lệ lỗi crawl cao {overall_error_rate:.1%}{worst_detail} "
                f"(cảnh báo >= {getattr(app_config, 'CRAWL_ERROR_RATE_WARN', 0.15):.1%})"
            ),
            error_message=(
                f"Tỷ lệ lỗi crawl rất cao {overall_error_rate:.1%}{worst_detail} "
                f"(lỗi >= {getattr(app_config, 'CRAWL_ERROR_RATE_ERROR', 0.3):.1%})"
            ),
        )

        dead_level = self._classify_threshold(
            float(dead_stats.story_count),
            getattr(app_config, "DEAD_STORY_WARN_THRESHOLD", 5),
            getattr(app_config, "DEAD_STORY_ERROR_THRESHOLD", 20),
        )
        alerts["dead_story"] = dead_level
        metrics["dead_story_alert_level"] = dead_level
        metrics["dead_story_alert_level_code"] = ALERT_LEVEL_TO_CODE.get(dead_level, -1)

        self._log_alert_transition(
            "dead_story",
            dead_level,
            ok_message=(
                f"Số truyện lỗi dead_chapters ở mức an toàn ({dead_stats.story_count} truyện)"
            ),
            warn_message=(
                f"Có {dead_stats.story_count} truyện bị dead_chapters (cảnh báo >= "
                f"{getattr(app_config, 'DEAD_STORY_WARN_THRESHOLD', 5)})"
            ),
            error_message=(
                f"Có {dead_stats.story_count} truyện bị dead_chapters (lỗi >= "
                f"{getattr(app_config, 'DEAD_STORY_ERROR_THRESHOLD', 20)})"
            ),
        )

        return alerts

    def _classify_threshold(
        self,
        value: float | None,
        warn_threshold: float | None,
        error_threshold: float | None,
    ) -> str:
        if value is None:
            return "unknown"
        if error_threshold is not None and value >= float(error_threshold):
            return "error"
        if warn_threshold is not None and value >= float(warn_threshold):
            return "warn"
        return "ok"

    def _log_alert_transition(
        self,
        name: str,
        level: str,
        *,
        ok_message: str,
        warn_message: str,
        error_message: str,
    ) -> None:
        previous = self._alert_levels.get(name)
        if level == previous:
            return
        self._alert_levels[name] = level
        if level == "ok":
            logger.info("[SYSTEM][ALERT] %s", ok_message)
        elif level == "warn":
            logger.warning("[SYSTEM][ALERT] %s", warn_message)
        elif level == "error":
            logger.error("[SYSTEM][ALERT] %s", error_message)
        else:
            logger.info("[SYSTEM][ALERT] %s", ok_message)

    async def _push_prometheus(
        self,
        metrics: dict[str, object],
        per_site_error_rate: dict[str, float],
        per_site_rate_limit_ratio: dict[str, float],
        alert_levels: dict[str, str],
    ) -> None:
        publisher = self._prometheus
        if publisher is None:
            return
        now = time.time()
        if now - self._last_prometheus_push < self._prometheus_push_interval:
            return
        self._last_prometheus_push = now
        await asyncio.to_thread(
            publisher.publish,
            metrics=metrics,
            per_site_error_rate=per_site_error_rate,
            per_site_rate_limit_ratio=per_site_rate_limit_ratio,
            alert_levels=alert_levels,
        )


_monitor_lock = LoopBoundLock()
_monitor_instance: SystemMetricsMonitor | None = None
_monitor_refcount = 0


async def start_system_metrics_monitor() -> None:
    """Start the shared system metrics monitor if it isn't already running."""

    global _monitor_refcount, _monitor_instance

    async with _monitor_lock:
        _monitor_refcount += 1
        if _monitor_instance is None:
            _monitor_instance = SystemMetricsMonitor()
            _monitor_instance.start()


async def stop_system_metrics_monitor() -> None:
    """Decrease the monitor's refcount and stop it when unused."""

    global _monitor_refcount, _monitor_instance

    instance: SystemMetricsMonitor | None = None
    async with _monitor_lock:
        if _monitor_refcount == 0:
            return
        _monitor_refcount -= 1
        if _monitor_refcount == 0 and _monitor_instance is not None:
            instance = _monitor_instance
            _monitor_instance = None

    if instance is not None:
        await instance.stop()


__all__ = [
    "SystemMetricsMonitor",
    "start_system_metrics_monitor",
    "stop_system_metrics_monitor",
]


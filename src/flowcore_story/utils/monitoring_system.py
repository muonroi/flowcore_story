"""Monitoring System - Central orchestrator for compliance and monitoring.

This is the main orchestrator that ties together:
- Cloudflare log pulling and analysis
- SIEM integration (Splunk/ELK/OTel)
- Alert management and notifications
- Auto-quarantine for profiles and proxies

Usage:
    from flowcore_story.utils.monitoring_system import get_monitoring_system

    monitor = get_monitoring_system()
    await monitor.start()  # Start monitoring loop

    # Monitor will:
    # 1. Pull Cloudflare logs periodically
    # 2. Analyze for anomalies
    # 3. Send to SIEM
    # 4. Trigger alerts if thresholds exceeded
    # 5. Auto-quarantine flagged entities
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from flowcore_story.utils.alert_manager import Alert, get_alert_manager
from flowcore_story.utils.cloudflare_log_puller import CloudflareLogStats, get_cloudflare_puller
from flowcore_story.utils.logger import logger
from flowcore_story.utils.quarantine_manager import QuarantineIntegration, get_quarantine_manager
from flowcore_story.utils.siem_integration import SIEMEvent, get_siem_manager


@dataclass
class MonitoringConfig:
    """Monitoring system configuration."""

    # Cloudflare log pulling
    cf_poll_interval: float = 300.0  # 5 minutes
    cf_lookback_hours: int = 1
    cf_enabled: bool = True

    # SIEM sending
    siem_enabled: bool = True
    siem_send_cf_logs: bool = True
    siem_send_alerts: bool = True
    siem_send_quarantines: bool = True

    # Alert checking
    alert_check_interval: float = 60.0  # 1 minute
    alert_enabled: bool = True

    # Auto-quarantine
    quarantine_enabled: bool = True
    quarantine_from_alerts: bool = True
    quarantine_from_cf_logs: bool = True

    # Request tracking (for metrics)
    track_requests: bool = True
    request_buffer_size: int = 1000


class MonitoringSystem:
    """Central monitoring and compliance system."""

    def __init__(self, config: MonitoringConfig | None = None):
        """Initialize monitoring system.

        Args:
            config: MonitoringConfig (uses defaults if None)
        """
        self.config = config or MonitoringConfig()
        self.logger = logger.bind(component="MonitoringSystem")

        # Components
        self.cf_puller = get_cloudflare_puller() if self.config.cf_enabled else None
        self.siem = get_siem_manager() if self.config.siem_enabled else None
        self.alerts = get_alert_manager() if self.config.alert_enabled else None
        self.quarantine = get_quarantine_manager() if self.config.quarantine_enabled else None
        self.quarantine_integration = QuarantineIntegration(self.quarantine) if self.quarantine else None

        # State
        self._running = False
        self._tasks = []

        # Statistics
        self._stats = {
            'cf_logs_pulled': 0,
            'siem_events_sent': 0,
            'alerts_triggered': 0,
            'entities_quarantined': 0,
        }

    async def start(self) -> None:
        """Start monitoring system."""
        if self._running:
            self.logger.warning("Monitoring system already running")
            return

        self._running = True
        self.logger.info("Starting monitoring system")

        # Log configuration
        self.logger.info("Configuration:")
        self.logger.info(f"  Cloudflare logs: {self.config.cf_enabled}")
        self.logger.info(f"  SIEM: {self.config.siem_enabled}")
        self.logger.info(f"  Alerts: {self.config.alert_enabled}")
        self.logger.info(f"  Quarantine: {self.config.quarantine_enabled}")

        # Start tasks
        if self.config.cf_enabled and self.cf_puller:
            task = asyncio.create_task(self._cf_log_pulling_loop())
            self._tasks.append(task)

        if self.config.alert_enabled and self.alerts:
            task = asyncio.create_task(self._alert_checking_loop())
            self._tasks.append(task)

        self.logger.info(f"Started {len(self._tasks)} monitoring tasks")

    async def stop(self) -> None:
        """Stop monitoring system."""
        if not self._running:
            return

        self._running = False
        self.logger.info("Stopping monitoring system")

        # Cancel all tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close connections
        if self.cf_puller:
            await self.cf_puller.close()
        if self.siem:
            await self.siem.close()
        if self.alerts:
            await self.alerts.close()

        self.logger.info("Monitoring system stopped")

    async def _cf_log_pulling_loop(self) -> None:
        """Cloudflare log pulling loop."""
        self.logger.info(f"Started Cloudflare log pulling (interval: {self.config.cf_poll_interval}s)")

        while self._running:
            try:
                await self._pull_and_analyze_cf_logs()
            except Exception as e:
                self.logger.error(f"Error in CF log pulling: {e}", exc_info=True)

            await asyncio.sleep(self.config.cf_poll_interval)

    async def _pull_and_analyze_cf_logs(self) -> None:
        """Pull and analyze Cloudflare logs."""
        if not self.cf_puller:
            return

        self.logger.debug("Pulling Cloudflare logs...")

        # Pull logs
        logs = await self.cf_puller.pull_logs()

        if not logs:
            self.logger.debug("No logs retrieved")
            return

        self.logger.info(f"Pulled {len(logs)} Cloudflare log entries")
        self._stats['cf_logs_pulled'] += len(logs)

        # Analyze logs
        stats = self.cf_puller.analyze_logs(logs)

        self.logger.info(
            f"CF Log Analysis: {stats.total_requests} requests, "
            f"{stats.success_rate:.1%} success, {stats.block_rate:.1%} blocked"
        )

        # Send to SIEM
        if self.config.siem_send_cf_logs and self.siem:
            await self._send_cf_logs_to_siem(stats)

        # Check for flagged fingerprints
        if self.config.quarantine_from_cf_logs and self.quarantine_integration:
            await self._process_cf_flagged_fingerprints(stats)

    async def _send_cf_logs_to_siem(self, stats: CloudflareLogStats) -> None:
        """Send Cloudflare log stats to SIEM.

        Args:
            stats: CloudflareLogStats
        """
        if not self.siem:
            return

        event = SIEMEvent(
            timestamp=time.time(),
            source="storyflow-monitoring",
            event_type="cloudflare_stats",
            severity="info",
            message=f"Cloudflare log analysis: {stats.total_requests} requests, {stats.success_rate:.1%} success",
            tags=["cloudflare", "logs", "analysis"],
            extra={
                'total_requests': stats.total_requests,
                'success_requests': stats.success_requests,
                'rate_limited_requests': stats.rate_limited_requests,
                'blocked_requests': stats.blocked_requests,
                'success_rate': stats.success_rate,
                'block_rate': stats.block_rate,
                'bot_detected_count': stats.bot_detected_count,
                'fingerprints_flagged_count': len(stats.fingerprints_flagged),
                'ips_flagged_count': len(stats.ips_flagged),
            }
        )

        await self.siem.queue_event(event)
        self._stats['siem_events_sent'] += 1

    async def _process_cf_flagged_fingerprints(self, stats: CloudflareLogStats) -> None:
        """Process flagged fingerprints from CF logs.

        Args:
            stats: CloudflareLogStats
        """
        if not stats.fingerprints_flagged:
            return

        # Get detailed flagged fingerprints
        flagged = await self.cf_puller.get_flagged_fingerprints(
            lookback_hours=self.config.cf_lookback_hours
        )

        if flagged and self.quarantine_integration:
            self.logger.warning(f"Found {len(flagged)} flagged fingerprints in CF logs")
            await self.quarantine_integration.auto_quarantine_from_cf_logs(flagged)
            self._stats['entities_quarantined'] += len(flagged)

    async def _alert_checking_loop(self) -> None:
        """Alert checking loop."""
        self.logger.info(f"Started alert checking (interval: {self.config.alert_check_interval}s)")

        while self._running:
            try:
                await self._check_and_send_alerts()
            except Exception as e:
                self.logger.error(f"Error in alert checking: {e}", exc_info=True)

            await asyncio.sleep(self.config.alert_check_interval)

    async def _check_and_send_alerts(self) -> None:
        """Check metrics and send alerts."""
        if not self.alerts:
            return

        # Analyze metrics
        alerts = await self.alerts.analyze_metrics()

        if not alerts:
            return

        self.logger.info(f"Generated {len(alerts)} alerts")

        # Send alerts
        for alert in alerts:
            await self.alerts.send_alert(alert)
            self._stats['alerts_triggered'] += 1

            # Send to SIEM
            if self.config.siem_send_alerts and self.siem:
                await self._send_alert_to_siem(alert)

            # Auto-quarantine
            if self.config.quarantine_from_alerts and self.quarantine_integration:
                await self.quarantine_integration.auto_quarantine_from_alert(alert)
                self._stats['entities_quarantined'] += 1

    async def _send_alert_to_siem(self, alert: Alert) -> None:
        """Send alert to SIEM.

        Args:
            alert: Alert
        """
        if not self.siem:
            return

        event = SIEMEvent(
            timestamp=alert.timestamp,
            source="storyflow-monitoring",
            event_type="alert",
            severity=alert.severity.value,
            message=alert.message,
            tags=["alert", alert.alert_type.value],
            site_key=alert.site_key,
            profile_id=alert.profile_id,
            proxy_url=alert.proxy_url,
            fingerprint_id=alert.fingerprint_id,
            extra=alert.extra,
        )

        await self.siem.queue_event(event)

    def track_request(
        self,
        site_key: str,
        status_code: int,
        profile_id: str | None = None,
        proxy_url: str | None = None,
        fingerprint_id: str | None = None,
        cf_bot_score: int | None = None,
        cf_ray_id: str | None = None,
    ) -> None:
        """Track a request for monitoring.

        This should be called after each request to feed metrics to the monitoring system.

        Args:
            site_key: Site key
            status_code: HTTP status code
            profile_id: Profile ID
            proxy_url: Proxy URL
            fingerprint_id: Fingerprint ID
            cf_bot_score: Cloudflare bot score
            cf_ray_id: Cloudflare Ray ID
        """
        if not self.config.track_requests or not self.alerts:
            return

        metric = {
            'timestamp': time.time(),
            'site_key': site_key,
            'status_code': status_code,
            'profile_id': profile_id,
            'proxy_url': proxy_url,
            'fingerprint_id': fingerprint_id,
            'cf_bot_score': cf_bot_score,
            'cf_ray_id': cf_ray_id,
        }

        self.alerts.add_metric(metric)

    def get_statistics(self) -> dict[str, Any]:
        """Get monitoring statistics.

        Returns:
            Dict with statistics
        """
        stats = self._stats.copy()

        # Add quarantine stats
        if self.quarantine:
            stats['quarantine'] = self.quarantine.get_statistics()

        return stats

    def print_status(self) -> None:
        """Print monitoring system status."""
        print("\n" + "=" * 70)
        print("Monitoring System Status")
        print("=" * 70)

        print(f"\nRunning: {self._running}")
        print(f"Active Tasks: {len(self._tasks)}")

        stats = self.get_statistics()
        print("\nStatistics:")
        print(f"  CF logs pulled: {stats['cf_logs_pulled']}")
        print(f"  SIEM events sent: {stats['siem_events_sent']}")
        print(f"  Alerts triggered: {stats['alerts_triggered']}")
        print(f"  Entities quarantined: {stats['entities_quarantined']}")

        if 'quarantine' in stats:
            q_stats = stats['quarantine']
            print("\nQuarantine:")
            print(f"  Active: {q_stats['active_quarantines']}")
            print(f"  Profiles: {q_stats['profiles_quarantined']}")
            print(f"  Proxies: {q_stats['proxies_quarantined']}")

        print("=" * 70)


# Global instance
_monitoring_system: MonitoringSystem | None = None


def get_monitoring_system(config: MonitoringConfig | None = None) -> MonitoringSystem:
    """Get global monitoring system instance.

    Args:
        config: MonitoringConfig (only used on first call)

    Returns:
        MonitoringSystem
    """
    global _monitoring_system

    if _monitoring_system is None:
        # Load config from environment if not provided
        if config is None:
            import os

            config = MonitoringConfig(
                cf_poll_interval=float(os.environ.get("MONITORING_CF_POLL_INTERVAL", "300")),
                cf_lookback_hours=int(os.environ.get("MONITORING_CF_LOOKBACK_HOURS", "1")),
                cf_enabled=os.environ.get("MONITORING_CF_ENABLED", "true").lower() == "true",
                siem_enabled=os.environ.get("MONITORING_SIEM_ENABLED", "true").lower() == "true",
                alert_enabled=os.environ.get("MONITORING_ALERT_ENABLED", "true").lower() == "true",
                quarantine_enabled=os.environ.get("MONITORING_QUARANTINE_ENABLED", "true").lower() == "true",
                alert_check_interval=float(os.environ.get("MONITORING_ALERT_CHECK_INTERVAL", "60")),
            )

        _monitoring_system = MonitoringSystem(config)

    return _monitoring_system


if __name__ == "__main__":
    # Test monitoring system
    async def main():
        import signal

        monitor = get_monitoring_system()

        # Handle Ctrl+C
        def signal_handler(sig, frame):
            print("\nShutting down...")
            asyncio.create_task(monitor.stop())

        signal.signal(signal.SIGINT, signal_handler)

        # Start monitoring
        await monitor.start()

        # Simulate some requests
        for i in range(50):
            monitor.track_request(
                site_key="tangthuvien",
                status_code=429 if i < 15 else (403 if i < 30 else 200),
                profile_id=f"profile_{i % 5}",
                proxy_url=f"http://proxy{i % 2}:8080" if i % 3 != 0 else None,
                fingerprint_id=f"fp_{i % 3}",
            )

        # Wait a bit
        await asyncio.sleep(5)

        # Print status
        monitor.print_status()

        # Keep running
        print("\nMonitoring system running... Press Ctrl+C to stop")

        try:
            while True:
                await asyncio.sleep(60)
                monitor.print_status()
        except asyncio.CancelledError:
            pass
        finally:
            await monitor.stop()

    asyncio.run(main())


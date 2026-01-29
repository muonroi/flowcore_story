#!/usr/bin/env python
"""
Continuous monitoring script for PostgreSQL state optimization health.
Reports alerts when metrics exceed thresholds.
"""

import asyncio
import os
import sys
import time
from typing import Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from flowcore_story.storage.db_health import check_db_health
from flowcore_story.utils.logger import logger


class StateHealthMonitor:
    """Monitor state storage health and alert on issues."""

    def __init__(self):
        self.alert_history: list[dict[str, Any]] = []
        self.check_interval = 60  # seconds

        # Alert thresholds
        self.thresholds = {
            "circuit_breaker_failures": 3,
            "batch_queue_size": 100,
            "success_rate_min": 95.0,
            "pool_busy_percent": 80.0,
            "avg_save_time_ms": 1000.0,
        }

    async def check_and_alert(self) -> list[str]:
        """Check health and return list of alerts."""
        alerts = []

        try:
            health = await check_db_health()

            # Check 1: Circuit Breaker State
            if health.get('circuit_breaker'):
                cb = health['circuit_breaker']
                if cb.get('state') == 'open':
                    alerts.append(
                        f"🚨 CRITICAL: Circuit breaker is OPEN! "
                        f"Failures: {cb.get('failure_count', 0)}"
                    )
                elif cb.get('failure_count', 0) >= self.thresholds['circuit_breaker_failures']:
                    alerts.append(
                        f"⚠️  WARNING: Circuit breaker has {cb['failure_count']} failures "
                        f"(threshold: {self.thresholds['circuit_breaker_failures']})"
                    )

            # Check 2: Batch Writer Queue Size
            if health.get('batch_writer'):
                bw = health['batch_writer']
                if bw.get('running') and bw.get('queue_size', 0) > self.thresholds['batch_queue_size']:
                    alerts.append(
                        f"⚠️  WARNING: Batch writer queue is large: {bw['queue_size']} items "
                        f"(threshold: {self.thresholds['batch_queue_size']})"
                    )

            # Check 3: Success Rate
            if health.get('metrics'):
                m = health['metrics']
                if m.get('available') is not False:
                    success_rate = m.get('success_rate', 100.0)
                    if success_rate < self.thresholds['success_rate_min']:
                        alerts.append(
                            f"🚨 CRITICAL: Success rate is low: {success_rate:.1f}% "
                            f"(threshold: {self.thresholds['success_rate_min']}%)"
                        )

            # Check 4: Database Pool Utilization
            if health.get('pool'):
                pool = health['pool']
                if pool.get('available'):
                    total = pool.get('size', 0)
                    busy = pool.get('busy', 0)
                    if total > 0:
                        busy_percent = (busy / total) * 100
                        if busy_percent > self.thresholds['pool_busy_percent']:
                            alerts.append(
                                f"⚠️  WARNING: Database pool is {busy_percent:.1f}% busy "
                                f"({busy}/{total} connections)"
                            )

            # Check 5: Save Performance
            if health.get('metrics'):
                m = health['metrics']
                if m.get('available') is not False:
                    avg_time = m.get('timing_ms', {}).get('avg', 0)
                    if avg_time > self.thresholds['avg_save_time_ms']:
                        alerts.append(
                            f"⚠️  WARNING: Slow save operations: avg {avg_time:.1f}ms "
                            f"(threshold: {self.thresholds['avg_save_time_ms']}ms)"
                        )

            # Check 6: Database Unavailable
            if health.get('status') == 'unhealthy':
                alerts.append("🚨 CRITICAL: Database is unhealthy!")
            elif health.get('status') == 'degraded':
                alerts.append("⚠️  WARNING: Database is degraded")

        except Exception as e:
            alerts.append(f"❌ ERROR: Health check failed: {e}")

        # Record alerts
        if alerts:
            self.alert_history.append({
                "timestamp": time.time(),
                "alerts": alerts
            })

        return alerts

    async def run_continuous(self):
        """Run continuous monitoring loop."""
        logger.info(f"[Monitor] Starting continuous health monitoring (interval={self.check_interval}s)")
        logger.info(f"[Monitor] Thresholds: {self.thresholds}")

        while True:
            try:
                alerts = await self.check_and_alert()

                if alerts:
                    print(f"\n{'='*60}")
                    print(f"ALERTS DETECTED - {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print('='*60)
                    for alert in alerts:
                        print(alert)
                        logger.warning(f"[Monitor] {alert}")
                    print('='*60)
                else:
                    print(f"✓ Health check OK - {time.strftime('%Y-%m-%d %H:%M:%S')}")

            except Exception as e:
                logger.error(f"[Monitor] Error in monitoring loop: {e}")

            await asyncio.sleep(self.check_interval)

    def get_alert_summary(self) -> dict[str, Any]:
        """Get summary of recent alerts."""
        recent_alerts = [
            a for a in self.alert_history
            if time.time() - a['timestamp'] < 3600  # Last hour
        ]

        return {
            "total_alerts_last_hour": len(recent_alerts),
            "recent_alerts": recent_alerts[-10:],  # Last 10
            "thresholds": self.thresholds
        }


async def run_once():
    """Run single health check and exit."""
    monitor = StateHealthMonitor()
    alerts = await monitor.check_and_alert()

    if alerts:
        print("\n" + "="*60)
        print("ALERTS DETECTED")
        print("="*60)
        for alert in alerts:
            print(alert)
        print("="*60)
        return 1
    else:
        print("✓ All health checks passed")
        return 0


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Monitor PostgreSQL state health")
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuous monitoring (default: single check)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Check interval in seconds (for continuous mode)"
    )

    args = parser.parse_args()

    if args.continuous:
        monitor = StateHealthMonitor()
        monitor.check_interval = args.interval
        await monitor.run_continuous()
    else:
        exit_code = await run_once()
        sys.exit(exit_code)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Monitor] Stopped by user")
        sys.exit(0)

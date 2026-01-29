"""Quarantine Manager - Auto-quarantine profiles and proxies.

This module automatically quarantines (isolates) profiles and proxies that are:
- Consistently blocked (403/451)
- Heavily rate-limited (429)
- Flagged by Cloudflare logs
- Detected by monitoring alerts

Provides mechanisms to:
- Quarantine and release
- Track quarantine reasons
- Auto-release after cooldown
- Integration with profile and proxy managers
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from flowcore_story.utils.alert_manager import Alert
from flowcore_story.utils.logger import logger


class QuarantineReason(Enum):
    """Quarantine reasons."""
    HIGH_BLOCK_RATE = "high_block_rate"
    HIGH_RATE_LIMIT = "high_rate_limit"
    CLOUDFLARE_FLAGGED = "cloudflare_flagged"
    BOT_SCORE_LOW = "bot_score_low"
    MANUAL = "manual"
    ALERT_TRIGGERED = "alert_triggered"


@dataclass
class QuarantineEntry:
    """Quarantine entry for a profile or proxy."""

    id: str  # profile_id or proxy_url
    entity_type: str  # "profile" or "proxy"
    reason: QuarantineReason
    quarantined_at: float
    quarantine_duration: float  # seconds

    # Context
    site_key: str | None = None

    # Metrics that triggered quarantine
    block_rate: float | None = None
    rate_limit_rate: float | None = None
    sample_size: int = 0

    # Additional info
    cf_ray_ids: list[str] = field(default_factory=list)
    notes: str = ""

    @property
    def expires_at(self) -> float:
        """Get expiration timestamp."""
        return self.quarantined_at + self.quarantine_duration

    @property
    def is_expired(self) -> bool:
        """Check if quarantine has expired."""
        return time.time() >= self.expires_at

    @property
    def time_remaining(self) -> float:
        """Get remaining quarantine time in seconds."""
        return max(0, self.expires_at - time.time())


class QuarantineManager:
    """Manages quarantined profiles and proxies."""

    def __init__(
        self,
        default_quarantine_duration: float = 3600.0,  # 1 hour
        max_quarantine_duration: float = 86400.0,  # 24 hours
        escalation_factor: float = 2.0,  # Double duration on repeat offenses
    ):
        """Initialize quarantine manager.

        Args:
            default_quarantine_duration: Default quarantine duration in seconds
            max_quarantine_duration: Maximum quarantine duration
            escalation_factor: Factor to increase duration on repeat offenses
        """
        self.default_duration = default_quarantine_duration
        self.max_duration = max_quarantine_duration
        self.escalation_factor = escalation_factor

        self.logger = logger.bind(component="QuarantineManager")

        # Active quarantines
        self._quarantines: dict[str, QuarantineEntry] = {}

        # Quarantine history (for escalation)
        self._history: dict[str, list[float]] = {}  # id -> list of quarantine timestamps

        # Auto-cleanup task
        self._cleanup_task: asyncio.Task | None = None

    def quarantine(
        self,
        id: str,
        entity_type: str,
        reason: QuarantineReason,
        site_key: str | None = None,
        duration: float | None = None,
        **kwargs
    ) -> QuarantineEntry:
        """Quarantine a profile or proxy.

        Args:
            id: Profile ID or proxy URL
            entity_type: "profile" or "proxy"
            reason: Quarantine reason
            site_key: Site key (optional)
            duration: Quarantine duration (None = use calculated duration)
            **kwargs: Additional context (block_rate, rate_limit_rate, etc.)

        Returns:
            QuarantineEntry
        """
        # Calculate duration with escalation
        if duration is None:
            duration = self._calculate_duration(id)

        entry = QuarantineEntry(
            id=id,
            entity_type=entity_type,
            reason=reason,
            quarantined_at=time.time(),
            quarantine_duration=duration,
            site_key=site_key,
            **kwargs
        )

        self._quarantines[id] = entry

        # Update history
        if id not in self._history:
            self._history[id] = []
        self._history[id].append(entry.quarantined_at)

        self.logger.warning(
            f"Quarantined {entity_type} '{id}' for {duration/60:.1f} minutes (reason: {reason.value}, site: {site_key or 'all'})"
        )

        return entry

    def _calculate_duration(self, id: str) -> float:
        """Calculate quarantine duration with escalation.

        Args:
            id: Entity ID

        Returns:
            Duration in seconds
        """
        # Count recent quarantines (last 7 days)
        week_ago = time.time() - (7 * 24 * 3600)
        recent_count = sum(1 for ts in self._history.get(id, []) if ts >= week_ago)

        # Apply escalation
        duration = self.default_duration * (self.escalation_factor ** recent_count)

        # Cap at max
        return min(duration, self.max_duration)

    def is_quarantined(self, id: str) -> bool:
        """Check if an entity is currently quarantined.

        Args:
            id: Profile ID or proxy URL

        Returns:
            True if quarantined
        """
        if id not in self._quarantines:
            return False

        entry = self._quarantines[id]

        if entry.is_expired:
            # Auto-release
            self.release(id)
            return False

        return True

    def get_quarantine(self, id: str) -> QuarantineEntry | None:
        """Get quarantine entry for an entity.

        Args:
            id: Profile ID or proxy URL

        Returns:
            QuarantineEntry or None
        """
        return self._quarantines.get(id)

    def release(self, id: str, reason: str = "auto") -> bool:
        """Release an entity from quarantine.

        Args:
            id: Profile ID or proxy URL
            reason: Release reason

        Returns:
            True if released
        """
        if id not in self._quarantines:
            return False

        entry = self._quarantines.pop(id)

        self.logger.info(
            f"Released {entry.entity_type} '{id}' from quarantine (reason: {reason}, was quarantined for {(time.time() - entry.quarantined_at)/60:.1f} minutes)"
        )

        return True

    def get_all_quarantines(self) -> list[QuarantineEntry]:
        """Get all active quarantines.

        Returns:
            List of QuarantineEntry
        """
        return list(self._quarantines.values())

    def get_quarantined_profiles(self) -> list[str]:
        """Get list of quarantined profile IDs.

        Returns:
            List of profile IDs
        """
        return [
            entry.id
            for entry in self._quarantines.values()
            if entry.entity_type == "profile" and not entry.is_expired
        ]

    def get_quarantined_proxies(self) -> list[str]:
        """Get list of quarantined proxy URLs.

        Returns:
            List of proxy URLs
        """
        return [
            entry.id
            for entry in self._quarantines.values()
            if entry.entity_type == "proxy" and not entry.is_expired
        ]

    def cleanup_expired(self) -> int:
        """Clean up expired quarantines.

        Returns:
            Number of entries cleaned up
        """
        expired = [id for id, entry in self._quarantines.items() if entry.is_expired]

        for id in expired:
            self.release(id, reason="expired")

        return len(expired)

    async def start_auto_cleanup(self, interval: float = 60.0) -> None:
        """Start automatic cleanup of expired quarantines.

        Args:
            interval: Cleanup interval in seconds
        """
        if self._cleanup_task and not self._cleanup_task.done():
            return

        async def auto_cleanup():
            while True:
                await asyncio.sleep(interval)
                count = self.cleanup_expired()
                if count > 0:
                    self.logger.info(f"Auto-cleanup: Released {count} expired quarantines")

        self._cleanup_task = asyncio.create_task(auto_cleanup())
        self.logger.info(f"Started auto-cleanup with {interval}s interval")

    def stop_auto_cleanup(self) -> None:
        """Stop automatic cleanup."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            self.logger.info("Stopped auto-cleanup")

    def get_statistics(self) -> dict:
        """Get quarantine statistics.

        Returns:
            Dict with statistics
        """
        active_quarantines = len(self._quarantines)
        profiles_quarantined = len(self.get_quarantined_profiles())
        proxies_quarantined = len(self.get_quarantined_proxies())

        # Reason distribution
        reason_counts = {}
        for entry in self._quarantines.values():
            reason_counts[entry.reason.value] = reason_counts.get(entry.reason.value, 0) + 1

        # Total quarantines in history
        total_historical = sum(len(timestamps) for timestamps in self._history.values())

        return {
            'active_quarantines': active_quarantines,
            'profiles_quarantined': profiles_quarantined,
            'proxies_quarantined': proxies_quarantined,
            'reason_distribution': reason_counts,
            'total_historical_quarantines': total_historical,
            'unique_entities_quarantined': len(self._history),
        }

    def print_status(self) -> None:
        """Print current quarantine status."""
        stats = self.get_statistics()

        print("\n" + "=" * 60)
        print("Quarantine Status")
        print("=" * 60)

        print(f"\nActive Quarantines: {stats['active_quarantines']}")
        print(f"  Profiles: {stats['profiles_quarantined']}")
        print(f"  Proxies: {stats['proxies_quarantined']}")

        if stats['reason_distribution']:
            print("\nBy Reason:")
            for reason, count in stats['reason_distribution'].items():
                print(f"  {reason}: {count}")

        print("\nHistorical:")
        print(f"  Total quarantines: {stats['total_historical_quarantines']}")
        print(f"  Unique entities: {stats['unique_entities_quarantined']}")

        if self._quarantines:
            print("\nActive Entries:")
            for entry in list(self._quarantines.values())[:5]:  # Show first 5
                print(f"  {entry.entity_type} '{entry.id[:40]}...'")
                print(f"    Reason: {entry.reason.value}")
                print(f"    Time remaining: {entry.time_remaining/60:.1f} minutes")

        print("=" * 60)


class QuarantineIntegration:
    """Integrates quarantine with profile and proxy managers."""

    def __init__(self, quarantine_manager: QuarantineManager):
        """Initialize integration.

        Args:
            quarantine_manager: QuarantineManager instance
        """
        self.quarantine = quarantine_manager
        self.logger = logger.bind(component="QuarantineIntegration")

    def filter_profiles(self, profile_ids: list[str]) -> list[str]:
        """Filter out quarantined profiles.

        Args:
            profile_ids: List of profile IDs

        Returns:
            Filtered list (non-quarantined only)
        """
        return [pid for pid in profile_ids if not self.quarantine.is_quarantined(pid)]

    def filter_proxies(self, proxy_urls: list[str]) -> list[str]:
        """Filter out quarantined proxies.

        Args:
            proxy_urls: List of proxy URLs

        Returns:
            Filtered list (non-quarantined only)
        """
        return [proxy for proxy in proxy_urls if not self.quarantine.is_quarantined(proxy)]

    async def auto_quarantine_from_alert(self, alert: "Alert") -> None:
        """Auto-quarantine based on alert.

        Args:
            alert: Alert from AlertManager
        """
        from flowcore_story.utils.alert_manager import AlertType

        # Profile quarantine
        if alert.alert_type == AlertType.FINGERPRINT_BLOCKED and alert.fingerprint_id:
            self.quarantine.quarantine(
                id=alert.fingerprint_id,
                entity_type="profile",
                reason=QuarantineReason.ALERT_TRIGGERED,
                site_key=alert.site_key,
                block_rate=alert.metric_value,
            )

        # Proxy quarantine
        elif alert.alert_type == AlertType.PROXY_BLOCKED and alert.proxy_url:
            self.quarantine.quarantine(
                id=alert.proxy_url,
                entity_type="proxy",
                reason=QuarantineReason.ALERT_TRIGGERED,
                site_key=alert.site_key,
                block_rate=alert.metric_value,
            )

        # Bot score degradation (quarantine all recent profiles)
        elif alert.alert_type == AlertType.BOT_SCORE_DEGRADATION:
            # This would need access to recent profile IDs from metrics
            self.logger.warning("Bot score degradation detected, manual review recommended")

    async def auto_quarantine_from_cf_logs(self, flagged_fingerprints: dict[str, Any]) -> None:
        """Auto-quarantine based on Cloudflare logs analysis.

        Args:
            flagged_fingerprints: Dict from CloudflareLogPuller.get_flagged_fingerprints()
        """
        for fp_key, data in flagged_fingerprints.items():
            # Extract user agent (before the '|')
            if '|' in fp_key:
                user_agent = fp_key.split('|')[0]
            else:
                user_agent = fp_key

            # Quarantine the fingerprint
            self.quarantine.quarantine(
                id=user_agent,
                entity_type="profile",
                reason=QuarantineReason.CLOUDFLARE_FLAGGED,
                block_rate=data.get('block_rate'),
                sample_size=data.get('total_requests', 0),
                cf_ray_ids=data.get('sample_ray_ids', []),
                notes=f"Flagged by Cloudflare logs: {data.get('blocked_requests')}/{data.get('total_requests')} blocked",
            )

            # Also quarantine affected IPs/proxies
            for ip in data.get('affected_ips', [])[:5]:  # Limit to first 5
                self.quarantine.quarantine(
                    id=ip,
                    entity_type="proxy",
                    reason=QuarantineReason.CLOUDFLARE_FLAGGED,
                    block_rate=data.get('block_rate'),
                )


# Global instance
_quarantine_manager: QuarantineManager | None = None


def get_quarantine_manager() -> QuarantineManager:
    """Get global quarantine manager instance.

    Returns:
        QuarantineManager
    """
    global _quarantine_manager

    if _quarantine_manager is None:
        import os

        _quarantine_manager = QuarantineManager(
            default_quarantine_duration=float(os.environ.get("QUARANTINE_DEFAULT_DURATION", "3600")),
            max_quarantine_duration=float(os.environ.get("QUARANTINE_MAX_DURATION", "86400")),
            escalation_factor=float(os.environ.get("QUARANTINE_ESCALATION_FACTOR", "2.0")),
        )

        # Start auto-cleanup
        asyncio.create_task(_quarantine_manager.start_auto_cleanup())

    return _quarantine_manager


if __name__ == "__main__":
    # Test quarantine manager
    async def main():
        manager = get_quarantine_manager()

        # Quarantine some entities
        manager.quarantine(
            "profile_123",
            "profile",
            QuarantineReason.HIGH_BLOCK_RATE,
            site_key="tangthuvien",
            block_rate=0.75,
            sample_size=100,
        )

        manager.quarantine(
            "http://proxy1:8080",
            "proxy",
            QuarantineReason.HIGH_RATE_LIMIT,
            rate_limit_rate=0.80,
        )

        # Check status
        print(f"Is profile_123 quarantined? {manager.is_quarantined('profile_123')}")
        print(f"Is profile_456 quarantined? {manager.is_quarantined('profile_456')}")

        # Get entry
        entry = manager.get_quarantine("profile_123")
        if entry:
            print("\nProfile_123 quarantine:")
            print(f"  Reason: {entry.reason.value}")
            print(f"  Time remaining: {entry.time_remaining/60:.1f} minutes")
            print(f"  Block rate: {entry.block_rate:.1%}")

        # Print status
        manager.print_status()

        # Test integration
        integration = QuarantineIntegration(manager)
        all_profiles = ["profile_123", "profile_456", "profile_789"]
        available = integration.filter_profiles(all_profiles)
        print(f"\nFiltered profiles: {available}")

        # Cleanup
        manager.stop_auto_cleanup()

    asyncio.run(main())


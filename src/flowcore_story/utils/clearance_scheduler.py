"""Clearance Scheduler - Pre-warm and refresh clearances on schedule.

This module provides scheduling and automated maintenance for challenge clearances:
- Pre-warm clearances before they're needed
- Monitor TTL and refresh before expiry
- Scheduled refresh cycles
- Distributed coordination
- Health monitoring
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from flowcore_story.utils.challenge_harvester import (
    ChallengeHarvester,
    ClearanceEntry,
    get_global_harvester,
)
from flowcore_story.utils.logger import logger


@dataclass
class ScheduleEntry:
    """Scheduled clearance refresh entry."""

    site_key: str
    profile_id: str | None = None
    proxy_url: str | None = None

    # Schedule settings
    interval: float = 3600.0  # Refresh every hour
    enabled: bool = True

    # State tracking
    last_run: float | None = None
    next_run: float | None = None
    run_count: int = 0
    success_count: int = 0
    failure_count: int = 0

    def should_run(self) -> bool:
        """Check if schedule should run now."""
        if not self.enabled:
            return False

        now = time.time()

        if self.next_run is None:
            return True

        return now >= self.next_run

    def mark_run(self, success: bool) -> None:
        """Mark schedule as run."""
        now = time.time()
        self.last_run = now
        self.next_run = now + self.interval
        self.run_count += 1

        if success:
            self.success_count += 1
        else:
            self.failure_count += 1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "site_key": self.site_key,
            "profile_id": self.profile_id,
            "proxy_url": self.proxy_url,
            "interval": self.interval,
            "enabled": self.enabled,
            "last_run": self.last_run,
            "next_run": self.next_run,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
        }


class ClearanceScheduler:
    """Manages scheduled clearance pre-warming and refresh."""

    def __init__(
        self,
        harvester: ChallengeHarvester | None = None,
        check_interval: float = 60.0,  # Check every minute
    ):
        """Initialize clearance scheduler.

        Args:
            harvester: ChallengeHarvester instance to use
            check_interval: How often to check for scheduled tasks
        """
        self.harvester = harvester or get_global_harvester()
        self.check_interval = check_interval

        # Schedules
        self._schedules: dict[str, ScheduleEntry] = {}

        # Running state
        self._running = False
        self._task: asyncio.Task | None = None

        # Callbacks
        self._on_refresh_callbacks: list[Callable] = []

        logger.info("[Scheduler] Initialized with check_interval=%.0fs", check_interval)

    def add_schedule(
        self,
        site_key: str,
        interval: float = 3600.0,
        profile_id: str | None = None,
        proxy_url: str | None = None,
        enabled: bool = True,
    ) -> ScheduleEntry:
        """Add a scheduled clearance refresh.

        Args:
            site_key: Site identifier
            interval: Refresh interval in seconds
            profile_id: Optional profile ID
            proxy_url: Optional proxy URL
            enabled: Whether schedule is enabled

        Returns:
            Created ScheduleEntry
        """
        schedule_key = self._make_schedule_key(site_key, profile_id, proxy_url)

        entry = ScheduleEntry(
            site_key=site_key,
            profile_id=profile_id,
            proxy_url=proxy_url,
            interval=interval,
            enabled=enabled,
        )

        self._schedules[schedule_key] = entry

        logger.info(
            "[Scheduler] Added schedule for %s (interval: %.0fs)",
            site_key,
            interval
        )

        return entry

    def remove_schedule(
        self,
        site_key: str,
        profile_id: str | None = None,
        proxy_url: str | None = None,
    ) -> bool:
        """Remove a schedule.

        Args:
            site_key: Site identifier
            profile_id: Optional profile ID
            proxy_url: Optional proxy URL

        Returns:
            True if schedule was removed
        """
        schedule_key = self._make_schedule_key(site_key, profile_id, proxy_url)

        if schedule_key in self._schedules:
            del self._schedules[schedule_key]
            logger.info("[Scheduler] Removed schedule for %s", site_key)
            return True

        return False

    def enable_schedule(
        self,
        site_key: str,
        profile_id: str | None = None,
        proxy_url: str | None = None,
    ) -> bool:
        """Enable a schedule.

        Args:
            site_key: Site identifier
            profile_id: Optional profile ID
            proxy_url: Optional proxy URL

        Returns:
            True if schedule was enabled
        """
        schedule_key = self._make_schedule_key(site_key, profile_id, proxy_url)
        entry = self._schedules.get(schedule_key)

        if entry:
            entry.enabled = True
            logger.info("[Scheduler] Enabled schedule for %s", site_key)
            return True

        return False

    def disable_schedule(
        self,
        site_key: str,
        profile_id: str | None = None,
        proxy_url: str | None = None,
    ) -> bool:
        """Disable a schedule.

        Args:
            site_key: Site identifier
            profile_id: Optional profile ID
            proxy_url: Optional proxy URL

        Returns:
            True if schedule was disabled
        """
        schedule_key = self._make_schedule_key(site_key, profile_id, proxy_url)
        entry = self._schedules.get(schedule_key)

        if entry:
            entry.enabled = False
            logger.info("[Scheduler] Disabled schedule for %s", site_key)
            return True

        return False

    def _make_schedule_key(
        self,
        site_key: str,
        profile_id: str | None = None,
        proxy_url: str | None = None,
    ) -> str:
        """Make key for schedule storage."""
        parts = [site_key]
        if profile_id:
            parts.append(profile_id)
        if proxy_url:
            parts.append(proxy_url)
        return ":".join(parts)

    def register_refresh_callback(self, callback: Callable) -> None:
        """Register callback to be called on refresh.

        Args:
            callback: Async function to call with (site_key, success) args
        """
        self._on_refresh_callbacks.append(callback)

    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            logger.warning("[Scheduler] Already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop())

        logger.info("[Scheduler] Started")

    async def stop(self) -> None:
        """Stop the scheduler."""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("[Scheduler] Stopped")

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        logger.info("[Scheduler] Starting main loop")

        try:
            while self._running:
                await self._check_and_refresh()
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            logger.info("[Scheduler] Loop cancelled")
        except Exception as e:
            logger.error(
                "[Scheduler] Error in main loop: %s",
                e,
                exc_info=True
            )

    async def _check_and_refresh(self) -> None:
        """Check for schedules and clearances needing refresh."""
        # Check scheduled refreshes
        for entry in list(self._schedules.values()):
            if entry.should_run():
                await self._run_schedule(entry)

        # Check clearances needing refresh based on TTL
        clearances = self.harvester.get_clearances_needing_refresh()
        for clearance in clearances:
            await self._refresh_clearance(clearance)

    async def _run_schedule(self, entry: ScheduleEntry) -> None:
        """Run a scheduled refresh.

        Args:
            entry: ScheduleEntry to run
        """
        logger.info(
            "[Scheduler] Running scheduled refresh for %s",
            entry.site_key
        )

        try:
            # Import here to avoid circular imports
            from flowcore_story.utils.profile_manager import get_profile_for_request

            # Get profile for the site
            get_profile_for_request(
                site_key=entry.site_key,
                proxy_url=entry.proxy_url,
                force_rotation=False
            )

            # Create Playwright context and solve challenge
            # This is a placeholder - actual implementation would need
            # to launch browser, navigate, detect challenge, solve, etc.

            # For now, just mark as attempted
            success = False  # Would be True if clearance obtained

            entry.mark_run(success)

            # Call callbacks
            for callback in self._on_refresh_callbacks:
                try:
                    await callback(entry.site_key, success)
                except Exception as e:
                    logger.warning(
                        "[Scheduler] Callback error: %s",
                        e
                    )

        except Exception as e:
            logger.error(
                "[Scheduler] Error running schedule for %s: %s",
                entry.site_key,
                e,
                exc_info=True
            )
            entry.mark_run(False)

    async def _refresh_clearance(self, clearance: ClearanceEntry) -> None:
        """Refresh a clearance that's approaching expiry.

        Args:
            clearance: ClearanceEntry to refresh
        """
        logger.info(
            "[Scheduler] Refreshing clearance for %s (expires in %.0fs)",
            clearance.site_key,
            clearance.time_until_expiry()
        )

        try:
            # Similar to _run_schedule but for existing clearance
            # Would need to use same profile/proxy as original

            # Placeholder
            pass

        except Exception as e:
            logger.error(
                "[Scheduler] Error refreshing clearance for %s: %s",
                clearance.site_key,
                e,
                exc_info=True
            )

    def get_schedules(self) -> list[ScheduleEntry]:
        """Get list of all schedules.

        Returns:
            List of ScheduleEntry objects
        """
        return list(self._schedules.values())

    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics.

        Returns:
            Dict with statistics
        """
        total_runs = sum(s.run_count for s in self._schedules.values())
        total_success = sum(s.success_count for s in self._schedules.values())
        total_failure = sum(s.failure_count for s in self._schedules.values())

        enabled = sum(1 for s in self._schedules.values() if s.enabled)

        return {
            "running": self._running,
            "total_schedules": len(self._schedules),
            "enabled_schedules": enabled,
            "total_runs": total_runs,
            "total_success": total_success,
            "total_failure": total_failure,
            "success_rate": total_success / total_runs if total_runs > 0 else 0.0,
        }


class DistributedCoordinator:
    """Coordinates clearance harvesting across multiple instances.

    This is a placeholder for distributed coordination logic.
    Actual implementation would use Redis, database, or message queue.
    """

    def __init__(
        self,
        instance_id: str,
        coordination_backend: str = "redis",
    ):
        """Initialize distributed coordinator.

        Args:
            instance_id: Unique ID for this instance
            coordination_backend: Backend to use (redis, postgres, etc.)
        """
        self.instance_id = instance_id
        self.coordination_backend = coordination_backend

        logger.info(
            "[Coordinator] Initialized instance %s with backend %s",
            instance_id,
            coordination_backend
        )

    async def claim_harvest_task(
        self,
        site_key: str,
        lease_duration: float = 300.0,
    ) -> bool:
        """Claim a harvest task for this instance.

        Args:
            site_key: Site to harvest for
            lease_duration: How long to hold the claim

        Returns:
            True if claim was successful
        """
        # Placeholder for distributed locking
        # Would use Redis SETNX or similar
        logger.debug(
            "[Coordinator] Claiming harvest task for %s",
            site_key
        )
        return True

    async def release_harvest_task(self, site_key: str) -> None:
        """Release a harvest task claim.

        Args:
            site_key: Site to release
        """
        logger.debug(
            "[Coordinator] Releasing harvest task for %s",
            site_key
        )

    async def share_clearance(
        self,
        clearance: ClearanceEntry,
    ) -> None:
        """Share clearance with other instances.

        Args:
            clearance: ClearanceEntry to share
        """
        logger.debug(
            "[Coordinator] Sharing clearance for %s",
            clearance.site_key
        )
        # Would store in shared backend (Redis, database, etc.)

    async def get_shared_clearance(
        self,
        site_key: str,
        profile_id: str | None = None,
        proxy_url: str | None = None,
    ) -> ClearanceEntry | None:
        """Get clearance shared by other instances.

        Args:
            site_key: Site identifier
            profile_id: Optional profile ID
            proxy_url: Optional proxy URL

        Returns:
            ClearanceEntry if available
        """
        logger.debug(
            "[Coordinator] Getting shared clearance for %s",
            site_key
        )
        # Would retrieve from shared backend
        return None


# Global scheduler instance
_global_scheduler: ClearanceScheduler | None = None


def get_global_scheduler() -> ClearanceScheduler:
    """Get or create global scheduler instance."""
    global _global_scheduler
    if _global_scheduler is None:
        _global_scheduler = ClearanceScheduler()
    return _global_scheduler


__all__ = [
    "ScheduleEntry",
    "ClearanceScheduler",
    "DistributedCoordinator",
    "get_global_scheduler",
]


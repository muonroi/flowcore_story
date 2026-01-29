"""Resource monitoring (Memory/CPU) and auto-cleanup utilities."""
from __future__ import annotations

import asyncio
import gc
import logging
import os
import time
from typing import Any, Callable, Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger("storyflow.resource_monitor")


class ResourceMonitor:
    """Monitor system resources (RAM, CPU) and trigger cleanup/restart when needed."""

    def __init__(
        self,
        max_memory_percent: float = 85.0,
        max_cpu_percent: float = 90.0,
        check_interval: int = 60,
        cleanup_callback: Optional[Callable] = None,
    ):
        """
        Initialize resource monitor.

        Args:
            max_memory_percent: Maximum memory usage percentage before triggering cleanup
            max_cpu_percent: Maximum CPU usage percentage before triggering cleanup
            check_interval: Interval in seconds to check resources
            cleanup_callback: Optional async callback to call when resources are critical
        """
        self.max_memory_percent = max_memory_percent
        self.max_cpu_percent = max_cpu_percent
        self.check_interval = check_interval
        self.cleanup_callback = cleanup_callback
        
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._high_cpu_streak = 0
        self._high_cpu_threshold = 3  # Trigger after 3 consecutive high checks

        if not PSUTIL_AVAILABLE:
            logger.warning("[ResourceMonitor] psutil not available, monitoring disabled")

    def get_resource_info(self) -> dict[str, Any]:
        """Get current resource usage information."""
        if not PSUTIL_AVAILABLE:
            return {
                "available": False,
                "error": "psutil not installed"
            }

        try:
            # System memory
            virtual_mem = psutil.virtual_memory()

            # Process info
            process = psutil.Process()
            
            # Memory
            process_mem = process.memory_info()
            
            # CPU (including children)
            # interval=0.1 ensures we get a non-blocking instant sample, 
            # but for accurate results we rely on psutil's internal tracking since last call.
            # However, for children we have to iterate.
            
            # Parent CPU
            proc_cpu = process.cpu_percent(interval=None)
            
            # Children CPU (sum)
            children_cpu = 0.0
            child_count = 0
            for child in process.children(recursive=True):
                try:
                    children_cpu += child.cpu_percent(interval=None)
                    child_count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            total_cpu = proc_cpu + children_cpu

            return {
                "available": True,
                "system_memory": {
                    "total_gb": round(virtual_mem.total / (1024**3), 2),
                    "used_gb": round(virtual_mem.used / (1024**3), 2),
                    "percent": virtual_mem.percent,
                },
                "process_memory": {
                    "rss_mb": round(process_mem.rss / (1024**2), 2),
                    "percent": process.memory_percent(),
                },
                "cpu": {
                    "process_percent": round(proc_cpu, 1),
                    "children_percent": round(children_cpu, 1),
                    "total_percent": round(total_cpu, 1),
                    "child_count": child_count,
                },
                "timestamp": time.time(),
            }
        except Exception as e:
            logger.error("[ResourceMonitor] Failed to get info: %s", e)
            return {
                "available": False,
                "error": str(e)
            }

    async def force_cleanup(self, reason: str = "manual") -> dict[str, Any]:
        """Force garbage collection and cleanup."""
        logger.info(f"[ResourceMonitor] Starting forced cleanup (reason: {reason})...")

        info_before = self.get_resource_info()

        # Run garbage collection multiple times for thorough cleanup
        collected = []
        for generation in range(3):
            count = gc.collect(generation)
            collected.append(count)
            await asyncio.sleep(0.1)

        # Call custom cleanup callback if provided
        if self.cleanup_callback:
            try:
                logger.info(f"[ResourceMonitor] Executing cleanup callback...")
                if asyncio.iscoroutinefunction(self.cleanup_callback):
                    await self.cleanup_callback()
                else:
                    self.cleanup_callback()
            except Exception as e:
                logger.error("[ResourceMonitor] Cleanup callback failed: %s", e)

        info_after = self.get_resource_info()

        result = {
            "collected_objects": sum(collected),
            "reason": reason,
        }

        if info_before.get("available") and info_after.get("available"):
            rss_before = info_before["process_memory"]["rss_mb"]
            rss_after = info_after["process_memory"]["rss_mb"]
            freed_mb = rss_before - rss_after
            
            result["freed_mb"] = round(freed_mb, 2)
            result["memory_after"] = rss_after
            
            logger.info(
                "[ResourceMonitor] Cleanup completed: freed %.2f MB, collected %d objects. Current RSS: %.2f MB",
                freed_mb,
                sum(collected),
                rss_after
            )

        return result

    async def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        logger.info(
            "[ResourceMonitor] Started monitoring (mem_max: %.1f%%, cpu_max: %.1f%%, interval: %ds)",
            self.max_memory_percent,
            self.max_cpu_percent,
            self.check_interval
        )

        # Initial CPU check to prime psutil counters (first call returns 0)
        _ = self.get_resource_info()

        while self._running:
            try:
                info = self.get_resource_info()

                if info.get("available"):
                    # Memory Check
                    sys_mem_percent = info["system_memory"]["percent"]
                    proc_mem_mb = info["process_memory"]["rss_mb"]
                    
                    # CPU Check
                    total_cpu = info["cpu"]["total_percent"]
                    child_count = info["cpu"]["child_count"]

                    # Log heartbeat (debug level)
                    logger.debug(
                        "[ResourceMonitor] Status: Mem=%.1f%% (Proc: %.0fMB), CPU=%.1f%% (Children: %d)",
                        sys_mem_percent, proc_mem_mb, total_cpu, child_count
                    )

                    # 1. MEMORY TRIGGER
                    if sys_mem_percent >= self.max_memory_percent:
                        logger.warning(
                            "[ResourceMonitor] CRITICAL MEMORY: %.1f%% >= %.1f%%",
                            sys_mem_percent, self.max_memory_percent
                        )
                        await self.force_cleanup(reason="high_memory")
                        # Reset CPU streak after cleanup to give it a chance to settle
                        self._high_cpu_streak = 0
                    
                    # 2. CPU TRIGGER
                    elif total_cpu >= self.max_cpu_percent:
                        self._high_cpu_streak += 1
                        logger.warning(
                            "[ResourceMonitor] HIGH CPU detected: %.1f%% >= %.1f%% (Streak: %d/%d)",
                            total_cpu, self.max_cpu_percent, self._high_cpu_streak, self._high_cpu_threshold
                        )
                        
                        if self._high_cpu_streak >= self._high_cpu_threshold:
                            logger.error(
                                "[ResourceMonitor] CPU STUCK detected (%d consecutive checks). Triggering cleanup/restart...",
                                self._high_cpu_streak
                            )
                            # This cleanup usually triggers a browser restart in the callback
                            await self.force_cleanup(reason="high_cpu_stuck")
                            self._high_cpu_streak = 0
                    else:
                        # Reset streak if CPU returns to normal
                        if self._high_cpu_streak > 0:
                            logger.info("[ResourceMonitor] CPU usage normalized (%.1f%%). Streak reset.", total_cpu)
                            self._high_cpu_streak = 0

            except Exception as e:
                logger.error("[ResourceMonitor] Error in monitor loop: %s", e)

            # Wait for next check
            await asyncio.sleep(self.check_interval)

    async def start(self) -> None:
        """Start monitoring."""
        if not PSUTIL_AVAILABLE:
            logger.warning("[ResourceMonitor] Cannot start monitoring, psutil not available")
            return

        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("[ResourceMonitor] Started")

    async def stop(self) -> None:
        """Stop monitoring."""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("[ResourceMonitor] Stopped")


class ContextPoolCleaner:
    """Auto cleanup for browser context pools."""

    def __init__(
        self,
        context_pool: dict,
        max_idle_time: int = 300,  # 5 minutes
        cleanup_interval: int = 300,  # 5 minutes
        max_pool_size: int = 10,
    ):
        """
        Initialize context pool cleaner.

        Args:
            context_pool: Dictionary of pooled contexts to manage
            max_idle_time: Maximum idle time in seconds before context is cleaned
            cleanup_interval: Interval in seconds to run cleanup
            max_pool_size: Maximum number of contexts to keep in pool
        """
        self.context_pool = context_pool
        self.max_idle_time = max_idle_time
        self.cleanup_interval = cleanup_interval
        self.max_pool_size = max_pool_size
        self._last_used: dict[tuple, float] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def mark_used(self, pool_key: tuple) -> None:
        """Mark a context as recently used."""
        self._last_used[pool_key] = time.time()

    async def cleanup_idle_contexts(self) -> dict[str, Any]:
        """Clean up idle contexts from pool."""
        if not self.context_pool:
            return {"cleaned": 0, "reason": "empty_pool"}

        now = time.time()
        cleaned = []

        # Find idle contexts
        for pool_key, context in list(self.context_pool.items()):
            last_used = self._last_used.get(pool_key, 0)
            idle_time = now - last_used

            if idle_time > self.max_idle_time:
                try:
                    await context.close()
                    self.context_pool.pop(pool_key, None)
                    self._last_used.pop(pool_key, None)
                    cleaned.append(pool_key)
                    logger.info(
                        "[ContextPoolCleaner] Cleaned idle context %s (idle: %.1fs)",
                        pool_key,
                        idle_time
                    )
                except Exception as e:
                    logger.error(
                        "[ContextPoolCleaner] Failed to clean context %s: %s",
                        pool_key,
                        e
                    )

        # If pool is still too large, clean oldest contexts
        if len(self.context_pool) > self.max_pool_size:
            # Sort by last used time
            sorted_keys = sorted(
                self._last_used.items(),
                key=lambda x: x[1]
            )

            # Clean oldest contexts
            to_remove = len(self.context_pool) - self.max_pool_size
            for pool_key, _ in sorted_keys[:to_remove]:
                try:
                    context = self.context_pool.get(pool_key)
                    if context:
                        await context.close()
                    self.context_pool.pop(pool_key, None)
                    self._last_used.pop(pool_key, None)
                    cleaned.append(pool_key)
                    logger.info(
                        "[ContextPoolCleaner] Cleaned excess context %s (pool limit: %d)",
                        pool_key,
                        self.max_pool_size
                    )
                except Exception as e:
                    logger.error(
                        "[ContextPoolCleaner] Failed to clean excess context %s: %s",
                        pool_key,
                        e
                    )

        return {
            "cleaned": len(cleaned),
            "cleaned_keys": cleaned,
            "pool_size": len(self.context_pool),
        }

    async def _cleanup_loop(self) -> None:
        """Background cleanup loop."""
        logger.info(
            "[ContextPoolCleaner] Started cleanup loop (interval: %ds, max_idle: %ds, max_size: %d)",
            self.cleanup_interval,
            self.max_idle_time,
            self.max_pool_size
        )

        while self._running:
            try:
                result = await self.cleanup_idle_contexts()
                if result["cleaned"] > 0:
                    logger.info(
                        "[ContextPoolCleaner] Cleanup completed: %s",
                        result
                    )
            except Exception as e:
                logger.error("[ContextPoolCleaner] Error in cleanup loop: %s", e)

            await asyncio.sleep(self.cleanup_interval)

    async def start(self) -> None:
        """Start cleanup loop."""
        if self._running:
            logger.warning("[ContextPoolCleaner] Already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._cleanup_loop())
        logger.info("[ContextPoolCleaner] Started")

    async def stop(self) -> None:
        """Stop cleanup loop."""
        if not self._running:
            return

        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("[ContextPoolCleaner] Stopped")

# Legacy alias for backward compatibility
MemoryMonitor = ResourceMonitor
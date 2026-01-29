"""Helpers to compute and cache statistics about stories with dead chapters."""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass

from flowcore_story.utils.logger import logger


@dataclass
class DeadStoryStats:
    """Aggregated information about stories that contain dead chapters."""

    story_count: int = 0
    dead_chapter_total: int = 0
    refreshed_at: float = 0.0


class DeadStoryMonitor:
    """Periodically scan crawl folders to summarise dead-chapter usage."""

    def __init__(
        self,
        folders: Iterable[str],
        *,
        refresh_interval: float,
    ) -> None:
        self._folders: list[str] = [path for path in folders if path]
        self._refresh_interval = max(float(refresh_interval), 30.0)
        self._lock = threading.Lock()
        self._stats = DeadStoryStats(refreshed_at=0.0)

    def refresh(self) -> DeadStoryStats:
        """Return cached stats or recompute when the cache is stale."""

        now = time.time()
        with self._lock:
            if now - self._stats.refreshed_at < self._refresh_interval:
                return self._stats

        stats = self._scan_folders()
        stats.refreshed_at = time.time()

        with self._lock:
            self._stats = stats
            return self._stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _scan_folders(self) -> DeadStoryStats:
        story_count = 0
        dead_chapter_total = 0

        for base_folder in self._folders:
            if not base_folder or not os.path.isdir(base_folder):
                continue
            for root, _, files in os.walk(base_folder):
                if "dead_chapters.json" not in files:
                    continue
                path = os.path.join(root, "dead_chapters.json")
                try:
                    with open(path, encoding="utf-8") as handle:
                        payload = json.load(handle)
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.warning("[DEAD][MONITOR] Không đọc được %s: %s", path, exc)
                    continue

                if isinstance(payload, list):
                    dead_entries = [item for item in payload if isinstance(item, dict)]
                    if dead_entries:
                        story_count += 1
                        dead_chapter_total += len(dead_entries)

        return DeadStoryStats(
            story_count=story_count,
            dead_chapter_total=dead_chapter_total,
            refreshed_at=time.time(),
        )


__all__ = ["DeadStoryMonitor", "DeadStoryStats"]


"""Enrichment Worker - Brain worker for cross-site metadata enrichment.

This worker runs periodically to:
1. Scan stories needing enrichment (missing author, description, cover, etc.)
2. Find duplicates across sites using fuzzy title matching
3. Score and compare content quality across sources
4. Enrich metadata from best available sources

Features:
- Metadata Enrichment: Fill missing fields from other sites
- Duplicate Detection: Find and merge duplicate stories
- Quality Scoring: Compare and rank sources by quality
- Statistics: Track enrichment success rates
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from flowcore.config import config as app_config
from flowcore.config.config import COMPLETED_FOLDER, DATA_FOLDER
from flowcore.utils.duplicate_detector import DuplicateDetector, DuplicateReport
from flowcore.utils.logger import logger
from flowcore.utils.metadata_enricher import EnrichmentReport, MetadataEnricher
from flowcore.utils.progress_emitter import compact_payload, emit_progress_event
from flowcore.utils.quality_scorer import QualityComparisonReport, QualityScorer


# Configuration from environment
LOOP_INTERVAL = int(os.getenv("ENRICHMENT_LOOP_INTERVAL", "86400"))  # 24 hours
BATCH_SIZE = int(os.getenv("ENRICHMENT_BATCH_SIZE", "50"))
FUZZY_THRESHOLD = float(os.getenv("ENRICHMENT_FUZZY_THRESHOLD", "0.8"))

# Feature flags
ENABLE_METADATA_ENRICHMENT = os.getenv(
    "ENRICHMENT_ENABLE_METADATA_ENRICHMENT", "true"
).lower() == "true"
ENABLE_DUPLICATE_DETECTION = os.getenv(
    "ENRICHMENT_ENABLE_DUPLICATE_DETECTION", "true"
).lower() == "true"
ENABLE_QUALITY_SCORING = os.getenv(
    "ENRICHMENT_ENABLE_QUALITY_SCORING", "true"
).lower() == "true"

# Sharding support
SHARD_TOTAL = max(int(os.getenv("ENRICHMENT_SHARD_TOTAL", "1")), 1)
SHARD_INDEX = int(os.getenv("ENRICHMENT_SHARD_INDEX", "0")) % SHARD_TOTAL

# Worker identification
WORKER_ID = os.getenv("ENRICHMENT_WORKER_ID", f"enrichment-worker-{SHARD_INDEX}")

# State file for tracking progress
STATE_FOLDER = os.getenv("STATE_FOLDER", "./state")
STATE_FILE = os.path.join(STATE_FOLDER, f"enrichment_worker_{SHARD_INDEX}.json")


@dataclass
class EnrichmentStats:
    """Statistics for an enrichment run."""

    total_stories: int = 0
    stories_processed: int = 0
    stories_enriched: int = 0
    stories_with_duplicates: int = 0
    stories_quality_scored: int = 0
    fields_enriched: int = 0
    duplicates_found: int = 0
    errors: int = 0
    started_at: float = field(default_factory=time.time)
    completed_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_stories": self.total_stories,
            "stories_processed": self.stories_processed,
            "stories_enriched": self.stories_enriched,
            "stories_with_duplicates": self.stories_with_duplicates,
            "stories_quality_scored": self.stories_quality_scored,
            "fields_enriched": self.fields_enriched,
            "duplicates_found": self.duplicates_found,
            "errors": self.errors,
            "duration": (self.completed_at or time.time()) - self.started_at,
        }


class EnrichmentWorker:
    """Brain worker for cross-site metadata enrichment."""

    def __init__(self) -> None:
        self._shutdown_event = asyncio.Event()
        self._enricher = MetadataEnricher(fuzzy_threshold=FUZZY_THRESHOLD)
        self._detector = DuplicateDetector(fuzzy_threshold=FUZZY_THRESHOLD)
        self._scorer = QualityScorer()
        self._state: dict[str, Any] = {}
        self._load_state()

    def _load_state(self) -> None:
        """Load worker state from file."""
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, encoding="utf-8") as f:
                    self._state = json.load(f)
        except Exception as e:
            logger.warning(f"[ENRICH] Could not load state: {e}")
            self._state = {}

    def _save_state(self) -> None:
        """Save worker state to file."""
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(self._state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[ENRICH] Could not save state: {e}")

    def _emit_event(
        self,
        action: str,
        *,
        status: str | None = None,
        started_at: float | None = None,
        error: Exception | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Emit progress event to Kafka."""
        payload: dict[str, Any] = {
            "action": action,
            "worker_id": WORKER_ID,
            "shard_index": SHARD_INDEX,
            "shard_total": SHARD_TOTAL,
            "timestamp": time.time(),
        }

        if status:
            payload["status"] = status

        if started_at:
            payload["duration"] = time.time() - started_at

        if error:
            payload["error"] = str(error)
            payload["error_type"] = error.__class__.__name__

        if extra:
            payload.update(extra)

        emit_progress_event("enrichment_worker", compact_payload(payload))

    def _get_story_folders(self) -> list[str]:
        """Get list of story folders to process."""
        folders: list[str] = []

        # Scan DATA_FOLDER
        if os.path.exists(DATA_FOLDER):
            for name in os.listdir(DATA_FOLDER):
                path = os.path.join(DATA_FOLDER, name)
                if os.path.isdir(path) and os.path.exists(
                    os.path.join(path, "metadata.json")
                ):
                    folders.append(path)

        # Scan COMPLETED_FOLDER subdirectories
        if os.path.exists(COMPLETED_FOLDER):
            for genre_name in os.listdir(COMPLETED_FOLDER):
                genre_path = os.path.join(COMPLETED_FOLDER, genre_name)
                if not os.path.isdir(genre_path):
                    continue
                for name in os.listdir(genre_path):
                    path = os.path.join(genre_path, name)
                    if os.path.isdir(path) and os.path.exists(
                        os.path.join(path, "metadata.json")
                    ):
                        folders.append(path)

        return sorted(folders)

    def _filter_by_shard(self, folders: list[str]) -> list[str]:
        """Filter folders by shard index for distributed processing."""
        if SHARD_TOTAL <= 1:
            return folders

        assigned: list[str] = []
        for folder in folders:
            folder_name = os.path.basename(folder)
            digest = int(hashlib.sha256(folder_name.encode()).hexdigest(), 16)
            if digest % SHARD_TOTAL == SHARD_INDEX:
                assigned.append(folder)

        logger.info(
            f"[ENRICH] Shard {SHARD_INDEX}/{SHARD_TOTAL}: "
            f"handling {len(assigned)}/{len(folders)} stories"
        )

        return assigned

    async def _process_story(
        self,
        story_folder: str,
        stats: EnrichmentStats,
    ) -> None:
        """Process a single story for enrichment."""
        metadata_path = os.path.join(story_folder, "metadata.json")

        try:
            with open(metadata_path, encoding="utf-8") as f:
                metadata = json.load(f)
        except Exception as e:
            logger.warning(f"[ENRICH] Could not load {metadata_path}: {e}")
            stats.errors += 1
            return

        title = metadata.get("title", os.path.basename(story_folder))

        # Step 1: Metadata Enrichment
        if ENABLE_METADATA_ENRICHMENT:
            try:
                report = await self._enricher.enrich_from_sources(
                    story_folder, metadata, save=True
                )
                if report.success:
                    stats.stories_enriched += 1
                    stats.fields_enriched += len(report.enrichments)

                    self._emit_event(
                        "story_enriched",
                        extra={
                            "story_title": title,
                            "fields_enriched": [e.field for e in report.enrichments],
                            "sources_used": list({e.source_site for e in report.enrichments}),
                        },
                    )

                    # Reload metadata after enrichment
                    with open(metadata_path, encoding="utf-8") as f:
                        metadata = json.load(f)

            except Exception as e:
                logger.warning(f"[ENRICH] Error enriching {title}: {e}")
                stats.errors += 1

        # Step 2: Duplicate Detection
        if ENABLE_DUPLICATE_DETECTION:
            try:
                dup_report = await self._detector.find_duplicates(
                    story_folder, metadata
                )
                if dup_report.duplicates:
                    stats.stories_with_duplicates += 1
                    stats.duplicates_found += len(dup_report.duplicates)

                    # Merge duplicates
                    await self._detector.merge_duplicates(
                        story_folder, dup_report.duplicates
                    )

                    self._emit_event(
                        "duplicates_found",
                        extra={
                            "story_title": title,
                            "duplicate_count": len(dup_report.duplicates),
                            "sites": [d.match_site_key for d in dup_report.duplicates],
                        },
                    )

                    # Reload metadata after merge
                    with open(metadata_path, encoding="utf-8") as f:
                        metadata = json.load(f)

            except Exception as e:
                logger.warning(f"[ENRICH] Error detecting duplicates for {title}: {e}")
                stats.errors += 1

        # Step 3: Quality Scoring (sample only, skip if too many sources)
        if ENABLE_QUALITY_SCORING:
            sources = metadata.get("sources", [])
            if 2 <= len(sources) <= 5:  # Only score if multiple sources exist
                try:
                    quality_report = await self._scorer.compare_sources(
                        story_folder, metadata
                    )
                    if quality_report.recommended_primary:
                        stats.stories_quality_scored += 1

                        # Log if recommendation differs from current primary
                        current_primary = next(
                            (
                                s.get("site_key")
                                for s in sources
                                if isinstance(s, dict) and s.get("is_primary")
                            ),
                            None,
                        )

                        if (
                            current_primary
                            and quality_report.recommended_primary != current_primary
                        ):
                            logger.info(
                                f"[QUALITY] '{title}': Recommend {quality_report.recommended_primary} "
                                f"over {current_primary} ({quality_report.recommendation_reason})"
                            )

                except Exception as e:
                    logger.debug(f"[ENRICH] Error scoring quality for {title}: {e}")

        stats.stories_processed += 1

    async def run_once(self) -> EnrichmentStats:
        """Run a single enrichment cycle."""
        started_at = time.time()
        stats = EnrichmentStats(started_at=started_at)

        self._emit_event("cycle_started")

        logger.info(
            f"[ENRICH] Starting enrichment cycle "
            f"(worker={WORKER_ID}, shard={SHARD_INDEX}/{SHARD_TOTAL})"
        )

        try:
            # Get and filter story folders
            all_folders = self._get_story_folders()
            folders = self._filter_by_shard(all_folders)
            stats.total_stories = len(folders)

            logger.info(f"[ENRICH] Processing {len(folders)} stories")

            # Process in batches
            for i in range(0, len(folders), BATCH_SIZE):
                if self._shutdown_event.is_set():
                    logger.info("[ENRICH] Shutdown requested, stopping batch")
                    break

                batch = folders[i : i + BATCH_SIZE]

                for story_folder in batch:
                    if self._shutdown_event.is_set():
                        break

                    await self._process_story(story_folder, stats)

                    # Small delay between stories
                    await asyncio.sleep(0.1)

                # Log batch progress
                logger.info(
                    f"[ENRICH] Batch {i // BATCH_SIZE + 1}: "
                    f"{stats.stories_processed}/{stats.total_stories} processed, "
                    f"{stats.stories_enriched} enriched"
                )

                # Delay between batches
                await asyncio.sleep(1.0)

        except Exception as e:
            logger.exception(f"[ENRICH] Error in cycle: {e}")
            stats.errors += 1
            self._emit_event("cycle_failed", error=e)

        stats.completed_at = time.time()

        # Emit statistics
        self._emit_event(
            "cycle_completed",
            started_at=started_at,
            extra=stats.to_dict(),
        )

        # Update state
        self._state["last_run"] = datetime.now().isoformat()
        self._state["last_stats"] = stats.to_dict()
        self._save_state()

        logger.info(
            f"[ENRICH] Cycle completed in {stats.completed_at - started_at:.1f}s: "
            f"{stats.stories_processed} processed, {stats.stories_enriched} enriched, "
            f"{stats.duplicates_found} duplicates, {stats.errors} errors"
        )

        return stats

    def request_shutdown(self) -> None:
        """Request worker shutdown."""
        logger.info("[ENRICH] Shutdown requested")
        self._shutdown_event.set()

    async def run(self) -> None:
        """Run the worker main loop."""
        logger.info(
            f"[ENRICH] Worker started (ID={WORKER_ID}, interval={LOOP_INTERVAL}s)"
        )

        # Setup signal handlers
        loop = asyncio.get_running_loop()

        def handle_signal(signum: int) -> None:
            logger.info(f"[ENRICH] Received signal {signum}")
            self.request_shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

        self._emit_event("worker_started")

        try:
            while not self._shutdown_event.is_set():
                try:
                    await self.run_once()
                except Exception as e:
                    logger.exception(f"[ENRICH] Error in run_once: {e}")
                    self._emit_event("cycle_error", error=e)

                # Check for one-shot mode
                if os.getenv("ENRICHMENT_ONE_SHOT", "").lower() == "true":
                    logger.info("[ENRICH] One-shot mode, exiting")
                    break

                # Wait for next cycle or shutdown
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=LOOP_INTERVAL,
                    )
                    break  # Shutdown requested
                except asyncio.TimeoutError:
                    pass  # Time for next cycle

        except asyncio.CancelledError:
            logger.info("[ENRICH] Worker cancelled")

        finally:
            self._emit_event("worker_stopped")
            logger.info("[ENRICH] Worker stopped")


# Module entry point for testing
async def main() -> None:
    """Main entry point."""
    worker = EnrichmentWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

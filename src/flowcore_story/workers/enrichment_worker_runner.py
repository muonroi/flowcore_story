#!/usr/bin/env python3
"""Entry point for the Enrichment Worker container.

This script initializes and runs the EnrichmentWorker, which handles:
- Metadata enrichment from cross-site sources
- Duplicate story detection and merging
- Content quality scoring and comparison

Usage:
    python -m flowcore_story.workers.enrichment_worker_runner

Environment Variables:
    ENRICHMENT_LOOP_INTERVAL: Seconds between cycles (default: 86400 = 24h)
    ENRICHMENT_BATCH_SIZE: Stories per batch (default: 50)
    ENRICHMENT_FUZZY_THRESHOLD: Fuzzy matching threshold (default: 0.8)
    ENRICHMENT_SHARD_TOTAL: Total number of worker shards (default: 1)
    ENRICHMENT_SHARD_INDEX: This worker's shard index (default: 0)
    ENRICHMENT_ONE_SHOT: Run once and exit (default: false)
    ENRICHMENT_ENABLE_METADATA_ENRICHMENT: Enable metadata enrichment (default: true)
    ENRICHMENT_ENABLE_DUPLICATE_DETECTION: Enable duplicate detection (default: true)
    ENRICHMENT_ENABLE_QUALITY_SCORING: Enable quality scoring (default: true)
"""

from __future__ import annotations

import asyncio
import os
import sys


def main() -> int:
    """Main entry point for the worker."""
    # Ensure src is in path
    src_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    # Import after path setup
    from flowcore_story.utils.logger import logger
    from flowcore_story.workers.enrichment_worker import EnrichmentWorker

    logger.info("=" * 60)
    logger.info("ENRICHMENT WORKER - Brain Worker for Cross-Site Enrichment")
    logger.info("=" * 60)

    # Log configuration
    config_vars = [
        "ENRICHMENT_LOOP_INTERVAL",
        "ENRICHMENT_BATCH_SIZE",
        "ENRICHMENT_FUZZY_THRESHOLD",
        "ENRICHMENT_SHARD_TOTAL",
        "ENRICHMENT_SHARD_INDEX",
        "ENRICHMENT_ONE_SHOT",
        "ENRICHMENT_ENABLE_METADATA_ENRICHMENT",
        "ENRICHMENT_ENABLE_DUPLICATE_DETECTION",
        "ENRICHMENT_ENABLE_QUALITY_SCORING",
    ]

    logger.info("Configuration:")
    for var in config_vars:
        value = os.getenv(var, "(not set)")
        logger.info(f"  {var}: {value}")

    logger.info("=" * 60)

    # Create and run worker
    worker = EnrichmentWorker()

    try:
        asyncio.run(worker.run())
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except Exception as e:
        logger.exception(f"Worker failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

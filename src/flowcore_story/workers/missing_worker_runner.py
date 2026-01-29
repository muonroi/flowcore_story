import asyncio
import os

from flowcore_story.utils.dead_letter_queue import init_dlq
from flowcore_story.utils.logger import logger
from flowcore_story.kafka.kafka_producer import get_producer
from flowcore_story.workers.crawler_missing_chapter import (
    WORKER_INSTANCE_ID,
    WORKER_SHARD_INDEX,
    WORKER_SHARD_TOTAL,
    loop_once_multi_sites,
)


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_interval() -> int:
    raw = os.getenv("MISSING_WORKER_LOOP_INTERVAL")
    if raw is None or not raw.strip():
        return 0
    try:
        interval = int(raw)
    except ValueError:
        logger.warning(
            "[MISSING][RUNNER] Invalid MISSING_WORKER_LOOP_INTERVAL='%s'; defaulting to 0",
            raw,
        )
        return 0
    return max(0, interval)


async def _run_once(force_unskip: bool) -> None:
    logger.info(
        "[MISSING][RUNNER] Starting crawl pass (worker=%s shard=%s/%s)",
        WORKER_INSTANCE_ID,
        WORKER_SHARD_INDEX,
        WORKER_SHARD_TOTAL,
    )
    await loop_once_multi_sites(force_unskip=force_unskip)
    logger.info(
        "[MISSING][RUNNER] Completed crawl pass (worker=%s shard=%s/%s)",
        WORKER_INSTANCE_ID,
        WORKER_SHARD_INDEX,
        WORKER_SHARD_TOTAL,
    )


async def main() -> None:
    # Initialize Dead Letter Queue (Kafka-backed, with dummy fallback if needed)
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "storyflow.crawl.dlq")
    producer = await get_producer()
    init_dlq(kafka_producer=producer, dlq_topic=dlq_topic)

    force_unskip = _parse_bool(os.getenv("MISSING_WORKER_FORCE_UNSKIP"), default=False)
    one_shot = _parse_bool(os.getenv("MISSING_WORKER_ONE_SHOT"), default=False)
    interval = _parse_interval()

    while True:
        await _run_once(force_unskip)
        if one_shot:
            break

        if interval <= 0:
            logger.info(
                "[MISSING][RUNNER] No sleep interval configured; starting next pass immediately."
            )
            continue

        logger.info(
            "[MISSING][RUNNER] Sleeping %s seconds before next crawl pass.",
            interval,
        )
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main())

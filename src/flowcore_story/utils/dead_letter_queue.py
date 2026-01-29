"""
Dead Letter Queue (DLQ) for permanently failed chapters.

Sends failed chapters to a Kafka DLQ topic for later analysis and retry.
"""
import json
import time
from typing import Any

from flowcore_story.utils.logger import logger

# Will be initialized by crawler workers
_kafka_producer: Any | None = None
_dlq_topic: str | None = None
_dlq_enabled: bool = False


def init_dlq(kafka_producer: Any, dlq_topic: str) -> None:
    """
    Initialize Dead Letter Queue with Kafka producer.

    Args:
        kafka_producer: Kafka producer instance
        dlq_topic: Topic name for DLQ (e.g., 'storyflow.crawl.dlq')
    """
    global _kafka_producer, _dlq_topic, _dlq_enabled

    _kafka_producer = kafka_producer
    _dlq_topic = dlq_topic
    _dlq_enabled = kafka_producer is not None and dlq_topic is not None

    if _dlq_enabled:
        logger.info(f"[DLQ] Initialized with topic: {dlq_topic}")
    else:
        logger.warning("[DLQ] Not initialized - failed chapters will only be marked locally")


async def send_to_dlq(
    chapter_url: str,
    story_folder: str,
    story_title: str,
    chapter_index: int,
    reason: str,
    retry_count: int,
    last_error: str | None = None,
    site_key: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """
    Send a permanently failed chapter to Dead Letter Queue.

    Args:
        chapter_url: URL of the failed chapter
        story_folder: Story folder path
        story_title: Title of the story
        chapter_index: Chapter index/number
        reason: Reason for failure (e.g., "max_retries_exceeded", "timeout", "404")
        retry_count: Number of retries attempted
        last_error: Last error message
        site_key: Source site key
        metadata: Additional metadata

    Returns:
        True if sent successfully, False otherwise
    """
    if not _dlq_enabled:
        logger.debug(f"[DLQ] Skipped (not enabled) for chapter {chapter_index} of {story_title}")
        return False

    dlq_payload = {
        "chapter_url": chapter_url,
        "story_folder": story_folder,
        "story_title": story_title,
        "chapter_index": chapter_index,
        "reason": reason,
        "retry_count": retry_count,
        "last_error": last_error,
        "site_key": site_key,
        "timestamp": time.time(),
        "metadata": metadata or {},
    }

    try:
        # Send to Kafka DLQ topic
        payload_json = json.dumps(dlq_payload, ensure_ascii=False, default=str)
        payload_bytes = payload_json.encode("utf-8")
        if hasattr(_kafka_producer, "send_and_wait"):
            await _kafka_producer.send_and_wait(_dlq_topic, payload_bytes)
        elif hasattr(_kafka_producer, "send"):
            await _kafka_producer.send(_dlq_topic, payload_bytes)
        else:
            logger.warning("[DLQ] Producer missing send API; skipping DLQ publish")
            return False

        logger.info(
            f"[DLQ] Sent chapter {chapter_index} of '{story_title}' "
            f"(reason: {reason}, retries: {retry_count})"
        )
        return True

    except Exception as e:
        logger.error(
            f"[DLQ] Failed to send chapter {chapter_index} of '{story_title}' to DLQ: {e}"
        )
        return False


def get_dlq_stats() -> dict[str, Any]:
    """
    Get DLQ statistics.

    Returns:
        Dictionary with DLQ stats (enabled, topic, etc.)
    """
    return {
        "enabled": _dlq_enabled,
        "topic": _dlq_topic,
        "producer_active": _kafka_producer is not None,
    }

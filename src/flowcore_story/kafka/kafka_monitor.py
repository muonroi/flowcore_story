"""Helpers to inspect Kafka consumer group health."""

from __future__ import annotations

import asyncio
import importlib.util
from collections.abc import Iterable
from typing import Any

from flowcore_story.utils.logger import logger

try:
    _AIOKAFKA_SPEC = importlib.util.find_spec("aiokafka")
except (ModuleNotFoundError, ValueError):  # pragma: no cover - optional dependency missing
    _AIOKAFKA_SPEC = None

if _AIOKAFKA_SPEC:
    try:  # pragma: no cover - import style depends on aiokafka version
        from aiokafka import AIOKafkaAdminClient, AIOKafkaConsumer  # type: ignore
    except ImportError:  # pragma: no cover - fallback for older releases
        try:
            from aiokafka.admin import AIOKafkaAdminClient  # type: ignore
        except ImportError:
            AIOKafkaAdminClient = None  # type: ignore
        try:
            from aiokafka import AIOKafkaConsumer  # type: ignore
        except ImportError:
            AIOKafkaConsumer = None  # type: ignore
    try:  # pragma: no cover - same reasoning as above
        from aiokafka.errors import KafkaConnectionError, KafkaError  # type: ignore
    except ImportError:
        KafkaConnectionError = KafkaError = Exception  # type: ignore
    try:  # pragma: no cover - aiokafka may expose TopicPartition in different modules
        from aiokafka.structs import TopicPartition  # type: ignore
    except ImportError:  # pragma: no cover - defensive guard
        TopicPartition = None  # type: ignore
else:  # pragma: no cover - optional dependency missing
    AIOKafkaAdminClient = None  # type: ignore
    AIOKafkaConsumer = None  # type: ignore
    TopicPartition = None  # type: ignore

if AIOKafkaAdminClient is None:  # pragma: no cover - optional dependency missing
    class KafkaConnectionError(Exception):
        """Fallback error type when aiokafka is unavailable."""

    class KafkaError(Exception):
        """Fallback error type when aiokafka is unavailable."""


async def _describe_consumer_group(
    group_id: str,
    bootstrap_servers: str,
) -> tuple[bool, str, Any]:
    if not group_id:
        return False, "missing group id", None

    if AIOKafkaAdminClient is None:
        return True, "aiokafka admin client unavailable", None

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        await admin.start()
    except KafkaConnectionError as exc:
        logger.error(
            "[KafkaMonitor] Không thể kết nối Kafka admin để kiểm tra consumer group %s: %s",
            group_id,
            exc,
        )
        return False, f"connection_failed: {exc}", None
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.exception(
            "[KafkaMonitor] Lỗi khi khởi tạo Kafka admin cho group %s: %s",
            group_id,
            exc,
        )
        return False, f"admin_start_failed: {exc}", None

    try:
        try:
            description = await admin.describe_consumer_groups([group_id])
        except KafkaError as exc:
            logger.error(
                "[KafkaMonitor] Không thể lấy thông tin consumer group %s: %s",
                group_id,
                exc,
            )
            return False, f"describe_failed: {exc}", None

        if not description:
            return False, "group_not_found", None

        group = description[0]
        error = getattr(group, "error", None)
        if error:
            return False, f"group_error: {error}", group

        state = (getattr(group, "state", "") or "").lower()
        members = getattr(group, "members", None) or []
        metadata = {
            "state": state,
            "members": len(members),
        }
        if state in {"dead", "empty"} or not members:
            return False, f"inactive: state={state} members={len(members)}", metadata

        return True, f"state={state} members={len(members)}", metadata
    finally:
        await admin.close()


async def check_consumer_group_health(
    group_id: str,
    bootstrap_servers: str,
    *,
    timeout: float = 5.0,
) -> tuple[bool, str]:
    """Check whether the given consumer group has an active member."""

    try:
        success, message, _ = await asyncio.wait_for(
            _describe_consumer_group(group_id, bootstrap_servers),
            timeout=timeout,
        )
    except TimeoutError:
        logger.error(
            "[KafkaMonitor] Kiểm tra consumer group %s bị timeout sau %.1f giây.",
            group_id,
            timeout,
        )
        return False, "timeout"

    if success:
        logger.debug(
            "[KafkaMonitor] Consumer group %s hoạt động (%s).",
            group_id,
            message,
        )
    else:
        logger.warning(
            "[KafkaMonitor] Consumer group %s không sẵn sàng (%s).",
            group_id,
            message,
        )
    return success, message


async def _fetch_consumer_offsets(
    admin: AIOKafkaAdminClient,
    topic: str,
    group_id: str,
) -> dict[TopicPartition, int]:
    offsets = await admin.list_consumer_group_offsets(group_id)
    topic_offsets: dict[TopicPartition, int] = {}
    for partition, metadata in offsets.items():
        if partition.topic != topic:
            continue
        topic_offsets[partition] = metadata.offset
    return topic_offsets


async def _fetch_end_offsets(
    topic: str,
    bootstrap_servers: str,
    partitions: Iterable[TopicPartition],
) -> dict[TopicPartition, int]:
    if AIOKafkaConsumer is None:
        raise RuntimeError("aiokafka consumer unavailable")

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        group_id=None,
    )
    try:
        await consumer.start()
        return await consumer.end_offsets(list(partitions))
    finally:  # pragma: no branch - ensure consumer stopped
        await consumer.stop()


async def _describe_topic_lag(
    topic: str,
    group_id: str,
    bootstrap_servers: str,
) -> tuple[int | None, dict[str, Any]]:
    if not topic or not group_id or not bootstrap_servers:
        return None, {"status": "missing_configuration"}
    if AIOKafkaAdminClient is None or TopicPartition is None:
        return None, {"status": "aiokafka_unavailable"}

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        try:
            await admin.start()
        except KafkaConnectionError as exc:
            return None, {"status": "connection_failed", "error": str(exc)}
        except Exception as exc:  # pragma: no cover - defensive guard
            return None, {"status": "admin_start_failed", "error": str(exc)}

        try:
            offsets = await _fetch_consumer_offsets(admin, topic, group_id)
        except KafkaConnectionError as exc:
            return None, {"status": "describe_failed", "error": str(exc)}
    finally:  # pragma: no branch - cleanup admin client
        await admin.close()

    if not offsets:
        return 0, {"status": "no_offsets"}

    try:
        end_offsets = await _fetch_end_offsets(topic, bootstrap_servers, offsets.keys())
    except KafkaConnectionError as exc:
        return None, {"status": "connection_failed", "error": str(exc)}
    except Exception as exc:  # pragma: no cover - defensive guard
        return None, {"status": "end_offsets_failed", "error": str(exc)}

    lag = 0
    for partition, latest in end_offsets.items():
        committed = offsets.get(partition, 0)
        if latest is None or committed is None:
            continue
        if latest < committed:
            continue
        lag += latest - committed

    return lag, {"status": "ok", "partitions": len(end_offsets)}


async def fetch_consumer_lag(
    topic: str,
    group_id: str,
    bootstrap_servers: str,
    *,
    timeout: float = 5.0,
) -> tuple[int | None, dict[str, Any]]:
    """Return the approximate lag for ``group_id`` on ``topic``.

    The function gracefully degrades when Kafka admin APIs are not available and
    returns diagnostic metadata that can be displayed on the dashboard.
    """

    if AIOKafkaAdminClient is None or TopicPartition is None:
        return None, {"status": "aiokafka_unavailable"}

    try:
        lag, metadata = await asyncio.wait_for(
            _describe_topic_lag(topic, group_id, bootstrap_servers),
            timeout=timeout,
        )
        return lag, metadata
    except TimeoutError:
        return None, {"status": "timeout"}


__all__ = ["check_consumer_group_health", "fetch_consumer_lag"]

"""Backward-compatible helpers for emitting jobs to Kafka.

Historically the StoryFlow project exposed ``utils.kafka_producer`` for
extensions such as the Telegram bot. The modern crawler uses the richer
``kafka.kafka_producer`` module which already features retry handling and a
dummy fallback when Kafka is unreachable. Some auxiliary scripts still import
this older module though, which meant they would crash as soon as Kafka was
offline – a regression compared to the new producer implementation.

To make the behaviour consistent across the code base we align this helper with
the newer producer. The key differences compared to the previous revision are:

* Connection attempts now honour ``KAFKA_BOOTSTRAP_MAX_RETRIES`` and
  ``KAFKA_BOOTSTRAP_RETRY_DELAY`` from the shared configuration.
* After exhausting retries we fall back to an in-memory "dummy" producer that
  simply logs the payload instead of raising ``KafkaConnectionError``. This
  allows manual commands and the Telegram bot to operate in environments where
  Kafka is intentionally disabled.
* Importing the module no longer fails when the optional ``aiokafka`` package is
  absent; we expose a small compatibility ``KafkaConnectionError`` class instead.

The end result is that callers receive the same resilient behaviour regardless
of which producer helper they use, and the noisy "Unable to request metadata"
errors disappear from the logs when Kafka is offline.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
from typing import Any

from flowcore_story.config import config as app_config
from flowcore_story.utils.async_primitives import LoopBoundLock
from flowcore_story.utils.kafka_ssl import build_ssl_context
from flowcore_story.utils.logger import logger

try:  # pragma: no cover - exercised via tests with custom stubs
    _AIOKAFKA_SPEC = importlib.util.find_spec("aiokafka")
except (ModuleNotFoundError, ValueError):  # pragma: no cover - optional dependency missing
    _AIOKAFKA_SPEC = None

if _AIOKAFKA_SPEC:
    try:  # pragma: no cover - import style depends on aiokafka version
        from aiokafka import AIOKafkaProducer  # type: ignore[misc,assignment]
    except ImportError:  # pragma: no cover - defensive guard for old releases
        AIOKafkaProducer = None  # type: ignore[misc,assignment]
else:  # pragma: no cover - optional dependency missing
    AIOKafkaProducer = None  # type: ignore[misc,assignment]

try:  # pragma: no cover - exercised via tests with custom stubs
    from aiokafka.errors import KafkaConnectionError  # type: ignore[attr-defined]
except (ImportError, AttributeError):  # pragma: no cover - optional dependency missing
    class KafkaConnectionError(Exception):  # type: ignore[override]
        """Fallback error used when aiokafka is unavailable."""


class _DummyProducer:
    """Simple stand-in that mimics the Kafka producer API."""

    def __init__(self) -> None:
        self._closed = False

    async def start(self) -> None:  # pragma: no cover - trivial behaviour
        self._closed = False

    async def stop(self) -> None:  # pragma: no cover - trivial behaviour
        self._closed = True

    async def send_and_wait(self, topic, payload) -> None:  # pragma: no cover
        logger.info("[Kafka Dummy] would send to %s: %s", topic, payload)


_producer: AIOKafkaProducer | None = None
_producer_lock = LoopBoundLock()


async def _initialise_dummy_producer():
    dummy = _DummyProducer()
    await dummy.start()
    return dummy


async def get_kafka_producer():
    """Return a singleton Kafka producer instance or a dummy fallback."""

    global _producer
    async with _producer_lock:
        if _producer is not None and not getattr(_producer, "_closed", False):
            return _producer

        if AIOKafkaProducer is None:
            logger.warning("aiokafka not installed; using in-memory dummy producer")
            _producer = await _initialise_dummy_producer()
            logger.info("[Kafka Producer] Dummy producer đã sẵn sàng.")
            return _producer

        logger.info(
            "[Kafka Producer] Initializing producer for brokers: %s",
            app_config.KAFKA_BOOTSTRAP_SERVERS,
        )

        kafka_auth_config: dict[str, Any] = {}
        protocol_raw = getattr(app_config, "KAFKA_SECURITY_PROTOCOL", None)
        protocol = protocol_raw.upper() if protocol_raw else None
        if protocol:
            kafka_auth_config["security_protocol"] = protocol

            if protocol in {"SSL", "SASL_SSL"}:
                ca_file = getattr(app_config, "KAFKA_SSL_CA_FILE", None)
                ssl_context = build_ssl_context(
                    ca_file,
                    verify=getattr(app_config, "KAFKA_SSL_VERIFY", True),
                )
                if ssl_context:
                    kafka_auth_config["ssl_context"] = ssl_context
                else:
                    raise RuntimeError(
                        f"Kafka security protocol is {protocol}, but failed to create SSL context. "
                        "Check KAFKA_SSL_CA_FILE config or disable verification via KAFKA_SSL_VERIFY=0."
                    )

            if protocol in {"SASL_SSL", "SASL_PLAINTEXT"}:
                missing = [
                    name
                    for name, value in (
                        ("KAFKA_SASL_MECHANISM", app_config.KAFKA_SASL_MECHANISM),
                        ("KAFKA_USERNAME", app_config.KAFKA_USERNAME),
                        ("KAFKA_PASSWORD", app_config.KAFKA_PASSWORD),
                    )
                    if not value
                ]
                if missing:
                    missing_vars = ", ".join(missing)
                    raise RuntimeError(
                        f"Kafka security protocol is set to {protocol} but the following environment "
                        f"variables are missing: {missing_vars}. Either provide the credentials or set "
                        "KAFKA_SECURITY_PROTOCOL=PLAINTEXT for the internal broker."
                    )

                kafka_auth_config["sasl_mechanism"] = app_config.KAFKA_SASL_MECHANISM
                kafka_auth_config["sasl_plain_username"] = app_config.KAFKA_USERNAME
                kafka_auth_config["sasl_plain_password"] = app_config.KAFKA_PASSWORD

        max_retries = max(getattr(app_config, "KAFKA_BOOTSTRAP_MAX_RETRIES", 0), 0)
        retry_delay = max(getattr(app_config, "KAFKA_BOOTSTRAP_RETRY_DELAY", 0.0), 0.0)

        attempt = 0
        while True:
            attempt += 1
            producer_instance = AIOKafkaProducer(
                bootstrap_servers=app_config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                request_timeout_ms=60000,
                retry_backoff_ms=1000,
                **kafka_auth_config,
            )

            try:
                logger.info(
                    "[Kafka Producer] Đang khởi tạo producer tới %s (lần thử %s/%s)...",
                    app_config.KAFKA_BOOTSTRAP_SERVERS,
                    attempt,
                    max_retries + 1,
                )
                await producer_instance.start()
                logger.info("[Kafka Producer] Producer đã sẵn sàng.")
                _producer = producer_instance  # type: ignore[assignment]
                return _producer
            except KafkaConnectionError as exc:
                logger.warning(
                    "[Kafka Producer] Lỗi kết nối lần %s: %s",
                    attempt,
                    exc,
                )
                with contextlib.suppress(Exception):
                    await producer_instance.stop()

                if attempt > max_retries:
                    logger.error(
                        "[Kafka Producer] Không thể kết nối tới Kafka sau nhiều lần thử. "
                        "Sử dụng dummy producer tạm thời.",
                    )
                    _producer = await _initialise_dummy_producer()
                    logger.info("[Kafka Producer] Dummy producer đã sẵn sàng.")
                    return _producer

                wait_time = retry_delay * (2 ** (attempt - 1)) if retry_delay else 0.0
                if wait_time:
                    logger.info(
                        "[Kafka Producer] Sẽ thử lại sau %.1f giây...",
                        wait_time,
                    )
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(0)
            except Exception:  # pragma: no cover - defensive guard
                with contextlib.suppress(Exception):
                    await producer_instance.stop()
                raise

    return _producer


async def send_kafka_job(job: dict):
    """Send a job payload to the configured Kafka topic."""

    if not job or not isinstance(job, dict):
        logger.error("[Kafka Producer] Invalid job format. Job must be a non-empty dictionary.")
        return False

    try:
        producer = await get_kafka_producer()
        if not producer:
            logger.error("[Kafka Producer] Producer is not available. Cannot send job.")
            return False

        logger.info(
            "[Kafka Producer] Sending job to topic '%s': %s",
            app_config.KAFKA_TOPIC,
            job,
        )
        await producer.send_and_wait(app_config.KAFKA_TOPIC, job)
        logger.info("[Kafka Producer] Job sent successfully: %s", job.get("type"))
        return True
    except Exception as exc:
        logger.exception("[Kafka Producer] Failed to send job: %s", exc)
        return False


async def stop_kafka_producer():
    """Stop the singleton Kafka producer instance if it is running."""

    global _producer
    async with _producer_lock:
        if _producer:
            logger.info("[Kafka Producer] Stopping producer...")
            await _producer.stop()
            _producer = None
            logger.info("[Kafka Producer] Producer stopped.")


if __name__ == "__main__":
    async def test_send_job():  # pragma: no cover - manual smoke helper
        test_job = {"type": "test_job", "data": "Hello Kafka from producer!"}
        success = await send_kafka_job(test_job)
        if success:
            print("Test job sent successfully.")
        else:
            print("Failed to send test job.")
        await stop_kafka_producer()

    asyncio.run(test_send_job())


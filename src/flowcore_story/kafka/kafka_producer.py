
import asyncio
import contextlib
import importlib.util
import json
import sys
from typing import Any

from flowcore_story.config import config as app_config
from flowcore_story.utils.kafka_ssl import build_ssl_context
from flowcore_story.utils.logger import logger

try:
    _AIOKAFKA_SPEC = importlib.util.find_spec("aiokafka")
except (ModuleNotFoundError, ValueError):  # pragma: no cover - optional dependency missing
    _AIOKAFKA_SPEC = None

if _AIOKAFKA_SPEC:
    from aiokafka import AIOKafkaProducer  # type: ignore
else:
    AIOKafkaProducer = None  # type: ignore

try:  # pragma: no cover - exercised via tests with custom stubs
    from aiokafka.errors import KafkaConnectionError  # type: ignore
except (ImportError, AttributeError):  # pragma: no cover - optional dependency missing
    class KafkaConnectionError(Exception):
        """Fallback error used when aiokafka is unavailable."""

_producer = None
_producer_lock = None


def _resolve_job_type(job_data: dict) -> str:
    """Return a descriptive job label for logging purposes."""

    candidate_keys = ("type", "job_type", "category", "event", "status", "action")
    for key in candidate_keys:
        value = job_data.get(key)
        if value:
            return str(value)
    return "unknown"


def _resolve_dispatch_job():
    """Locate the dispatcher fallback, honoring legacy module aliases."""

    module = sys.modules.get("kafka.kafka_dispatcher")
    if module is not None:
        dispatch = getattr(module, "dispatch_job", None)
        if callable(dispatch):
            return dispatch

    try:
        from flowcore_story.kafka.kafka_dispatcher import dispatch_job  # type: ignore
        return dispatch_job
    except Exception as primary_error:
        try:
            from kafka.kafka_dispatcher import dispatch_job  # type: ignore
            return dispatch_job
        except Exception:
            logger.error(
                "[Kafka Producer] Không thể fallback do không import được dispatcher: %s",
                primary_error,
            )
            raise


class _DummyProducer:
    def __init__(self) -> None:
        self._closed = False

    async def start(self) -> None:  # pragma: no cover - simple fallback
        self._closed = False

    async def stop(self) -> None:  # pragma: no cover
        self._closed = True

    async def send_and_wait(self, topic, payload) -> None:  # pragma: no cover
        logger.info("[Kafka Dummy] would send to %s: %s", topic, payload)

async def get_producer():
    """Initialise and return a singleton Kafka producer with retry handling."""

    global _producer, _producer_lock
    if _producer_lock is None:
        _producer_lock = asyncio.Lock()

    async with _producer_lock:
        if _producer is not None and not getattr(_producer, "_closed", False):
            return _producer

        if AIOKafkaProducer is None:
            logger.warning("aiokafka not installed; using in-memory dummy producer")
            _producer = _DummyProducer()
            await _producer.start()
            logger.info("[Kafka Producer] Dummy producer đã sẵn sàng.")
            return _producer

        max_retries = max(getattr(app_config, "KAFKA_BOOTSTRAP_MAX_RETRIES", 0), 0)
        retry_delay = max(getattr(app_config, "KAFKA_BOOTSTRAP_RETRY_DELAY", 0.0), 0.0)

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
                    raise ValueError(
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
                    raise ValueError(
                        f"Kafka security protocol is set to {protocol} but the following environment "
                        f"variables are missing: {missing_vars}. Either provide the credentials or set "
                        "KAFKA_SECURITY_PROTOCOL=PLAINTEXT for the internal broker."
                    )

                kafka_auth_config["sasl_mechanism"] = app_config.KAFKA_SASL_MECHANISM
                kafka_auth_config["sasl_plain_username"] = app_config.KAFKA_USERNAME
                kafka_auth_config["sasl_plain_password"] = app_config.KAFKA_PASSWORD

        attempt = 0
        while True:
            attempt += 1
            producer_instance = AIOKafkaProducer(
                bootstrap_servers=app_config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                request_timeout_ms=60000,
                retry_backoff_ms=1000,
                max_request_size=20 * 1024 * 1024,  # Increased to 20MB
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
                _producer = producer_instance
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
                        "Sử dụng dummy producer tạm thời."
                    )
                    _producer = _DummyProducer()
                    await _producer.start()
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
            except Exception:
                with contextlib.suppress(Exception):
                    await producer_instance.stop()
                raise

    return _producer

async def send_job(job_data: dict, topic: str = None):
    """
    Gửi một job (dictionary) tới topic Kafka được chỉ định.

    Args:
        job_data: Dữ liệu của job dưới dạng dictionary.
        topic: Tên topic để gửi. Nếu là None, sẽ dùng topic mặc định (có thể phân tầng).
    """
    site_key = job_data.get("site_key")
    
    if topic:
        target_topic = topic
    else:
        from flowcore_story.config.config import get_topic_for_site
        target_topic = get_topic_for_site(site_key, app_config.KAFKA_TOPIC)
        
    job_type = _resolve_job_type(job_data)
    
    # TASK 8: Enable sticky routing by using site_key as partition key
    # This ensures jobs for the same site go to the same consumer instance
    key_bytes = None
    if site_key:
        key_bytes = str(site_key).encode("utf-8")

    try:
        logger.debug("[Kafka Producer] Preparing to send job '%s' to topic '%s' (key=%s)", job_type, target_topic, site_key)
        producer = await get_producer()
        await producer.send_and_wait(target_topic, job_data, key=key_bytes)
        logger.debug(
            f"[Kafka Producer] ✅ Đã gửi job '{job_type}' tới topic '{target_topic}'."
        )
    except Exception as e:
        logger.exception(f"[Kafka Producer] ❌ Lỗi khi gửi job tới topic '{target_topic}': {e}")
        # Nếu có lỗi, thử đóng producer cũ để lần sau khỏi tạo lại
        global _producer
        if _producer:
            with contextlib.suppress(Exception):
                await _producer.stop()
            _producer = None

        # Với topic khác topic job chính (hoặc tiered topic), không có fallback hợp lệ
        # Check if it's the base topic or any of the tiered topics
        is_base_topic = target_topic == app_config.KAFKA_TOPIC
        is_tiered_topic = target_topic.startswith(f"{app_config.KAFKA_TOPIC}.")
        
        if not (is_base_topic or is_tiered_topic):
            raise

        dispatch_job = _resolve_dispatch_job()

        logger.warning(
            "[Kafka Producer] Kafka lỗi - đang chạy job '%s' trực tiếp qua dispatcher fallback.",
            job_type,
        )
        await dispatch_job(dict(job_data))

async def close_producer():
    """
    Đóng producer khi ứng dụng kết thúc.
    """
    global _producer
    if _producer:
        logger.info("[Kafka Producer] Đang đóng producer...")
        await _producer.stop()
        _producer = None
        logger.info("[Kafka Producer] Đã đóng producer.")


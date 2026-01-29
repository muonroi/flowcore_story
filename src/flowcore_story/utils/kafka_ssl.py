import logging
import ssl
from pathlib import Path

logger = logging.getLogger("StoryFlow.kafka")


def build_ssl_context(cafile: str | None, *, verify: bool = True) -> ssl.SSLContext | None:
    """Return an SSL context configured for Kafka clients.

    Always attempts to provide a usable SSL context, optionally augmented with
    a custom CA bundle when supplied. When ``verify`` is ``False`` the context
    will accept any server certificate (intended for local development only).
    """

    try:
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    except Exception as exc:
        logger.warning("[Kafka SSL] Khong the tao default SSL context: %s", exc)
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        except Exception as inner_exc:
            logger.error("[Kafka SSL] Khong the khoi tao SSL context: %s", inner_exc)
            return None
        try:
            context.load_default_certs()
        except Exception as load_exc:
            logger.warning(
                "[Kafka SSL] Khong the nap system trust store mac dinh: %s", load_exc
            )

    if verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED
    else:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        logger.warning("[Kafka SSL] Certificate verification disabled. Do not use in production.")

    if cafile:
        path = Path(cafile)
        if path.is_file():
            try:
                context.load_verify_locations(cafile=str(path))
            except Exception as exc:
                logger.warning(
                    "[Kafka SSL] Khong the nap CA %s: %s. Su dung trust store mac dinh.",
                    path,
                    exc,
                )
        else:
            logger.warning(
                "[Kafka SSL] CA file khong ton tai: %s. Su dung trust store mac dinh.",
                path,
            )

    return context

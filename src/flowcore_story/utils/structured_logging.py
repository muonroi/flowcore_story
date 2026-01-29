"""Helpers for emitting structured JSON logs for telemetry pipelines."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping, Sequence
from typing import Any

from flowcore_story.utils.logger import get_logger


def _normalise_value(value: Any) -> Any:
    """Convert ``value`` into a JSON-serialisable representation."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _normalise_value(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [_normalise_value(item) for item in value if item is not None]
    return str(value)


def _build_payload(category: str, **fields: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "category": category,
        "timestamp": round(time.time(), 3),
    }
    for key, value in fields.items():
        if value is None:
            continue
        payload[str(key)] = _normalise_value(value)
    return payload


def format_structured_event(category: str, **fields: Any) -> str:
    """Return a JSON string for a structured event payload."""

    payload = _build_payload(category, **fields)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def log_structured_event(
    level: int,
    *,
    category: str,
    logger: logging.Logger | None = None,
    **fields: Any,
) -> None:
    """Emit a structured log line to ``logger`` (defaults to the core logger)."""

    target = logger or get_logger("core")
    serialized = format_structured_event(category, **fields)
    target.log(level, "[event] %s", serialized)


def log_http_event(
    level: int,
    *,
    logger: logging.Logger,
    phase: str,
    site_key: str | None,
    url: str | None,
    status_code: int | None = None,
    proxy: str | None = None,
    cookie_id: str | None = None,
    attempt: int | None = None,
    retry_immediately: bool | None = None,
    is_success: bool | None = None,
    anti_bot: bool | None = None,
    cf_cache_status: str | None = None,
    cf_ray: str | None = None,
    cf_server: str | None = None,
    message: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> None:
    """Specialised helper for HTTP fetch telemetry logs."""

    fields: dict[str, Any] = {
        "phase": phase,
        "site_key": site_key,
        "url": url,
        "status_code": status_code,
        "proxy": proxy,
        "cookie_id": cookie_id,
        "attempt": attempt,
        "retry_immediately": retry_immediately,
        "success": is_success,
        "anti_bot": anti_bot,
        "cf_cache_status": cf_cache_status,
        "cf_ray": cf_ray,
        "cf_server": cf_server,
        "message": message,
    }
    if extra:
        fields.update(dict(extra))
    log_structured_event(level, category="http.fetch", logger=logger, **fields)


__all__ = [
    "format_structured_event",
    "log_http_event",
    "log_structured_event",
]

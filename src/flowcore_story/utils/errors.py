"""Utility helpers for normalising crawl error handling."""

from __future__ import annotations

import asyncio
from enum import Enum

try:  # pragma: no cover - optional dependency at import time
    from flowcore_story.utils.httpx_compat import httpx
except Exception:  # pragma: no cover - import guard for tests without httpx
    httpx = None  # type: ignore

try:  # pragma: no cover - optional dependency at import time
    from aiohttp import ClientConnectionError, ClientResponseError
except Exception:  # pragma: no cover - import guard for tests without aiohttp
    ClientConnectionError = None  # type: ignore
    ClientResponseError = None  # type: ignore


class CrawlError(str, Enum):
    """Categorised crawl error types used across the crawler."""

    TIMEOUT = "timeout"
    ANTI_BOT = "anti_bot"
    DEAD_LINK = "dead_link"
    NOT_FOUND = "not_found"
    RATE_LIMIT = "rate_limit"
    CONNECTION = "connection"
    TEMPORARY = "temporary"
    WRITE_FAIL = "write_fail"
    UNKNOWN = "unknown"


_RETRYABLE_ERRORS = {
    CrawlError.TIMEOUT,
    CrawlError.ANTI_BOT,
    CrawlError.RATE_LIMIT,
    CrawlError.CONNECTION,
    CrawlError.TEMPORARY,
}


def is_retryable_error(error: CrawlError) -> bool:
    """Return ``True`` if ``error`` is considered temporary and should be retried."""

    return error in _RETRYABLE_ERRORS


def classify_crawl_exception(exc: BaseException) -> CrawlError:
    """Best-effort mapping from arbitrary exceptions to :class:`CrawlError`."""

    if isinstance(exc, CrawlError):
        return exc

    status = _extract_status_code(exc)
    if status is not None:
        if status == 404:
            return CrawlError.NOT_FOUND
        if status in (401, 403):
            return CrawlError.ANTI_BOT
        if status == 429:
            return CrawlError.RATE_LIMIT
        if 500 <= status < 600:
            return CrawlError.TEMPORARY

    if _is_timeout_exception(exc):
        return CrawlError.TIMEOUT

    if _is_connection_exception(exc):
        return CrawlError.CONNECTION

    message = str(exc).lower()
    if any(keyword in message for keyword in ("anti-bot", "captcha", "cloudflare")):
        return CrawlError.ANTI_BOT
    if any(keyword in message for keyword in ("not found", "404")):
        return CrawlError.NOT_FOUND
    if any(keyword in message for keyword in ("rate limit", "too many requests")):
        return CrawlError.RATE_LIMIT
    if "timeout" in message or "timed out" in message:
        return CrawlError.TIMEOUT

    return CrawlError.UNKNOWN


def _extract_status_code(exc: BaseException) -> int | None:
    if httpx is not None:
        if isinstance(exc, httpx.HTTPStatusError):  # pragma: no branch - simple guard
            try:
                return exc.response.status_code
            except Exception:  # pragma: no cover - defensive guard
                return None
    if ClientResponseError is not None and isinstance(exc, ClientResponseError):
        return exc.status
    return None


def _is_timeout_exception(exc: BaseException) -> bool:
    if isinstance(exc, asyncio.TimeoutError):
        return True
    if httpx is not None and isinstance(exc, httpx.TimeoutException):
        return True
    return False


def _is_connection_exception(exc: BaseException) -> bool:
    if httpx is not None and isinstance(exc, httpx.RequestError):
        return True
    if ClientConnectionError is not None and isinstance(exc, ClientConnectionError):
        return True
    return False

"""Client for the dedicated challenge harvester service.

The challenge harvester is a lightweight microservice that keeps a long-lived
Playwright browser instance (potentially running in headful mode) so that the
main crawling workers do not have to bootstrap Playwright for every blocked
request. Workers send the target URL to the service and receive back the
cookies and optional Cloudflare Turnstile token required to access the
resource.

This module provides an asynchronous client used by :mod:`scraper` when a
request is suspected to be blocked by anti-bot mechanisms.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from typing import Any

try:  # pragma: no cover - optional dependency
    import aiohttp  # type: ignore[import]
except ModuleNotFoundError:  # pragma: no cover - optional dependency missing
    aiohttp = None  # type: ignore[assignment]

from flowcore_story.config import config as app_config
from flowcore_story.utils.logger import logger


@dataclass(slots=True)
class ChallengeClearance:
    """Represents a successful challenge clearance provided by the service."""

    body: str | None = None
    status: int | None = None
    cookies: Sequence[Mapping[str, Any]] | None = None
    cf_turnstile_token: str | None = None
    headers: Mapping[str, str] | None = None
    user_agent: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_body(self) -> bool:
        return bool(self.body)

    @property
    def has_cookies(self) -> bool:
        return bool(self.cookies)


class ChallengeHarvesterError(RuntimeError):
    """Raised when the challenge harvester service cannot fulfil a request."""


class ChallengeHarvesterClient:
    """Thin asynchronous HTTP client for the challenge harvester microservice."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.lock() if hasattr(asyncio, "lock") else asyncio.Lock()
        
        # Circuit breaker state
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._circuit_state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

        # Configuration
        self.FAILURE_THRESHOLD = 5      # Open after 5 failures
        self.SUCCESS_THRESHOLD = 2      # Close after 2 successes in HALF_OPEN
        self.CIRCUIT_TIMEOUT = 60.0     # Try again after 60s

    def _record_success(self):
        """Record successful request."""
        if self._circuit_state == "HALF_OPEN":
            self._success_count += 1
            if self._success_count >= self.SUCCESS_THRESHOLD:
                logger.info("[Harvester] Circuit breaker CLOSED (recovered)")
                self._circuit_state = "CLOSED"
                self._failure_count = 0
                self._success_count = 0
        else:
            # Reset failure count on success
            self._failure_count = 0
        
        self._report_metrics()

    def _record_failure(self):
        """Record failed request."""
        self._failure_count += 1
        self._last_failure_time = time.time()
        self._success_count = 0  # Reset success count

        if self._failure_count >= self.FAILURE_THRESHOLD:
            if self._circuit_state != "OPEN":
                logger.warning(
                    f"[Harvester] Circuit breaker OPEN ({self._failure_count} consecutive failures)"
                )
                self._circuit_state = "OPEN"
        
        self._report_metrics()

    def _report_metrics(self):
        """Report current state to metrics tracker."""
        try:
            from flowcore_story.utils.metrics_tracker import metrics_tracker
            metrics_tracker.update_system_metrics(
                harvester_circuit_state=self._circuit_state,
                harvester_failures=self._failure_count,
            )
        except Exception:
            pass

    def _can_attempt(self) -> bool:
        """Check if request is allowed."""
        if self._circuit_state == "CLOSED":
            return True

        if self._circuit_state == "OPEN":
            # Check if timeout has passed
            elapsed = time.time() - self._last_failure_time
            if elapsed > self.CIRCUIT_TIMEOUT:
                logger.info("[Harvester] Circuit breaker entering HALF_OPEN (testing recovery)")
                self._circuit_state = "HALF_OPEN"
                return True
            return False

        # HALF_OPEN: Allow limited requests to test
        return True

    @property
    def endpoint(self) -> str | None:
        url = getattr(app_config, "CHALLENGE_HARVESTER_URL", None)
        if url:
            url = url.strip()
        return url or "http://harvester-lb:9090/harvest"

    @property
    def timeout(self) -> float:
        # FIX 2025-12-10: Increased default from 45s to 150s
        # Must be > service navigation timeout (120s) to avoid premature client timeout
        try:
            timeout = float(getattr(app_config, "CHALLENGE_HARVESTER_TIMEOUT", 150.0))
        except (TypeError, ValueError):
            timeout = 150.0
        return max(timeout, 30.0)  # Minimum 30s instead of 5s

    @property
    def is_enabled(self) -> bool:
        if aiohttp is None:
            return False
        enabled_flag = bool(getattr(app_config, "CHALLENGE_HARVESTER_ENABLED", False))
        return bool(enabled_flag and self.endpoint)

    async def close(self) -> None:
        async with self._session_lock:
            if aiohttp is None:
                self._session = None
                return
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if aiohttp is None:
            raise ChallengeHarvesterError("aiohttp is required for the challenge harvester client")

        if self._session and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    async def request_clearance(
        self,
        url: str,
        site_key: str,
        *,
        headers: MutableMapping[str, str] | None = None,
        wait_for_selector: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        referer: str | None = None,
        proxy: str | None = None,
        impersonate: str | None = None,
    ) -> ChallengeClearance | None:
        """Send a clearance request to the harvester service."""

        if not self.is_enabled:
            return None

        # Circuit breaker check
        if not self._can_attempt():
            logger.warning(f"[Harvester] Circuit breaker OPEN, rejecting request to {url}")
            raise ChallengeHarvesterError("Circuit breaker open - harvester unavailable")

        endpoint = self.endpoint
        if not endpoint:
            return None

        payload: dict[str, Any] = {
            "url": url,
            "site_key": site_key,
        }
        if headers:
            payload["headers"] = dict(headers)
        if wait_for_selector:
            payload["wait_for_selector"] = wait_for_selector
        if extra_headers:
            payload["extra_headers"] = dict(extra_headers)
        if referer:
            payload["referer"] = referer
        if proxy:
            payload["proxy"] = proxy
        if impersonate:
            payload["impersonate"] = impersonate

        session = await self._get_session()
        max_retries = 2
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                async with session.post(endpoint, json=payload) as resp:
                    status = resp.status
                    data: dict[str, Any]
                    try:
                        data = await resp.json()
                    except Exception as exc:  # pragma: no cover - defensive guard
                        text = await resp.text()
                        logger.warning(
                            "[challenge-harvester] Invalid JSON response (%s): %s", status, text
                        )
                        self._record_failure()
                        raise ChallengeHarvesterError("Invalid response from challenge harvester") from exc

                    if status >= 500:
                        message = data.get("error") if isinstance(data, dict) else None
                        self._record_failure()
                        raise ChallengeHarvesterError(
                            message or f"Challenge harvester responded with status {status}"
                        )

                    if status >= 400:
                        message = data.get("error") if isinstance(data, dict) else None
                        logger.warning(
                            "[challenge-harvester] Request rejected (%s): %s", status, message
                        )
                        # Client error (4xx) usually doesn't mean the service is down
                        return None
                    
                    # Success
                    if not isinstance(data, dict):
                        logger.warning("[challenge-harvester] Unexpected payload: %r", data)
                        self._record_failure()
                        return None

                    if data.get("error"):
                        logger.warning(
                            "[challenge-harvester] Service reported error for %s: %s", url, data["error"]
                        )
                        self._record_failure()
                        return None

                    self._record_success()
                    clearance = ChallengeClearance()
                    clearance.body = data.get("body")
                    clearance.status = data.get("status")
                    raw_cookies = data.get("cookies")
                    if isinstance(raw_cookies, Sequence):
                        clearance.cookies = [cookie for cookie in raw_cookies if isinstance(cookie, Mapping)]
                    clearance.cf_turnstile_token = data.get("cf_turnstile_token")
                    raw_headers = data.get("headers")
                    if isinstance(raw_headers, Mapping):
                        clearance.headers = dict(raw_headers)
                    user_agent = data.get("user_agent")
                    if isinstance(user_agent, str):
                        clearance.user_agent = user_agent
                    metadata = data.get("metadata")
                    if isinstance(metadata, Mapping):
                        clearance.metadata.update(metadata)
                    return clearance

            except (TimeoutError, aiohttp.ClientError) as exc:
                last_exception = exc
                self._record_failure()
                if attempt < max_retries:
                    delay = 2 ** attempt
                    logger.warning(
                        "[challenge-harvester] Request failed for %s (attempt %d/%d), retrying in %ds: %s", 
                        url, attempt + 1, max_retries + 1, delay, exc
                    )
                    await asyncio.sleep(delay)
                    continue
                
                logger.warning("[challenge-harvester] Request failed for %s after %d retries: %s", url, max_retries, exc)
                raise ChallengeHarvesterError("Challenge harvester request failed") from exc

        # Should be unreachable if logic is correct
        return None


_client: ChallengeHarvesterClient | None = None


async def close_challenge_harvester_client() -> None:
    global _client
    if _client is None:
        return
    await _client.close()
    _client = None


def get_challenge_harvester_client() -> ChallengeHarvesterClient:
    """Return a singleton instance of :class:`ChallengeHarvesterClient`."""

    global _client
    if _client is None:
        _client = ChallengeHarvesterClient()
    return _client


__all__ = [
    "ChallengeClearance",
    "ChallengeHarvesterClient",
    "ChallengeHarvesterError",
    "get_challenge_harvester_client",
    "close_challenge_harvester_client",
]

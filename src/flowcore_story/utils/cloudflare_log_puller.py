"""Cloudflare Logpull API Integration.

This module integrates with Cloudflare's Logpull API to:
- Pull HTTP request logs
- Analyze fingerprint detection patterns
- Identify blocked requests (429, 403)
- Extract cf-ray patterns
- Detect bot scores and challenges

Supports both Enterprise API and Logs via API.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import httpx

from flowcore_story.utils.logger import logger


@dataclass
class CloudflareLogEntry:
    """Single Cloudflare log entry."""

    timestamp: float
    ray_id: str
    client_ip: str
    user_agent: str
    status_code: int
    url: str

    # Cloudflare specific fields
    cf_bot_score: int | None = None
    cf_threat_score: int | None = None
    cf_challenge_type: str | None = None
    cf_ja3_hash: str | None = None

    # Request details
    tls_version: str | None = None
    http_version: str | None = None
    request_headers: dict[str, str] = field(default_factory=dict)

    # Response details
    cache_status: str | None = None
    origin_status: int | None = None

    # Geographic info
    country: str | None = None
    datacenter: str | None = None


@dataclass
class CloudflareLogStats:
    """Statistics from Cloudflare logs analysis."""

    total_requests: int = 0
    success_requests: int = 0
    rate_limited_requests: int = 0  # 429
    blocked_requests: int = 0  # 403
    challenge_requests: int = 0

    # Bot detection
    bot_detected_count: int = 0
    high_threat_score_count: int = 0

    # By fingerprint
    fingerprints_flagged: set[str] = field(default_factory=set)
    ja3_hashes_flagged: set[str] = field(default_factory=set)

    # By IP/Proxy
    ips_flagged: set[str] = field(default_factory=set)

    # Patterns
    cf_ray_patterns: dict[str, int] = field(default_factory=dict)
    user_agent_patterns: dict[str, int] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        return self.success_requests / self.total_requests if self.total_requests > 0 else 0.0

    @property
    def block_rate(self) -> float:
        """Calculate block rate."""
        return (self.blocked_requests + self.rate_limited_requests) / self.total_requests if self.total_requests > 0 else 0.0


class CloudflareLogPuller:
    """Pulls and analyzes Cloudflare logs.

    Supports:
    - Cloudflare Logpull API (Enterprise)
    - Cloudflare GraphQL API (Logs via API)
    """

    def __init__(
        self,
        zone_id: str,
        api_token: str,
        api_type: str = "logpull",  # or "graphql"
        base_url: str = "https://api.cloudflare.com/client/v4",
    ):
        """Initialize Cloudflare log puller.

        Args:
            zone_id: Cloudflare zone ID
            api_token: API token with Logs Read permission
            api_type: API type (logpull or graphql)
            base_url: Cloudflare API base URL
        """
        self.zone_id = zone_id
        self.api_token = api_token
        self.api_type = api_type
        self.base_url = base_url
        self.logger = logger.bind(component="CloudflareLogPuller")

        self.client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
            },
            timeout=60.0,
        )

    async def pull_logs(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        fields: list[str] | None = None,
        sample_rate: float = 1.0,
    ) -> list[CloudflareLogEntry]:
        """Pull logs from Cloudflare.

        Args:
            start_time: Start time (default: 1 hour ago)
            end_time: End time (default: now)
            fields: Fields to fetch (None = default set)
            sample_rate: Sampling rate (0.01 to 1.0)

        Returns:
            List of CloudflareLogEntry
        """
        if start_time is None:
            start_time = datetime.utcnow() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.utcnow()

        if self.api_type == "logpull":
            return await self._pull_logs_logpull(start_time, end_time, fields, sample_rate)
        elif self.api_type == "graphql":
            return await self._pull_logs_graphql(start_time, end_time, fields, sample_rate)
        else:
            raise ValueError(f"Unsupported API type: {self.api_type}")

    async def _pull_logs_logpull(
        self,
        start_time: datetime,
        end_time: datetime,
        fields: list[str] | None,
        sample_rate: float,
    ) -> list[CloudflareLogEntry]:
        """Pull logs using Logpull API (Enterprise).

        https://developers.cloudflare.com/logs/logpull/
        """
        if fields is None:
            fields = [
                "ClientRequestHost",
                "ClientRequestMethod",
                "ClientRequestURI",
                "EdgeStartTimestamp",
                "EdgeEndTimestamp",
                "EdgeResponseStatus",
                "ClientIP",
                "ClientRequestUserAgent",
                "RayID",
                "ClientRequestPath",
                "ClientRequestProtocol",
                "ClientSSLProtocol",
                "EdgeRequestHost",
                "OriginResponseStatus",
                "CacheResponseStatus",
                "ClientCountry",
                "EdgeColoCode",
                # Bot Management fields (requires Cloudflare Bot Management)
                "BotScore",
                "BotScoreClass",
                "BotScoreSrc",
                # Security fields
                "ClientDeviceType",
                "ClientIPClass",
                "ClientRequestBytes",
                "ClientRequestReferer",
            ]

        # Convert to RFC3339 format
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        url = f"{self.base_url}/zones/{self.zone_id}/logs/received"
        params = {
            "start": start_str,
            "end": end_str,
            "fields": ",".join(fields),
            "sample": sample_rate,
        }

        self.logger.info(f"Pulling logs from {start_str} to {end_str}")

        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()

            # Parse NDJSON (newline-delimited JSON)
            logs = []
            for line in response.text.strip().split("\n"):
                if not line:
                    continue

                import json
                log_data = json.loads(line)
                entry = self._parse_log_entry(log_data)
                if entry:
                    logs.append(entry)

            self.logger.info(f"Pulled {len(logs)} log entries")
            return logs

        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP error pulling logs: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            self.logger.error(f"Error pulling logs: {e}", exc_info=True)
            return []

    async def _pull_logs_graphql(
        self,
        start_time: datetime,
        end_time: datetime,
        fields: list[str] | None,
        sample_rate: float,
    ) -> list[CloudflareLogEntry]:
        """Pull logs using GraphQL API.

        https://developers.cloudflare.com/logs/graphql-api/
        """
        self.logger.warning("GraphQL API not yet implemented, using placeholder")
        return []

    def _parse_log_entry(self, log_data: dict[str, Any]) -> CloudflareLogEntry | None:
        """Parse a single log entry.

        Args:
            log_data: Raw log data from Cloudflare

        Returns:
            CloudflareLogEntry or None if parsing fails
        """
        try:
            # Extract timestamp
            timestamp = log_data.get("EdgeStartTimestamp", 0)
            if isinstance(timestamp, str):
                # Parse RFC3339
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                timestamp = dt.timestamp()

            return CloudflareLogEntry(
                timestamp=timestamp / 1000000000 if timestamp > 1e15 else timestamp,  # Convert nanoseconds if needed
                ray_id=log_data.get("RayID", ""),
                client_ip=log_data.get("ClientIP", ""),
                user_agent=log_data.get("ClientRequestUserAgent", ""),
                status_code=log_data.get("EdgeResponseStatus", 0),
                url=log_data.get("ClientRequestURI", ""),
                cf_bot_score=log_data.get("BotScore"),
                cf_threat_score=log_data.get("ThreatScore"),
                cf_challenge_type=log_data.get("ChallengeType"),
                cf_ja3_hash=log_data.get("JA3Hash"),
                tls_version=log_data.get("ClientSSLProtocol"),
                http_version=log_data.get("ClientRequestProtocol"),
                cache_status=log_data.get("CacheResponseStatus"),
                origin_status=log_data.get("OriginResponseStatus"),
                country=log_data.get("ClientCountry"),
                datacenter=log_data.get("EdgeColoCode"),
            )

        except Exception as e:
            self.logger.debug(f"Failed to parse log entry: {e}")
            return None

    def analyze_logs(self, logs: list[CloudflareLogEntry]) -> CloudflareLogStats:
        """Analyze logs and generate statistics.

        Args:
            logs: List of log entries

        Returns:
            CloudflareLogStats
        """
        stats = CloudflareLogStats()

        for log in logs:
            stats.total_requests += 1

            # Classify by status code
            if log.status_code in [200, 201, 204, 304]:
                stats.success_requests += 1
            elif log.status_code == 429:
                stats.rate_limited_requests += 1
            elif log.status_code in [403, 451]:
                stats.blocked_requests += 1

            # Bot detection
            if log.cf_bot_score is not None and log.cf_bot_score < 30:
                stats.bot_detected_count += 1

            if log.cf_threat_score is not None and log.cf_threat_score > 50:
                stats.high_threat_score_count += 1

            # Challenge detection
            if log.cf_challenge_type:
                stats.challenge_requests += 1

            # Track flagged fingerprints (blocked or rate-limited)
            if log.status_code in [403, 429]:
                if log.user_agent:
                    ua_key = log.user_agent[:50]
                    stats.fingerprints_flagged.add(ua_key)

                if log.cf_ja3_hash:
                    stats.ja3_hashes_flagged.add(log.cf_ja3_hash)

                if log.client_ip:
                    stats.ips_flagged.add(log.client_ip)

            # Track cf-ray patterns (first 3 chars = datacenter code)
            if log.ray_id and len(log.ray_id) >= 3:
                datacenter_code = log.ray_id[:3]
                stats.cf_ray_patterns[datacenter_code] = stats.cf_ray_patterns.get(datacenter_code, 0) + 1

            # Track user-agent patterns
            if log.user_agent:
                ua_key = log.user_agent[:50]
                stats.user_agent_patterns[ua_key] = stats.user_agent_patterns.get(ua_key, 0) + 1

        return stats

    async def get_flagged_fingerprints(
        self,
        lookback_hours: int = 24,
        threshold: float = 0.5,  # 50% block rate to be flagged
    ) -> dict[str, Any]:
        """Get fingerprints that are being flagged by Cloudflare.

        Args:
            lookback_hours: Hours to look back
            threshold: Block rate threshold to flag

        Returns:
            Dict with flagged fingerprints and reasons
        """
        start_time = datetime.utcnow() - timedelta(hours=lookback_hours)
        end_time = datetime.utcnow()

        logs = await self.pull_logs(start_time, end_time)

        # Group by fingerprint (user-agent + ja3)
        fingerprint_stats: dict[str, dict[str, Any]] = {}

        for log in logs:
            fp_key = f"{log.user_agent[:50]}|{log.cf_ja3_hash or 'unknown'}"

            if fp_key not in fingerprint_stats:
                fingerprint_stats[fp_key] = {
                    'total': 0,
                    'blocked': 0,
                    'user_agent': log.user_agent[:50],
                    'ja3_hash': log.cf_ja3_hash,
                    'ips': set(),
                    'ray_ids': [],
                }

            fingerprint_stats[fp_key]['total'] += 1
            fingerprint_stats[fp_key]['ips'].add(log.client_ip)

            if log.status_code in [403, 429]:
                fingerprint_stats[fp_key]['blocked'] += 1
                fingerprint_stats[fp_key]['ray_ids'].append(log.ray_id)

        # Identify flagged fingerprints
        flagged = {}
        for fp_key, stats in fingerprint_stats.items():
            if stats['total'] < 5:  # Need minimum sample size
                continue

            block_rate = stats['blocked'] / stats['total']
            if block_rate >= threshold:
                flagged[fp_key] = {
                    'block_rate': block_rate,
                    'total_requests': stats['total'],
                    'blocked_requests': stats['blocked'],
                    'user_agent': stats['user_agent'],
                    'ja3_hash': stats['ja3_hash'],
                    'affected_ips': list(stats['ips']),
                    'sample_ray_ids': stats['ray_ids'][:5],
                }

        return flagged

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


# Global instance
_cloudflare_puller: CloudflareLogPuller | None = None


def get_cloudflare_puller(
    zone_id: str | None = None,
    api_token: str | None = None,
) -> CloudflareLogPuller | None:
    """Get global Cloudflare log puller instance.

    Args:
        zone_id: Cloudflare zone ID (from env if not provided)
        api_token: API token (from env if not provided)

    Returns:
        CloudflareLogPuller or None if not configured
    """
    global _cloudflare_puller

    if _cloudflare_puller is None:
        import os

        zone_id = zone_id or os.environ.get("CLOUDFLARE_ZONE_ID")
        api_token = api_token or os.environ.get("CLOUDFLARE_API_TOKEN")

        if not zone_id or not api_token:
            logger.warning("Cloudflare log puller not configured (missing CLOUDFLARE_ZONE_ID or CLOUDFLARE_API_TOKEN)")
            return None

        _cloudflare_puller = CloudflareLogPuller(
            zone_id=zone_id,
            api_token=api_token,
        )

    return _cloudflare_puller


if __name__ == "__main__":
    # Test Cloudflare log puller
    async def main():
        import os

        zone_id = os.environ.get("CLOUDFLARE_ZONE_ID")
        api_token = os.environ.get("CLOUDFLARE_API_TOKEN")

        if not zone_id or not api_token:
            print("Set CLOUDFLARE_ZONE_ID and CLOUDFLARE_API_TOKEN environment variables")
            return

        puller = CloudflareLogPuller(zone_id=zone_id, api_token=api_token)

        # Pull logs from last hour
        logs = await puller.pull_logs()
        print(f"Pulled {len(logs)} logs")

        # Analyze
        stats = puller.analyze_logs(logs)
        print("\nStatistics:")
        print(f"  Total: {stats.total_requests}")
        print(f"  Success: {stats.success_requests} ({stats.success_rate:.1%})")
        print(f"  Blocked: {stats.blocked_requests}")
        print(f"  Rate Limited: {stats.rate_limited_requests}")
        print(f"  Block Rate: {stats.block_rate:.1%}")
        print(f"  Flagged fingerprints: {len(stats.fingerprints_flagged)}")
        print(f"  Flagged IPs: {len(stats.ips_flagged)}")

        # Get flagged fingerprints
        flagged = await puller.get_flagged_fingerprints(lookback_hours=24)
        print(f"\nFlagged fingerprints: {len(flagged)}")
        for _, data in list(flagged.items())[:3]:
            print(f"  {data['user_agent']}: {data['block_rate']:.1%} block rate")

        await puller.close()

    asyncio.run(main())


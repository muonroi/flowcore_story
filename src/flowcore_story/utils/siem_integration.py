"""SIEM Integration - Send logs to Splunk, ELK, OpenTelemetry.

This module provides integration with popular SIEM systems:
- Splunk HTTP Event Collector (HEC)
- Elasticsearch (ELK Stack)
- OpenTelemetry (OTel)

Sends structured logs for monitoring, alerting, and compliance.
"""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import httpx

from flowcore_story.utils.logger import logger


class SIEMType(Enum):
    """SIEM system types."""
    SPLUNK = "splunk"
    ELASTICSEARCH = "elasticsearch"
    OPENTELEMETRY = "opentelemetry"


@dataclass
class SIEMEvent:
    """Structured event for SIEM systems."""

    timestamp: float
    source: str  # e.g., "storyflow-crawler"
    event_type: str  # e.g., "request", "block", "quarantine"
    severity: str  # info, warning, error, critical

    # Core fields
    message: str
    tags: list[str]

    # Context
    site_key: str | None = None
    profile_id: str | None = None
    proxy_url: str | None = None

    # Request details
    url: str | None = None
    status_code: int | None = None
    user_agent: str | None = None

    # Cloudflare specifics
    cf_ray_id: str | None = None
    cf_bot_score: int | None = None
    cf_challenge_type: str | None = None

    # Fingerprint
    fingerprint_id: str | None = None
    ja3_hash: str | None = None
    tls_profile: str | None = None

    # Additional data
    extra: dict[str, Any] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict."""
        data = asdict(self)
        # Remove None values
        return {k: v for k, v in data.items() if v is not None}


class SIEMIntegration(ABC):
    """Base class for SIEM integrations."""

    @abstractmethod
    async def send_event(self, event: SIEMEvent) -> bool:
        """Send a single event."""
        pass

    @abstractmethod
    async def send_events_batch(self, events: list[SIEMEvent]) -> bool:
        """Send a batch of events."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close connections."""
        pass


class SplunkIntegration(SIEMIntegration):
    """Splunk HTTP Event Collector (HEC) integration.

    https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector
    """

    def __init__(
        self,
        hec_url: str,
        hec_token: str,
        index: str = "main",
        source: str = "storyflow",
        sourcetype: str = "json",
        verify_ssl: bool = True,
    ):
        """Initialize Splunk integration.

        Args:
            hec_url: HEC endpoint URL (e.g., https://splunk.example.com:8088/services/collector)
            hec_token: HEC token
            index: Splunk index name
            source: Event source
            sourcetype: Source type
            verify_ssl: Verify SSL certificates
        """
        self.hec_url = hec_url.rstrip("/")
        self.hec_token = hec_token
        self.index = index
        self.source = source
        self.sourcetype = sourcetype
        self.logger = logger.bind(siem="splunk")

        self.client = httpx.AsyncClient(
            headers={
                "Authorization": f"Splunk {hec_token}",
                "Content-Type": "application/json",
            },
            verify=verify_ssl,
            timeout=30.0,
        )

    async def send_event(self, event: SIEMEvent) -> bool:
        """Send a single event to Splunk.

        Args:
            event: SIEMEvent

        Returns:
            True if successful
        """
        payload = {
            "time": event.timestamp,
            "source": self.source,
            "sourcetype": self.sourcetype,
            "index": self.index,
            "event": event.to_dict(),
        }

        try:
            response = await self.client.post(f"{self.hec_url}/event", json=payload)
            response.raise_for_status()
            return True
        except Exception as e:
            self.logger.error(f"Failed to send event to Splunk: {e}")
            return False

    async def send_events_batch(self, events: list[SIEMEvent]) -> bool:
        """Send batch of events to Splunk.

        Args:
            events: List of SIEMEvent

        Returns:
            True if successful
        """
        # Splunk HEC batch format: NDJSON (newline-delimited JSON)
        payloads = []
        for event in events:
            payloads.append({
                "time": event.timestamp,
                "source": self.source,
                "sourcetype": self.sourcetype,
                "index": self.index,
                "event": event.to_dict(),
            })

        # Convert to NDJSON
        ndjson = "\n".join(json.dumps(p) for p in payloads)

        try:
            response = await self.client.post(
                f"{self.hec_url}/event",
                content=ndjson,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            self.logger.info(f"Sent {len(events)} events to Splunk")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send batch to Splunk: {e}")
            return False

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


class ElasticsearchIntegration(SIEMIntegration):
    """Elasticsearch (ELK Stack) integration."""

    def __init__(
        self,
        es_url: str,
        index_pattern: str = "storyflow-logs",
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        verify_ssl: bool = True,
    ):
        """Initialize Elasticsearch integration.

        Args:
            es_url: Elasticsearch URL (e.g., https://es.example.com:9200)
            index_pattern: Index pattern (can include date, e.g., "logs-{date}")
            api_key: API key for authentication
            username: Username for basic auth
            password: Password for basic auth
            verify_ssl: Verify SSL certificates
        """
        self.es_url = es_url.rstrip("/")
        self.index_pattern = index_pattern
        self.logger = logger.bind(siem="elasticsearch")

        headers = {"Content-Type": "application/json"}
        auth = None

        if api_key:
            headers["Authorization"] = f"ApiKey {api_key}"
        elif username and password:
            auth = (username, password)

        self.client = httpx.AsyncClient(
            headers=headers,
            auth=auth,
            verify=verify_ssl,
            timeout=30.0,
        )

    def _get_index_name(self, timestamp: float) -> str:
        """Get index name with date substitution.

        Args:
            timestamp: Event timestamp

        Returns:
            Index name
        """
        from datetime import datetime
        dt = datetime.fromtimestamp(timestamp)
        return self.index_pattern.replace("{date}", dt.strftime("%Y.%m.%d"))

    async def send_event(self, event: SIEMEvent) -> bool:
        """Send a single event to Elasticsearch.

        Args:
            event: SIEMEvent

        Returns:
            True if successful
        """
        index_name = self._get_index_name(event.timestamp)
        url = f"{self.es_url}/{index_name}/_doc"

        doc = event.to_dict()
        doc["@timestamp"] = event.timestamp * 1000  # Elasticsearch expects milliseconds

        try:
            response = await self.client.post(url, json=doc)
            response.raise_for_status()
            return True
        except Exception as e:
            self.logger.error(f"Failed to send event to Elasticsearch: {e}")
            return False

    async def send_events_batch(self, events: list[SIEMEvent]) -> bool:
        """Send batch of events to Elasticsearch using Bulk API.

        Args:
            events: List of SIEMEvent

        Returns:
            True if successful
        """
        # Elasticsearch bulk format
        bulk_data = []
        for event in events:
            index_name = self._get_index_name(event.timestamp)

            # Index action
            bulk_data.append(json.dumps({"index": {"_index": index_name}}))

            # Document
            doc = event.to_dict()
            doc["@timestamp"] = event.timestamp * 1000
            bulk_data.append(json.dumps(doc))

        bulk_body = "\n".join(bulk_data) + "\n"

        try:
            response = await self.client.post(
                f"{self.es_url}/_bulk",
                content=bulk_body,
                headers={"Content-Type": "application/x-ndjson"},
            )
            response.raise_for_status()

            result = response.json()
            if result.get("errors"):
                self.logger.warning("Some events failed in bulk request")
            else:
                self.logger.info(f"Sent {len(events)} events to Elasticsearch")

            return not result.get("errors", False)
        except Exception as e:
            self.logger.error(f"Failed to send batch to Elasticsearch: {e}")
            return False

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


class OpenTelemetryIntegration(SIEMIntegration):
    """OpenTelemetry (OTel) integration.

    Sends logs via OTLP (OpenTelemetry Protocol).
    """

    def __init__(
        self,
        otlp_endpoint: str,
        service_name: str = "storyflow-crawler",
        headers: dict[str, str] | None = None,
        verify_ssl: bool = True,
    ):
        """Initialize OpenTelemetry integration.

        Args:
            otlp_endpoint: OTLP endpoint (e.g., https://otel-collector:4318)
            service_name: Service name
            headers: Additional headers (e.g., for authentication)
            verify_ssl: Verify SSL certificates
        """
        self.otlp_endpoint = otlp_endpoint.rstrip("/")
        self.service_name = service_name
        self.logger = logger.bind(siem="opentelemetry")

        client_headers = {"Content-Type": "application/json"}
        if headers:
            client_headers.update(headers)

        self.client = httpx.AsyncClient(
            headers=client_headers,
            verify=verify_ssl,
            timeout=30.0,
        )

    async def send_event(self, event: SIEMEvent) -> bool:
        """Send a single log event via OTLP.

        Args:
            event: SIEMEvent

        Returns:
            True if successful
        """
        return await self.send_events_batch([event])

    async def send_events_batch(self, events: list[SIEMEvent]) -> bool:
        """Send batch of logs via OTLP.

        Args:
            events: List of SIEMEvent

        Returns:
            True if successful
        """
        # OTLP JSON format for logs
        # https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto

        log_records = []
        for event in events:
            record = {
                "timeUnixNano": str(int(event.timestamp * 1_000_000_000)),
                "severityText": event.severity.upper(),
                "body": {"stringValue": event.message},
                "attributes": [],
            }

            # Add attributes
            event_dict = event.to_dict()
            for key, value in event_dict.items():
                if key in ["timestamp", "message", "severity"]:
                    continue

                if isinstance(value, (str, int, float, bool)):
                    record["attributes"].append({
                        "key": key,
                        "value": {self._get_otlp_value_type(value): value}
                    })

            log_records.append(record)

        payload = {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": self.service_name}}
                        ]
                    },
                    "scopeLogs": [
                        {
                            "logRecords": log_records
                        }
                    ]
                }
            ]
        }

        try:
            response = await self.client.post(
                f"{self.otlp_endpoint}/v1/logs",
                json=payload,
            )
            response.raise_for_status()
            self.logger.info(f"Sent {len(events)} events to OpenTelemetry")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send batch to OpenTelemetry: {e}")
            return False

    def _get_otlp_value_type(self, value: Any) -> str:
        """Get OTLP value type name.

        Args:
            value: Value

        Returns:
            Type name (stringValue, intValue, doubleValue, boolValue)
        """
        if isinstance(value, str):
            return "stringValue"
        elif isinstance(value, int):
            return "intValue"
        elif isinstance(value, float):
            return "doubleValue"
        elif isinstance(value, bool):
            return "boolValue"
        else:
            return "stringValue"

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


class SIEMManager:
    """Manages multiple SIEM integrations."""

    def __init__(self):
        """Initialize SIEM manager."""
        self.integrations: dict[str, SIEMIntegration] = {}
        self.logger = logger.bind(component="SIEMManager")
        self._buffer: list[SIEMEvent] = []
        self._buffer_size = 100
        self._flush_task: asyncio.Task | None = None

    def add_integration(self, name: str, integration: SIEMIntegration) -> None:
        """Add a SIEM integration.

        Args:
            name: Integration name
            integration: SIEMIntegration instance
        """
        self.integrations[name] = integration
        self.logger.info(f"Added SIEM integration: {name}")

    async def send_event(self, event: SIEMEvent) -> None:
        """Send event to all integrations.

        Args:
            event: SIEMEvent
        """
        tasks = [
            integration.send_event(event)
            for integration in self.integrations.values()
        ]

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for integration_name, result in zip(self.integrations.keys(), results, strict=False):
                if isinstance(result, Exception):
                    self.logger.error(f"Error sending to {integration_name}: {result}")

    async def send_events_batch(self, events: list[SIEMEvent]) -> None:
        """Send batch of events to all integrations.

        Args:
            events: List of SIEMEvent
        """
        tasks = [
            integration.send_events_batch(events)
            for integration in self.integrations.values()
        ]

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for integration_name, result in zip(self.integrations.keys(), results, strict=False):
                if isinstance(result, Exception):
                    self.logger.error(f"Error sending batch to {integration_name}: {result}")

    async def queue_event(self, event: SIEMEvent) -> None:
        """Queue event for batched sending.

        Args:
            event: SIEMEvent
        """
        self._buffer.append(event)

        if len(self._buffer) >= self._buffer_size:
            await self.flush()

    async def flush(self) -> None:
        """Flush buffered events."""
        if not self._buffer:
            return

        events = self._buffer[:]
        self._buffer.clear()

        await self.send_events_batch(events)

    def start_auto_flush(self, interval: float = 10.0) -> None:
        """Start automatic flushing of buffer.

        Args:
            interval: Flush interval in seconds
        """
        if self._flush_task and not self._flush_task.done():
            return

        async def auto_flush():
            while True:
                await asyncio.sleep(interval)
                await self.flush()

        self._flush_task = asyncio.create_task(auto_flush())
        self.logger.info(f"Started auto-flush with {interval}s interval")

    def stop_auto_flush(self) -> None:
        """Stop automatic flushing."""
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            self.logger.info("Stopped auto-flush")

    async def close(self) -> None:
        """Close all integrations."""
        # Flush remaining events
        await self.flush()

        # Stop auto-flush
        self.stop_auto_flush()

        # Close all integrations
        for name, integration in self.integrations.items():
            await integration.close()
            self.logger.info(f"Closed SIEM integration: {name}")


# Global instance
_siem_manager: SIEMManager | None = None


def get_siem_manager() -> SIEMManager:
    """Get global SIEM manager instance.

    Returns:
        SIEMManager
    """
    global _siem_manager

    if _siem_manager is None:
        _siem_manager = SIEMManager()

        # Auto-configure from environment
        import os

        # Splunk
        splunk_hec_url = os.environ.get("SPLUNK_HEC_URL")
        splunk_hec_token = os.environ.get("SPLUNK_HEC_TOKEN")
        if splunk_hec_url and splunk_hec_token:
            splunk = SplunkIntegration(
                hec_url=splunk_hec_url,
                hec_token=splunk_hec_token,
                index=os.environ.get("SPLUNK_INDEX", "main"),
            )
            _siem_manager.add_integration("splunk", splunk)

        # Elasticsearch
        es_url = os.environ.get("ELASTICSEARCH_URL")
        if es_url:
            es = ElasticsearchIntegration(
                es_url=es_url,
                index_pattern=os.environ.get("ELASTICSEARCH_INDEX", "storyflow-logs-{date}"),
                api_key=os.environ.get("ELASTICSEARCH_API_KEY"),
                username=os.environ.get("ELASTICSEARCH_USERNAME"),
                password=os.environ.get("ELASTICSEARCH_PASSWORD"),
            )
            _siem_manager.add_integration("elasticsearch", es)

        # OpenTelemetry
        otlp_endpoint = os.environ.get("OTLP_ENDPOINT")
        if otlp_endpoint:
            otlp_headers = {}
            if os.environ.get("OTLP_HEADERS"):
                # Parse headers from env (format: "key1=value1,key2=value2")
                for header in os.environ.get("OTLP_HEADERS", "").split(","):
                    if "=" in header:
                        key, value = header.split("=", 1)
                        otlp_headers[key.strip()] = value.strip()

            otel = OpenTelemetryIntegration(
                otlp_endpoint=otlp_endpoint,
                service_name=os.environ.get("OTEL_SERVICE_NAME", "storyflow-crawler"),
                headers=otlp_headers if otlp_headers else None,
            )
            _siem_manager.add_integration("opentelemetry", otel)

        # Start auto-flush
        _siem_manager.start_auto_flush(interval=float(os.environ.get("SIEM_FLUSH_INTERVAL", "10.0")))

    return _siem_manager


if __name__ == "__main__":
    # Test SIEM integrations
    async def main():
        manager = SIEMManager()

        # Add mock integrations (would use real credentials in production)
        print("SIEM Manager Test")
        print("To test, set environment variables:")
        print("  SPLUNK_HEC_URL, SPLUNK_HEC_TOKEN")
        print("  ELASTICSEARCH_URL, ELASTICSEARCH_API_KEY")
        print("  OTLP_ENDPOINT")

        # Create sample event
        event = SIEMEvent(
            timestamp=time.time(),
            source="storyflow-test",
            event_type="test",
            severity="info",
            message="Test event from SIEM integration",
            tags=["test", "demo"],
            site_key="tangthuvien",
            status_code=200,
        )

        print(f"\nSample event: {event.to_dict()}")

        # Get configured manager
        manager = get_siem_manager()
        if manager.integrations:
            print(f"\nConfigured integrations: {list(manager.integrations.keys())}")
            await manager.send_event(event)
        else:
            print("\nNo integrations configured")

    asyncio.run(main())


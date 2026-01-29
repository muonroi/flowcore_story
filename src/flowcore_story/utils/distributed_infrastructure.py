"""Distributed Infrastructure Manager - Region/ASN distribution.

This module manages distributed infrastructure to reduce correlation:
- Region-based distribution (US-East, US-West, EU, Asia, etc.)
- ASN (Autonomous System Number) distribution
- Geographic correlation reduction
- Load balancing across regions
- ASN rotation to avoid patterns

Helps avoid detection by spreading requests across diverse network paths.
"""

import random
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

from flowcore_story.utils.logger import logger


class Region(Enum):
    """Geographic regions."""
    US_EAST = "us-east"
    US_WEST = "us-west"
    US_CENTRAL = "us-central"
    EU_WEST = "eu-west"
    EU_CENTRAL = "eu-central"
    ASIA_EAST = "asia-east"
    ASIA_SOUTHEAST = "asia-southeast"
    OCEANIA = "oceania"
    SOUTH_AMERICA = "south-america"
    UNKNOWN = "unknown"


@dataclass
class NetworkLocation:
    """Network location information."""

    asn: int  # Autonomous System Number
    asn_name: str  # ASN organization name
    region: Region
    country: str
    city: str | None = None
    isp: str | None = None

    # Network characteristics
    is_datacenter: bool = False
    is_residential: bool = False
    is_mobile: bool = False

    # Metadata
    latitude: float | None = None
    longitude: float | None = None


@dataclass
class DistributedEndpoint:
    """A distributed endpoint (proxy or worker)."""

    id: str
    endpoint_url: str  # Proxy URL or worker URL
    network_location: NetworkLocation

    # Usage tracking
    request_count: int = 0
    last_used: float = 0.0

    # Performance
    avg_response_time: float = 0.0
    success_rate: float = 1.0

    # Status
    is_active: bool = True
    cooldown_until: float = 0.0

    def is_available(self) -> bool:
        """Check if endpoint is available."""
        if not self.is_active:
            return False
        if time.time() < self.cooldown_until:
            return False
        return True


class DistributedInfrastructureManager:
    """Manages distributed infrastructure across regions and ASNs."""

    def __init__(
        self,
        min_asn_diversity: int = 5,  # Minimum different ASNs to use
        max_correlation_window: int = 300,  # 5 minutes
        max_requests_per_asn_window: int = 10,
    ):
        """Initialize distributed infrastructure manager.

        Args:
            min_asn_diversity: Minimum number of different ASNs to distribute across
            max_correlation_window: Time window to track ASN usage (seconds)
            max_requests_per_asn_window: Max requests per ASN in time window
        """
        self.min_asn_diversity = min_asn_diversity
        self.max_correlation_window = max_correlation_window
        self.max_requests_per_asn_window = max_requests_per_asn_window

        self.logger = logger.bind(component="DistributedInfra")

        # Endpoints
        self._endpoints: dict[str, DistributedEndpoint] = {}

        # Groupings
        self._by_region: dict[Region, list[str]] = defaultdict(list)
        self._by_asn: dict[int, list[str]] = defaultdict(list)
        self._by_type: dict[str, list[str]] = defaultdict(list)  # residential, datacenter, mobile

        # Usage tracking
        self._asn_usage_history: dict[int, list[float]] = defaultdict(list)  # ASN -> timestamps
        self._region_usage_history: dict[Region, list[float]] = defaultdict(list)

    def add_endpoint(self, endpoint: DistributedEndpoint) -> None:
        """Add an endpoint to the infrastructure.

        Args:
            endpoint: DistributedEndpoint
        """
        self._endpoints[endpoint.id] = endpoint

        # Index by region
        self._by_region[endpoint.network_location.region].append(endpoint.id)

        # Index by ASN
        self._by_asn[endpoint.network_location.asn].append(endpoint.id)

        # Index by type
        if endpoint.network_location.is_residential:
            self._by_type['residential'].append(endpoint.id)
        elif endpoint.network_location.is_datacenter:
            self._by_type['datacenter'].append(endpoint.id)
        elif endpoint.network_location.is_mobile:
            self._by_type['mobile'].append(endpoint.id)

        self.logger.info(
            f"Added endpoint {endpoint.id} (ASN: {endpoint.network_location.asn}, "
            f"Region: {endpoint.network_location.region.value})"
        )

    def remove_endpoint(self, endpoint_id: str) -> bool:
        """Remove an endpoint.

        Args:
            endpoint_id: Endpoint ID

        Returns:
            True if removed
        """
        if endpoint_id not in self._endpoints:
            return False

        endpoint = self._endpoints.pop(endpoint_id)

        # Remove from indexes
        self._by_region[endpoint.network_location.region].remove(endpoint_id)
        self._by_asn[endpoint.network_location.asn].remove(endpoint_id)

        for type_list in self._by_type.values():
            if endpoint_id in type_list:
                type_list.remove(endpoint_id)

        return True

    def select_endpoint(
        self,
        prefer_residential: bool = True,
        required_region: Region | None = None,
        exclude_asns: set[int] | None = None,
        site_key: str | None = None,
    ) -> DistributedEndpoint | None:
        """Select an endpoint with geographic and ASN diversity.

        Args:
            prefer_residential: Prefer residential over datacenter
            required_region: Specific region requirement
            exclude_asns: ASNs to exclude
            site_key: Site key (for site-specific selection)

        Returns:
            DistributedEndpoint or None
        """
        # Filter available endpoints
        candidates = [
            ep for ep in self._endpoints.values()
            if ep.is_available()
        ]

        if not candidates:
            self.logger.warning("No available endpoints")
            return None

        # Filter by region if specified
        if required_region:
            candidates = [ep for ep in candidates if ep.network_location.region == required_region]

        # Filter by ASN exclusion
        if exclude_asns:
            candidates = [ep for ep in candidates if ep.network_location.asn not in exclude_asns]

        if not candidates:
            self.logger.warning("No candidates after filtering")
            return None

        # Prioritize by type
        if prefer_residential:
            residential = [ep for ep in candidates if ep.network_location.is_residential]
            if residential:
                candidates = residential

        # Get ASN usage in recent window
        recent_asn_usage = self._get_recent_asn_usage()

        # Score candidates based on diversity
        scored_candidates = []
        for ep in candidates:
            score = self._score_endpoint(ep, recent_asn_usage)
            scored_candidates.append((score, ep))

        # Sort by score (higher is better)
        scored_candidates.sort(reverse=True, key=lambda x: x[0])

        # Select from top candidates with some randomness
        top_n = min(3, len(scored_candidates))
        selected = random.choice(scored_candidates[:top_n])[1]

        # Record usage
        self._record_endpoint_usage(selected)

        return selected

    def _get_recent_asn_usage(self) -> dict[int, int]:
        """Get recent ASN usage counts.

        Returns:
            Dict of ASN -> count in recent window
        """
        now = time.time()
        cutoff = now - self.max_correlation_window

        recent_usage = {}
        for asn, timestamps in self._asn_usage_history.items():
            recent_count = sum(1 for ts in timestamps if ts >= cutoff)
            if recent_count > 0:
                recent_usage[asn] = recent_count

        return recent_usage

    def _score_endpoint(
        self,
        endpoint: DistributedEndpoint,
        recent_asn_usage: dict[int, int]
    ) -> float:
        """Score an endpoint for selection.

        Args:
            endpoint: DistributedEndpoint
            recent_asn_usage: Recent ASN usage counts

        Returns:
            Score (higher is better)
        """
        score = 100.0

        # Penalize recently used ASNs
        asn_recent_count = recent_asn_usage.get(endpoint.network_location.asn, 0)
        if asn_recent_count > 0:
            penalty = min(50, asn_recent_count * 5)
            score -= penalty

        # Penalize if near max requests for this ASN
        if asn_recent_count >= self.max_requests_per_asn_window:
            score -= 1000  # Heavily penalize

        # Bonus for residential
        if endpoint.network_location.is_residential:
            score += 20

        # Bonus for good performance
        score += endpoint.success_rate * 10

        # Penalize slow endpoints
        if endpoint.avg_response_time > 5.0:
            score -= 10

        # Penalize if used very recently
        time_since_use = time.time() - endpoint.last_used
        if time_since_use < 60:  # Used in last minute
            score -= 20

        return score

    def _record_endpoint_usage(self, endpoint: DistributedEndpoint) -> None:
        """Record endpoint usage.

        Args:
            endpoint: DistributedEndpoint
        """
        now = time.time()

        endpoint.request_count += 1
        endpoint.last_used = now

        # Record ASN usage
        self._asn_usage_history[endpoint.network_location.asn].append(now)

        # Record region usage
        self._region_usage_history[endpoint.network_location.region].append(now)

        # Cleanup old history
        self._cleanup_usage_history()

    def _cleanup_usage_history(self) -> None:
        """Clean up old usage history."""
        now = time.time()
        cutoff = now - self.max_correlation_window

        # Cleanup ASN history
        for asn in list(self._asn_usage_history.keys()):
            self._asn_usage_history[asn] = [
                ts for ts in self._asn_usage_history[asn]
                if ts >= cutoff
            ]
            if not self._asn_usage_history[asn]:
                del self._asn_usage_history[asn]

        # Cleanup region history
        for region in list(self._region_usage_history.keys()):
            self._region_usage_history[region] = [
                ts for ts in self._region_usage_history[region]
                if ts >= cutoff
            ]
            if not self._region_usage_history[region]:
                del self._region_usage_history[region]

    def get_asn_diversity_score(self) -> float:
        """Get current ASN diversity score.

        Returns:
            Score from 0 to 1 (higher is better)
        """
        recent_usage = self._get_recent_asn_usage()
        unique_asns = len(recent_usage)

        if unique_asns == 0:
            return 1.0

        # Calculate diversity (closer to min_asn_diversity is better)
        if unique_asns >= self.min_asn_diversity:
            return 1.0

        return unique_asns / self.min_asn_diversity

    def get_statistics(self) -> dict:
        """Get infrastructure statistics.

        Returns:
            Dict with statistics
        """
        total_endpoints = len(self._endpoints)
        active_endpoints = sum(1 for ep in self._endpoints.values() if ep.is_active)
        available_endpoints = sum(1 for ep in self._endpoints.values() if ep.is_available())

        # By type
        residential_count = len(self._by_type.get('residential', []))
        datacenter_count = len(self._by_type.get('datacenter', []))
        mobile_count = len(self._by_type.get('mobile', []))

        # By region
        region_counts = {
            region.value: len(endpoint_ids)
            for region, endpoint_ids in self._by_region.items()
        }

        # ASN diversity
        unique_asns = len(self._by_asn)
        recent_asn_usage = self._get_recent_asn_usage()
        active_asns = len(recent_asn_usage)
        diversity_score = self.get_asn_diversity_score()

        return {
            'total_endpoints': total_endpoints,
            'active_endpoints': active_endpoints,
            'available_endpoints': available_endpoints,
            'residential_count': residential_count,
            'datacenter_count': datacenter_count,
            'mobile_count': mobile_count,
            'region_distribution': region_counts,
            'unique_asns': unique_asns,
            'active_asns': active_asns,
            'asn_diversity_score': diversity_score,
            'min_asn_diversity': self.min_asn_diversity,
        }

    def print_status(self) -> None:
        """Print infrastructure status."""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("Distributed Infrastructure Status")
        print("=" * 70)

        print("\nEndpoints:")
        print(f"  Total: {stats['total_endpoints']}")
        print(f"  Active: {stats['active_endpoints']}")
        print(f"  Available: {stats['available_endpoints']}")

        print("\nBy Type:")
        print(f"  Residential: {stats['residential_count']}")
        print(f"  Datacenter: {stats['datacenter_count']}")
        print(f"  Mobile: {stats['mobile_count']}")

        print("\nBy Region:")
        for region, count in sorted(stats['region_distribution'].items()):
            print(f"  {region}: {count}")

        print("\nASN Diversity:")
        print(f"  Unique ASNs: {stats['unique_asns']}")
        print(f"  Recently Active: {stats['active_asns']}")
        print(f"  Diversity Score: {stats['asn_diversity_score']:.2f}")
        print(f"  Target Diversity: {stats['min_asn_diversity']}")

        print("=" * 70)


# Utility functions for ASN/Region detection

def detect_asn_from_ip(ip: str) -> tuple[int, str] | None:
    """Detect ASN from IP address.

    This is a placeholder - in production, use:
    - MaxMind GeoIP2 ASN database
    - IP-API.com
    - ipinfo.io
    - Team Cymru IP to ASN mapping

    Args:
        ip: IP address

    Returns:
        Tuple of (ASN number, ASN name) or None
    """
    # Placeholder implementation
    # In production, use actual ASN lookup service
    return None


def detect_region_from_country(country: str) -> Region:
    """Detect region from country code.

    Args:
        country: ISO 3166-1 alpha-2 country code

    Returns:
        Region
    """
    # Simple mapping
    region_map = {
        'US': Region.US_EAST,
        'CA': Region.US_EAST,
        'MX': Region.US_CENTRAL,
        'BR': Region.SOUTH_AMERICA,
        'GB': Region.EU_WEST,
        'FR': Region.EU_WEST,
        'DE': Region.EU_CENTRAL,
        'NL': Region.EU_WEST,
        'CN': Region.ASIA_EAST,
        'JP': Region.ASIA_EAST,
        'KR': Region.ASIA_EAST,
        'SG': Region.ASIA_SOUTHEAST,
        'TH': Region.ASIA_SOUTHEAST,
        'AU': Region.OCEANIA,
        'NZ': Region.OCEANIA,
    }

    return region_map.get(country, Region.UNKNOWN)


# Global instance
_distributed_infra: DistributedInfrastructureManager | None = None


def get_distributed_infrastructure() -> DistributedInfrastructureManager:
    """Get global distributed infrastructure manager.

    Returns:
        DistributedInfrastructureManager
    """
    global _distributed_infra

    if _distributed_infra is None:
        import os

        _distributed_infra = DistributedInfrastructureManager(
            min_asn_diversity=int(os.environ.get("INFRA_MIN_ASN_DIVERSITY", "5")),
            max_correlation_window=int(os.environ.get("INFRA_CORRELATION_WINDOW", "300")),
            max_requests_per_asn_window=int(os.environ.get("INFRA_MAX_REQUESTS_PER_ASN", "10")),
        )

    return _distributed_infra


if __name__ == "__main__":
    # Test distributed infrastructure
    manager = get_distributed_infrastructure()

    # Add some test endpoints
    for i in range(20):
        region = random.choice(list(Region))
        if region == Region.UNKNOWN:
            continue

        is_residential = i % 3 == 0
        asn = 15169 + (i % 10)  # Different ASNs

        endpoint = DistributedEndpoint(
            id=f"endpoint_{i}",
            endpoint_url=f"http://proxy{i}:8080",
            network_location=NetworkLocation(
                asn=asn,
                asn_name=f"ASN-{asn}",
                region=region,
                country="US",
                is_residential=is_residential,
                is_datacenter=not is_residential,
            )
        )

        manager.add_endpoint(endpoint)

    # Print status
    manager.print_status()

    # Select some endpoints
    print("\nSelecting endpoints:")
    for i in range(15):
        endpoint = manager.select_endpoint(prefer_residential=True)
        if endpoint:
            print(f"  {i+1}. {endpoint.id} (ASN: {endpoint.network_location.asn}, Region: {endpoint.network_location.region.value})")

    # Print status again
    manager.print_status()


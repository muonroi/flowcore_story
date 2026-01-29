"""Advanced Protocol Fingerprinting - HTTP/2, HTTP/3, QUIC settings.

Module này cung cấp control chi tiết về protocol-level fingerprints
bao gồm HTTP/2 frames, HTTP/3/QUIC parameters, và TCP behaviors.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

# HTTP/2 Frame Types
HTTP2_FRAME_TYPES = {
    "DATA": 0x0,
    "HEADERS": 0x1,
    "PRIORITY": 0x2,
    "RST_STREAM": 0x3,
    "SETTINGS": 0x4,
    "PUSH_PROMISE": 0x5,
    "PING": 0x6,
    "GOAWAY": 0x7,
    "WINDOW_UPDATE": 0x8,
    "CONTINUATION": 0x9,
}

# HTTP/2 Settings Parameters
HTTP2_SETTINGS_PARAMS = {
    "HEADER_TABLE_SIZE": 0x1,
    "ENABLE_PUSH": 0x2,
    "MAX_CONCURRENT_STREAMS": 0x3,
    "INITIAL_WINDOW_SIZE": 0x4,
    "MAX_FRAME_SIZE": 0x5,
    "MAX_HEADER_LIST_SIZE": 0x6,
    "ENABLE_CONNECT_PROTOCOL": 0x8,
}

# HTTP/3 Settings (QUIC)
HTTP3_SETTINGS = {
    "QPACK_MAX_TABLE_CAPACITY": 0x1,
    "MAX_FIELD_SECTION_SIZE": 0x6,
    "QPACK_BLOCKED_STREAMS": 0x7,
}

# QUIC Transport Parameters
QUIC_TRANSPORT_PARAMS = {
    "max_idle_timeout": "max_idle_timeout",
    "max_udp_payload_size": "max_udp_payload_size",
    "initial_max_data": "initial_max_data",
    "initial_max_stream_data_bidi_local": "initial_max_stream_data_bidi_local",
    "initial_max_stream_data_bidi_remote": "initial_max_stream_data_bidi_remote",
    "initial_max_stream_data_uni": "initial_max_stream_data_uni",
    "initial_max_streams_bidi": "initial_max_streams_bidi",
    "initial_max_streams_uni": "initial_max_streams_uni",
    "ack_delay_exponent": "ack_delay_exponent",
    "max_ack_delay": "max_ack_delay",
    "disable_active_migration": "disable_active_migration",
}


@dataclass
class HTTP2Profile:
    """HTTP/2 Protocol fingerprint configuration."""

    # Settings frame parameters
    header_table_size: int = 65536
    enable_push: int = 0
    max_concurrent_streams: int = 1000
    initial_window_size: int = 6291456
    max_frame_size: int = 16384
    max_header_list_size: int = 262144
    enable_connect_protocol: int | None = None

    # Window update
    connection_window_update: int = 15663105
    stream_window_update: int = 6291456

    # Priority frame
    stream_dependency: int = 0
    stream_weight: int = 255
    exclusive: bool = False

    # Frame order and timing
    settings_order: list[str] = field(default_factory=lambda: [
        "HEADER_TABLE_SIZE",
        "ENABLE_PUSH",
        "MAX_CONCURRENT_STREAMS",
        "INITIAL_WINDOW_SIZE",
        "MAX_FRAME_SIZE",
        "MAX_HEADER_LIST_SIZE",
    ])

    # HPACK dynamic table
    hpack_dynamic_table_size: int = 4096
    hpack_huffman_encoding: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "HEADER_TABLE_SIZE": self.header_table_size,
            "ENABLE_PUSH": self.enable_push,
            "MAX_CONCURRENT_STREAMS": self.max_concurrent_streams,
            "INITIAL_WINDOW_SIZE": self.initial_window_size,
            "MAX_FRAME_SIZE": self.max_frame_size,
            "MAX_HEADER_LIST_SIZE": self.max_header_list_size,
        }

    def get_settings_frame_bytes(self) -> bytes:
        """Generate HTTP/2 SETTINGS frame payload."""
        # This is a simplified version - actual implementation would need proper encoding
        settings = []
        for param_name in self.settings_order:
            if param_name == "HEADER_TABLE_SIZE":
                settings.append((HTTP2_SETTINGS_PARAMS[param_name], self.header_table_size))
            elif param_name == "ENABLE_PUSH":
                settings.append((HTTP2_SETTINGS_PARAMS[param_name], self.enable_push))
            elif param_name == "MAX_CONCURRENT_STREAMS":
                settings.append((HTTP2_SETTINGS_PARAMS[param_name], self.max_concurrent_streams))
            elif param_name == "INITIAL_WINDOW_SIZE":
                settings.append((HTTP2_SETTINGS_PARAMS[param_name], self.initial_window_size))
            elif param_name == "MAX_FRAME_SIZE":
                settings.append((HTTP2_SETTINGS_PARAMS[param_name], self.max_frame_size))
            elif param_name == "MAX_HEADER_LIST_SIZE":
                settings.append((HTTP2_SETTINGS_PARAMS[param_name], self.max_header_list_size))

        # Return as pseudo-bytes (actual implementation would encode properly)
        return b""  # Placeholder


@dataclass
class HTTP3Profile:
    """HTTP/3 (QUIC) Protocol fingerprint configuration."""

    # HTTP/3 Settings
    qpack_max_table_capacity: int = 4096
    max_field_section_size: int = 16384
    qpack_blocked_streams: int = 100

    # QUIC Transport Parameters
    max_idle_timeout: int = 30000  # milliseconds
    max_udp_payload_size: int = 1472
    initial_max_data: int = 10485760  # 10MB
    initial_max_stream_data_bidi_local: int = 6291456
    initial_max_stream_data_bidi_remote: int = 6291456
    initial_max_stream_data_uni: int = 6291456
    initial_max_streams_bidi: int = 100
    initial_max_streams_uni: int = 100
    ack_delay_exponent: int = 3
    max_ack_delay: int = 25  # milliseconds
    disable_active_migration: bool = False

    # QUIC version
    quic_version: str = "1"  # QUIC v1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "qpack_max_table_capacity": self.qpack_max_table_capacity,
            "max_field_section_size": self.max_field_section_size,
            "qpack_blocked_streams": self.qpack_blocked_streams,
            "max_idle_timeout": self.max_idle_timeout,
            "max_udp_payload_size": self.max_udp_payload_size,
            "initial_max_data": self.initial_max_data,
        }


@dataclass
class TCPFingerprint:
    """TCP-level fingerprint parameters (if transport supports)."""

    # TCP Window
    window_size: int = 65535
    window_scale: int = 8

    # TCP Options order
    mss: int = 1460  # Maximum Segment Size
    sack_permitted: bool = True
    timestamps: bool = True
    window_scale_option: bool = True

    # TCP flags behavior
    initial_ttl: int = 64  # Time to Live

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "window_size": self.window_size,
            "window_scale": self.window_scale,
            "mss": self.mss,
            "sack_permitted": self.sack_permitted,
            "timestamps": self.timestamps,
        }


# Predefined HTTP/2 profiles for different browsers
HTTP2_PROFILES = {
    "chrome_default": HTTP2Profile(
        header_table_size=65536,
        enable_push=0,
        max_concurrent_streams=1000,
        initial_window_size=6291456,
        max_frame_size=16384,
        max_header_list_size=262144,
        connection_window_update=15663105,
        stream_weight=255,
    ),
    "firefox_default": HTTP2Profile(
        header_table_size=65536,
        enable_push=1,
        max_concurrent_streams=100,
        initial_window_size=131072,
        max_frame_size=16384,
        max_header_list_size=262144,
        connection_window_update=12517377,
        stream_weight=200,
    ),
    "safari_default": HTTP2Profile(
        header_table_size=4096,
        enable_push=0,
        max_concurrent_streams=100,
        initial_window_size=2097152,
        max_frame_size=16384,
        max_header_list_size=8192,
        connection_window_update=10485760,
        stream_weight=255,
        exclusive=True,
    ),
}

# Predefined HTTP/3 profiles
HTTP3_PROFILES = {
    "chrome_default": HTTP3Profile(
        qpack_max_table_capacity=4096,
        max_field_section_size=16384,
        qpack_blocked_streams=100,
        max_idle_timeout=30000,
        initial_max_data=10485760,
    ),
    "firefox_default": HTTP3Profile(
        qpack_max_table_capacity=0,
        max_field_section_size=16384,
        qpack_blocked_streams=0,
        max_idle_timeout=60000,
        initial_max_data=15728640,
    ),
}


def create_http2_profile_variant(base: HTTP2Profile, variation: float = 0.05) -> HTTP2Profile:
    """Create a variant of HTTP/2 profile with slight randomization.

    Args:
        base: Base HTTP2Profile
        variation: Variation percentage (default 5%)

    Returns:
        New HTTP2Profile with randomized values
    """
    def vary(value: int, pct: float = variation) -> int:
        """Vary an integer value by percentage."""
        if value == 0:
            return 0
        delta = int(value * pct)
        return max(1, value + random.randint(-delta, delta))

    return HTTP2Profile(
        header_table_size=vary(base.header_table_size),
        enable_push=base.enable_push,
        max_concurrent_streams=vary(base.max_concurrent_streams, 0.1),
        initial_window_size=vary(base.initial_window_size),
        max_frame_size=base.max_frame_size,  # Usually fixed
        max_header_list_size=vary(base.max_header_list_size),
        connection_window_update=vary(base.connection_window_update),
        stream_window_update=vary(base.stream_window_update),
        stream_dependency=base.stream_dependency,
        stream_weight=base.stream_weight,
        exclusive=base.exclusive,
        settings_order=base.settings_order.copy(),
        hpack_dynamic_table_size=vary(base.hpack_dynamic_table_size),
        hpack_huffman_encoding=base.hpack_huffman_encoding,
    )


def randomize_settings_order(base_order: list[str]) -> list[str]:
    """Randomize HTTP/2 settings order slightly while keeping it realistic.

    Args:
        base_order: Base settings order

    Returns:
        Slightly randomized order
    """
    # Don't fully randomize - just swap adjacent pairs sometimes
    order = base_order.copy()
    for i in range(len(order) - 1):
        if random.random() < 0.2:  # 20% chance to swap
            order[i], order[i + 1] = order[i + 1], order[i]
    return order


def select_http_version(
    supports_h3: bool = False,
    prefer_h3: bool = False,
) -> str:
    """Select HTTP version based on capabilities and preferences.

    Args:
        supports_h3: Whether HTTP/3 is supported
        prefer_h3: Whether to prefer HTTP/3 when available

    Returns:
        HTTP version string: "h3", "h2", or "http/1.1"
    """
    if supports_h3 and prefer_h3:
        # 80% chance to use H3 if preferred
        if random.random() < 0.8:
            return "h3"

    # Default to HTTP/2 if not using H3
    # 90% use h2, 10% fallback to http/1.1 for diversity
    if random.random() < 0.9:
        return "h2"
    else:
        return "http/1.1"


def get_alpn_for_version(http_version: str) -> list[str]:
    """Get ALPN protocol list for HTTP version.

    Args:
        http_version: HTTP version ("h3", "h2", "http/1.1")

    Returns:
        List of ALPN protocols in negotiation order
    """
    if http_version == "h3":
        return ["h3", "h3-29", "h2", "http/1.1"]
    elif http_version == "h2":
        return ["h2", "http/1.1"]
    else:
        return ["http/1.1"]


__all__ = [
    "HTTP2Profile",
    "HTTP3Profile",
    "TCPFingerprint",
    "HTTP2_PROFILES",
    "HTTP3_PROFILES",
    "create_http2_profile_variant",
    "randomize_settings_order",
    "select_http_version",
    "get_alpn_for_version",
]


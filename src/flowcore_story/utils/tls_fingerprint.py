"""TLS và HTTP/2 fingerprinting với JA3 string customization.

Module này cung cấp khả năng tạo và quản lý TLS/HTTP2 fingerprints đa dạng
để tránh bị phát hiện dựa trên JA3 signature và HTTP/2 settings.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import asdict, dataclass
from typing import Any

# Cipher Suites (TLS 1.3 and TLS 1.2)
CIPHER_SUITES = {
    # TLS 1.3
    "TLS_AES_128_GCM_SHA256": 0x1301,
    "TLS_AES_256_GCM_SHA384": 0x1302,
    "TLS_CHACHA20_POLY1305_SHA256": 0x1303,
    # TLS 1.2
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": 0xC02B,
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256": 0xC02F,
    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": 0xC02C,
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384": 0xC030,
    "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256": 0xCCA9,
    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256": 0xCCA8,
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA": 0xC013,
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA": 0xC014,
    "TLS_RSA_WITH_AES_128_GCM_SHA256": 0x009C,
    "TLS_RSA_WITH_AES_256_GCM_SHA384": 0x009D,
    "TLS_RSA_WITH_AES_128_CBC_SHA": 0x002F,
    "TLS_RSA_WITH_AES_256_CBC_SHA": 0x0035,
}

# TLS Extensions
TLS_EXTENSIONS = {
    "server_name": 0,
    "max_fragment_length": 1,
    "status_request": 5,
    "supported_groups": 10,
    "ec_point_formats": 11,
    "signature_algorithms": 13,
    "use_srtp": 14,
    "heartbeat": 15,
    "application_layer_protocol_negotiation": 16,
    "signed_certificate_timestamp": 18,
    "client_certificate_type": 19,
    "server_certificate_type": 20,
    "padding": 21,
    "encrypt_then_mac": 22,
    "extended_master_secret": 23,
    "session_ticket": 35,
    "supported_versions": 43,
    "psk_key_exchange_modes": 45,
    "key_share": 51,
    "renegotiation_info": 65281,
    "compress_certificate": 27,
    "delegated_credentials": 34,
}

# ALPN Protocols
ALPN_PROTOCOLS = {
    "http/1.1": "http/1.1",
    "h2": "h2",  # HTTP/2
    "h3": "h3",  # HTTP/3
    "h3-29": "h3-29",  # HTTP/3 draft 29
}

# Browser impersonation profiles với JA3 strings thực tế
# JA3 format: SSLVersion,Ciphers,Extensions,EllipticCurves,EllipticCurvePointFormats
TLS_PROFILES = {
    "chrome120": {
        "impersonate": "chrome120",
        "ja3_string": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-21,29-23-24,0",
        "cipher_suites": [0x1301, 0x1302, 0x1303, 0xC02B, 0xC02F, 0xC02C, 0xC030, 0xCCA9, 0xCCA8, 0xC013, 0xC014, 0x009C, 0x009D, 0x002F, 0x0035],
        "tls_extensions": [0, 23, 65281, 10, 11, 35, 16, 5, 13, 18, 51, 45, 43, 27, 21],
        "supported_groups": [29, 23, 24],  # x25519, secp256r1, secp384r1
        "alpn_protocols": ["h2", "http/1.1"],
        "http2_settings": {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 6291456,
            "MAX_HEADER_LIST_SIZE": 262144,
            "MAX_FRAME_SIZE": 16384,
        },
        "http2_window_update": 15663105,
        "http2_priority": (0, 255, False),  # (stream_id, weight, exclusive)
        "http2_stream_weight": 255,
        "http2_header_priority": (0, 0, False),
        "supports_h3": True,
        "description": "Chrome 120+ impersonation"
    },
    "chrome119": {
        "impersonate": "chrome119",
        "ja3_string": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-21,29-23-24,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 6291456,
            "MAX_HEADER_LIST_SIZE": 262144,
        },
        "http2_window_update": 15663105,
        "http2_priority": (0, 255, False),
        "description": "Chrome 119 impersonation"
    },
    "chrome110": {
        "impersonate": "chrome110",
        "ja3_string": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-17513,29-23-24,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 6291456,
            "MAX_HEADER_LIST_SIZE": 262144,
        },
        "http2_window_update": 15663105,
        "http2_priority": (0, 255, False),
        "description": "Chrome 110 impersonation"
    },
    "chrome107": {
        "impersonate": "chrome107",
        "ja3_string": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27,29-23-24,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 6291456,
            "MAX_HEADER_LIST_SIZE": 262144,
        },
        "http2_window_update": 15663105,
        "http2_priority": (0, 255, False),
        "description": "Chrome 107 impersonation"
    },
    "edge120": {
        "impersonate": "edge120",
        "ja3_string": "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-21,29-23-24,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 1000,
            "INITIAL_WINDOW_SIZE": 6291456,
            "MAX_HEADER_LIST_SIZE": 262144,
        },
        "http2_window_update": 15663105,
        "http2_priority": (0, 255, False),
        "description": "Edge 120+ impersonation"
    },
    "firefox120": {
        "impersonate": "firefox120",
        "ja3_string": "771,4865-4867-4866-49195-49199-52393-52392-49196-49200-49162-49161-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-34-51-43-13-45-28-21,29-23-24-25-256-257,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 1,
            "MAX_CONCURRENT_STREAMS": 100,
            "INITIAL_WINDOW_SIZE": 131072,
            "MAX_FRAME_SIZE": 16384,
        },
        "http2_window_update": 12517377,
        "http2_priority": (3, 200, False),
        "description": "Firefox 120+ impersonation"
    },
    "firefox119": {
        "impersonate": "firefox119",
        "ja3_string": "771,4865-4867-4866-49195-49199-52393-52392-49196-49200-49162-49161-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-34-51-43-13-45-28-21,29-23-24-25-256-257,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 65536,
            "ENABLE_PUSH": 1,
            "MAX_CONCURRENT_STREAMS": 100,
            "INITIAL_WINDOW_SIZE": 131072,
            "MAX_FRAME_SIZE": 16384,
        },
        "http2_window_update": 12517377,
        "http2_priority": (3, 200, False),
        "description": "Firefox 119 impersonation"
    },
    "safari17": {
        "impersonate": "safari17_0",
        "ja3_string": "771,4865-4866-4867-49196-49195-52393-49200-49199-52392-49162-49161-49172-49171-157-156-53-47-49160-49170-10,0-23-65281-10-11-16-5-13-18-51-45-43-27-21,29-23-24-25,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 4096,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 100,
            "INITIAL_WINDOW_SIZE": 2097152,
            "MAX_FRAME_SIZE": 16384,
            "MAX_HEADER_LIST_SIZE": 8192,
        },
        "http2_window_update": 10485760,
        "http2_priority": (0, 255, True),
        "description": "Safari 17 impersonation"
    },
    "safari16": {
        "impersonate": "safari16_0",
        "ja3_string": "771,4865-4866-4867-49196-49195-52393-49200-49199-52392-49162-49161-49172-49171-157-156-53-47-49160-49170-10,0-23-65281-10-11-16-5-13-18-51-45-43-27,29-23-24-25,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 4096,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 100,
            "INITIAL_WINDOW_SIZE": 2097152,
            "MAX_FRAME_SIZE": 16384,
            "MAX_HEADER_LIST_SIZE": 8192,
        },
        "http2_window_update": 10485760,
        "http2_priority": (0, 255, True),
        "description": "Safari 16 impersonation"
    },
    "safari15": {
        "impersonate": "safari15_6",
        "ja3_string": "771,4865-4866-4867-49196-49195-52393-49200-49199-52392-49162-49161-49172-49171-157-156-53-47-49160-49170-10,0-23-65281-10-11-16-5-13-18-51-45-43,29-23-24-25,0",
        "http2_settings": {
            "HEADER_TABLE_SIZE": 4096,
            "ENABLE_PUSH": 0,
            "MAX_CONCURRENT_STREAMS": 100,
            "INITIAL_WINDOW_SIZE": 2097152,
            "MAX_FRAME_SIZE": 16384,
            "MAX_HEADER_LIST_SIZE": 8192,
        },
        "http2_window_update": 10485760,
        "http2_priority": (0, 255, True),
        "description": "Safari 15 impersonation"
    },
}

# User-Agent to TLS profile mapping patterns
UA_TO_TLS_PROFILE_MAP = {
    "Chrome/124": ["chrome120", "chrome119"],
    "Chrome/123": ["chrome120", "chrome119"],
    "Chrome/122": ["chrome120", "chrome119"],
    "Chrome/121": ["chrome120", "chrome119"],
    "Chrome/120": ["chrome120"],
    "Chrome/119": ["chrome119"],
    "Chrome/110": ["chrome110"],
    "Chrome/107": ["chrome107"],
    "Edg/124": ["edge120"],
    "Edg/123": ["edge120"],
    "Edg/122": ["edge120"],
    "Edg/121": ["edge120"],
    "Edg/120": ["edge120"],
    "Firefox/125": ["firefox120", "firefox119"],
    "Firefox/124": ["firefox120", "firefox119"],
    "Firefox/123": ["firefox120", "firefox119"],
    "Firefox/120": ["firefox120"],
    "Firefox/119": ["firefox119"],
    "Version/17": ["safari17"],
    "Version/16": ["safari16"],
    "Version/15": ["safari15"],
}


@dataclass
class TLSFingerprint:
    """Đại diện cho TLS/HTTP2 fingerprint configuration."""

    profile_id: str
    impersonate: str
    ja3_string: str
    http2_settings: dict[str, int]
    http2_window_update: int
    http2_priority: tuple[int, int, bool]
    description: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    def get_ja3_hash(self) -> str:
        """Calculate JA3 hash from JA3 string."""
        return hashlib.md5(self.ja3_string.encode()).hexdigest()

    @classmethod
    def from_profile(cls, profile_name: str) -> TLSFingerprint | None:
        """Create TLSFingerprint from profile name."""
        profile_data = TLS_PROFILES.get(profile_name)
        if not profile_data:
            return None

        return cls(
            profile_id=profile_name,
            impersonate=profile_data["impersonate"],
            ja3_string=profile_data["ja3_string"],
            http2_settings=profile_data["http2_settings"],
            http2_window_update=profile_data["http2_window_update"],
            http2_priority=profile_data["http2_priority"],
            description=profile_data["description"],
        )


def select_tls_profile_for_user_agent(
    user_agent: str,
    randomize: bool = True
) -> TLSFingerprint | None:
    """Chọn TLS profile phù hợp với User-Agent.

    Args:
        user_agent: User-Agent string
        randomize: Nếu True, random chọn từ các profiles phù hợp

    Returns:
        TLSFingerprint object hoặc None nếu không tìm thấy
    """
    if not user_agent:
        return None

    # Try to match user agent with profile patterns
    matching_profiles: list[str] = []

    for ua_pattern, profiles in UA_TO_TLS_PROFILE_MAP.items():
        if ua_pattern in user_agent:
            matching_profiles.extend(profiles)
            break

    # Fallback to default Chrome profile if no match
    if not matching_profiles:
        # Default to recent Chrome versions
        matching_profiles = ["chrome120", "chrome119", "chrome110"]

    # Select profile
    if randomize:
        profile_name = random.choice(matching_profiles)
    else:
        profile_name = matching_profiles[0]

    return TLSFingerprint.from_profile(profile_name)


def get_impersonate_profile_for_httpx(tls_fingerprint: TLSFingerprint | None) -> str:
    """Lấy impersonate profile string cho httpx-curl-cffi.

    Args:
        tls_fingerprint: TLSFingerprint object

    Returns:
        Impersonate profile string (e.g., "chrome120", "firefox119")
    """
    if tls_fingerprint:
        return tls_fingerprint.impersonate
    return "chrome110"  # Default fallback


def randomize_http2_settings(
    base_settings: dict[str, int],
    variation_percent: float = 0.05
) -> dict[str, int]:
    """Randomize HTTP/2 settings một chút để tạo sự đa dạng.

    Args:
        base_settings: Base HTTP/2 settings dict
        variation_percent: Phần trăm biến đổi (default 5%)

    Returns:
        Dict với settings đã được randomize nhẹ
    """
    randomized = {}
    for key, value in base_settings.items():
        if isinstance(value, int) and value > 0:
            # Add random variation within specified percentage
            variation = int(value * variation_percent)
            offset = random.randint(-variation, variation)
            randomized[key] = max(1, value + offset)
        else:
            randomized[key] = value
    return randomized


def create_tls_profile_pool(
    count: int = 10,
    browser_types: list[str] | None = None
) -> list[TLSFingerprint]:
    """Tạo một pool các TLS profiles đa dạng.

    Args:
        count: Số lượng profiles cần tạo
        browser_types: List các loại browser cần tạo profile (e.g., ["chrome", "firefox"])

    Returns:
        List of TLSFingerprint objects
    """
    if browser_types is None:
        browser_types = ["chrome", "firefox", "safari", "edge"]

    available_profiles = []
    for profile_name in TLS_PROFILES.keys():
        for browser_type in browser_types:
            if browser_type.lower() in profile_name.lower():
                available_profiles.append(profile_name)
                break

    if not available_profiles:
        available_profiles = list(TLS_PROFILES.keys())

    # Create pool
    pool = []
    for i in range(count):
        profile_name = available_profiles[i % len(available_profiles)]
        profile = TLSFingerprint.from_profile(profile_name)
        if profile:
            pool.append(profile)

    return pool


def derive_tls_profile_from_user_agent(
    user_agent: str | None,
    legacy_mode: bool = False
) -> str | None:
    """Derive TLS profile string từ User-Agent (compatible với existing code).

    Args:
        user_agent: User-Agent string
        legacy_mode: Nếu True, trả về legacy format (e.g., "chrome120" thay vì full object)

    Returns:
        TLS profile string hoặc None
    """
    if not user_agent:
        return None

    tls_fp = select_tls_profile_for_user_agent(user_agent, randomize=True)
    if tls_fp:
        if legacy_mode:
            return tls_fp.profile_id
        return tls_fp.impersonate
    return None


def get_tls_fingerprint_variants(base_profile: str, count: int = 5) -> list[TLSFingerprint]:
    """Tạo các biến thể của một TLS profile để tạo sự đa dạng.

    Args:
        base_profile: Tên profile gốc (e.g., "chrome120")
        count: Số lượng variants cần tạo

    Returns:
        List of TLSFingerprint variants
    """
    base = TLSFingerprint.from_profile(base_profile)
    if not base:
        return []

    variants = [base]

    for i in range(1, count):
        # Create variant with slightly randomized HTTP/2 settings
        variant_settings = randomize_http2_settings(base.http2_settings, variation_percent=0.03)

        # Slightly adjust window update
        window_update_variation = int(base.http2_window_update * 0.02)
        variant_window_update = base.http2_window_update + random.randint(
            -window_update_variation, window_update_variation
        )

        variant = TLSFingerprint(
            profile_id=f"{base.profile_id}_var{i}",
            impersonate=base.impersonate,
            ja3_string=base.ja3_string,
            http2_settings=variant_settings,
            http2_window_update=max(1, variant_window_update),
            http2_priority=base.http2_priority,
            description=f"{base.description} (variant {i})",
        )
        variants.append(variant)

    return variants


__all__ = [
    "TLSFingerprint",
    "TLS_PROFILES",
    "select_tls_profile_for_user_agent",
    "get_impersonate_profile_for_httpx",
    "randomize_http2_settings",
    "create_tls_profile_pool",
    "derive_tls_profile_from_user_agent",
    "get_tls_fingerprint_variants",
]


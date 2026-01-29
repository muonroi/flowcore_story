"""Compatibility helpers for ``httpx`` and impersonation transports.

The project prefers to use ``httpx`` together with the ``httpx-curl-cffi``
transport so that outgoing requests benefit from curl's impersonation
capabilities.  When the extra dependency is missing (for example in a minimal
development environment), we silently fall back to the stock ``httpx`` client.

Enhanced với advanced protocol fingerprinting:
- JA3 fingerprint control
- Cipher suites customization
- ALPN protocol negotiation
- HTTP/2 settings và H3/QUIC support
"""

from __future__ import annotations

import os
import random
from typing import Any

import httpx  # type: ignore

try:  # pragma: no cover - exercised implicitly via import side effects
    from httpx_curl_cffi.transport import AsyncCurlTransport  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback when the extra transport is absent
    AsyncCurlTransport = None  # type: ignore[misc,assignment]

try:  # pragma: no cover - only available when curl_cffi is installed
    from curl_cffi.const import CurlHttpVersion  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - defensive guard
    CurlHttpVersion = None  # type: ignore

try:  # pragma: no cover - optional dependency
    from curl_cffi.requests import impersonate as _impersonate_mod  # type: ignore
except (ModuleNotFoundError, ImportError):  # pragma: no cover - defensive guard
    _impersonate_mod = None  # type: ignore

# Try to import advanced fingerprinting
try:
    from flowcore_story.utils.protocol_fingerprint import (
        HTTP2_PROFILES,
        select_http_version,
    )
    _PROTOCOL_FINGERPRINT_AVAILABLE = True
except ImportError:
    _PROTOCOL_FINGERPRINT_AVAILABLE = False

# FIX: Allow forcing pure httpx to avoid curl_cffi event loop issues
_FORCE_PURE_HTTPX = os.environ.get("FORCE_PURE_HTTPX", "false").lower() == "true"
USING_HTTPX_IMPERSONATE = AsyncCurlTransport is not None and not _FORCE_PURE_HTTPX

# Default impersonate profile - now with more variety
_DEFAULT_IMPERSONATE_OPTIONS = [
    "chrome120",
    "chrome119",
    "chrome110",
    "chrome107",
    "edge101",
]
_DEFAULT_IMPERSONATE = "chrome110"  # Fallback if random selection fails

# Enable H3/QUIC by default if supported
_ENABLE_H3 = os.environ.get("ENABLE_HTTP3", "false").lower() == "true"
_PREFER_H3 = os.environ.get("PREFER_HTTP3", "false").lower() == "true"
_TRANSPORT_ONLY_KWARGS = {
    "akamai",
    "async_curl",
    "cert",
    "curl_infos",
    "curl_options",
    "debug",
    "default_headers",
    "extra_fp",
    "http_version",
    "impersonate",
    "ja3",
    "local_address",
    "loop",
    "max_connections",
    "proxies",
    "proxy",
    "verify",
}

if CurlHttpVersion is not None:
    _HTTP_VERSION_ALIASES = {
        "h1": getattr(CurlHttpVersion, "V1_1", 1),
        "http/1.1": getattr(CurlHttpVersion, "V1_1", 1),
        "http1.1": getattr(CurlHttpVersion, "V1_1", 1),
        "1.1": getattr(CurlHttpVersion, "V1_1", 1),
        "http/1.0": getattr(CurlHttpVersion, "V1_0", 1),
        "http1.0": getattr(CurlHttpVersion, "V1_0", 1),
        "1.0": getattr(CurlHttpVersion, "V1_0", 1),
        "h2": getattr(CurlHttpVersion, "V2TLS", 2),
        "http/2": getattr(CurlHttpVersion, "V2TLS", 2),
        "http2": getattr(CurlHttpVersion, "V2TLS", 2),
        "2": getattr(CurlHttpVersion, "V2TLS", 2),
        "2.0": getattr(CurlHttpVersion, "V2TLS", 2),
        "h2-prior": getattr(CurlHttpVersion, "V2_PRIOR_KNOWLEDGE", 2),
        "prior_knowledge": getattr(CurlHttpVersion, "V2_PRIOR_KNOWLEDGE", 2),
    }
    # Add HTTP/3 aliases only if available in this version of curl_cffi
    if hasattr(CurlHttpVersion, "V3"):
        _HTTP_VERSION_ALIASES.update({
            "h3": CurlHttpVersion.V3,
            "http/3": CurlHttpVersion.V3,
            "http3": CurlHttpVersion.V3,
            "3": CurlHttpVersion.V3,
            "3.0": CurlHttpVersion.V3,
        })
    if hasattr(CurlHttpVersion, "V3ONLY"):
        _HTTP_VERSION_ALIASES["h3only"] = CurlHttpVersion.V3ONLY
else:  # pragma: no cover - executed when curl_cffi is unavailable
    _HTTP_VERSION_ALIASES = {}

if _impersonate_mod is not None:
    try:  # pragma: no cover - typing helper
        _SUPPORTED_IMPERSONATE = set(_impersonate_mod.BrowserTypeLiteral.__args__)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive guard
        _SUPPORTED_IMPERSONATE = set()
else:
    _SUPPORTED_IMPERSONATE = set()

_IMPERSONATE_COMPAT_MAP = {
    "edge120": "edge101",
    "firefox120": "firefox135",
    "firefox119": "firefox135",
    "safari17_0": "safari170",
    "safari16_0": "safari155",
    "safari15_6": "safari153",
}


def _build_transport_kwargs(params: dict[str, Any]) -> dict[str, Any]:
    transport_params: dict[str, Any] = {}
    # IMPORTANT: list() creates snapshot to prevent "dictionary changed size" during pop()
    for key in tuple(params):
        if key not in _TRANSPORT_ONLY_KWARGS:
            continue
        transport_params[key] = params.pop(key)

    # ``httpx.AsyncClient`` exposes ``proxies`` whereas the transport expects
    # ``proxy``.  Normalise the value and drop empty entries.
    proxy_value = transport_params.pop("proxies", None)
    if proxy_value is not None:
        transport_params.setdefault("proxy", proxy_value)

    if transport_params.get("proxy") is None:
        transport_params.pop("proxy", None)

    if "impersonate" in transport_params:
        if transport_params["impersonate"] is None:
            transport_params.pop("impersonate")
        else:
            transport_params["impersonate"] = _normalize_impersonate_option(transport_params["impersonate"])
    else:
        # Randomize default impersonate profile for diversity
        try:
            transport_params["impersonate"] = _normalize_impersonate_option(
                random.choice(_DEFAULT_IMPERSONATE_OPTIONS)
            )
        except Exception:
            transport_params["impersonate"] = _normalize_impersonate_option(_DEFAULT_IMPERSONATE)

    return transport_params


def _normalize_http_version(value: Any) -> Any:
    """Map human readable HTTP version specifiers to curl_cffi constants."""

    if value is None or not _HTTP_VERSION_ALIASES:
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()
        mapped = _HTTP_VERSION_ALIASES.get(normalized)
        if mapped is not None:
            return int(mapped)
        return value

    if CurlHttpVersion is not None and isinstance(value, CurlHttpVersion):
        return int(value)

    return value


def _normalize_impersonate_option(value: str | None) -> str | None:
    """Clamp impersonate aliases to the set supported by curl_cffi."""

    if value is None:
        return None

    normalized = _IMPERSONATE_COMPAT_MAP.get(value, value)
    if _SUPPORTED_IMPERSONATE and normalized not in _SUPPORTED_IMPERSONATE:
        return _DEFAULT_IMPERSONATE
    return normalized


def create_async_client(**kwargs: Any) -> httpx.AsyncClient:
    """Create an AsyncClient with advanced protocol fingerprinting.

    Enhanced features:
    - Dynamic TLS fingerprinting and randomization
    - HTTP/2 settings customization
    - HTTP/3/QUIC support (if enabled)
    - ALPN protocol negotiation
    """

    params: dict[str, Any] = dict(kwargs)
    if USING_HTTPX_IMPERSONATE and params.get("transport") is None:
        transport_params = _build_transport_kwargs(params)

        # Advanced protocol fingerprinting
        if "impersonate" in transport_params and _PROTOCOL_FINGERPRINT_AVAILABLE:
            impersonate_profile = transport_params.get("impersonate", _DEFAULT_IMPERSONATE)

            # Determine HTTP version to use
            if _ENABLE_H3:
                http_version = select_http_version(supports_h3=True, prefer_h3=_PREFER_H3)
            else:
                # Default to HTTP/2
                http_version = "h2"

            # Set HTTP version if supported
            if http_version == "h3" and _ENABLE_H3:
                transport_params.setdefault("http_version", _normalize_http_version("h3"))
            elif http_version == "h2":
                transport_params.setdefault("http_version", _normalize_http_version("h2"))

            # Note: ALPN setting depends on curl_cffi support
            # This is placeholder - actual implementation may vary
            # transport_params.setdefault("alpn_protocols", alpn_protocols)

            # HTTP/2 specific settings
            if http_version == "h2":
                # Get browser type from impersonate profile
                if "chrome" in impersonate_profile:
                    browser_type = "chrome"
                elif "firefox" in impersonate_profile:
                    browser_type = "firefox"
                elif "safari" in impersonate_profile:
                    browser_type = "safari"
                else:
                    browser_type = "chrome"

                # Get HTTP/2 profile
                http2_profile = HTTP2_PROFILES.get(f"{browser_type}_default")
                if http2_profile:
                    # Apply HTTP/2 settings
                    # Note: These settings may need to be passed differently depending on curl_cffi API
                    transport_params.setdefault("http2_settings", http2_profile.to_dict())
        elif "impersonate" in transport_params:
            # Fallback: Just set HTTP/2 if no advanced fingerprinting
            transport_params.setdefault("http_version", _normalize_http_version("h2"))

        if "http_version" in transport_params:
            transport_params["http_version"] = _normalize_http_version(transport_params["http_version"])

        params.setdefault("transport", AsyncCurlTransport(**transport_params))  # type: ignore[misc]

    return httpx.AsyncClient(**params)


def create_client_with_ja3(
    ja3_string: str | None = None,
    cipher_suites: list[int] | None = None,
    tls_extensions: list[int] | None = None,
    alpn_protocols: list[str] | None = None,
    **kwargs: Any
) -> httpx.AsyncClient:
    """Create AsyncClient with manual JA3 control (experimental).

    Args:
        ja3_string: JA3 fingerprint string
        cipher_suites: List of cipher suite IDs
        tls_extensions: List of TLS extension IDs
        alpn_protocols: List of ALPN protocols
        **kwargs: Additional client kwargs

    Returns:
        httpx.AsyncClient configured with specified JA3 parameters

    Note:
        This is experimental and depends on curl_cffi having JA3 control APIs.
        May not work with all versions of httpx-curl-cffi.
    """
    if not USING_HTTPX_IMPERSONATE:
        # Fallback to standard client if impersonation not available
        return httpx.AsyncClient(**kwargs)

    params: dict[str, Any] = dict(kwargs)
    transport_params = _build_transport_kwargs(params)

    # Manual JA3 configuration (if supported by transport)
    if ja3_string:
        transport_params["ja3"] = ja3_string

    if cipher_suites:
        # Note: This API may not exist - placeholder for future support
        transport_params["cipher_suites"] = cipher_suites

    if tls_extensions:
        # Note: This API may not exist - placeholder for future support
        transport_params["tls_extensions"] = tls_extensions

    if alpn_protocols:
        transport_params["alpn_protocols"] = alpn_protocols

    params.setdefault("transport", AsyncCurlTransport(**transport_params))  # type: ignore[misc]

    return httpx.AsyncClient(**params)


__all__ = [
    "httpx",
    "USING_HTTPX_IMPERSONATE",
    "create_async_client",
    "create_client_with_ja3",
]

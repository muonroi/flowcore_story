"""Helpers for persisting and reusing site/proxy scoped cookies."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import socket
import threading
import time
import uuid
from urllib.parse import quote
from collections import OrderedDict
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any

from flowcore_story.utils.cookie_storage import get_cookie_storage
from flowcore_story.utils.logger import logger

_DEFAULT_PROXY_KEY = "__no_proxy__"

_NODE_PROXY_ID = (os.environ.get("COOKIE_NODE_ID") or socket.gethostname() or _DEFAULT_PROXY_KEY).strip()

if not _NODE_PROXY_ID:
    _NODE_PROXY_ID = _DEFAULT_PROXY_KEY

_ENTRY_DEFAULTS: dict[str, Any] = {
    "success_count": 0,
    "total_requests": 0,
    "error_429_count": 0,
    "rate_limit_ratio": 0.0,
    "consecutive_429": 0,
    "quality_score": 1.0,
    "cooldown_until": None,
    "last_status": None,
    "last_success_at": None,
    "last_rate_limited_at": None,
    "last_response_at": None,
    "tls_profile": None,
}

_COOLDOWN_SECONDS = float(os.environ.get("COOKIE_COOLDOWN_SECONDS", 120))
_MAX_COOLDOWN_SECONDS = float(os.environ.get("COOKIE_MAX_COOLDOWN_SECONDS", 3600))
_CONSECUTIVE_429_COOLDOWN_THRESHOLD = int(os.environ.get("COOKIE_COOLDOWN_THRESHOLD", 2))


@dataclass(frozen=True)
class CookieSelection:
    """Represents a stored cookie entry chosen for a request."""

    entry_id: str
    header: str
    user_agent: str | None
    fingerprint: str | None
    tls_profile: str | None
    cookies: list[dict[str, Any]]
    expires: float | None


@dataclass(frozen=True)
class CookieEntryInfo:
    """Summary information about a stored cookie entry."""

    entry_id: str
    proxy: str
    proxy_id: str
    expires: float | None
    updated_at: float | None
    last_used: float | None


_CHROME_VERSION_RE = re.compile(r"Chrome/(\d+)")
_EDGE_VERSION_RE = re.compile(r"(?:Edg|EdgiOS|EdgA|Edge)/(\d+)")
_FIREFOX_VERSION_RE = re.compile(r"Firefox/(\d+)")
_SAFARI_VERSION_RE = re.compile(r"Version/(\d+)(?:\.(\d+))?(?:\.(\d+))?")

_CHROME_VERSION_RANGE = (90, 130)
_EDGE_VERSION_RANGE = (90, 130)
_FIREFOX_VERSION_RANGE = (90, 130)
_SUPPORTED_SAFARI_PROFILES: set[str] = {
    "17_5",
    "17_4",
    "17_3",
    "17_2",
    "17_0",
    "16_6",
    "16_5",
    "16_3",
    "16_1",
    "16_0",
    "15_6",
    "15_5",
    "15_4",
    "15_3",
    "15_0",
    "14_1",
    "13_1",
}

_COOKIE_HEADER_SAFE_CHARS = "".join(chr(code) for code in range(0x20, 0x7F))


try:
    _configured_l1_capacity = int(os.environ.get("COOKIE_CACHE_L1_SIZE", "256"))
except ValueError:
    logger.warning(
        "[COOKIE][CACHE] Invalid COOKIE_CACHE_L1_SIZE=%r, falling back to default",
        os.environ.get("COOKIE_CACHE_L1_SIZE"),
    )
    _configured_l1_capacity = 256

_L1_CACHE_CAPACITY = max(_configured_l1_capacity, 0)
_L1_CACHE: OrderedDict[str, dict[str, Any]] = OrderedDict()
_L1_CACHE_LOCK = threading.RLock()


def _l1_cache_enabled() -> bool:
    return _L1_CACHE_CAPACITY > 0


def _l1_get(site_key: str) -> dict[str, Any] | None:
    if not _l1_cache_enabled():
        return None
    with _L1_CACHE_LOCK:
        payload = _L1_CACHE.get(site_key)
        if payload is None:
            return None
        _L1_CACHE.move_to_end(site_key, last=True)
        return copy.deepcopy(payload)


def _l1_set(site_key: str, payload: dict[str, Any]) -> None:
    if not _l1_cache_enabled():
        return
    snapshot = copy.deepcopy(payload)
    with _L1_CACHE_LOCK:
        _L1_CACHE[site_key] = snapshot
        _L1_CACHE.move_to_end(site_key, last=True)
        while len(_L1_CACHE) > _L1_CACHE_CAPACITY:
            _L1_CACHE.popitem(last=False)


def _l1_invalidate(site_key: str) -> None:
    if not _l1_cache_enabled():
        return
    with _L1_CACHE_LOCK:
        _L1_CACHE.pop(site_key, None)


def _version_in_range(version: str, bounds: tuple[int, int]) -> bool:
    try:
        major = int(version)
    except (TypeError, ValueError):
        return False
    lower, upper = bounds
    return lower <= major <= upper


def _derive_safari_profile(match: re.Match[str]) -> str | None:
    major_raw = match.group(1)
    minor_raw = match.group(2)
    try:
        major = int(major_raw) if major_raw is not None else None
    except (TypeError, ValueError):
        major = None
    if major is None:
        return None
    minor = 0
    if minor_raw is not None:
        try:
            minor = int(minor_raw)
        except (TypeError, ValueError):
            minor = 0
    candidate = f"{major}_{minor}"
    if candidate in _SUPPORTED_SAFARI_PROFILES:
        return f"safari{candidate}"
    fallback = f"{major}_0"
    if fallback in _SUPPORTED_SAFARI_PROFILES:
        return f"safari{fallback}"
    return None


def derive_tls_profile_from_user_agent(user_agent: str | None) -> str | None:
    """Best-effort guess of the impersonation profile for ``user_agent``."""

    if not user_agent:
        return None
    candidate = user_agent.strip()
    if not candidate:
        return None

    edge_match = _EDGE_VERSION_RE.search(candidate)
    if edge_match:
        version = edge_match.group(1)
        if _version_in_range(version, _EDGE_VERSION_RANGE):
            return f"edge{int(version)}"

    chrome_match = _CHROME_VERSION_RE.search(candidate)
    if chrome_match:
        version = chrome_match.group(1)
        if _version_in_range(version, _CHROME_VERSION_RANGE):
            return f"chrome{int(version)}"

    firefox_match = _FIREFOX_VERSION_RE.search(candidate)
    if firefox_match:
        version = firefox_match.group(1)
        if _version_in_range(version, _FIREFOX_VERSION_RANGE):
            return f"firefox{int(version)}"

    safari_match = _SAFARI_VERSION_RE.search(candidate)
    if safari_match:
        safari_profile = _derive_safari_profile(safari_match)
        if safari_profile:
            return safari_profile

    return None


def derive_tls_profile_from_headers(headers: Mapping[str, str] | None) -> str | None:
    """Best-effort guess of the TLS profile from the ``User-Agent`` header."""

    if not headers:
        return None
    return derive_tls_profile_from_user_agent(headers.get("User-Agent"))


def _normalize_proxy_key(proxy_url: str | None) -> str:
    return proxy_url.strip() if proxy_url else _DEFAULT_PROXY_KEY


def _normalize_proxy_identity(proxy_url: str | None, proxy_id: str | None) -> str:
    candidate = (proxy_id or "").strip()
    if candidate:
        return candidate
    if proxy_url:
        return _normalize_proxy_key(proxy_url)
    return _NODE_PROXY_ID or _DEFAULT_PROXY_KEY


def _load_payload(site_key: str, *, force_refresh: bool = False) -> dict[str, Any]:
    if not site_key:
        return {}
    if not force_refresh:
        cached = _l1_get(site_key)
        if cached is not None:
            return cached
    storage = get_cookie_storage()
    payload = storage.load(site_key)
    if not isinstance(payload, dict):
        payload = {}

    # Backwards compatibility for the legacy single-cookie layout.
    if "entries" not in payload:
        cookies = payload.get("cookies")
        header = payload.get("header")
        if isinstance(cookies, list) or isinstance(header, str):
            entry = {
                "id": str(uuid.uuid4()),
                "proxy": _DEFAULT_PROXY_KEY,
                "cookies": [cookie for cookie in (cookies or []) if isinstance(cookie, dict)],
                "header": header or "",
                "user_agent": None,
                "fingerprint": None,
                "expires": None,
                "updated_at": time.time(),
                "last_used": None,
            }
            payload = {"entries": [entry]}
        else:
            payload = {"entries": []}
    entries = payload.get("entries")
    if not isinstance(entries, list):
        payload["entries"] = []
    _l1_set(site_key, payload)
    return copy.deepcopy(payload)


def _save_payload(site_key: str, payload: dict[str, Any]) -> None:
    storage = get_cookie_storage()
    storage.save(site_key, payload)
    if not site_key:
        return
    _l1_set(site_key, payload)


def _ensure_ascii_cookie_part(value: str) -> str:
    try:
        value.encode("ascii")
    except UnicodeEncodeError:
        return quote(value, safe=_COOKIE_HEADER_SAFE_CHARS)
    return value


def _compose_cookie_header(cookies: Iterable[dict[str, Any]]) -> str | None:
    parts: list[str] = []
    for item in cookies or []:
        name = item.get("name")
        value = item.get("value")
        if not name or value is None:
            continue
        name = _ensure_ascii_cookie_part(str(name))
        value = _ensure_ascii_cookie_part(str(value))
        parts.append(f"{name}={value}")
    if not parts:
        return None
    return "; ".join(parts)


def fingerprint_headers(headers: Mapping[str, str] | None) -> str | None:
    """Compute a stable fingerprint for the given HTTP headers."""

    if not headers:
        return None
    normalized = {str(k).lower(): str(v) for k, v in headers.items() if v is not None}
    if not normalized:
        return None
    serialized = json.dumps(sorted(normalized.items()), ensure_ascii=False)
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


def _current_timestamp() -> float:
    return time.time()


def _coerce_float(value: Any) -> float | None:
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return None
    return candidate


def _clamp(value: float, *, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def _recalculate_quality(entry: dict[str, Any]) -> bool:
    total_requests = int(entry.get("total_requests") or 0)
    error_429 = int(entry.get("error_429_count") or 0)
    consecutive = int(entry.get("consecutive_429") or 0)

    if total_requests > 0:
        ratio = _clamp(error_429 / total_requests)
    else:
        ratio = 0.0

    penalty = min(0.5, 0.1 * consecutive)
    quality = _clamp(1.0 - ratio - penalty)

    modified = False
    if abs(float(entry.get("rate_limit_ratio") or 0.0) - ratio) > 1e-9:
        entry["rate_limit_ratio"] = ratio
        modified = True
    if abs(float(entry.get("quality_score") or 0.0) - quality) > 1e-9:
        entry["quality_score"] = quality
        modified = True
    return modified


def _ensure_entry_defaults(entry: dict[str, Any]) -> bool:
    modified = False
    for key, default in _ENTRY_DEFAULTS.items():
        if key not in entry:
            entry[key] = default
            modified = True
    desired_proxy_id = _normalize_proxy_identity(entry.get("proxy"), entry.get("proxy_id"))
    if entry.get("proxy_id") != desired_proxy_id:
        entry["proxy_id"] = desired_proxy_id
        modified = True
    if _recalculate_quality(entry):
        modified = True
    return modified


def _ensure_entries_defaults(entries: Sequence[dict[str, Any]]) -> bool:
    modified = False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if _ensure_entry_defaults(entry):
            modified = True
    return modified


def _prune_expired(entries: list[dict[str, Any]]) -> bool:
    now = _current_timestamp()
    original_len = len(entries)
    pruned: list[dict[str, Any]] = []
    for entry in entries:
        expires = entry.get("expires")
        if expires in (None, "", 0):
            pruned.append(entry)
            continue
        try:
            expires_val = float(expires)
        except (TypeError, ValueError):
            pruned.append(entry)
            continue
        if expires_val <= 0 or expires_val >= now:
            entry["expires"] = expires_val
            pruned.append(entry)
    entries[:] = pruned
    return len(entries) != original_len


def _profiles_compatible(existing: str | None, desired: str | None) -> bool:
    if existing and desired:
        return existing == desired
    return True


def _pick_entry(
    entries: Sequence[dict[str, Any]],
    proxy_key: str,
    proxy_id: str,
    *,
    fingerprint: str | None,
    user_agent: str | None,
    tls_profile: str | None,
    exclude_ids: set[str] | None,
    preferred_entry_id: str | None,
) -> dict[str, Any] | None:
    now = _current_timestamp()
    candidates: list[dict[str, Any]] = []
    for entry in entries:
        if entry.get("proxy") != proxy_key:
            continue
        entry_proxy_id = entry.get("proxy_id") or proxy_key
        if entry_proxy_id != proxy_id:
            continue
        cooldown_until = _coerce_float(entry.get("cooldown_until"))
        if cooldown_until and cooldown_until > now:
            continue
        candidates.append(entry)
    if not candidates:
        return None

    exclude_ids = exclude_ids or set()
    preferred_id = str(preferred_entry_id) if preferred_entry_id else None
    ordered = sorted(
        candidates,
        key=lambda e: (
            float(e.get("quality_score") or 0.0),
            float(e.get("last_used") or 0.0),
            float(e.get("updated_at") or 0.0),
        ),
        reverse=True,
    )

    if preferred_id:
        for entry in ordered:
            if str(entry.get("id")) != preferred_id:
                continue
            if entry.get("id") in exclude_ids:
                continue
            if not _profiles_compatible(entry.get("tls_profile"), tls_profile):
                continue
            return entry

    for entry in ordered:
        if entry.get("id") in exclude_ids:
            continue
        if not _profiles_compatible(entry.get("tls_profile"), tls_profile):
            continue
        if fingerprint and entry.get("fingerprint") == fingerprint:
            return entry

    if user_agent:
        for entry in ordered:
            if entry.get("id") in exclude_ids:
                continue
            if not _profiles_compatible(entry.get("tls_profile"), tls_profile):
                continue
            if entry.get("user_agent") == user_agent:
                return entry

    for entry in ordered:
        if entry.get("id") in exclude_ids:
            continue
        if not _profiles_compatible(entry.get("tls_profile"), tls_profile):
            continue
        return entry
    return None


def select_cookie(
    site_key: str,
    *,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
    fingerprint: str | None = None,
    user_agent: str | None = None,
    tls_profile: str | None = None,
    exclude_ids: set[str] | None = None,
    preferred_entry_id: str | None = None,
) -> CookieSelection | None:
    """Choose the best matching cookie entry for a request."""

    if not site_key:
        return None

    payload = _load_payload(site_key)
    entries = payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    modified = _ensure_entries_defaults(entries)
    if _prune_expired(entries):
        payload["entries"] = entries
        _save_payload(site_key, payload)
    elif modified:
        payload["entries"] = entries
        _save_payload(site_key, payload)

    proxy_key = _normalize_proxy_key(proxy_url)
    normalized_proxy_id = _normalize_proxy_identity(proxy_url, proxy_id)
    entry = _pick_entry(
        entries,
        proxy_key,
        normalized_proxy_id,
        fingerprint=fingerprint,
        user_agent=user_agent,
        tls_profile=tls_profile,
        exclude_ids=exclude_ids,
        preferred_entry_id=preferred_entry_id,
    )
    if not entry:
        return None

    entry_id = entry.setdefault("id", str(uuid.uuid4()))
    entry["last_used"] = _current_timestamp()
    payload["entries"] = entries
    _save_payload(site_key, payload)

    header = entry.get("header") or _compose_cookie_header(entry.get("cookies") or []) or ""
    cookies: list[dict[str, Any]] = [cookie for cookie in (entry.get("cookies") or []) if isinstance(cookie, dict)]
    return CookieSelection(
        entry_id=entry_id,
        header=header,
        user_agent=entry.get("user_agent"),
        fingerprint=entry.get("fingerprint"),
        tls_profile=entry.get("tls_profile"),
        cookies=cookies,
        expires=entry.get("expires"),
    )


def get_cookie_header(
    site_key: str,
    *,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
    fingerprint: str | None = None,
    user_agent: str | None = None,
    exclude_ids: set[str] | None = None,
    preferred_entry_id: str | None = None,
) -> str | None:
    """Return the cookie header string for a site/proxy combination."""

    tls_profile = derive_tls_profile_from_user_agent(user_agent)
    selection = select_cookie(
        site_key,
        proxy_url=proxy_url,
        proxy_id=proxy_id,
        fingerprint=fingerprint,
        user_agent=user_agent,
        tls_profile=tls_profile,
        exclude_ids=exclude_ids,
        preferred_entry_id=preferred_entry_id,
    )
    if selection and selection.header:
        return selection.header
    return None


def get_cookie_entry_info(
    site_key: str, *, proxy_url: str | None = None, proxy_id: str | None = None
) -> CookieEntryInfo | None:
    """Return the most recently updated cookie entry for ``site_key``."""

    if not site_key:
        return None

    payload = _load_payload(site_key)
    entries = payload.get("entries", [])
    if not isinstance(entries, list) or not entries:
        return None
    _ensure_entries_defaults(entries)

    proxy_key = _normalize_proxy_key(proxy_url)
    normalized_proxy_id = _normalize_proxy_identity(proxy_url, proxy_id)
    candidates = [
        entry
        for entry in entries
        if entry.get("proxy") == proxy_key
        and (entry.get("proxy_id") or proxy_key) == normalized_proxy_id
    ]
    if not candidates:
        return None

    def _sort_key(entry: dict[str, Any]) -> tuple[float, float]:
        return (
            float(entry.get("updated_at") or 0.0),
            float(entry.get("last_used") or 0.0),
        )

    chosen = max(candidates, key=_sort_key)
    return CookieEntryInfo(
        entry_id=str(chosen.get("id") or ""),
        proxy=str(proxy_key),
        proxy_id=str(chosen.get("proxy_id") or normalized_proxy_id),
        expires=_coerce_float(chosen.get("expires")),
        updated_at=_coerce_float(chosen.get("updated_at")),
        last_used=_coerce_float(chosen.get("last_used")),
    )


def _derive_entry_expires(cookies: Sequence[dict[str, Any]]) -> float | None:
    """Derive the earliest expiry time from a list of cookies.

    Handles both timestamp floats and date strings (e.g., 'Tue, 25 Nov 2025 18:57:43 GMT').
    """
    import email.utils

    expiries = []
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue

        expires_val = cookie.get("expires")
        if not expires_val:
            continue

        try:
            # Try to parse as float first (timestamp)
            if isinstance(expires_val, (int, float)):
                if expires_val > 0:
                    expiries.append(float(expires_val))
            elif isinstance(expires_val, str):
                # Try parsing as timestamp string
                try:
                    timestamp = float(expires_val)
                    if timestamp > 0:
                        expiries.append(timestamp)
                except ValueError:
                    # Parse as date string (e.g., "Tue, 25 Nov 2025 18:57:43 GMT")
                    try:
                        parsed_time = email.utils.parsedate_to_datetime(expires_val)
                        timestamp = parsed_time.timestamp()
                        if timestamp > 0:
                            expiries.append(timestamp)
                    except (TypeError, ValueError):
                        # Skip unparseable dates
                        continue
        except Exception:
            # Skip any cookie with unparseable expiry
            continue

    if not expiries:
        return None
    return min(expiries)


def _upsert_entry(
    entries: list[dict[str, Any]],
    new_entry: dict[str, Any],
) -> dict[str, Any]:
    proxy_key = new_entry.get("proxy")
    proxy_identity = new_entry.get("proxy_id") or proxy_key
    fingerprint = new_entry.get("fingerprint")
    user_agent = new_entry.get("user_agent")
    tls_profile = new_entry.get("tls_profile")

    for entry in entries:
        if entry.get("proxy") != proxy_key:
            continue
        entry_proxy_id = entry.get("proxy_id") or proxy_key
        if entry_proxy_id != proxy_identity:
            continue
        if (
            fingerprint
            and entry.get("fingerprint") == fingerprint
            and _profiles_compatible(entry.get("tls_profile"), tls_profile)
        ):
            preserved_last_used = entry.get("last_used")
            entry.update(new_entry)
            if preserved_last_used is not None and entry.get("last_used") is None:
                entry["last_used"] = preserved_last_used
            return entry
        if (
            user_agent
            and entry.get("user_agent") == user_agent
            and _profiles_compatible(entry.get("tls_profile"), tls_profile)
        ):
            preserved_last_used = entry.get("last_used")
            entry.update(new_entry)
            if preserved_last_used is not None and entry.get("last_used") is None:
                entry["last_used"] = preserved_last_used
            return entry
        if tls_profile and entry.get("tls_profile") == tls_profile:
            preserved_last_used = entry.get("last_used")
            entry.update(new_entry)
            if preserved_last_used is not None and entry.get("last_used") is None:
                entry["last_used"] = preserved_last_used
            return entry

    entries.append(new_entry)
    return new_entry


def set_cookie_header(
    site_key: str,
    header: str,
    *,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
    user_agent: str | None = None,
    fingerprint: str | None = None,
    tls_profile: str | None = None,
    expires: float | None = None,
) -> None:
    """Persist a raw Cookie header string for ``site_key`` and proxy."""

    if not site_key or not header:
        return

    payload = _load_payload(site_key)
    entries: list[dict[str, Any]] = payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []

    proxy_key = _normalize_proxy_key(proxy_url)
    normalized_proxy_id = _normalize_proxy_identity(proxy_url, proxy_id)

    tls_profile_value = tls_profile or derive_tls_profile_from_user_agent(user_agent)

    entry_payload = {
        "id": str(uuid.uuid4()),
        "proxy": proxy_key,
        "proxy_id": normalized_proxy_id,
        "cookies": [],
        "header": header,
        "user_agent": user_agent,
        "fingerprint": fingerprint,
        "tls_profile": tls_profile_value,
        "expires": expires,
        "updated_at": _current_timestamp(),
        "last_used": None,
    }
    stored = _upsert_entry(entries, entry_payload)
    stored.setdefault("id", entry_payload["id"])
    _ensure_entry_defaults(stored)
    payload["entries"] = entries
    _save_payload(site_key, payload)


def set_cookies(
    site_key: str,
    cookies: Iterable[dict[str, Any]],
    *,
    url: str | None = None,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
    headers: Mapping[str, str] | None = None,
    fingerprint: str | None = None,
    tls_profile: str | None = None,
    ttl_seconds: float | None = 1800.0,
) -> str | None:
    """Persist cookie dictionaries and return the composed Cookie header."""

    if not site_key:
        return None

    cookies_list = [cookie for cookie in cookies if isinstance(cookie, dict)]
    header = _compose_cookie_header(cookies_list) or ""
    payload = _load_payload(site_key)
    entries: list[dict[str, Any]] = payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []

    user_agent = headers.get("User-Agent") if headers else None
    fingerprint_value = fingerprint or fingerprint_headers(headers)
    tls_profile_value = tls_profile or derive_tls_profile_from_user_agent(user_agent)

    proxy_key = _normalize_proxy_key(proxy_url)
    normalized_proxy_id = _normalize_proxy_identity(proxy_url, proxy_id)

    derived = _derive_entry_expires(cookies_list)
    expires = derived
    if ttl_seconds is not None and ttl_seconds > 0:
        limit = _current_timestamp() + ttl_seconds
        if derived is None or derived > limit:
            expires = limit

    entry_payload = {
        "id": str(uuid.uuid4()),
        "url": url,
        "proxy": proxy_key,
        "proxy_id": normalized_proxy_id,
        "cookies": cookies_list,
        "header": header,
        "user_agent": user_agent,
        "fingerprint": fingerprint_value,
        "tls_profile": tls_profile_value,
        "expires": expires,
        "updated_at": _current_timestamp(),
        "last_used": None,
    }
    stored = _upsert_entry(entries, entry_payload)
    stored.setdefault("id", entry_payload["id"])
    _ensure_entry_defaults(stored)
    payload["entries"] = entries
    _save_payload(site_key, payload)
    return header or None


def record_cookie_feedback(
    site_key: str,
    entry_id: str,
    *,
    status_code: int | None = None,
    successful: bool = False,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
) -> None:
    """Update stored metadata for a cookie entry after a request."""

    if not site_key or not entry_id:
        return

    payload = _load_payload(site_key)
    entries = payload.get("entries", [])
    if not isinstance(entries, list) or not entries:
        return

    modified = False
    now = _current_timestamp()
    proxy_key = _normalize_proxy_key(proxy_url)
    normalized_proxy_id = _normalize_proxy_identity(proxy_url, proxy_id)

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("id")) != entry_id:
            continue
        if proxy_url is not None and entry.get("proxy") != proxy_key:
            continue
        if (entry.get("proxy_id") or proxy_key) != normalized_proxy_id:
            continue

        if _ensure_entry_defaults(entry):
            modified = True

        entry["total_requests"] = int(entry.get("total_requests") or 0) + 1
        modified = True

        if status_code is not None:
            entry["last_status"] = int(status_code)
            modified = True

        if status_code == 429:
            entry["error_429_count"] = int(entry.get("error_429_count") or 0) + 1
            entry["consecutive_429"] = int(entry.get("consecutive_429") or 0) + 1
            entry["last_rate_limited_at"] = now
            if entry.get("consecutive_429", 0) >= _CONSECUTIVE_429_COOLDOWN_THRESHOLD and _COOLDOWN_SECONDS > 0:
                backoff_step = entry["consecutive_429"] - _CONSECUTIVE_429_COOLDOWN_THRESHOLD + 1
                cooldown_seconds = min(
                    _MAX_COOLDOWN_SECONDS,
                    _COOLDOWN_SECONDS * (2 ** max(0, backoff_step - 1)),
                )
                candidate_until = now + cooldown_seconds
                existing_until = _coerce_float(entry.get("cooldown_until")) or 0.0
                entry["cooldown_until"] = max(existing_until, candidate_until)
            modified = True
        else:
            if status_code is not None and status_code != 429:
                entry["consecutive_429"] = 0
                modified = True

        if successful:
            entry["success_count"] = int(entry.get("success_count") or 0) + 1
            entry["last_success_at"] = now
            entry["consecutive_429"] = 0
            entry["cooldown_until"] = None
            modified = True

        if _recalculate_quality(entry):
            modified = True
        entry["last_response_at"] = now
        modified = True
        break

    if modified:
        payload["entries"] = entries
        _save_payload(site_key, payload)


def disable_cookie_entry(
    site_key: str,
    entry_id: str,
    *,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
    cooldown_seconds: float | None = None,
) -> bool:
    """Force a cookie entry into cooldown, effectively disabling it temporarily."""

    if not site_key or not entry_id:
        return False

    payload = _load_payload(site_key)
    entries = payload.get("entries", [])
    if not isinstance(entries, list) or not entries:
        return False

    now = _current_timestamp()
    proxy_key = _normalize_proxy_key(proxy_url)
    normalized_proxy_id = _normalize_proxy_identity(proxy_url, proxy_id)
    cooldown = _MAX_COOLDOWN_SECONDS if cooldown_seconds is None else max(0.0, float(cooldown_seconds))

    modified = False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("id")) != entry_id:
            continue
        if entry.get("proxy") != proxy_key:
            continue
        if (entry.get("proxy_id") or proxy_key) != normalized_proxy_id:
            continue

        if _ensure_entry_defaults(entry):
            modified = True

        entry["cooldown_until"] = now + cooldown
        entry["quality_score"] = 0.0
        entry["consecutive_429"] = int(entry.get("consecutive_429") or 0)
        entry["last_rate_limited_at"] = now
        modified = True
        logger.warning(
            "[COOKIE] Disabled entry %s for site %s via cooldown (proxy=%s)",
            entry_id,
            site_key,
            proxy_key,
        )
        break

    if modified:
        payload["entries"] = entries
        _save_payload(site_key, payload)
    return modified


def prewarm_cookie_cache(site_keys: Iterable[str]) -> None:
    """Populate the in-memory cache for ``site_keys`` using stored payloads."""

    if not site_keys or not _l1_cache_enabled():
        return
    warmed = 0
    for site_key in site_keys:
        if not site_key:
            continue
        try:
            payload = _load_payload(site_key, force_refresh=True)
        except Exception:  # pragma: no cover - defensive guard against backend errors
            logger.exception("[COOKIE][CACHE] Failed to prewarm cache for %s", site_key)
            _l1_invalidate(site_key)
            continue
        if payload.get("entries"):
            warmed += 1
    if warmed:
        logger.debug("[COOKIE][CACHE] Prewarmed L1 cache for %d site(s)", warmed)


async def apply_cookies_to_context(
    context,
    site_key: str | None,
    *,
    url: str | None = None,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
    headers: MutableMapping[str, str] | None = None,
    selection: CookieSelection | None = None,
) -> None:
    """Load stored cookies for ``site_key`` into the given Playwright context."""

    if not site_key:
        return

    if selection is None:
        fingerprint = fingerprint_headers(headers) if headers else None
        user_agent = headers.get("User-Agent") if headers else None
        tls_profile = derive_tls_profile_from_headers(headers)
        selection = select_cookie(
            site_key,
            proxy_url=proxy_url,
            proxy_id=proxy_id,
            fingerprint=fingerprint,
            user_agent=user_agent,
            tls_profile=tls_profile,
        )

    if selection is None:
        return

    if headers is not None and selection.user_agent:
        headers["User-Agent"] = selection.user_agent

    transferable: list[dict[str, Any]] = []
    for cookie in selection.cookies:
        prepared: dict[str, Any] = {}
        name = cookie.get("name")
        value = cookie.get("value")
        if not name or value is None:
            continue
        prepared["name"] = name
        prepared["value"] = value

        if cookie.get("domain"):
            prepared["domain"] = cookie["domain"]
        elif url:
            prepared["url"] = url
        if cookie.get("path"):
            prepared["path"] = cookie["path"]
        if cookie.get("expires"):
            prepared["expires"] = cookie["expires"]
        if cookie.get("httpOnly") is not None:
            prepared["httpOnly"] = bool(cookie["httpOnly"])
        if cookie.get("secure") is not None:
            prepared["secure"] = bool(cookie["secure"])
        same_site = cookie.get("sameSite")
        if same_site:
            prepared["sameSite"] = same_site
        transferable.append(prepared)

    if transferable:
        try:
            await context.add_cookies(transferable)
        except Exception:
            pass


async def store_context_cookies(
    context,
    site_key: str | None,
    *,
    proxy_url: str | None = None,
    proxy_id: str | None = None,
    headers: Mapping[str, str] | None = None,
    fingerprint: str | None = None,
) -> CookieEntryInfo | None:
    """Persist cookies captured from the Playwright context for ``site_key``."""

    if not site_key:
        return None
    try:
        cookies = await context.cookies()
    except Exception:
        return None
    tls_profile = derive_tls_profile_from_headers(headers)
    header = set_cookies(
        site_key,
        cookies,
        proxy_url=proxy_url,
        proxy_id=proxy_id,
        headers=headers,
        fingerprint=fingerprint,
        tls_profile=tls_profile,
    )
    if header is None:
        return None
    return get_cookie_entry_info(
        site_key,
        proxy_url=proxy_url,
        proxy_id=proxy_id,
    )


def get_expiring_entries(threshold_seconds: float = 300.0) -> list[tuple[str, CookieEntryInfo, dict[str, Any]]]:
    """Find cookie entries that are expiring soon."""
    storage = get_cookie_storage()
    site_keys = storage.list_keys()
    expiring = []
    now = _current_timestamp()
    limit = now + threshold_seconds

    for site_key in site_keys:
        payload = _load_payload(site_key)
        entries = payload.get("entries", [])
        for entry in entries:
            cooldown = _coerce_float(entry.get("cooldown_until"))
            if cooldown and cooldown > now:
                continue

            expires = _coerce_float(entry.get("expires"))
            if expires and now < expires < limit:
                if not entry.get("url"):
                    continue

                info = CookieEntryInfo(
                    entry_id=str(entry["id"]),
                    proxy=str(entry.get("proxy", _DEFAULT_PROXY_KEY)),
                    proxy_id=str(entry.get("proxy_id", "")),
                    expires=expires,
                    updated_at=_coerce_float(entry.get("updated_at")),
                    last_used=_coerce_float(entry.get("last_used")),
                )
                expiring.append((site_key, info, entry))
    return expiring


__all__ = [
    "CookieSelection",
    "CookieEntryInfo",
    "apply_cookies_to_context",
    "derive_tls_profile_from_headers",
    "derive_tls_profile_from_user_agent",
    "fingerprint_headers",
    "get_cookie_header",
    "get_cookie_entry_info",
    "get_expiring_entries",
    "prewarm_cookie_cache",
    "select_cookie",
    "set_cookie_header",
    "set_cookies",
    "record_cookie_feedback",
    "disable_cookie_entry",
    "store_context_cookies",
]

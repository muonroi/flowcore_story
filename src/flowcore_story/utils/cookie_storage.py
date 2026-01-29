"""Storage backends for persisting shared cookie payloads."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    redis = None  # type: ignore
    from flowcore_story.utils.logger import logger


class CookieStorage(ABC):
    """Abstract base class for cookie storage backends."""

    @abstractmethod
    def load(self, site_key: str) -> dict[str, Any]:
        """Return the stored payload for ``site_key`` or an empty mapping."""

    @abstractmethod
    def save(self, site_key: str, payload: dict[str, Any]) -> None:
        """Persist ``payload`` for ``site_key``."""

    @abstractmethod
    def list_keys(self) -> list[str]:
        """Return a list of all stored site keys."""


class SqliteCookieStorage(CookieStorage):
    """SQLite backed cookie storage suitable for multiple workers."""

    def __init__(self, db_path: str) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialise(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cookie_payloads (
                    site_key TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
                """
            )

    def load(self, site_key: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM cookie_payloads WHERE site_key = ?", (site_key,)
            ).fetchone()
        if not row:
            return {}
        try:
            return json.loads(row["payload"])
        except (TypeError, json.JSONDecodeError):
            logger.warning("[COOKIE][STORE] Invalid payload for site %s, resetting", site_key)
            return {}

    def save(self, site_key: str, payload: dict[str, Any]) -> None:
        serialized = json.dumps(payload, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cookie_payloads(site_key, payload, updated_at)
                VALUES(?, ?, strftime('%s', 'now'))
                ON CONFLICT(site_key) DO UPDATE SET
                    payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                (site_key, serialized),
            )

    def list_keys(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT site_key FROM cookie_payloads").fetchall()
        return [row["site_key"] for row in rows]


class RedisCookieStorage(CookieStorage):
    """Redis backed storage for cookie payloads."""

    def __init__(self, url: str, *, prefix: str = "cookie:payload:") -> None:
        if redis is None:  # pragma: no cover - runtime guard
            raise RuntimeError("redis package is required for RedisCookieStorage")
        self._redis = redis.Redis.from_url(url, decode_responses=True)
        self._prefix = prefix

    def _key(self, site_key: str) -> str:
        return f"{self._prefix}{site_key}"

    def load(self, site_key: str) -> dict[str, Any]:
        key = self._key(site_key)
        try:
            data = self._redis.get(key)
        except redis.RedisError as exc:
            logger.error("[COOKIE][STORE] Redis load failed for %s: %s", site_key, exc)
            return {}
        if not data:
            return {}
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            logger.warning("[COOKIE][STORE] Invalid Redis payload for %s, resetting", site_key)
            return {}

    def save(self, site_key: str, payload: dict[str, Any]) -> None:
        key = self._key(site_key)
        data = json.dumps(payload, ensure_ascii=False)
        try:
            self._redis.set(key, data)
        except redis.RedisError as exc:
            logger.error("[COOKIE][STORE] Redis save failed for %s: %s", site_key, exc)

    def list_keys(self) -> list[str]:
        try:
            keys = self._redis.keys(f"{self._prefix}*")
            return [key[len(self._prefix):] for key in keys]
        except redis.RedisError as exc:
            logger.error("[COOKIE][STORE] Redis list_keys failed: %s", exc)
            return []


_storage_instance: CookieStorage | None = None
_storage_lock = threading.Lock()


def _default_sqlite_path() -> str:
    base_dir = Path(os.environ.get("COOKIE_STORAGE_DIR", Path(__file__).resolve().parent.parent / "config"))
    base_dir.mkdir(parents=True, exist_ok=True)
    return str(base_dir / "cookies.sqlite3")


def _create_storage_from_url(url: str | None) -> CookieStorage:
    if not url:
        return SqliteCookieStorage(_default_sqlite_path())

    if url.startswith("redis://") or url.startswith("rediss://"):
        return RedisCookieStorage(url)

    if url.startswith("sqlite://"):
        # sqlite:///absolute/path or sqlite://relative/path
        path = url[len("sqlite://") :]
        if path.startswith("/"):
            # Handle Windows paths like /C:/Users/... -> C:/Users/...
            if len(path) > 2 and path[2] == ":":
                resolved = path[1:]  # Remove leading /
            else:
                resolved = path
        else:
            resolved = str((Path.cwd() / path).resolve())
        return SqliteCookieStorage(resolved)

    if url.startswith("file://"):
        return SqliteCookieStorage(url[len("file://") :])

    # Fallback: treat as direct filesystem path
    return SqliteCookieStorage(url)


def get_cookie_storage() -> CookieStorage:
    """Return the configured cookie storage backend."""

    global _storage_instance
    if _storage_instance is not None:
        return _storage_instance

    with _storage_lock:
        if _storage_instance is None:
            storage_url = os.environ.get("COOKIE_STORAGE_URL")
            _storage_instance = _create_storage_from_url(storage_url)
    return _storage_instance


__all__ = [
    "CookieStorage",
    "SqliteCookieStorage",
    "RedisCookieStorage",
    "get_cookie_storage",
]

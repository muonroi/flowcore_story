from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from flowcore_story.utils.logger import logger


@dataclass(slots=True)
class MetadataRecord:
    key: str
    data: dict[str, Any]


class MetadataStore:
    """Persist story metadata in a lightweight SQLite database."""

    def __init__(self, db_path: str) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._initialize()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=30, isolation_level=None)

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS story_metadata (
                    key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    def _normalise_key(self, metadata: dict[str, Any], fallback_key: str) -> str:
        key = metadata.get("url") or metadata.get("slug") or fallback_key
        return key

    def upsert(self, metadata: dict[str, Any], *, fallback_key: str) -> MetadataRecord:
        key = self._normalise_key(metadata, fallback_key)
        payload = json.dumps(metadata, ensure_ascii=False)

        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO story_metadata(key, data, updated_at)
                VALUES(?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET
                    data=excluded.data,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (key, payload),
            )
            logger.debug(
                "[META][DB] total_changes=%s path=%s", conn.total_changes, self._db_path
            )

        logger.debug("[META][DB] Đã lưu metadata vào SQLite cho key=%s", key)
        return MetadataRecord(key=key, data=metadata)

    def fetch(self, key: str) -> MetadataRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT key, data FROM story_metadata WHERE key=?", (key,)
            ).fetchone()
        if not row:
            return None
        stored_data = json.loads(row[1])
        return MetadataRecord(key=row[0], data=stored_data)


_metadata_store: MetadataStore | None = None


def get_metadata_store(db_path: str | None) -> MetadataStore | None:
    global _metadata_store
    if not db_path:
        return None
    if _metadata_store is None:
        _metadata_store = MetadataStore(db_path)
    elif Path(db_path) != _metadata_store.db_path:
        # Allow reloading configuration at runtime.
        _metadata_store = MetadataStore(db_path)
    return _metadata_store


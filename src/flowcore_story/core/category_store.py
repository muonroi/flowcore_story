from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from flowcore.utils.logger import logger


@dataclass(frozen=True)
class SnapshotInfo:
    """Metadata returned after persisting a snapshot."""

    id: int
    site_key: str
    version: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "site_key": self.site_key,
            "version": self.version,
            "created_at": self.created_at,
        }


class CategoryStore:
    """Persist category → story discovery snapshots in SQLite."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        directory = os.path.dirname(os.path.abspath(db_path))
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._initialise()

    def _initialise(self) -> None:
        attempts = 0
        while True:
            try:
                with self._connect() as conn:
                    conn.executescript(
                        """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA foreign_keys=ON;

                CREATE TABLE IF NOT EXISTS category_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_key TEXT NOT NULL,
                    version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(site_key, version)
                );

                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_key TEXT NOT NULL,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    metadata TEXT,
                    raw_data TEXT,
                    story_count INTEGER NOT NULL DEFAULT 0,
                    first_snapshot_id INTEGER,
                    last_seen_snapshot_id INTEGER,
                    ended_snapshot_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(site_key, url)
                );

                CREATE TABLE IF NOT EXISTS stories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_key TEXT NOT NULL,
                    url TEXT NOT NULL,
                    title TEXT,
                    data TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(site_key, url)
                );

                CREATE TABLE IF NOT EXISTS category_story_membership (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category_id INTEGER NOT NULL,
                    story_id INTEGER NOT NULL,
                    first_snapshot_id INTEGER NOT NULL,
                    last_seen_snapshot_id INTEGER NOT NULL,
                    ended_snapshot_id INTEGER,
                    position INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(category_id, story_id, first_snapshot_id),
                    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE,
                    FOREIGN KEY(story_id) REFERENCES stories(id) ON DELETE CASCADE,
                    FOREIGN KEY(first_snapshot_id) REFERENCES category_snapshots(id) ON DELETE CASCADE,
                    FOREIGN KEY(last_seen_snapshot_id) REFERENCES category_snapshots(id) ON DELETE CASCADE,
                    FOREIGN KEY(ended_snapshot_id) REFERENCES category_snapshots(id) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_categories_site_url
                    ON categories(site_key, url);
                CREATE INDEX IF NOT EXISTS idx_stories_site_url
                    ON stories(site_key, url);
                CREATE INDEX IF NOT EXISTS idx_membership_category_active
                    ON category_story_membership(category_id, ended_snapshot_id);
                CREATE INDEX IF NOT EXISTS idx_membership_story_active
                    ON category_story_membership(story_id, ended_snapshot_id);
                """
                    )
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempts >= 5:
                    raise
                attempts += 1
                backoff = min(1.0, 0.1 * attempts)
                time.sleep(backoff)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _normalise_json(self, data: dict[str, Any] | None) -> str | None:
        if not data:
            return None
        try:
            return json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
        except TypeError:
            serialisable = {key: repr(value) for key, value in data.items()}
            return json.dumps(serialisable, ensure_ascii=False, sort_keys=True)

    def persist_snapshot(
        self,
        site_key: str,
        crawl_plan,
        *,
        version: str | None = None,
    ) -> SnapshotInfo:
        from flowcore_story.core.crawl_planner import CrawlPlan

        if not isinstance(crawl_plan, CrawlPlan):  # defensive: accept duck-typed
            raise TypeError("crawl_plan must be a CrawlPlan instance")

        with self._lock:
            with self._connect() as conn:
                created_at = self._now()
                base_version = version or self._generate_version()
                snapshot_id, version_value = self._ensure_snapshot(
                    conn, site_key, base_version, created_at
                )
                seen_category_ids = []
                for category in crawl_plan.categories:
                    category_id = self._upsert_category(
                        conn,
                        site_key,
                        category.name,
                        category.url,
                        self._normalise_json(category.metadata),
                        self._normalise_json(category.raw_genre),
                        len(category.stories),
                        snapshot_id,
                        created_at,
                    )
                    seen_category_ids.append(category_id)
                    self._sync_category_membership(
                        conn,
                        category_id,
                        site_key,
                        category.stories,
                        snapshot_id,
                        created_at,
                    )
                self._finalise_missing_categories(
                    conn,
                    site_key,
                    seen_category_ids,
                    snapshot_id,
                    created_at,
                )
                conn.commit()
        logger.info(
            "[CATEGORY_STORE] Persisted snapshot %s for %s with %d categories",\
            version_value,
            site_key,
            len(crawl_plan.categories),
        )
        return SnapshotInfo(
            id=snapshot_id,
            site_key=site_key,
            version=version_value,
            created_at=created_at,
        )

    def _generate_version(self) -> str:
        return datetime.utcnow().strftime("%Y%m%d%H%M%S%f")

    def _ensure_snapshot(
        self,
        conn: sqlite3.Connection,
        site_key: str,
        version: str,
        created_at: str,
    ) -> tuple[int, str]:
        attempt = 0
        base_version = version
        while True:
            try_version = base_version if attempt == 0 else self._dedupe_version(base_version)
            try:
                cur = conn.execute(
                    """
                    INSERT INTO category_snapshots(site_key, version, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (site_key, try_version, created_at),
                )
                return int(cur.lastrowid), try_version
            except sqlite3.IntegrityError as exc:
                if "UNIQUE constraint failed" not in str(exc):
                    raise
                logger.warning(
                    "[CATEGORY_STORE] Snapshot version collision for %s/%s; generating a unique suffix",
                    site_key,
                    try_version,
                )
                attempt += 1

    def _dedupe_version(self, base_version: str) -> str:
        return f"{base_version}+{uuid.uuid4().hex[:8]}"

    def _upsert_category(
        self,
        conn: sqlite3.Connection,
        site_key: str,
        name: str,
        url: str,
        metadata_json: str | None,
        raw_data_json: str | None,
        story_count: int,
        snapshot_id: int,
        timestamp: str,
    ) -> int:
        row = conn.execute(
            "SELECT id, first_snapshot_id FROM categories WHERE site_key = ? AND url = ?",
            (site_key, url),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE categories
                   SET name = ?,
                       metadata = ?,
                       raw_data = ?,
                       story_count = ?,
                       last_seen_snapshot_id = ?,
                       ended_snapshot_id = NULL,
                       updated_at = ?
                 WHERE id = ?
                """,
                (
                    name,
                    metadata_json,
                    raw_data_json,
                    story_count,
                    snapshot_id,
                    timestamp,
                    int(row["id"]),
                ),
            )
            return int(row["id"])
        cur = conn.execute(
            """
            INSERT INTO categories(
                site_key, name, url, metadata, raw_data, story_count,
                first_snapshot_id, last_seen_snapshot_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_key,
                name,
                url,
                metadata_json,
                raw_data_json,
                story_count,
                snapshot_id,
                snapshot_id,
                timestamp,
                timestamp,
            ),
        )
        return int(cur.lastrowid)

    def _sync_category_membership(
        self,
        conn: sqlite3.Connection,
        category_id: int,
        site_key: str,
        stories: Iterable[dict[str, Any]],
        snapshot_id: int,
        timestamp: str,
    ) -> None:
        seen_story_ids = set()
        for index, story in enumerate(stories, start=1):
            if not isinstance(story, dict):
                continue
            story_url = story.get("url")
            if not story_url:
                logger.warning(
                    "[CATEGORY_STORE] Bỏ qua story thiếu URL trong category %s", category_id
                )
                continue
            story_id = self._upsert_story(conn, site_key, story, timestamp)
            seen_story_ids.add(story_id)
            existing = conn.execute(
                """
                SELECT id FROM category_story_membership
                 WHERE category_id = ? AND story_id = ? AND ended_snapshot_id IS NULL
                """,
                (category_id, story_id),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE category_story_membership
                       SET last_seen_snapshot_id = ?,
                           position = ?,
                           updated_at = ?
                     WHERE id = ?
                    """,
                    (snapshot_id, index, timestamp, int(existing["id"])),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO category_story_membership(
                        category_id, story_id, first_snapshot_id,
                        last_seen_snapshot_id, position, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        category_id,
                        story_id,
                        snapshot_id,
                        snapshot_id,
                        index,
                        timestamp,
                        timestamp,
                    ),
                )
        stale_rows = conn.execute(
            """
            SELECT id FROM category_story_membership
             WHERE category_id = ?
               AND ended_snapshot_id IS NULL
               AND last_seen_snapshot_id < ?
            """,
            (category_id, snapshot_id),
        ).fetchall()
        for row in stale_rows:
            conn.execute(
                """
                UPDATE category_story_membership
                   SET ended_snapshot_id = ?,
                       updated_at = ?
                 WHERE id = ?
                """,
                (snapshot_id, timestamp, int(row["id"])),
            )

    def _upsert_story(
        self,
        conn: sqlite3.Connection,
        site_key: str,
        story: dict[str, Any],
        timestamp: str,
    ) -> int:
        url = story.get("url")
        title = story.get("title") or story.get("name")
        extra = {k: v for k, v in story.items() if k not in {"title", "name", "url"}}
        data_json = self._normalise_json(extra) if extra else None
        row = conn.execute(
            "SELECT id, title, data FROM stories WHERE site_key = ? AND url = ?",
            (site_key, url),
        ).fetchone()
        if row:
            needs_update = False
            if title != row["title"]:
                needs_update = True
            if (row["data"] or "") != (data_json or ""):
                needs_update = True
            if needs_update:
                conn.execute(
                    """
                    UPDATE stories
                       SET title = ?,
                           data = ?,
                           updated_at = ?
                     WHERE id = ?
                    """,
                    (title, data_json, timestamp, int(row["id"])),
                )
            else:
                conn.execute(
                    "UPDATE stories SET updated_at = ? WHERE id = ?",
                    (timestamp, int(row["id"])),
                )
            return int(row["id"])
        cur = conn.execute(
            """
            INSERT INTO stories(site_key, url, title, data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (site_key, url, title, data_json, timestamp, timestamp),
        )
        return int(cur.lastrowid)

    def ensure_story_record(self, site_key: str, story: dict[str, Any]) -> int:
        """Ensure a story exists in the ``stories`` table and return its id."""

        if not isinstance(story, dict):
            raise TypeError("story must be a mapping")

        with self._lock:
            with self._connect() as conn:
                timestamp = self._now()
                return self._upsert_story(conn, site_key, story, timestamp)

    def get_story_id(self, site_key: str, story_url: str) -> int | None:
        """Look up the ``stories`` id for ``story_url`` if it exists."""

        if not story_url:
            return None

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT id FROM stories WHERE site_key = ? AND url = ?",
                    (site_key, story_url),
                ).fetchone()
                return int(row["id"]) if row else None

    def get_story_site_key(self, story_id: int) -> str | None:
        """Return the ``site_key`` owning ``story_id`` if it exists."""

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT site_key FROM stories WHERE id = ?",
                    (story_id,),
                ).fetchone()
                return str(row["site_key"]) if row else None

    def get_site_keys_for_story_ids(self, story_ids: Iterable[int]) -> dict[int, str]:
        """Resolve multiple story ids to their owning ``site_key`` values."""

        ids = [int(story_id) for story_id in set(story_ids)]
        if not ids:
            return {}

        placeholders = ",".join("?" for _ in ids)
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    f"SELECT id, site_key FROM stories WHERE id IN ({placeholders})",
                    ids,
                ).fetchall()
                return {int(row["id"]): str(row["site_key"]) for row in rows}

    def list_active_site_keys(self) -> Iterable[str]:
        """Return site keys that currently have categories or stories."""

        try:
            with self._lock:
                with self._connect() as conn:
                    rows = conn.execute(
                        "SELECT DISTINCT site_key FROM stories ORDER BY site_key",
                    ).fetchall()
                    return [str(row["site_key"]) for row in rows]
        except Exception:
            # Return empty list if table doesn't exist yet
            return []

    def count_active_genres(self) -> int:
        """Count active categories that have not been archived."""

        try:
            with self._lock:
                with self._connect() as conn:
                    row = conn.execute(
                        "SELECT COUNT(*) AS cnt FROM categories WHERE ended_snapshot_id IS NULL",
                    ).fetchone()
                    return int(row["cnt"] or 0)
        except Exception:
            # Return 0 if table doesn't exist yet
            return 0

    def _finalise_missing_categories(
        self,
        conn: sqlite3.Connection,
        site_key: str,
        seen_category_ids: Iterable[int],
        snapshot_id: int,
        timestamp: str,
    ) -> None:
        seen_ids = list(seen_category_ids)
        params: list[Any]
        if seen_ids:
            placeholders = ",".join("?" for _ in seen_ids)
            params = [site_key, *seen_ids]
            query = (
                """
                SELECT id FROM categories
                 WHERE site_key = ?
                   AND ended_snapshot_id IS NULL
                   AND id NOT IN ("""
                + placeholders
                + ")"
            )
        else:
            params = [site_key]
            query = (
                """
                SELECT id FROM categories
                 WHERE site_key = ?
                   AND ended_snapshot_id IS NULL
                """
            )
        rows = conn.execute(query, params).fetchall()
        for row in rows:
            conn.execute(
                """
                UPDATE categories
                   SET ended_snapshot_id = ?,
                       updated_at = ?
                 WHERE id = ?
                """,
                (snapshot_id, timestamp, int(row["id"])),
            )

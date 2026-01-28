"""Storage backends for :mod:`core.story_registry`."""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from flowcore_story.core.category_store import CategoryStore
from flowcore_story.core.story_registry_models import (
    ACQUIRABLE_STATUSES,
    TERMINAL_STATUSES,
    StoryCrawlStatus,
    StoryRegistryEntry,
)
from flowcore.utils.logger import logger


class StoryRegistryBackend(Protocol):
    """Protocol describing a pluggable registry storage backend."""

    def ensure_entry(
        self,
        site_key: str,
        story: Mapping[str, Any],
        *,
        category_name: str | None = None,
    ) -> tuple[StoryRegistryEntry, bool]:
        ...

    def try_acquire(
        self,
        story_id: int,
        *,
        category_name: str | None = None,
    ) -> tuple[StoryRegistryEntry | None, bool]:
        ...

    def mark_status(
        self,
        story_id: int,
        status: StoryCrawlStatus,
        *,
        result: str | None = None,
        category_name: str | None = None,
    ) -> tuple[StoryRegistryEntry | None, bool]:
        ...

    def get_entry(self, story_id: int) -> StoryRegistryEntry | None:
        ...

    def get_backlog_overview(self) -> dict[str, Any]:
        ...

    def reset_in_progress(
        self,
        *,
        site_key: str | None = None,
        older_than_seconds: float | None = None,
        story_ids: Iterable[int] | None = None,
    ) -> tuple[int, bool]:
        ...

    def mark_story_pending(
        self,
        *,
        story_id: int | None = None,
        site_key: str | None = None,
        story_url: str | None = None,
    ) -> tuple[StoryRegistryEntry | None, bool]:
        ...

    def mark_category_pending(
        self, site_key: str, category_name: str
    ) -> tuple[int, bool]:
        ...


class SQLiteStoryRegistryBackend:
    """SQLite-backed registry storage with optional configuration hooks."""

    def __init__(self, db_path: str, category_store: CategoryStore) -> None:
        self.db_path = db_path
        directory = os.path.dirname(os.path.abspath(db_path))
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        self._store = category_store
        self._lock = threading.Lock()
        self._missing_tables_logged: set[str] = set()
        self._initialise()

    # -- connection helpers -------------------------------------------------

    def _connect(
        self,
        *,
        site_key: str | None = None,
        story_id: int | None = None,
    ) -> sqlite3.Connection:
        del site_key, story_id  # Unused in the base implementation
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialise(self) -> None:
        with self._connect() as conn:
            self._initialise_connection(conn)

    def _initialise_connection(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS story_crawl_registry (
                story_id INTEGER PRIMARY KEY,
                site_key TEXT NOT NULL,
                status TEXT NOT NULL,
                last_category TEXT,
                last_crawled_at TEXT,
                last_result TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(story_id) REFERENCES stories(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_story_registry_status
                ON story_crawl_registry(status);
            CREATE INDEX IF NOT EXISTS idx_story_registry_last_crawled
                ON story_crawl_registry(last_crawled_at);
            CREATE INDEX IF NOT EXISTS idx_story_registry_category
                ON story_crawl_registry(last_category);
            CREATE INDEX IF NOT EXISTS idx_story_registry_site
                ON story_crawl_registry(site_key);
            """
        )

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        try:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table_name,),
            ).fetchone()
        except sqlite3.Error:
            return False
        return bool(row)

    def _log_missing_table(self, table_name: str) -> None:
        if table_name in self._missing_tables_logged:
            return
        logger.warning(
            "[REGISTRY][SQLite] Missing table '%s' when computing backlog metrics for %s. "
            "Returning partial metrics until the table is created or restored.",
            table_name,
            self.db_path,
        )
        self._missing_tables_logged.add(table_name)

    # -- shared helpers -----------------------------------------------------

    def _ensure_story_id(self, site_key: str, story: Mapping[str, Any]) -> int:
        url = story.get("url") if isinstance(story, Mapping) else None
        if not url:
            raise ValueError("story missing required 'url' field for registry tracking")
        return self._store.ensure_story_record(site_key, dict(story))

    # -- mutation helpers ---------------------------------------------------

    def ensure_entry(
        self,
        site_key: str,
        story: Mapping[str, Any],
        *,
        category_name: str | None = None,
    ) -> tuple[StoryRegistryEntry, bool]:
        story_id = self._ensure_story_id(site_key, story)
        should_emit = False
        with self._lock:
            with self._connect(site_key=site_key, story_id=story_id) as conn:
                now = self._now()
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                if row:
                    if category_name and row["last_category"] != category_name:
                        conn.execute(
                            "UPDATE story_crawl_registry SET last_category = ?, updated_at = ? WHERE story_id = ?",
                            (category_name, now, story_id),
                        )
                        row = conn.execute(
                            "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                            (story_id,),
                        ).fetchone()
                        should_emit = True
                    assert row is not None
                    entry = StoryRegistryEntry.from_row(row)
                else:
                    conn.execute(
                        """
                        INSERT INTO story_crawl_registry(
                            story_id, site_key, status, last_category, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            story_id,
                            site_key,
                            StoryCrawlStatus.PENDING.value,
                            category_name,
                            now,
                            now,
                        ),
                    )
                    row = conn.execute(
                        "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                        (story_id,),
                    ).fetchone()
                    assert row is not None
                    entry = StoryRegistryEntry.from_row(row)
                    should_emit = True
        return entry, should_emit

    def try_acquire(
        self,
        story_id: int,
        *,
        category_name: str | None = None,
    ) -> tuple[StoryRegistryEntry | None, bool]:
        should_emit = False
        with self._lock:
            with self._connect(story_id=story_id) as conn:
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                if not row:
                    return None, False

                current_entry = StoryRegistryEntry.from_row(row)
                if current_entry.status not in ACQUIRABLE_STATUSES:
                    return current_entry, False

                now = self._now()
                cursor = conn.execute(
                    """
                    UPDATE story_crawl_registry
                       SET status = ?,
                           updated_at = ?,
                           last_crawled_at = ?,
                           last_category = COALESCE(?, last_category),
                           last_result = NULL
                     WHERE story_id = ?
                       AND status = ?
                    """,
                    (
                        StoryCrawlStatus.IN_PROGRESS.value,
                        now,
                        now,
                        category_name,
                        story_id,
                        current_entry.status.value,
                    ),
                )
                if cursor.rowcount == 0:
                    row = conn.execute(
                        "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                        (story_id,),
                    ).fetchone()
                    return (StoryRegistryEntry.from_row(row) if row else None, False)

                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                assert row is not None
                should_emit = True
                entry = StoryRegistryEntry.from_row(row)
        return entry, should_emit

    def mark_status(
        self,
        story_id: int,
        status: StoryCrawlStatus,
        *,
        result: str | None = None,
        category_name: str | None = None,
    ) -> tuple[StoryRegistryEntry | None, bool]:
        should_emit = False
        entry: StoryRegistryEntry | None
        with self._lock:
            with self._connect(story_id=story_id) as conn:
                now = self._now()
                last_crawled_at = now if status != StoryCrawlStatus.PENDING else None
                conn.execute(
                    """
                    UPDATE story_crawl_registry
                       SET status = ?,
                           last_result = ?,
                           updated_at = ?,
                           last_category = COALESCE(?, last_category),
                           last_crawled_at = COALESCE(?, last_crawled_at)
                     WHERE story_id = ?
                    """,
                    (
                        status.value,
                        result,
                        now,
                        category_name,
                        last_crawled_at,
                        story_id,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                entry = StoryRegistryEntry.from_row(row) if row else None
                should_emit = True if entry else False
        return entry, should_emit

    def get_entry(self, story_id: int) -> StoryRegistryEntry | None:
        with self._lock:
            with self._connect(story_id=story_id) as conn:
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (story_id,),
                ).fetchone()
                return StoryRegistryEntry.from_row(row) if row else None

    # -- reporting ----------------------------------------------------------

    def _collect_category_payload(self, row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "category_id": int(row["category_id"]),
            "category_name": row["category_name"],
            "category_url": row["category_url"],
            "site_key": row["site_key"],
            "backlog": int(row["backlog"]),
        }

    def _fetch_backlog_overview(
        self,
        conn: sqlite3.Connection,
        *,
        site_key: str | None = None,
    ) -> dict[str, Any]:
        terminal_statuses = tuple(status.value for status in TERMINAL_STATUSES)
        placeholders = ",".join("?" for _ in terminal_statuses) or "''"

        backlog_filters = [f"status NOT IN ({placeholders})"]
        backlog_params: list[Any] = list(terminal_statuses)
        if site_key:
            backlog_filters.append("site_key = ?")
            backlog_params.append(site_key)

        backlog_where = " WHERE " + " AND ".join(backlog_filters)

        total_backlog = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
              FROM story_crawl_registry
            {backlog_where}
            """,
            backlog_params,
        ).fetchone()["cnt"]

        in_progress_filters = ["status = ?"]
        in_progress_params: list[Any] = [StoryCrawlStatus.IN_PROGRESS.value]
        if site_key:
            in_progress_filters.append("site_key = ?")
            in_progress_params.append(site_key)
        in_progress_where = " WHERE " + " AND ".join(in_progress_filters)

        in_progress = conn.execute(
            f"""
            SELECT COUNT(*) AS cnt
              FROM story_crawl_registry
            {in_progress_where}
            """,
            in_progress_params,
        ).fetchone()["cnt"]

        has_categories = self._table_exists(conn, "categories")
        has_membership = self._table_exists(conn, "category_story_membership")

        planned_total = 0
        total_genres = 0
        genres_done = 0
        category_rows: list[sqlite3.Row] = []

        if has_categories:
            planned_filters = ["ended_snapshot_id IS NULL"]
            planned_params: list[Any] = []
            if site_key:
                planned_filters.append("site_key = ?")
                planned_params.append(site_key)
            planned_where = (
                " WHERE " + " AND ".join(planned_filters) if planned_filters else ""
            )

            planned_total = conn.execute(
                f"""
                SELECT COALESCE(SUM(story_count), 0) AS cnt
                  FROM categories
                {planned_where}
                """,
                planned_params,
            ).fetchone()["cnt"]
        else:
            self._log_missing_table("categories")

        category_filters = [f"r.status NOT IN ({placeholders})"]
        category_params: list[Any] = list(terminal_statuses)
        if site_key:
            category_filters.append("r.site_key = ?")
            category_params.append(site_key)
        category_where = " WHERE " + " AND ".join(category_filters)

        if has_categories and has_membership:
            category_rows = conn.execute(
                f"""
                SELECT c.id            AS category_id,
                       c.name          AS category_name,
                       c.url           AS category_url,
                       c.site_key      AS site_key,
                       COUNT(DISTINCT r.story_id) AS backlog
                  FROM story_crawl_registry AS r
                  JOIN category_story_membership AS m
                    ON m.story_id = r.story_id
                   AND m.ended_snapshot_id IS NULL
                  JOIN categories AS c
                    ON c.id = m.category_id
                {category_where}
              GROUP BY c.id, c.name, c.url, c.site_key
              ORDER BY backlog DESC, c.name COLLATE NOCASE
                """,
                category_params,
            ).fetchall()

            total_genres = conn.execute(
                "SELECT COUNT(*) AS cnt FROM categories WHERE ended_snapshot_id IS NULL",
            ).fetchone()["cnt"]

            categories_with_backlog = {int(row["category_id"]) for row in category_rows}
            genres_done = max(total_genres - len(categories_with_backlog), 0)
        elif has_categories and not has_membership:
            self._log_missing_table("category_story_membership")
            total_genres = conn.execute(
                "SELECT COUNT(*) AS cnt FROM categories WHERE ended_snapshot_id IS NULL",
            ).fetchone()["cnt"]
        else:
            # Nothing else to compute if categories table does not exist
            total_genres = 0

        return {
            "generated_at": self._now(),
            "total_backlog": int(total_backlog or 0),
            "planned_total": int(planned_total or 0),
            "stories_active": int(in_progress or 0),
            "total_genres": int(total_genres or 0),
            "genres_done": int(genres_done),
            "by_category": [self._collect_category_payload(row) for row in category_rows],
        }

    def get_backlog_overview(self) -> dict[str, Any]:
        with self._lock:
            with self._connect() as conn:
                return self._fetch_backlog_overview(conn)

    # -- reset helpers ------------------------------------------------------

    def reset_in_progress(
        self,
        *,
        site_key: str | None = None,
        older_than_seconds: float | None = None,
        story_ids: Iterable[int] | None = None,
    ) -> tuple[int, bool]:
        filters: list[str] = []
        params: list[Any] = []

        if site_key:
            filters.append("site_key = ?")
            params.append(site_key)

        if story_ids:
            ids = list(story_ids)
            if ids:
                placeholders = ",".join("?" for _ in ids)
                filters.append(f"story_id IN ({placeholders})")
                params.extend(ids)

        if older_than_seconds is not None:
            cutoff_ts = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(time.time() - float(older_than_seconds)),
            )
            filters.append("updated_at <= ?")
            params.append(cutoff_ts)

        where_clause = " AND ".join(filters)
        if where_clause:
            where_clause = " AND " + where_clause

        updated = 0
        with self._lock:
            with self._connect(site_key=site_key) as conn:
                now = self._now()
                cursor = conn.execute(
                    f"""
                    UPDATE story_crawl_registry
                       SET status = ?,
                           last_result = NULL,
                           updated_at = ?,
                           last_crawled_at = NULL
                     WHERE status = ?{where_clause}
                    """,
                    (
                        StoryCrawlStatus.NEEDS_RETRY.value,
                        now,
                        StoryCrawlStatus.IN_PROGRESS.value,
                        *params,
                    ),
                )
                updated = cursor.rowcount
        return updated, bool(updated)

    def mark_story_pending(
        self,
        *,
        story_id: int | None = None,
        site_key: str | None = None,
        story_url: str | None = None,
    ) -> tuple[StoryRegistryEntry | None, bool]:
        resolved_id: int | None = None
        if story_id is not None:
            resolved_id = story_id
        elif site_key and story_url:
            resolved_id = self._store.get_story_id(site_key, story_url)
        else:
            raise ValueError("Must provide story_id or site_key and story_url")

        if not resolved_id:
            return None, False

        should_emit = False
        entry: StoryRegistryEntry | None
        with self._lock:
            with self._connect(site_key=site_key, story_id=resolved_id) as conn:
                now = self._now()
                conn.execute(
                    """
                    UPDATE story_crawl_registry
                       SET status = ?,
                           last_result = NULL,
                           last_crawled_at = NULL,
                           updated_at = ?
                     WHERE story_id = ?
                    """,
                    (
                        StoryCrawlStatus.PENDING.value,
                        now,
                        resolved_id,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM story_crawl_registry WHERE story_id = ?",
                    (resolved_id,),
                ).fetchone()
                entry = StoryRegistryEntry.from_row(row) if row else None
                should_emit = True if entry else False
        return entry, should_emit

    def mark_category_pending(
        self, site_key: str, category_name: str
    ) -> tuple[int, bool]:
        if not category_name:
            return 0, False

        updated = 0
        with self._lock:
            with self._connect(site_key=site_key) as conn:
                now = self._now()
                cursor = conn.execute(
                    """
                    UPDATE story_crawl_registry
                       SET status = ?,
                           last_result = NULL,
                           last_crawled_at = NULL,
                           updated_at = ?
                     WHERE site_key = ?
                       AND last_category = ?
                    """,
                    (
                        StoryCrawlStatus.PENDING.value,
                        now,
                        site_key,
                        category_name,
                    ),
                )
                updated = cursor.rowcount
        return updated, bool(updated)


class ShardedSQLiteStoryRegistryBackend(SQLiteStoryRegistryBackend):
    """Shard registry data by ``site_key`` using independent SQLite files."""

    def __init__(
        self,
        db_path: str,
        category_store: CategoryStore,
        *,
        shard_directory: str | None = None,
    ) -> None:
        self._base_db_path = os.path.abspath(db_path)
        self._basename = os.path.basename(self._base_db_path)
        self._shard_directory = os.path.abspath(
            shard_directory or os.path.dirname(self._base_db_path)
        )
        self._initialised_paths: set[str] = set()
        super().__init__(db_path, category_store)

    def _initialise(self) -> None:
        # Only initialise the metadata database to keep compatibility with the
        # original layout. Individual shards will be initialised lazily.
        with super()._connect() as conn:
            self._initialise_connection(conn)

    def _resolve_site_key(self, story_id: int | None, site_key: str | None) -> str | None:
        if site_key:
            return site_key
        if story_id is None:
            return None
        return self._store.get_story_site_key(story_id)

    def _resolve_db_path(self, site_key: str | None) -> str:
        if not site_key:
            return self._base_db_path
        filename = f"{site_key}_{self._basename}" if self._basename else site_key
        return os.path.join(self._shard_directory, filename)

    def _connect(
        self,
        *,
        site_key: str | None = None,
        story_id: int | None = None,
    ) -> sqlite3.Connection:
        resolved_site = self._resolve_site_key(story_id, site_key)
        db_path = self._resolve_db_path(resolved_site)
        directory = os.path.dirname(db_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=OFF")

        if db_path not in self._initialised_paths:
            self._initialise_connection(conn)
            self._initialised_paths.add(db_path)
        return conn

    def _initialise_connection(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS story_crawl_registry (
                story_id INTEGER PRIMARY KEY,
                site_key TEXT NOT NULL,
                status TEXT NOT NULL,
                last_category TEXT,
                last_crawled_at TEXT,
                last_result TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_story_registry_status
                ON story_crawl_registry(status);
            CREATE INDEX IF NOT EXISTS idx_story_registry_last_crawled
                ON story_crawl_registry(last_crawled_at);
            CREATE INDEX IF NOT EXISTS idx_story_registry_category
                ON story_crawl_registry(last_category);
            CREATE INDEX IF NOT EXISTS idx_story_registry_site
                ON story_crawl_registry(site_key);
            """
        )

    def get_backlog_overview(self) -> dict[str, Any]:
        # Aggregate statistics across every known site using the category store
        # to discover active shards.
        summaries: dict[str, Any] = {
            "generated_at": self._now(),
            "total_backlog": 0,
            "planned_total": 0,
            "stories_active": 0,
            "total_genres": self._store.count_active_genres(),
            "genres_done": 0,
            "by_category": [],
        }

        category_map: dict[tuple[int, str], dict[str, Any]] = {}
        categories_with_backlog: set[tuple[int, str]] = set()

        for site in self._store.list_active_site_keys():
            with self._lock:
                with self._connect(site_key=site) as conn:
                    site_summary = self._fetch_backlog_overview(conn, site_key=site)

            summaries["total_backlog"] += site_summary.get("total_backlog", 0)
            summaries["planned_total"] += site_summary.get("planned_total", 0)
            summaries["stories_active"] += site_summary.get("stories_active", 0)

            for category in site_summary.get("by_category", []):
                key = (int(category["category_id"]), category["site_key"])
                categories_with_backlog.add(key)
                if key not in category_map:
                    category_map[key] = dict(category)
                else:
                    category_map[key]["backlog"] += int(category["backlog"])

        summaries["genres_done"] = max(
            summaries["total_genres"] - len(categories_with_backlog),
            0,
        )

        summaries["by_category"] = sorted(
            category_map.values(),
            key=lambda item: (-int(item["backlog"]), str(item["category_name"]).lower()),
        )

        return summaries

    def reset_in_progress(
        self,
        *,
        site_key: str | None = None,
        older_than_seconds: float | None = None,
        story_ids: Iterable[int] | None = None,
    ) -> tuple[int, bool]:
        if site_key:
            return super().reset_in_progress(
                site_key=site_key,
                older_than_seconds=older_than_seconds,
                story_ids=story_ids,
            )

        resolved_sites: set[str] = set()
        if story_ids:
            resolved_sites.update(
                filter(
                    None,
                    self._store.get_site_keys_for_story_ids(story_ids).values(),
                )
            )

        if not resolved_sites:
            resolved_sites.update(self._store.list_active_site_keys())

        total_updated = 0
        emitted = False
        for site in resolved_sites:
            updated, should_emit = super().reset_in_progress(
                site_key=site,
                older_than_seconds=older_than_seconds,
                story_ids=story_ids,
            )
            total_updated += updated
            emitted = emitted or should_emit
        return total_updated, emitted

    def mark_story_pending(
        self,
        *,
        story_id: int | None = None,
        site_key: str | None = None,
        story_url: str | None = None,
    ) -> tuple[StoryRegistryEntry | None, bool]:
        resolved_site = site_key
        if not resolved_site and story_id is not None:
            resolved_site = self._store.get_story_site_key(story_id)
        return super().mark_story_pending(
            story_id=story_id,
            site_key=resolved_site,
            story_url=story_url,
        )

    def mark_category_pending(
        self, site_key: str, category_name: str
    ) -> tuple[int, bool]:
        return super().mark_category_pending(site_key, category_name)

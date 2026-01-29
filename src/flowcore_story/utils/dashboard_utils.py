"""Helpers for building canonical dashboard signatures shared across services."""

from __future__ import annotations

from typing import Any


def _coerce_optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_category_signature(entry: dict[str, Any]) -> tuple[Any, ...]:
    return (
        entry.get("site_key"),
        entry.get("category_url") or entry.get("category_name"),
        _coerce_optional_int(entry.get("backlog")),
    )


def _build_genre_signature(entry: dict[str, Any]) -> tuple[Any, ...]:
    genres = []
    raw_genres = entry.get("genres")
    if isinstance(raw_genres, list):
        for genre in raw_genres:
            if not isinstance(genre, dict):
                continue
            genres.append(
                (
                    genre.get("url"),
                    genre.get("name"),
                    _coerce_optional_int(genre.get("stories")),
                    _coerce_optional_int(genre.get("total_stories")),
                    genre.get("status"),
                )
            )
    genres.sort()
    return entry.get("site_key"), tuple(genres)


def build_overview_signature(snapshot: dict[str, Any]) -> tuple[Any, ...]:
    """Produce a tuple describing the high level dashboard overview state."""

    aggregates = snapshot.get("aggregates") if isinstance(snapshot.get("aggregates"), dict) else {}
    registry_payload = snapshot.get("registry") if isinstance(snapshot.get("registry"), dict) else {}
    system_payload = snapshot.get("system") if isinstance(snapshot.get("system"), dict) else {}

    aggregates_signature = (
        _coerce_optional_int(aggregates.get("stories_in_progress")),
        _coerce_optional_int(aggregates.get("stories_completed")),
        _coerce_optional_int(aggregates.get("stories_skipped")),
        _coerce_optional_int(aggregates.get("genres_total_configured")),
        _coerce_optional_int(aggregates.get("genres_total_completed")),
        _coerce_optional_int(aggregates.get("registry_total_backlog")),
        _coerce_optional_int(aggregates.get("registry_planned_total")),
        _coerce_optional_int(aggregates.get("registry_stories_active")),
        _coerce_optional_int(aggregates.get("registry_total_genres")),
        _coerce_optional_int(aggregates.get("registry_genres_done")),
    )

    categories_signature: list[tuple[Any, ...]] = []
    categories_payload = registry_payload.get("by_category")
    if isinstance(categories_payload, list):
        for entry in categories_payload:
            if not isinstance(entry, dict):
                continue
            categories_signature.append(_build_category_signature(entry))
    categories_signature.sort()

    site_genres_signature: list[tuple[Any, ...]] = []
    site_genres_payload = snapshot.get("site_genres")
    if isinstance(site_genres_payload, list):
        for entry in site_genres_payload:
            if not isinstance(entry, dict):
                continue
            site_genres_signature.append(_build_genre_signature(entry))
    site_genres_signature.sort()

    completed_ids: list[str] = []
    stories_payload = snapshot.get("stories") if isinstance(snapshot.get("stories"), dict) else {}
    completed_entries = stories_payload.get("completed")
    if isinstance(completed_entries, list):
        for item in completed_entries:
            if isinstance(item, dict) and item.get("id") is not None:
                completed_ids.append(str(item.get("id")))
    completed_ids.sort()

    system_signature = tuple(
        (key, system_payload[key])
        for key in sorted(system_payload.keys())
        if system_payload.get(key) is not None
    )

    return (
        aggregates_signature,
        tuple(categories_signature),
        tuple(site_genres_signature),
        tuple(completed_ids),
        system_signature,
    )


__all__ = ["build_overview_signature"]

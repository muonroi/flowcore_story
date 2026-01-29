#!/usr/bin/env python3
"""Hiển thị trạng thái crawl hiện tại dựa trên ``state/dashboard.json``."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

DEFAULT_DASHBOARD_FILE = os.environ.get(
    "STORYFLOW_DASHBOARD_FILE",
    os.path.join("state", "dashboard.json"),
)

SYSTEM_FIELD_LABELS = {
    "proxies_in_use": "Proxy đang dùng",
    "kafka_queue_backlog": "Kafka backlog",
    "kafka_queue_status": "Kafka trạng thái",
    "kafka_queue_error": "Kafka cảnh báo",
    "kafka_queue_partitions": "Kafka partitions",
    "retry_queue_total_enqueued": "Retry tổng đã enqueue",
    "retry_queue_last_alert": "Retry cảnh báo gần nhất",
    "skipped_queue_size": "Skip queue",
    "registry_backlog": "Backlog registry",
    "stories_in_progress": "Truyện đang crawl",
    "stories_completed": "Truyện hoàn thành",
    "stories_skipped": "Truyện bị skip",
}

for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8")


ANSI_COLORS = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "red": "\033[31m",
    "yellow": "\033[33m",
    "green": "\033[32m",
    "cyan": "\033[36m",
}


def _supports_color() -> bool:
    if os.environ.get("NO_COLOR") is not None:
        return False
    stream = getattr(sys.stdout, "isatty", None)
    return bool(stream and stream())


def _colorize(text: str, color: str, *, bold: bool = False, enabled: bool = True) -> str:
    if not enabled or color not in ANSI_COLORS:
        return text
    prefix = ""
    if bold:
        prefix += ANSI_COLORS["bold"]
    prefix += ANSI_COLORS[color]
    return f"{prefix}{text}{ANSI_COLORS['reset']}"


def _color_for_ratio(ratio: float) -> str:
    if ratio < 0.35:
        return "red"
    if ratio < 0.7:
        return "yellow"
    return "green"


def render_progress_bar(
    current: int | None,
    total: int | None,
    *,
    width: int = 24,
    colorize: bool = False,
) -> str:
    if not total or total <= 0:
        return str(current or 0)

    current_value = max(current or 0, 0)
    ratio = min(max(current_value / total, 0.0), 1.0)
    filled_units = int(round(ratio * width))
    filled_units = min(max(filled_units, 0), width)
    if filled_units == 0 and current_value > 0:
        filled_units = 1
    if filled_units == width - 1 and current_value >= total:
        filled_units = width
    bar_body = "█" * filled_units + "░" * (width - filled_units)
    percent_text = f"{ratio * 100:5.1f}%"
    if colorize:
        color = _color_for_ratio(ratio)
        bar_body = _colorize(bar_body, color, enabled=True)
        percent_text = _colorize(percent_text, color, bold=True, enabled=True)
    return f"[{bar_body}] {percent_text} ({current_value}/{total})"


def _format_ratio_line(
    label: str,
    current: int | None,
    total: int | None,
    *,
    colorize: bool,
) -> str | None:
    if total and total > 0:
        return f"  - {label}: {render_progress_bar(current, total, colorize=colorize)}"
    if current is None:
        return None
    return f"  - {label}: {current}"



def load_dashboard(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy dashboard tại {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _compute_story_percent(story: dict[str, Any]) -> int | None:
    """Tính toán phần trăm chương đã crawl của một truyện."""

    crawled = _coerce_int(story.get("crawled_chapters")) or 0
    missing = _coerce_int(story.get("missing_chapters")) or 0
    total = _coerce_int(story.get("total_chapters")) or 0

    crawled = max(crawled, 0)
    missing = max(missing, 0)
    total = max(total, 0)

    if (story.get("status") or "").lower() == "completed":
        return 100

    inferred_total = crawled + missing
    dynamic_total = total if total > 0 else inferred_total

    if dynamic_total <= 0:
        return None

    percent = int((crawled / dynamic_total) * 100)
    return max(0, min(percent, 100))


def _latest_by_updated_at(entries: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    entries = list(entries)
    if not entries:
        return None

    def _key(item: dict[str, Any]) -> str:
        ts = item.get("updated_at")
        return ts if isinstance(ts, str) else ""

    return max(entries, key=_key)


def _extract_global_story_totals(data: dict[str, Any]) -> tuple[int, int | None]:
    global_payload = data.get("global_story_totals")
    completed = None
    total_estimate = None
    if isinstance(global_payload, dict):
        completed = _coerce_int(global_payload.get("completed"))
        total_estimate = _coerce_int(global_payload.get("total_estimate"))

    aggregates = data.get("aggregates", {})
    if completed is None:
        completed = _coerce_int(aggregates.get("globally_completed_stories_count"))

    if completed is None:
        state_section = data.get("crawl_state")
        if isinstance(state_section, dict):
            urls = state_section.get("globally_completed_story_urls")
            if isinstance(urls, list):
                completed = len(urls)

    if completed is None:
        completed = _coerce_int(aggregates.get("stories_completed"))

    registry_summary = data.get("registry", {})
    if total_estimate is None:
        total_estimate = _coerce_int(registry_summary.get("planned_total"))
    if total_estimate is None:
        total_estimate = _coerce_int(aggregates.get("registry_planned_total"))

    if total_estimate is None:
        site_genres = data.get("site_genres", [])
        accumulated = 0
        for site in site_genres:
            genres = site.get("genres")
            if not isinstance(genres, list):
                continue
            for genre in genres:
                total_value = _coerce_int(genre.get("total_stories"))
                if total_value:
                    accumulated += max(total_value, 0)
        total_estimate = accumulated or None

    completed_value = max(completed or 0, 0)
    if isinstance(total_estimate, int) and total_estimate < completed_value:
        total_estimate = completed_value

    return completed_value, total_estimate if isinstance(total_estimate, int) else None


def _format_system_metrics(system: dict[str, Any]) -> list[str]:
    formatted: list[str] = []
    for key, value in sorted(system.items()):
        label = SYSTEM_FIELD_LABELS.get(key, key.replace("_", " ").capitalize())
        if isinstance(value, float):
            if value.is_integer():
                value_display = str(int(value))
            else:
                value_display = f"{value:.2f}"
        else:
            value_display = str(value)
        formatted.append(f"{label}: {value_display}")
    return formatted


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        return None


def _format_timedelta(delta: datetime | None) -> tuple[str, float | None]:
    if delta is None:
        return "-", None
    now = datetime.now(UTC)
    seconds = (now - delta).total_seconds()
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{int(seconds)} giây trước", seconds
    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)} phút trước", seconds
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f} giờ trước", seconds
    days = hours / 24
    return f"{days:.1f} ngày trước", seconds


def _categorize_alert(seconds_since_update: float | None) -> str | None:
    if seconds_since_update is None:
        return None
    if seconds_since_update >= 60 * 30:
        return "danger"
    if seconds_since_update >= 60 * 10:
        return "warning"
    return None


def _detect_alerts(data: dict[str, Any]) -> list[tuple[str, str]]:
    alerts: list[tuple[str, str]] = []
    updated_at = _parse_timestamp(data.get("updated_at"))
    _, seconds_since_update = _format_timedelta(updated_at)
    level = _categorize_alert(seconds_since_update)
    if level:
        alerts.append(("Dashboard chưa được cập nhật trong thời gian dài", level))

    aggregates = data.get("aggregates", {})
    registry_summary = data.get("registry", {})
    stories_in_progress = _coerce_int(aggregates.get("stories_in_progress")) or 0
    backlog_total = _coerce_int(registry_summary.get("total_backlog")) or 0
    if backlog_total > 0 and stories_in_progress == 0:
        alerts.append(("Crawler không xử lý backlog dù vẫn còn công việc", "danger"))

    system_metrics = data.get("system", {}) or {}
    kafka_error = system_metrics.get("kafka_queue_error")
    if kafka_error:
        alerts.append((f"Kafka cảnh báo: {kafka_error}", "danger"))
    kafka_status = (system_metrics.get("kafka_queue_status") or "").lower()
    if kafka_status and kafka_status not in {"healthy", "ok", "connected"}:
        alerts.append((f"Kafka trạng thái bất thường: {kafka_status}", "warning"))

    retry_alert = system_metrics.get("retry_queue_last_alert")
    if retry_alert:
        alerts.append((f"Retry queue cảnh báo: {retry_alert}", "warning"))

    skipped_queue_size = _coerce_int(aggregates.get("skipped_queue_size")) or 0
    if skipped_queue_size > 0:
        severity = "warning" if skipped_queue_size < 100 else "danger"
        alerts.append((f"Skip queue đang có {skipped_queue_size} mục", severity))

    sites = data.get("sites", [])
    if isinstance(sites, list):
        for site in sites:
            failure_rate = site.get("failure_rate")
            if not isinstance(failure_rate, (int, float)):
                continue
            severity = None
            if failure_rate >= 0.5:
                severity = "danger"
            elif failure_rate >= 0.25:
                severity = "warning"
            if severity:
                alerts.append(
                    (
                        f"Site {site.get('site_key')} có tỷ lệ lỗi {failure_rate * 100:.1f}%",
                        severity,
                    )
                )
    return alerts


def build_updated_timestamp_line(data: dict[str, Any]) -> tuple[str, str | None]:
    updated_at = _parse_timestamp(data.get("updated_at"))
    relative_text, seconds_since_update = _format_timedelta(updated_at)
    timestamp_line = f"Cập nhật lúc: {data.get('updated_at', '-')}"
    if relative_text != "-":
        timestamp_line += f" ({relative_text})"
    level = _categorize_alert(seconds_since_update)
    return timestamp_line, level


def summarize_alerts(data: dict[str, Any], *, colorize: bool = False) -> list[str]:
    lines: list[str] = []
    alerts = _detect_alerts(data)
    if not alerts:
        return lines
    for message, severity in alerts:
        color = "red" if severity == "danger" else "yellow"
        lines.append(_colorize(message, color, bold=False, enabled=colorize))
    return lines


def summarize_key_metrics(data: dict[str, Any], *, colorize: bool = False) -> list[str]:
    aggregates = data.get("aggregates", {})
    registry_summary = data.get("registry", {})
    lines: list[str] = []
    completed_global, total_global = _extract_global_story_totals(data)
    line = _format_ratio_line(
        "Tiến độ truyện toàn hệ thống",
        completed_global,
        total_global,
        colorize=colorize,
    )
    if line:
        lines.append(line)

    planned_total = _coerce_int(registry_summary.get("planned_total"))
    stories_completed = _coerce_int(aggregates.get("stories_completed"))
    if stories_completed is not None:
        line = _format_ratio_line(
            "Truyện hoàn thành so với kế hoạch",
            stories_completed,
            planned_total,
            colorize=colorize,
        )
        if line:
            lines.append(line)

    genres_total = _coerce_int(registry_summary.get("total_genres")) or _coerce_int(
        aggregates.get("genres_total_configured")
    )
    genres_done = _coerce_int(registry_summary.get("genres_done")) or _coerce_int(
        aggregates.get("genres_total_completed")
    )
    if genres_done is not None:
        line = _format_ratio_line(
            "Thể loại đã xử lý",
            genres_done,
            genres_total,
            colorize=colorize,
        )
        if line:
            lines.append(line)

    backlog_total = _coerce_int(registry_summary.get("total_backlog"))
    if backlog_total is not None:
        backlog_text = f"  - Backlog còn lại: {backlog_total}"
        severity = None
        if backlog_total > 0 and backlog_total < 50:
            severity = "green"
        elif backlog_total >= 200:
            severity = "red"
        elif backlog_total >= 100:
            severity = "yellow"
        if severity and colorize:
            backlog_text = _colorize(backlog_text, severity, enabled=True)
        lines.append(backlog_text)

    return lines


def format_genre_summary(genre: dict[str, Any]) -> list[str]:
    site_key = genre.get("site_key") or "-"
    name = genre.get("name") or "?"
    position = _coerce_int(genre.get("position"))
    total_genres = _coerce_int(genre.get("total_genres"))
    prefix = f"[{site_key}] "
    if position and total_genres:
        prefix += f"({position}/{total_genres}) "

    total_pages = _coerce_int(genre.get("total_pages"))
    crawled_pages = _coerce_int(genre.get("crawled_pages")) or 0
    completed_pages = max(crawled_pages, 0)
    if total_pages:
        completed_pages = min(completed_pages, total_pages)
        page_progress = f"{completed_pages}/{total_pages}"
    else:
        page_progress = str(completed_pages)
    current_page = _coerce_int(genre.get("current_page"))
    status = genre.get("status") or ""
    page_note = None
    if current_page and current_page > completed_pages:
        page_note = f"đang lấy trang {current_page}"
    elif status == "processing_stories" and total_pages:
        page_note = "đã quét đủ"

    processed = _coerce_int(genre.get("processed_stories")) or 0
    processed = max(processed, 0)
    total_candidates: list[int] = []
    stories_collection = genre.get("stories")
    if isinstance(stories_collection, list):
        total_candidates.append(len(stories_collection))
    total_stories_value = _coerce_int(genre.get("total_stories"))
    if total_stories_value is not None:
        total_candidates.append(max(total_stories_value, 0))
    total_stories = max(total_candidates) if total_candidates else None
    if total_stories is not None:
        total_stories = max(total_stories, processed)
        story_progress = f"{processed}/{total_stories}"
    else:
        story_progress = str(processed)

    active_details = genre.get("active_story_details") or []
    active_stories = genre.get("active_stories") or []
    active_count = len(active_details) or len(active_stories)

    headline = f"  - {prefix}{name} — Trang {page_progress}"
    if page_note:
        headline += f" ({page_note})"
    headline += f", Truyện {story_progress}"
    if active_count:
        headline += f" (đang mở {active_count} truyện)"
    lines_out: list[str] = [headline]

    if status:
        lines_out.append(f"      Trạng thái: {status.replace('_', ' ')}")

    current_story_title = genre.get("current_story_title")
    current_story_page = _coerce_int(genre.get("current_story_page"))
    current_story_position = _coerce_int(genre.get("current_story_position"))
    if current_story_title:
        detail_bits = []
        if current_story_page:
            detail_bits.append(f"trang {current_story_page}")
        if current_story_position:
            detail_bits.append(f"vị trí #{current_story_position}")
        suffix = f" ({', '.join(detail_bits)})" if detail_bits else ""
        lines_out.append(f"      Đang xử lý: {current_story_title}{suffix}")

    if active_details:
        rendered = []
        for item in active_details[:5]:
            title = item.get("title")
            if not title:
                continue
            bits = []
            page_val = _coerce_int(item.get("page"))
            if page_val:
                bits.append(f"trang {page_val}")
            pos_val = _coerce_int(item.get("position"))
            if pos_val:
                bits.append(f"#{pos_val}")
            note = " (" + ", ".join(bits) + ")" if bits else ""
            rendered.append(f"{title}{note}")
        if rendered:
            lines_out.append("      Hàng đợi truyện: " + "; ".join(rendered))
    elif active_stories:
        preview = ", ".join(active_stories[:5])
        lines_out.append(f"      Tiến độ truyện: {preview}")
        if len(active_stories) > 5:
            lines_out.append(f"      ... và {len(active_stories) - 5} truyện khác")

    last_error = genre.get("last_error")
    if last_error:
        lines_out.append(f"      Lỗi gần nhất: {last_error}")

    return lines_out

def print_dashboard(data: dict[str, Any]) -> None:
    aggregates = data.get("aggregates", {})
    sites = data.get("sites", [])
    registry_summary = data.get("registry", {})
    colorize = _supports_color()

    print("=== StoryFlow Crawl Dashboard ===")
    timestamp_line, level = build_updated_timestamp_line(data)
    if level == "danger":
        timestamp_line = _colorize(timestamp_line, "red", bold=True, enabled=colorize)
    elif level == "warning":
        timestamp_line = _colorize(timestamp_line, "yellow", enabled=colorize)
    print(timestamp_line)
    print()

    key_metrics = summarize_key_metrics(data, colorize=colorize)
    if key_metrics:
        title = _colorize("Chỉ số chính:", "cyan", bold=True, enabled=colorize)
        print(title)
        for line in key_metrics:
            print(line)
        print()

    alerts = summarize_alerts(data, colorize=colorize)
    if alerts:
        header = _colorize("⚠️  Cảnh báo phát hiện:", "red", bold=True, enabled=colorize)
        print(header)
        for alert in alerts:
            print(f"  - {alert}")
        print()

    print("Tổng quan:")
    print(f"  - Truyện đang crawl : {aggregates.get('stories_in_progress', 0)}")
    print(f"  - Truyện hoàn thành: {aggregates.get('stories_completed', 0)}")
    print(f"  - Truyện bị skip   : {aggregates.get('stories_skipped', 0)}")
    print(f"  - Tổng chương thiếu: {aggregates.get('total_missing_chapters', 0)}")
    print(f"  - Hàng đợi skip    : {aggregates.get('skipped_queue_size', 0)}")
    backlog_total = _coerce_int(registry_summary.get("total_backlog")) or 0
    print(f"  - Backlog toàn hệ thống: {backlog_total}")
    planned_total = _coerce_int(registry_summary.get("planned_total"))
    if planned_total is not None:
        print(f"  - Tổng truyện đã lên kế hoạch: {planned_total}")
    stories_active = _coerce_int(registry_summary.get("stories_active"))
    if stories_active is not None:
        print(f"  - Truyện đang active (IN_PROGRESS): {stories_active}")
    total_genres = _coerce_int(registry_summary.get("total_genres"))
    genres_done = _coerce_int(registry_summary.get("genres_done")) or 0
    if total_genres is not None:
        print(f"  - Tổng thể loại: {total_genres} (đã xong {genres_done})")
    genres_total = _coerce_int(aggregates.get("genres_total_configured"))
    genres_done = _coerce_int(aggregates.get("genres_total_completed")) or 0
    if genres_total is not None:
        print(f"  - Đã crawl thể loại: {genres_done}/{genres_total}")
    else:
        print(f"  - Đã crawl thể loại: {genres_done}")

    completed_global, total_global = _extract_global_story_totals(data)
    if total_global is not None:
        print(f"  - Tổng truyện đã crawl: {completed_global}/{total_global}")
    else:
        print(f"  - Tổng truyện đã crawl: {completed_global}")
    print()

    system_metrics = data.get("system", {})
    if isinstance(system_metrics, dict):
        print("Tình trạng hệ thống:")
        if system_metrics:
            for line in _format_system_metrics(system_metrics):
                text = line
                lowered = line.lower()
                if "cảnh báo" in lowered or "lỗi" in lowered:
                    text = _colorize(text, "red", enabled=colorize)
                print(f"  - {text}")
        else:
            print("  - (Chưa có dữ liệu)")
        print()

    stories_section = data.get("stories", {})
    active = stories_section.get("in_progress", [])
    active_genres = data.get("genres", {}).get("in_progress", [])

    current_genre = _latest_by_updated_at(active_genres)
    current_story = _latest_by_updated_at(active)

    status_announced = False
    if current_genre:
        genre_name_display = current_genre.get("name") or current_genre.get("url") or "?"
        genre_position = _coerce_int(current_genre.get("position"))
        genre_total = _coerce_int(current_genre.get("total_genres"))
        if genre_total is None:
            genre_total = total_genres
        if genre_position and genre_total:
            print(f"Đang crawl: {genre_name_display} ({genre_position}/{genre_total} thể loại)")
            status_announced = True
        elif genre_total:
            print(f"Đang crawl: {genre_name_display} (tổng {genre_total} thể loại)")
            status_announced = True
        current_story_title = current_genre.get("current_story_title")
        story_position = _coerce_int(current_genre.get("current_story_position"))
        story_total = _coerce_int(current_genre.get("total_stories"))
        processed_stories = _coerce_int(current_genre.get("processed_stories")) or 0
        if story_total is None and processed_stories:
            story_total = max(processed_stories, story_position or 0)
        if story_position and story_total:
            story_total = max(story_total, story_position)
        if current_story_title and story_position:
            total_display = story_total or story_position
            print(
                f"Truyện đang xử lý: {current_story_title} thuộc {genre_name_display}, "
                f"{story_position}/{total_display}"
            )
            status_announced = True
    if status_announced:
        print()

    if current_genre or current_story:
        print("Tiến độ hiện tại:")
    if current_genre:
        site_key = current_genre.get("site_key") or "-"
        genre_name = current_genre.get("name") or current_genre.get("url") or "?"
        total_pages = _coerce_int(current_genre.get("total_pages"))
        crawled_pages = _coerce_int(current_genre.get("crawled_pages")) or 0
        current_page = _coerce_int(current_genre.get("current_page"))
        if current_page:
            if total_pages:
                page_text = f"{current_page}/{total_pages}"
            else:
                page_text = str(current_page)
        elif total_pages:
            page_text = f"{crawled_pages}/{total_pages}"
        else:
            page_text = str(crawled_pages)
        status = current_genre.get("status")
        status_text = f" — trạng thái: {status.replace('_', ' ')}" if status else ""
        print(
            f"  - Thể loại: [{site_key}] {genre_name} — trang hiện tại: {page_text}{status_text}"
        )
    if current_story:
        title = current_story.get("title") or current_story.get("id") or "?"
        crawled_chapters = _coerce_int(current_story.get("crawled_chapters")) or 0
        total_chapters = _coerce_int(current_story.get("total_chapters"))
        if total_chapters:
            chapter_text = f"{crawled_chapters}/{total_chapters}"
        else:
            chapter_text = str(crawled_chapters)
        missing_chapters = _coerce_int(current_story.get("missing_chapters"))
        details: list[str] = []
        genre_name = current_story.get("genre_name")
        if genre_name:
            details.append(f"Thể loại: {genre_name}")
        genre_site_key = current_story.get("genre_site_key") or current_story.get("primary_site")
        if genre_site_key:
            details.append(f"Site: {genre_site_key}")
        detail_suffix = f" ({'; '.join(details)})" if details else ""
        print(f"  - Truyện: {title} — chương hiện tại: {chapter_text}{detail_suffix}")
        if missing_chapters:
            print(f"      Còn thiếu: {missing_chapters} chương")
    if current_genre or current_story:
        print()

    if active:
        print("Đang crawl:")
        for story in active[:10]:  # show top 10
            details: list[str] = []
            genre_name = story.get("genre_name")
            if genre_name:
                details.append(f"thể loại: {genre_name}")
            genre_site_key = story.get("genre_site_key") or story.get("primary_site")
            if genre_site_key:
                details.append(f"site: {genre_site_key}")
            detail_suffix = f" ({'; '.join(details)})" if details else ""
            percent = _compute_story_percent(story)
            percent_text = f", tiến độ {percent}%" if percent is not None else ""
            print(
                f"  * {story.get('title')} — {story.get('crawled_chapters', 0)}/"
                f"{story.get('total_chapters', 0)} chương, còn thiếu {story.get('missing_chapters', 0)}"
                f"{percent_text}{detail_suffix}"
            )
        if len(active) > 10:
            print(f"  ... và {len(active) - 10} truyện khác")
        print()

    if active_genres:
        print("Thể loại đang xử lý:")
        for genre in active_genres[:10]:
            for line in format_genre_summary(genre):
                print(line)
        if len(active_genres) > 10:
            print(f"  ... và {len(active_genres) - 10} thể loại khác")
        print()

    if sites:
        print("Sức khỏe site:")
        for site in sorted(sites, key=lambda item: item.get("failure_rate", 0), reverse=True):
            failure_rate = site.get("failure_rate", 0.0)
            print(
                f"  - {site.get('site_key')}: {site.get('success', 0)} OK / {site.get('failure', 0)} lỗi"
                f" (tỷ lệ lỗi {failure_rate * 100:.2f}%)"
            )
        print()

    site_genres = data.get("site_genres", [])
    if site_genres:
        print("Tổng kết thể loại theo site:")
        for site in site_genres:
            total = site.get("total_genres") or 0
            completed = site.get("completed_genres") or 0
            print(
                f"  - {site.get('site_key')}: {completed}/{total} thể loại, cập nhật {site.get('updated_at', '-')}")
            for genre in site.get("genres", [])[:10]:
                status = genre.get("status", "completed")
                extra = f" ({status})" if status != "completed" else ""
                stories_done = _coerce_int(genre.get("stories")) or 0
                total_for_genre = _coerce_int(genre.get("total_stories"))
                if total_for_genre is not None:
                    total_for_genre = max(total_for_genre, stories_done)
                    story_text = f"{stories_done}/{total_for_genre} truyện"
                else:
                    story_text = f"{stories_done} truyện"
                print(
                    f"      * {genre.get('name')} — {story_text}{extra}"
                )
            if len(site.get("genres", [])) > 10:
                print(f"      ... và {len(site['genres']) - 10} thể loại khác")
        print()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Hiển thị dashboard crawl hiện tại")
    parser.add_argument(
        "--file",
        default=DEFAULT_DASHBOARD_FILE,
        help="Đường dẫn file dashboard.json (mặc định: state/dashboard.json)",
    )
    args = parser.parse_args(argv)
    try:
        dashboard = load_dashboard(args.file)
    except FileNotFoundError as exc:
        print(exc)
        return 1
    except json.JSONDecodeError as exc:
        print(f"File dashboard hỏng: {exc}")
        return 1

    print_dashboard(dashboard)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main(sys.argv[1:]))

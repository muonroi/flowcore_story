from __future__ import annotations

from typing import Any

from flowcore.utils.logger import logger


def update_category_progress(
    crawl_state: dict[str, Any],
    genre_name: str,
    total: int,
    processed: int,
    *,
    completed: bool = False,
) -> None:
    """Persist per-category progress for observability.

    This helper previously lived in ``main.py`` and mutated the shared
    ``crawl_state`` dictionary directly. Extracting it keeps progress reporting
    concerns separate from the orchestration code and makes the behaviour easier
    to unit test in isolation.
    """

    safe_total = max(int(total), 0)
    safe_processed = max(
        0, min(int(processed), safe_total if safe_total else int(processed))
    )
    progress = crawl_state.setdefault("category_progress", {})
    progress[genre_name] = {
        "total": safe_total,
        "processed": safe_processed,
        "completed": bool(
            completed or (safe_total and safe_processed >= safe_total)
        ),
    }


def log_category_resume_overview(site_key: str, crawl_state: dict[str, Any]) -> None:
    """Log a human-friendly summary of stored category progress."""

    plan = crawl_state.get("category_story_plan")
    progress = crawl_state.get("category_progress")
    if not isinstance(plan, dict) or not plan:
        return

    resolved_progress: dict[str, dict[str, Any]] = {}
    if isinstance(progress, dict):
        for key, value in progress.items():
            if isinstance(value, dict):
                resolved_progress[key] = value

    total_genres = len(plan)
    completed_genres = sum(1 for data in resolved_progress.values() if data.get("completed"))
    logger.info(
        "[STATE][%s] Đã xử lý %d/%d thể loại từ lần crawl trước.",
        site_key,
        completed_genres,
        total_genres,
    )

    for genre_name, stories in plan.items():
        story_list: list[str] = []
        if isinstance(stories, list):
            for item in stories:
                if isinstance(item, dict):
                    title = item.get("title")
                    if isinstance(title, str) and title.strip():
                        story_list.append(title.strip())
                        continue
                    url = item.get("url")
                    if isinstance(url, str) and url.strip():
                        story_list.append(url.strip())
                elif isinstance(item, str) and item.strip():
                    story_list.append(item.strip())

        progress_entry = resolved_progress.get(genre_name, {})
        processed = int(progress_entry.get("processed") or 0)
        total = progress_entry.get("total")
        if not isinstance(total, int) or total <= 0:
            total = len(story_list)
        completed = progress_entry.get("completed")
        status_suffix = " ✅" if completed else ""
        logger.info(
            "    • %s: %d/%d truyện%s",
            genre_name,
            processed,
            total,
            status_suffix,
        )

        if story_list:
            max_titles = 10
            displayed = story_list[:max_titles]
            remaining = len(story_list) - len(displayed)
            story_preview = ", ".join(displayed)
            if remaining > 0:
                story_preview += f" … (+{remaining} truyện)"
            logger.info("      Danh sách truyện: %s", story_preview)

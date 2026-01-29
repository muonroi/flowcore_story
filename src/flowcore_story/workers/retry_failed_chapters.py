import os
import time

from flowcore_story.adapters.factory import get_adapter
from flowcore_story.apps.scraper import initialize_scraper
from flowcore_story.utils.anti_bot import is_anti_bot_content
from flowcore_story.utils.chapter_utils import async_save_chapter_with_hash_check, get_category_name
from flowcore_story.utils.io_utils import ensure_directory_exists
from flowcore_story.utils.logger import logger
from flowcore_story.utils.progress_emitter import compact_payload, emit_progress_event

_RETRY_TERMINAL_ACTIONS = {"succeeded", "failed", "completed"}


def _emit_retry_event(
    action: str,
    *,
    site_key: str | None = None,
    story_title: str | None = None,
    chapter_title: str | None = None,
    filename: str | None = None,
    started_at: float | None = None,
    status: str | None = None,
    error: Exception | None = None,
    extra: dict | None = None,
) -> None:
    payload = {
        "action": action,
        "site_key": site_key,
        "story_title": story_title,
        "chapter_title": chapter_title,
        "filename": filename,
        "status": status,
    }

    if started_at is not None and action in _RETRY_TERMINAL_ACTIONS:
        payload["duration"] = max(time.time() - started_at, 0.0)

    if extra:
        payload.update(extra)

    if error is not None:
        payload["error"] = str(error)
        payload["error_type"] = error.__class__.__name__

    emit_progress_event("retry_worker", compact_payload(payload))

async def retry_single_chapter(chapter_data: dict):
    """
    Xử lý việc tải lại một chương duy nhất từ thông tin job trên Kafka.
    """
    site_key = chapter_data.get('site') or chapter_data.get('site_key')
    url = chapter_data.get('chapter_url')
    chapter_title = chapter_data.get('chapter_title')
    story_title = chapter_data.get('story_title')
    filename_path = chapter_data.get('filename')
    started_at = time.time()

    _emit_retry_event(
        "started",
        site_key=site_key,
        story_title=story_title,
        chapter_title=chapter_title,
        filename=filename_path,
    )

    if not all([site_key, url, chapter_title, story_title, filename_path]):
        logger.error(f"[RetryWorker] Job thiếu thông tin cần thiết: {chapter_data}")
        _emit_retry_event(
            "failed",
            site_key=site_key,
            story_title=story_title,
            chapter_title=chapter_title,
            filename=filename_path,
            started_at=started_at,
            status="missing_fields",
        )
        return

    logger.info(f"[RetryWorker] Bắt đầu retry chương: '{chapter_title}' của truyện '{story_title}'")

    try:
        adapter = get_adapter(site_key)
        await initialize_scraper(site_key)

        content = await adapter.get_chapter_content(url, chapter_title, site_key)

        if content and not is_anti_bot_content(content):
            # Lấy category từ chapter_data nếu có, nếu không thì để rỗng
            story_data_item = chapter_data.get('story_data_item', {})
            current_discovery_genre_data = chapter_data.get('current_discovery_genre_data', {})
            category_name = get_category_name(story_data_item, current_discovery_genre_data)

            # Gộp nội dung chuẩn file .txt
            full_content = (
                f"Nguồn: {url}\n\nTruyện: {story_title}\n"
                f"Thể loại: {category_name}\n"
                f"Chương: {chapter_title}\n\n"
                f"{content}"
            )

            dir_path = os.path.dirname(filename_path)
            await ensure_directory_exists(dir_path)
            save_result = await async_save_chapter_with_hash_check(filename_path, full_content)
            logger.info(f"[RetryWorker] ✅ Retry thành công chương '{chapter_title}'. Kết quả: {save_result}")
            _emit_retry_event(
                "succeeded",
                site_key=site_key,
                story_title=story_title,
                chapter_title=chapter_title,
                filename=filename_path,
                started_at=started_at,
                extra={"save_result": save_result},
            )
        else:
            reason = "anti-bot" if content and is_anti_bot_content(content) else "empty"
            logger.warning(f"[RetryWorker] ❌ Retry thất bại, {reason}: '{chapter_title}'")
            _emit_retry_event(
                "failed",
                site_key=site_key,
                story_title=story_title,
                chapter_title=chapter_title,
                filename=filename_path,
                started_at=started_at,
                status=reason,
            )
            return

    except Exception as e:
        logger.exception(f"[RetryWorker] ❌ Lỗi nghiêm trọng khi retry chương '{chapter_title}': {e}")
        _emit_retry_event(
            "failed",
            site_key=site_key,
            story_title=story_title,
            chapter_title=chapter_title,
            filename=filename_path,
            started_at=started_at,
            error=e,
        )
        raise

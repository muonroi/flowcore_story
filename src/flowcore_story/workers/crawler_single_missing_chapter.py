import asyncio
import json
import os
import sys
import time
from datetime import datetime

from flowcore_story.adapters.factory import get_adapter
from flowcore.apps.scraper import initialize_scraper
from flowcore_story.config.config import (
    COMPLETED_FOLDER,
    DATA_FOLDER,
    PROXIES_FILE,
    PROXIES_FOLDER,
)
from flowcore_story.config.proxy_provider import load_proxies
from flowcore.utils.cache_utils import cached_get_chapter_list, cached_get_story_details
from flowcore.utils.chapter_utils import (
    count_dead_chapters,
    count_txt_files,
    crawl_missing_chapters_for_story,
    export_chapter_metadata_sync,
    extract_real_chapter_number,
    get_missing_chapters,  # Thêm import này
)
from flowcore.utils.cleaner import ensure_sources_priority
from flowcore.utils.domain_utils import get_site_key_from_url
from flowcore.utils.io_utils import (
    create_proxy_template_if_not_exists,
    move_story_to_completed,
)
from flowcore.utils.logger import logger
from flowcore.utils.meta_utils import (
    derive_story_slug,
    get_primary_category_name,
    normalize_categories_field,
)
from flowcore.utils.progress_emitter import compact_payload, emit_progress_event
from flowcore.utils.state_utils import get_missing_worker_state_file, load_crawl_state

_SINGLE_STORY_TERMINAL_ACTIONS = {"succeeded", "failed", "completed"}
COMPLETED_FOLDER = COMPLETED_FOLDER  # re-export for tests that monkeypatch


def _emit_single_story_event(
    action: str,
    *,
    site_key: str | None = None,
    story_url: str | None = None,
    story_title: str | None = None,
    story_folder: str | None = None,
    started_at: float | None = None,
    status: str | None = None,
    error: BaseException | None = None,
    extra: dict | None = None,
) -> None:
    payload = {
        "action": action,
        "site_key": site_key,
        "story_url": story_url,
        "story_title": story_title,
        "story_folder": story_folder,
        "status": status,
    }

    if started_at is not None and action in _SINGLE_STORY_TERMINAL_ACTIONS:
        payload["duration"] = max(time.time() - started_at, 0.0)

    if extra:
        payload.update(extra)

    if error is not None:
        payload["error"] = str(error)
        payload["error_type"] = error.__class__.__name__

    emit_progress_event("single_story_worker", compact_payload(payload))

# --- Auto-fix sources nếu thiếu ---
def autofix_sources(meta, meta_path=None):
    url = meta.get("url")
    site_key = meta.get("site_key") or (get_site_key_from_url(url) if url else None)
    if not meta.get("sources") and url and site_key:
        meta["sources"] = [{"url": url, "site_key": site_key}]
    # Bổ sung priority cho tất cả nguồn
    if "sources" in meta:
        meta["sources"] = ensure_sources_priority(meta["sources"])
    if meta_path:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
    logger.info(f"[AUTO-FIX] Đã bổ sung sources cho truyện: {meta.get('title')}")
    return meta

def autofix_metadata(folder, site_key=None):
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    folder_name = os.path.basename(folder)
    meta_path = os.path.join(folder, "metadata.json")
    meta = {
        "title": folder_name.replace("-", " ").replace("_", " ").title().strip(),
        "url": "",
        "total_chapters_on_site": 0,
        "description": "",
        "author": "",
        "cover": "",
        "categories": [],
        "sources": [],
        "skip_crawl": False,
        "site_key": site_key
    }
    # Nếu có url đầu vào thì set vào meta luôn cho chuẩn
    if site_key and site_key in meta:
        meta['site_key'] = site_key
    if not os.path.exists(meta_path):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info(f"[AUTO-FIX] Đã tạo metadata cho '{meta['title']}' (0 chương)")
    return meta


async def crawl_single_story_worker(
    story_url: str | None = None,
    title: str | None = None,
    story_folder_path: str | None = None,
    site_key: str | None = None,
):
    started_at = time.time()
    story_title_context = title
    story_url_context = story_url
    story_folder_context = story_folder_path
    site_key_context: str | None = site_key
    final_extra: dict = {}
    final_status = "completed"
    final_error: BaseException | None = None

    _emit_single_story_event(
        "started",
        site_key=site_key_context,
        story_url=story_url_context,
        story_title=story_title_context,
        story_folder=story_folder_context,
    )

    try:
        result = await _crawl_single_story_worker_core(
            story_url=story_url,
            title=title,
            story_folder_path=story_folder_path,
            site_key=site_key,
        )
    except ValueError as exc:
        final_error = exc
        final_status = "invalid_request"
        raise
    except Exception as exc:
        final_error = exc
        raise
    else:
        final_extra = dict(result or {})
        site_key_context = final_extra.get("site_key", site_key_context)
        story_title_context = final_extra.get("story_title", story_title_context)
        story_url_context = final_extra.get("story_url", story_url_context)
        story_folder_context = final_extra.get("story_folder", story_folder_context)
        final_status = final_extra.pop("status", "completed") or "completed"
    finally:
        payload_extra = dict(final_extra)
        payload_extra.setdefault("story_folder", story_folder_context)
        if final_error is not None and final_status != "completed":
            payload_extra.setdefault("status", final_status)

        if final_error is not None:
            _emit_single_story_event(
                "failed",
                site_key=site_key_context,
                story_url=story_url_context,
                story_title=story_title_context,
                story_folder=story_folder_context,
                started_at=started_at,
                status="exception",
                error=final_error,
                extra=payload_extra,
            )
        elif final_status != "completed":
            _emit_single_story_event(
                "failed",
                site_key=site_key_context,
                story_url=story_url_context,
                story_title=story_title_context,
                story_folder=story_folder_context,
                started_at=started_at,
                status=final_status,
                extra=payload_extra,
            )
        else:
            _emit_single_story_event(
                "succeeded",
                site_key=site_key_context,
                story_url=story_url_context,
                story_title=story_title_context,
                story_folder=story_folder_context,
                started_at=started_at,
                extra=payload_extra,
            )


async def _crawl_single_story_worker_core(
    story_url: str | None = None,
    title: str | None = None,
    story_folder_path: str | None = None,
    site_key: str | None = None,
):
    await create_proxy_template_if_not_exists(PROXIES_FILE, PROXIES_FOLDER)
    await load_proxies(PROXIES_FILE)

    if story_folder_path:
        folder = story_folder_path
    elif story_url or title:
        slug = derive_story_slug(title, story_url)
        folder = os.path.join(DATA_FOLDER, slug)
    else:
        raise ValueError("Cần truyền vào story_folder_path, story_url hoặc title!")

    logger.info(f"[SingleStoryWorker] Bắt đầu xử lý: {folder}")
    status = "completed"
    meta_path = os.path.join(folder, "metadata.json")
    # Nếu chưa có folder → tạo mới
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    # Nếu chưa có metadata hoặc metadata lỗi thì tạo auto-fix
    meta = None
    if not os.path.exists(meta_path):
        site_key = site_key or (get_site_key_from_url(story_url) if story_url else None)
        await initialize_scraper(site_key)
        meta = autofix_metadata(folder, site_key)
        # Nếu có url truyền vào thì bổ sung luôn cho meta
        if story_url:
            meta['url'] = story_url
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=4)
    else:
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
            fields = ["title", "url", "total_chapters_on_site", "categories", "site_key"]
            if not all(meta.get(f) for f in fields):
                logger.warning("[AUTO-FIX] metadata.json thiếu trường, sẽ autofix lại.")
                site_key = site_key or (get_site_key_from_url(story_url) if story_url else meta.get("site_key"))
                meta = autofix_metadata(folder, site_key)
                if story_url:
                    meta['url'] = story_url
                    with open(meta_path, "w", encoding="utf-8") as f:
                        json.dump(meta, f, ensure_ascii=False, indent=4)
        except Exception as ex:
            logger.warning(f"[AUTO-FIX] metadata.json lỗi/parsing fail ({ex}), sẽ autofix lại.")
            site_key = site_key or (get_site_key_from_url(story_url) if story_url else None)
            meta = autofix_metadata(folder, site_key)
            if story_url:
                meta['url'] = story_url
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=4)

    # --- Luôn reload metadata và autofix sources ---
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    meta = autofix_sources(meta, meta_path)
    meta = normalize_categories_field(meta, meta_path)
    site_key = site_key or meta.get("site_key") or (get_site_key_from_url(meta.get("url")) if meta.get("url") else None)
    adapter = get_adapter(site_key)

    # --- Always update lại metadata từ web ---
    logger.info(f"[SYNC] Đang cập nhật lại metadata từ web cho '{meta['title']}'...")
    details = await cached_get_story_details(adapter, meta.get("url"), meta.get("title"))
    if details:
        for k, v in details.items():
            if v is not None and v != "" and meta.get(k) != v:
                meta[k] = v
        autofix_sources(meta)
        normalize_categories_field(meta)
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)
        logger.info("[SYNC] Metadata đã được cập nhật lại từ web!")

    chapters = None
    for source in meta["sources"]:
        url = source.get("url")
        src_site_key = source.get("site_key") or meta.get("site_key")
        adapter_src = get_adapter(src_site_key)
        try:
            chapters = await cached_get_chapter_list(
                adapter_src,
                url,
                meta.get("title"),
                src_site_key,
            )
            if chapters and len(chapters) > 0:
                meta["total_chapters_on_site"] = len(chapters)
                for source in meta.get("sources", []):
                    if source.get("url") == url:
                        source["total_chapters"] = len(chapters)
                        source["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=4)
                break
        except Exception as ex:
            logger.warning(f"[SOURCE] Lỗi lấy chapter list từ {src_site_key}: {ex}")



    if not chapters or len(chapters) == 0:
        logger.error(f"[CRAWL] Không lấy được danh sách chương từ bất kỳ nguồn nào! meta.sources = {meta['sources']}")
        return {
            "site_key": site_key,
            "story_title": meta.get("title") or title,
            "story_url": meta.get("url") or story_url,
            "story_folder": folder,
            "status": "no_chapters",
            "source_count": len(meta.get("sources", [])),
        }
    else:
        logger.info(f"[CRAWL] Đã lấy được {len(chapters)} chương từ nguồn {src_site_key}")

    # Sắp xếp danh sách chương theo số thực sự để đảm bảo đúng thứ tự
    chapters.sort(key=lambda ch: extract_real_chapter_number(ch.get('title', '')) or 0)

    # Cập nhật lại tổng số chương trong metadata nếu cần
    real_total = len(chapters)
    if real_total != meta.get("total_chapters_on_site"):
        meta["total_chapters_on_site"] = real_total
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=4)

    # Lưu lại danh sách chapter vào chapter_metadata.json để lấy đúng title
    export_chapter_metadata_sync(folder, [
        {"index": extract_real_chapter_number(ch.get('title', '')) or (i+1),
        "title": ch.get('title', ''),
        "url": ch.get('url', '')}
        for i, ch in enumerate(chapters)
    ])


    # --- Load crawl state và check chương missing ---
    state_file = get_missing_worker_state_file(site_key)
    crawl_state = await load_crawl_state(state_file, site_key)

    # Sử dụng hàm get_missing_chapters từ utils
    missing_chapters = get_missing_chapters(
        folder, chapters, 
        remote_story_title=meta.get("title"),
        remote_author=meta.get("author")
    )
    missing_count = len(missing_chapters)

    if missing_chapters:
        logger.info(f"[RETRY] Tìm thấy {missing_count} chương còn thiếu, bắt đầu crawl...")
        # Gọi thẳng hàm crawl từ utils, nó đã có sẵn logic retry bên trong
        await crawl_missing_chapters_for_story(
            site_key,
            None, # session không cần thiết khi dùng adapter
            missing_chapters,
            meta,
            meta.get("categories", [{}])[0] if meta.get("categories") else {},
            folder,
            crawl_state,
            num_batches=max(1, (len(missing_chapters) + 119) // 120),
            state_file=state_file,
            adapter=adapter,
        )
    else:
        logger.info(f"[COMPLETE] Không có chương nào thiếu cho '{meta['title']}'.")

    # --- Recount lại sau crawl ---
    num_txt = count_txt_files(folder)
    real_total = len(chapters)
    logger.info(f"[CHECK] {meta.get('title')} - txt: {num_txt} / web: {real_total}")

    dead_count = count_dead_chapters(folder)
    # --- Move sang completed nếu đủ ---
    if num_txt + dead_count >= real_total and real_total > 0:
        genre = get_primary_category_name(meta)
        await move_story_to_completed(folder, genre)
    else:
        logger.warning(
            f"[WARNING] Truyện chưa đủ chương ({num_txt}+{dead_count}/{real_total})"
        )

    return {
        "site_key": site_key,
        "story_title": meta.get("title") or title,
        "story_url": meta.get("url") or story_url,
        "story_folder": folder,
        "chapters_total": real_total,
        "chapter_count": num_txt,
        "dead_chapter_count": dead_count,
        "source_count": len(meta.get("sources", [])),
        "missing_chapter_count": missing_count,
        "status": status,
    }

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="Link truyện muốn crawl/update")
    parser.add_argument("--title", help="Tên truyện (nếu đã từng crawl)")
    args = parser.parse_args()
    if args.url or args.title:
        asyncio.run(crawl_single_story_worker(story_url=args.url, title=args.title))
    else:
        print("Phải truyền vào --url hoặc --title!")
        sys.exit(1)

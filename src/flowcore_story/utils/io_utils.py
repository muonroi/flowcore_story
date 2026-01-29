import asyncio
import json
import os
import re
import shutil
import tempfile
import time

import aiofiles
from filelock import Timeout

from flowcore_story.config.config import COMPLETED_FOLDER, DATA_FOLDER, FAILED_GENRES_FILE
from flowcore_story.utils.lock_utils import DEFAULT_STALE_LOCK_SECONDS, robust_file_lock
from flowcore_story.utils.logger import logger


async def async_rename(src: str, dst: str) -> None:
    await asyncio.to_thread(os.rename, src, dst)


async def async_remove(path: str) -> None:
    await asyncio.to_thread(os.remove, path)


async def ensure_directory_exists(dir_path: str) -> bool:
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, dir_path)
    if not exists:
        try:
            # Use exist_ok=True to avoid creating directories with incorrect
            # permissions (passing ``True`` positionally sets the mode).
            await loop.run_in_executor(
                None,
                lambda: os.makedirs(dir_path, exist_ok=True),
            )
            logger.info(f"Đã tạo thư mục: {dir_path}")
            return True
        except Exception as e:
            logger.error(f"LỖI khi tạo thư mục {dir_path}: {e}")
            return False
    return True


def _infer_genre_from_metadata(metadata: dict) -> str | None:
    for key in ("genre_name", "genre", "category_name", "category"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    categories = metadata.get("categories")
    if isinstance(categories, list) and categories:
        first = categories[0]
        if isinstance(first, dict):
            value = first.get("name") or first.get("title")
            if isinstance(value, str) and value.strip():
                return value.strip()
        if isinstance(first, str) and first.strip():
            return first.strip()
    if isinstance(categories, str) and categories.strip():
        return categories.strip()
    return None


def resolve_completed_story_path(story_folder: str, genre_name: str | None = None) -> str | None:
    """Return the completed story path if it exists, otherwise None."""
    if not story_folder:
        return None

    completed_root = os.path.abspath(COMPLETED_FOLDER)
    story_abs = os.path.abspath(story_folder)
    if story_abs == completed_root or story_abs.startswith(completed_root + os.sep):
        return story_folder

    resolved_genre = genre_name.strip() if isinstance(genre_name, str) else ""
    if not resolved_genre:
        meta_path = os.path.join(story_abs, "metadata.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path, encoding="utf-8") as f:
                    metadata = json.load(f)
                resolved_genre = _infer_genre_from_metadata(metadata) or ""
            except Exception as exc:
                logger.debug("[SYNC] Failed to read metadata for %s: %s", story_abs, exc)

    if not resolved_genre:
        return None

    candidate = os.path.join(COMPLETED_FOLDER, resolved_genre, os.path.basename(story_abs))
    if os.path.exists(candidate):
        return candidate

    # If the story is still in DATA_FOLDER or not moved yet, skip for now.
    data_root = os.path.abspath(DATA_FOLDER)
    if story_abs.startswith(data_root + os.sep):
        return None
    return None

async def create_proxy_template_if_not_exists(proxies_file_path: str, proxies_folder_path: str) -> bool:
    if not await ensure_directory_exists(proxies_folder_path):
        return False
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(None, os.path.exists, proxies_file_path)
    if not exists:
        try:
            await safe_write_file(proxies_file_path, """# Thêm proxy của bạn ở đây, mỗi proxy một dòng.
# Ví dụ: http://host:port
# Ví dụ: http://user:pass@host:port
# Ví dụ (IP:PORT sẽ dùng GLOBAL credentials): 123.45.67.89:1080
""")
            logger.info(f"Đã tạo file proxies mẫu: {proxies_file_path}")
            return True
        except Exception as e:
            logger.error(f"LỖI khi tạo file proxies mẫu {proxies_file_path}: {e}")
            return False
    return True

async def safe_write_json(
    filepath,
    obj,
    *,
    timeout=60,
    stale_lock_seconds: int | None = DEFAULT_STALE_LOCK_SECONDS,
):
    lock_path = filepath + '.lock'
    tmp_path = filepath + '.tmp'
    logger.debug("[LOCK][JSON] Waiting for %s (timeout=%ss)", lock_path, timeout)
    try:
        with robust_file_lock(
            lock_path,
            timeout=timeout,
            stale_after=stale_lock_seconds,
            log=logger,
        ):
            logger.debug("[LOCK][JSON] Acquired %s", lock_path)
            # Use to_thread for json.dumps as it can be blocking for large objects
            content = await asyncio.to_thread(json.dumps, obj, ensure_ascii=False, indent=4)
            async with aiofiles.open(tmp_path, 'w', encoding='utf-8') as f:
                await f.write(content)
            await asyncio.sleep(0.05)
            # Use to_thread for os.replace to avoid blocking the event loop
            await asyncio.to_thread(os.replace, tmp_path, filepath)
            logger.debug("[LOCK][JSON] Released %s", lock_path)
    except Timeout:
        logger.error(f"Timeout khi ghi file {filepath}. File lock: {lock_path} bị kẹt!")
        # Tự remove lock nếu cần, như bên trên
    except Exception as e:
        logger.error(f"Lỗi khi ghi file {filepath}: {e}")


def safe_write_json_sync(
    filepath: str,
    obj,
    *,
    timeout: int = 60,
    stale_lock_seconds: int | None = DEFAULT_STALE_LOCK_SECONDS,
) -> bool:
    """
    Synchronous version of safe_write_json.

    Uses atomic write pattern (write to .tmp then os.replace)
    to prevent file corruption if process crashes during write.

    Args:
        filepath: Target JSON file path
        obj: Object to serialize to JSON
        timeout: Lock timeout in seconds
        stale_lock_seconds: Consider lock stale after this many seconds

    Returns:
        True if write succeeded, False otherwise
    """
    lock_path = filepath + '.lock'
    tmp_path = filepath + '.tmp'
    logger.debug("[LOCK][JSON-SYNC] Waiting for %s (timeout=%ss)", lock_path, timeout)

    try:
        with robust_file_lock(
            lock_path,
            timeout=timeout,
            stale_after=stale_lock_seconds,
            log=logger,
        ):
            logger.debug("[LOCK][JSON-SYNC] Acquired %s", lock_path)

            # Step 1: Serialize to string first (to catch serialization errors before touching file)
            content = json.dumps(obj, ensure_ascii=False, indent=4)

            # Step 2: Write to temp file
            with open(tmp_path, 'w', encoding='utf-8') as f:
                f.write(content)

            # Step 3: Atomic replace (this is atomic on POSIX systems)
            os.replace(tmp_path, filepath)

            logger.debug("[LOCK][JSON-SYNC] Released %s", lock_path)
            return True

    except Timeout:
        logger.error(f"[JSON-SYNC] Timeout khi ghi file {filepath}. File lock: {lock_path} bị kẹt!")
        return False
    except Exception as e:
        logger.error(f"[JSON-SYNC] Lỗi khi ghi file {filepath}: {e}")
        # Clean up temp file if it exists
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        return False


def ensure_backup_folder(backup_folder="backup"):
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

def load_patterns(pattern_file):
    with open(pattern_file, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    patterns = [re.compile(pattern_text, re.IGNORECASE) for pattern_text in lines]
    return patterns

def filter_lines_by_patterns(lines, patterns):
    result = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if any(p.search(stripped) for p in patterns):
            continue
        if len(stripped) < 5 and not stripped.endswith(('.', '?', '!', ':', '"', "'")):
            continue
        result.append(stripped)
    return result

async def move_story_to_completed(story_folder, genre_name, retries: int = 3, completed_folder: str = COMPLETED_FOLDER) -> bool:
    """Di chuyển truyện sang thư mục completed với retry."""
    dest_genre_folder = os.path.join(completed_folder, genre_name)
    await ensure_directory_exists(dest_genre_folder)
    dest_folder = os.path.join(dest_genre_folder, os.path.basename(story_folder))
    logger.info(f"Bắt đầu di chuyển {story_folder} -> {dest_folder}")

    # Xóa các file .lock còn sót lại trước khi di chuyển
    try:
        for item_name in os.listdir(story_folder):
            if item_name.endswith('.lock'):
                lock_path = os.path.join(story_folder, item_name)
                try:
                    os.remove(lock_path)
                    logger.debug(f"[CLEANUP] Đã xóa file lock: {lock_path}")
                except FileNotFoundError:
                    continue
                except Exception as ex:
                    logger.warning(f"[CLEANUP] Không xóa được lock {lock_path}: {ex}")
    except FileNotFoundError:
        return False

    for attempt in range(1, retries + 1):
        tmp_dest = None
        try:
            logger.info(f"Lần thử {attempt}/{retries}...")
            tmp_dest = tempfile.mkdtemp(
                prefix=f"{os.path.basename(story_folder)}.tmp-", dir=dest_genre_folder
            )
            logger.info(f"Tạo thư mục tạm: {tmp_dest}")

            logger.info(f"Sao chép nội dung từ {story_folder} -> {tmp_dest}")
            for item_name in os.listdir(story_folder):
                src_item = os.path.join(story_folder, item_name)
                dst_item = os.path.join(tmp_dest, item_name)
                if os.path.isdir(src_item):
                    shutil.copytree(src_item, dst_item)
                else:
                    shutil.copy2(src_item, dst_item)

            if os.path.exists(dest_folder):
                logger.info(f"Thư mục đích đã tồn tại, xóa: {dest_folder}")
                try:
                    shutil.rmtree(dest_folder)
                    logger.info(f"Đã xóa thư mục đích cũ: {dest_folder}")
                except Exception as cleanup_error:
                    logger.error(
                        f"Không thể xóa thư mục đích cũ {dest_folder}: {cleanup_error}"
                    )
                    raise

            logger.info(f"Di chuyển thư mục tạm -> thư mục đích: {tmp_dest} -> {dest_folder}")
            os.replace(tmp_dest, dest_folder)
            tmp_dest = None
            logger.info(f"✅ Di chuyển thành công sang {dest_folder}")
            logger.info(f"Xóa thư mục nguồn: {story_folder}")
            shutil.rmtree(story_folder, ignore_errors=True)
            if os.path.exists(story_folder):
                logger.warning(f"Không thể xóa thư mục nguồn: {story_folder}")
            try:
                meta_path = os.path.join(dest_folder, "metadata.json")
                metadata = {}
                if os.path.exists(meta_path):
                    with open(meta_path, encoding="utf-8") as f:
                        metadata = json.load(f)
                sync_event = {
                    "type": "story_updated",
                    "story_path": dest_folder,
                    "site_key": metadata.get("site_key") or metadata.get("source"),
                    "story_title": metadata.get("title"),
                    "genre_name": genre_name,
                    "sync_chapters": True,
                    "timestamp": time.time(),
                }
                from flowcore_story.kafka.kafka_producer import send_job

                asyncio.create_task(send_job(sync_event, topic="storyflow.sync"))
            except Exception as exc:
                logger.warning("[SYNC] Failed to emit completed story event: %s", exc)
            return True
        except FileNotFoundError:
            logger.warning(
                f"Thư mục {story_folder} không tồn tại khi đang di chuyển. Bỏ qua."
            )
            return False
        except Exception as ex:
            logger.error(f"Lỗi di chuyển {story_folder} -> {dest_folder} ({attempt}/{retries}): {ex}")
            await asyncio.sleep(2)
        finally:
            if tmp_dest and os.path.exists(tmp_dest):
                logger.info(f"Xóa thư mục tạm: {tmp_dest}")
                shutil.rmtree(tmp_dest, ignore_errors=True)
    logger.error(f"Di chuyển thất bại sau {retries} lần thử.")
    return False


def log_failed_genre(genre_data):
    try:
        if os.path.exists(FAILED_GENRES_FILE):
            with open(FAILED_GENRES_FILE, encoding="utf-8") as f:
                arr = json.load(f)
        else:
            arr = []
        if genre_data not in arr:
            arr.append(genre_data)
            with open(FAILED_GENRES_FILE, "w", encoding="utf-8") as f:
                json.dump(arr, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Lỗi khi log failed genre: {e}")

async def safe_write_file(
    file_path,
    content,
    *,
    timeout=30,
    auto_remove_lock=True,
    stale_lock_seconds: int | None = DEFAULT_STALE_LOCK_SECONDS,
):
    lock_path = file_path + ".lock"
    tmp_path = file_path + ".tmp"
    def _write_sync() -> None:
        logger.debug("[LOCK][TXT] Waiting for %s (timeout=%ss)", lock_path, timeout)
        with robust_file_lock(
            lock_path,
            timeout=timeout,
            stale_after=stale_lock_seconds,
            log=logger,
        ):
            logger.debug("[LOCK][TXT] Acquired %s", lock_path)
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp_path, file_path)
            logger.debug("[LOCK][TXT] Released %s", lock_path)

    try:
        await asyncio.to_thread(_write_sync)
    except Timeout:
        logger.error(
            f"Timeout khi ghi file {file_path}. File lock: {lock_path} bị kẹt! "
            "Hãy kiểm tra tiến trình đang giữ lock và xóa thủ công nếu phù hợp."
        )
        if auto_remove_lock:
            logger.warning(
                "auto_remove_lock được bật nhưng bỏ qua việc tự xóa lock để tránh "
                "ghi chồng dữ liệu của tiến trình khác."
            )
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError as cleanup_err:
                logger.debug(
                    f"Không thể xóa file tạm {tmp_path} sau khi timeout: {cleanup_err}"
                )
    except Exception as e:
        logger.error(f"Lỗi khi ghi file {file_path}: {e}")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError as cleanup_err:
                logger.debug(
                    f"Không thể xóa file tạm {tmp_path} sau khi gặp lỗi: {cleanup_err}"
                )

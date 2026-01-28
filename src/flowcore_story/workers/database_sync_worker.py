"""
Database Sync Worker
Quét thư mục completed_stories và đồng bộ vào database
Hỗ trợ: MySQL, PostgreSQL, MS SQL Server
"""
import asyncio
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import IntegrityError, OperationalError
from unidecode import unidecode

# Setup path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from flowcore_story.config import config as app_config
from flowcore_story.database.connection import db_manager
from flowcore_story.database.models import Category, Chapter, Story, StoryCategory, StorySource, SyncLog
from flowcore.utils.logger import logger


class DatabaseSyncWorker:
    """Worker để đồng bộ dữ liệu từ file system vào database"""

    def __init__(self):
        self.completed_folder = os.getenv("COMPLETED_FOLDER", "/app/completed_stories")
        self.completed_root = os.path.abspath(self.completed_folder)
        self.sync_interval = int(os.getenv("DB_SYNC_INTERVAL", "300"))  # 5 phút
        self.batch_size = int(os.getenv("DB_SYNC_BATCH_SIZE", "100"))
        # FIX ISSUE #4: Default to FALSE for better performance
        # Content is already stored in files, no need to duplicate in MySQL
        self.sync_content = os.getenv("DB_SYNC_CONTENT", "false").lower() == "true"
        # Batch commit interval (commit every N stories instead of each)
        self.batch_commit_interval = int(os.getenv("DB_SYNC_BATCH_COMMIT", "10"))

        # OPTIMIZATION: Incremental sync - only sync modified stories
        # Set to "true" to enable incremental sync (only sync stories modified since last cycle)
        # Set to "false" to always do full sync (default for backward compatibility)
        self.incremental_sync = os.getenv("DB_SYNC_INCREMENTAL", "true").lower() == "true"

        # Full sync interval - do full scan every N cycles even with incremental enabled
        # Default: every 12 cycles (1 hour with 5 min interval)
        self.full_sync_every_n_cycles = int(os.getenv("DB_SYNC_FULL_EVERY_N", "12"))
        self._cycle_count = 0
        self._last_sync_time = 0.0  # Unix timestamp of last successful sync
        # Skip syncing chapter contents on incremental scans to reduce CPU/IO.
        self.sync_chapters_on_incremental = (
            os.getenv("DB_SYNC_CHAPTERS_ON_INCREMENTAL", "false").lower() == "true"
        )

        # Kafka config
        self.kafka_brokers = app_config.KAFKA_BOOTSTRAP_SERVERS
        self.kafka_topic = "storyflow.sync"
        self.kafka_group_id = "storyflow-db-sync"
        self.kafka_max_poll_interval_ms = int(
            os.getenv("DB_SYNC_KAFKA_MAX_POLL_INTERVAL_MS", "900000")
        )
        self.kafka_max_poll_records = int(
            os.getenv("DB_SYNC_KAFKA_MAX_POLL_RECORDS", "1")
        )
        self.kafka_session_timeout_ms = int(
            os.getenv("DB_SYNC_KAFKA_SESSION_TIMEOUT_MS", "30000")
        )
        self.kafka_heartbeat_interval_ms = int(
            os.getenv("DB_SYNC_KAFKA_HEARTBEAT_INTERVAL_MS", "10000")
        )
        self.data_root = os.path.abspath(getattr(app_config, "DATA_FOLDER", "/app/truyen_data"))

        # Stats
        self.stats = {
            "total_stories_scanned": 0,
            "total_stories_synced": 0,
            "total_chapters_synced": 0,
            "total_errors": 0
        }
        self._missing_title_warned: set[str] = set()

    async def start_kafka_consumer(self):
        """Lắng nghe sự kiện đồng bộ từ Kafka"""
        logger.info(f"[DB-Sync] Starting Kafka consumer for topic {self.kafka_topic}")
        consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_brokers,
            group_id=self.kafka_group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            max_poll_interval_ms=self.kafka_max_poll_interval_ms,
            max_poll_records=self.kafka_max_poll_records,
            session_timeout_ms=self.kafka_session_timeout_ms,
            heartbeat_interval_ms=self.kafka_heartbeat_interval_ms,
        )
        await consumer.start()
        try:
            async for msg in consumer:
                try:
                    data = msg.value
                    event_type = data.get("type")
                    if event_type == "chapter_updated":
                        story_path = data.get("story_path") or (
                            os.path.dirname(data.get("chapter_path", ""))
                        )
                        chapter_path = data.get("chapter_path")
                        resolved_story_path, resolved_chapter_path = self._resolve_event_paths(
                            story_path,
                            chapter_path,
                        )
                        if resolved_story_path:
                            logger.info(f"[DB-Sync] Received {event_type} event for {resolved_story_path}")
                            if not resolved_chapter_path or not os.path.exists(resolved_chapter_path):
                                logger.warning(
                                    "[DB-Sync] Invalid chapter path in event: %s",
                                    chapter_path,
                                )
                                continue
                            with db_manager.get_session() as session:
                                genre_folder = os.path.basename(os.path.dirname(resolved_story_path))
                                story = self._ensure_story_record(
                                    session,
                                    resolved_story_path,
                                    genre_folder,
                                )
                                if not story:
                                    logger.warning(
                                        "[DB-Sync] Story record missing for event: %s",
                                        resolved_story_path,
                                    )
                                    continue
                                chapter_file = os.path.basename(resolved_chapter_path)
                                self.sync_chapters(session, story, [chapter_file], resolved_story_path)
                                self._commit_with_retry(session)
                        else:
                            logger.warning(f"[DB-Sync] Invalid story path in event: {story_path}")
                    elif event_type == "story_updated":
                        story_path = data.get("story_path")
                        if story_path and os.path.exists(story_path):
                            logger.info(f"[DB-Sync] Received {event_type} event for {story_path}")
                            with db_manager.get_session() as session:
                                genre_folder = os.path.basename(os.path.dirname(story_path))
                                self.sync_story(
                                    session,
                                    story_path,
                                    genre_folder,
                                    sync_chapters=bool(data.get("sync_chapters")),
                                )
                        else:
                            logger.warning(f"[DB-Sync] Invalid story path in event: {story_path}")
                except Exception as e:
                    logger.error(f"[DB-Sync] Error processing Kafka message: {e}")
        finally:
            await consumer.stop()

    def _resolve_event_paths(
        self,
        story_path: str | None,
        chapter_path: str | None,
    ) -> tuple[str | None, str | None]:
        story_candidate = story_path if story_path and os.path.exists(story_path) else None

        if chapter_path and os.path.exists(chapter_path):
            if not story_candidate:
                story_candidate = os.path.dirname(chapter_path)
            return story_candidate, chapter_path

        if story_candidate and chapter_path:
            alt_chapter = os.path.join(story_candidate, os.path.basename(chapter_path))
            if os.path.exists(alt_chapter):
                return story_candidate, alt_chapter

        if story_candidate and chapter_path and story_candidate.startswith(self.completed_root + os.sep):
            data_story = os.path.join(self.data_root, os.path.basename(story_candidate))
            data_chapter = os.path.join(data_story, os.path.basename(chapter_path))
            if os.path.exists(data_chapter):
                return data_story, data_chapter

        if not story_candidate and story_path:
            data_story = os.path.join(self.data_root, os.path.basename(story_path))
            if os.path.exists(data_story):
                if chapter_path:
                    data_chapter = os.path.join(data_story, os.path.basename(chapter_path))
                    if os.path.exists(data_chapter):
                        return data_story, data_chapter
                return data_story, None

        return story_candidate, None

    def _commit_with_retry(self, session, max_attempts: int = 3, base_delay: float = 1.0):
        """Commit với retry để tránh deadlock tạm thời (MySQL 1213/1205)."""
        for attempt in range(1, max_attempts + 1):
            try:
                session.commit()
                return
            except OperationalError as e:
                err_args = getattr(getattr(e, "orig", None), "args", [])
                err_code = err_args[0] if err_args else None
                if err_code in (1213, 1205):
                    session.rollback()
                    delay = base_delay * attempt
                    logger.warning(
                        f"[DB-Sync] Deadlock/lock wait (code={err_code}) khi commit, thử lại {attempt}/{max_attempts} sau {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue
                session.rollback()
                raise
            except IntegrityError:
                session.rollback()
                raise
        session.rollback()
        raise RuntimeError("[DB-Sync] Commit thất bại sau nhiều lần retry do deadlock")

    def _delete_with_retry(
        self,
        session,
        query_factory,
        action: str,
        max_attempts: int = 3,
        base_delay: float = 1.0,
    ) -> None:
        """Delete rows with retry to avoid aborting on transient deadlocks/lock waits."""
        for attempt in range(1, max_attempts + 1):
            try:
                query_factory().delete(synchronize_session=False)
                return
            except OperationalError as e:
                err_args = getattr(getattr(e, "orig", None), "args", [])
                err_code = err_args[0] if err_args else None
                if err_code in (1205, 1213) and attempt < max_attempts:
                    session.rollback()
                    delay = base_delay * attempt
                    logger.warning(
                        f"[DB-Sync] Deadlock/lock wait (code={err_code}) khi delete {action}, "
                        f"thử lại {attempt}/{max_attempts} sau {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue
                session.rollback()
                raise

    def slugify(self, text: str) -> str:
        """Tạo slug từ text (bỏ dấu tiếng Việt với unidecode)"""
        if not text:
            return ""
        # Remove Vietnamese diacritics using unidecode
        slug = unidecode(text)
        # Convert to lowercase and replace spaces with hyphens
        slug = slug.lower().strip()
        slug = slug.replace(" ", "-")
        # Remove special characters (keep only alphanumeric and hyphens)
        slug = re.sub(r'[^\w\s-]', '', slug)
        # Replace whitespace with hyphens
        slug = re.sub(r'[\s]+', '-', slug)
        # Remove consecutive hyphens
        slug = re.sub(r'-+', '-', slug)
        return slug.strip("-")

    def _normalize_story_status(self, status: Any, story_folder: str) -> str:
        """Normalize status to avoid invalid/corrupted values in DB."""
        if status is None:
            return ""
        if not isinstance(status, str):
            status = str(status)
        status = status.strip()
        if not status:
            return ""

        if "<" in status and ">" in status:
            status = re.sub(r"<[^>]+>", "", status).strip()

        if len(status) > 100:
            logger.warning(
                "[DB-Sync] Status quá dài (%s chars) tại %s, bỏ qua giá trị này.",
                len(status),
                story_folder,
            )
            return ""

        return status

    def ensure_unique_slug(self, session, base_slug: str, exclude_folder_hash: str = None) -> str:
        """Ensure slug is unique by adding suffix if needed

        Args:
            session: SQLAlchemy session
            base_slug: Base slug to check
            exclude_folder_hash: Folder hash to exclude from uniqueness check (for updates)

        Returns:
            Unique slug (with suffix if needed)
        """
        slug = base_slug
        suffix = 1

        while True:
            # Check if slug exists (excluding current story if updating)
            query = session.query(Story).filter(Story.slug == slug)
            if exclude_folder_hash:
                query = query.filter(Story.folder_path_hash != exclude_folder_hash)

            existing = query.first()
            if not existing:
                return slug

            # Slug exists, try with suffix
            slug = f"{base_slug}-{suffix}"
            suffix += 1

            # Safety limit to avoid infinite loop
            if suffix > 100:
                # Use hash as last resort
                import hashlib
                hash_suffix = hashlib.md5(f"{base_slug}{exclude_folder_hash}".encode()).hexdigest()[:8]
                return f"{base_slug}-{hash_suffix}"

    def hash_folder_path(self, folder_path: str) -> str:
        """Create SHA256 hash of folder path for unique constraint"""
        return hashlib.sha256(folder_path.encode('utf-8')).hexdigest()

    def read_metadata(self, story_folder: str) -> dict[str, Any] | None:
        """Đọc metadata.json từ thư mục truyện"""
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            logger.warning(f"[DB-Sync] Không tìm thấy metadata.json: {metadata_path}")
            return None

        try:
            with open(metadata_path, encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi đọc metadata.json từ {metadata_path}: {e}")
            return None

    def load_chapter_meta_titles(self, story_folder: str) -> tuple[dict[str, str], dict[int, str], dict[str, int]]:
        """
        Load mapping từ metadata.json, chapter_metadata.json hoặc metadata-chapter.json để lấy tiêu đề gốc.
        Map theo chapter pattern (phần sau dấu _) để tránh lỗi khi file được đánh số lại.
        Trả về (filename_map, chapter_num_map)
        """
        title_map: dict[str, str] = {}
        num_map: dict[int, str] = {}
        file_num_map: dict[str, int] = {}

        # BUGFIX: Prioritize chapter_metadata.json first as it contains full file mappings
        # metadata.json chapters array often lacks 'file' field, causing lookup failures
        for filename in ("chapter_metadata.json", "metadata-chapter.json", "metadata.json"):
            path = os.path.join(story_folder, filename)
            if not os.path.exists(path):
                continue
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    items = data.get("chapters") or data.get("items")
                    if not isinstance(items, list):
                        continue
                elif isinstance(data, list):
                    items = data
                else:
                    continue

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    title = item.get("title")
                    if not title:
                        continue

                    # Clean title: loại bỏ "Chương XXX:" prefix
                    cleaned_title = self.clean_chapter_title(title)

                    # Option 1: Có field "file" trực tiếp (old format)
                    file_name = (
                        item.get("file")
                        or item.get("file_name")
                        or item.get("filename")
                    )

                    if file_name:
                        # Map cả full filename VÀ pattern (phần sau _)
                        title_map[file_name] = cleaned_title
                        
                        # Extract chapter number if available
                        if isinstance(item.get("chapter_number"), int):
                             file_num_map[file_name] = item.get("chapter_number")
                        # Extract pattern: "0076_chuong-76-..." -> "chuong-76-..."
                        if '_' in file_name:
                            pattern = file_name.split('_', 1)[1]
                            title_map[pattern] = cleaned_title
                    else:
                        # Option 2: Không có field "file", extract từ URL (new standard format)
                        # URL: ".../chuong-76" hoặc ".../chapter-76"
                        # Filename: "0001_chuong-76-tro-lai-suon-nui...txt"
                        url = item.get("url", "")
                        if url:
                            # Extract chapter pattern từ URL: "chuong-76", "chapter-123", etc.
                            # Pattern: last part of URL path
                            url_parts = url.rstrip('/').split('/')
                            if url_parts:
                                chapter_pattern = url_parts[-1]  # e.g., "chuong-76"
                                # Map theo pattern này để match với "0001_chuong-76-..."
                                title_map[chapter_pattern] = cleaned_title
                                # Also extract chapter number from URL for num_map fallback
                                url_match = re.search(r'(?:chuong|chapter)-(\d+)', chapter_pattern, re.IGNORECASE)
                                if url_match:
                                    try:
                                        url_chapter_num = int(url_match.group(1))
                                        num_map[url_chapter_num] = cleaned_title
                                    except ValueError:
                                        pass

                    # Map theo số chương trong title (cho trường hợp file bị rename/reindex)
                    # Tìm số chương trong title gốc: "Chương 3220: ..."
                    match = re.search(r'(?:Chương|Chuong|chương|chuong)\s+(\d+)', title, re.IGNORECASE)
                    if match:
                        try:
                            c_num = int(match.group(1))
                            num_map[c_num] = cleaned_title
                        except ValueError:
                            pass

                if title_map or num_map:
                    logger.debug(
                        "[DB-Sync] Loaded %d titles, %d numbered chapters từ %s",
                        len(title_map),
                        len(num_map),
                        path,
                    )
                    return title_map, num_map, file_num_map
            except Exception as exc:
                logger.warning(
                    "[DB-Sync] Không đọc được %s tại %s: %s; fallback sang tên file",
                    filename,
                    path,
                    exc,
                )
        return title_map, num_map, file_num_map

    def get_chapter_files(self, story_folder: str) -> list[str]:
        """Lấy danh sách các file chapter (.txt) trong thư mục truyện"""
        try:
            files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
            # Sắp xếp theo tên file
            files.sort()
            return files
        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi đọc chapter files từ {story_folder}: {e}")
            return []

    def count_chapter_files(self, story_folder: str) -> int:
        """Đếm số file chapter (.txt) với chi phí thấp hơn list/sort."""
        try:
            return sum(
                1
                for entry in os.scandir(story_folder)
                if entry.is_file() and entry.name.endswith(".txt")
            )
        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi đếm chapter files từ {story_folder}: {e}")
            return 0

    def read_chapter_content(self, chapter_path: str) -> str | None:
        """Đọc nội dung chapter từ file và bỏ metadata headers"""
        if not self.sync_content:
            return None

        try:
            with open(chapter_path, encoding='utf-8') as f:
                content = f.read()

            # Strip metadata headers (4 dòng đầu: Nguon, Truyen, The loai, Chuong)
            # Dùng regex để detect và remove, không force cứng để tránh mất content
            # Pattern khớp với 4 dòng metadata (có hoặc không dấu, case-insensitive)
            # Nguon/Nguồn: ... \n Truyen/Truyện: ... \n The loai/Thể loại: ... \n Chuong/Chương: ...
            metadata_pattern = re.compile(
                r'^(Nguon|Nguồn):\s*.*?\n'
                r'(Truyen|Truyện):\s*.*?\n'
                r'(The loai|Thể loại):\s*.*?\n'
                r'(Chuong|Chương):\s*.*?\n',
                re.IGNORECASE | re.MULTILINE
            )

            # Chỉ strip nếu tìm thấy pattern ở đầu content
            match = metadata_pattern.match(content)
            if match:
                # Bỏ phần metadata, giữ lại content thật
                content = content[match.end():]
                logger.debug(f"[DB-Sync] Stripped metadata headers ({match.end()} chars) from {chapter_path}")

            # Loại bỏ dòng title lặp lại (Chương XXX: Tên chương) ở đầu content
            # Pattern: "Chương 123: Tên chương" hoặc "Chương 123 - Tên chương"
            title_pattern = re.compile(
                r'^(Chương|Chuong|chương|chuong)\s+\d+[\s:：\-–—]+.*?(\n|$)',
                re.IGNORECASE
            )
            content = title_pattern.sub('', content, count=1)

            return content.strip()  # Strip whitespace ở đầu/cuối

        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi đọc chapter content từ {chapter_path}: {e}")
            return None

    def clean_chapter_title(self, title: str) -> str:
        """
        Loại bỏ prefix "Chương XXX:" khỏi title, chỉ giữ lại tên chương.
        Ví dụ: "Chương 260:  Thoát khỏi – gặp nguy" -> "Thoát khỏi – gặp nguy"
        """
        if not title:
            return title

        # Pattern: "Chương 123:" hoặc "Chương 123 -" hoặc "Chương 123  "
        # Loại bỏ và chỉ giữ phần sau
        clean_pattern = re.compile(
            r'^(Chương|Chuong|chương|chuong)\s+\d+[\s:：\-–—]*\s*',
            re.IGNORECASE
        )
        cleaned = clean_pattern.sub('', title).strip()
        return cleaned if cleaned else title

    def parse_semantic_chapter_number(self, filename: str) -> int | None:
        """
        Parse số chương ngữ nghĩa từ tên file (vd: ...chuong-3220...)
        Khác với parse_chapter_number (lấy prefix index).
        """
        try:
            # Tìm pattern "chuong-XXX" hoặc "chapter-XXX"
            match = re.search(r'(?:_|^)(?:chuong|chapter)-(\d+)', filename, re.IGNORECASE)
            if match:
                return int(match.group(1))
        except Exception:
            pass
        return None

    def parse_chapter_number(self, filename: str) -> int:
        """Parse chapter number từ tên file (ví dụ: 0001_chuong-1.txt -> 1)"""
        try:
            # Tách phần đầu là số (0001)
            number_part = filename.split('_')[0]
            return int(number_part)
        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi parse chapter number từ {filename}: {e}")
            return 0

    def parse_chapter_title(self, filename: str) -> str:
        """Parse chapter title từ tên file, bỏ prefix số (001_, 002_,...) và prefix 'Chương XXX:'"""
        try:
            # Bỏ phần số prefix và extension
            # Ví dụ: 0001_chuong-1-tieu-de.txt -> chuong 1 tieu de
            parts = filename.replace('.txt', '').split('_', 1)
            if len(parts) > 1:
                # Bỏ prefix số, giữ nguyên encoding UTF-8
                title = parts[1].replace('-', ' ')
                # Capitalize first letter only (không dùng .title() vì nó làm hỏng tiếng Việt)
                title = title[0].upper() + title[1:] if title else title
                # Clean "Chương XXX:" prefix nếu có
                title = self.clean_chapter_title(title)
                return title
            return filename.replace('.txt', '')
        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi parse chapter title từ {filename}: {e}")
            return filename

    def get_or_create_category(self, session, category_name: str, category_url: str = "") -> Category | None:
        """Lấy hoặc tạo mới category"""
        if not category_name:
            return None

        slug = self.slugify(category_name)

        # Tìm category đã tồn tại theo NAME (unique constraint) trước
        # Vì slug có thể thay đổi (thêm/bỏ dấu), nhưng name là stable
        category = session.query(Category).filter_by(name=category_name).first()

        if category:
            # Category đã tồn tại, update slug nếu khác (để migrate sang slug mới không dấu)
            if category.slug != slug:
                logger.debug(f"[DB-Sync] Update category slug: '{category_name}' ({category.slug} -> {slug})")
                category.slug = slug
        else:
            # Tạo mới category
            category = Category(
                name=category_name,
                slug=slug,
                url=category_url
            )
            session.add(category)
            session.flush()  # Để lấy ID ngay lập tức
            logger.debug(f"[DB-Sync] Tạo mới category: {category_name} (slug: {slug})")

        return category

    def sync_story_no_commit(
        self,
        session,
        story_folder: str,
        genre_folder: str,
        *,
        sync_chapters: bool = True,
    ) -> bool:
        """Đồng bộ một truyện vào database (không commit - để batch commit)"""
        # Note: This is the new method without commit for batch processing
        # Old sync_story method calls this and commits
        return self._sync_story_internal(
            session,
            story_folder,
            genre_folder,
            do_commit=False,
            sync_chapters=sync_chapters,
        )

    def sync_story(
        self,
        session,
        story_folder: str,
        genre_folder: str,
        *,
        sync_chapters: bool = True,
    ) -> bool:
        """Đồng bộ một truyện vào database (with commit)"""
        return self._sync_story_internal(
            session,
            story_folder,
            genre_folder,
            do_commit=True,
            sync_chapters=sync_chapters,
        )

    def _sync_story_internal(
        self,
        session,
        story_folder: str,
        genre_folder: str,
        *,
        do_commit: bool = True,
        sync_chapters: bool = True,
    ) -> bool:
        """Internal method for syncing story with optional commit"""
        try:
            # Đọc metadata
            metadata = self.read_metadata(story_folder)
            if not metadata:
                self.stats["total_errors"] += 1
                return False
            if (
                not genre_folder
                or os.path.abspath(story_folder).startswith(self.data_root + os.sep)
                or genre_folder == os.path.basename(self.data_root)
            ):
                genre_folder = (
                    metadata.get("genre_name")
                    or metadata.get("category_name")
                    or metadata.get("category")
                    or metadata.get("genre")
                    or ""
                )
                if not genre_folder:
                    categories = metadata.get("categories")
                    if isinstance(categories, list) and categories:
                        first = categories[0]
                        if isinstance(first, dict):
                            genre_folder = first.get("name") or first.get("title") or ""
                        elif isinstance(first, str):
                            genre_folder = first
                    elif isinstance(categories, str):
                        genre_folder = categories

            # Tạo slug từ folder name hoặc title
            folder_name = os.path.basename(story_folder)
            base_slug = self.slugify(metadata.get('title', folder_name))

            # Kiểm tra story đã tồn tại theo folder_path_hash (TRUE unique identifier)
            folder_hash = self.hash_folder_path(story_folder)
            existing_story = session.query(Story).filter_by(folder_path_hash=folder_hash).first()

            # Ensure slug is unique (thêm suffix nếu trùng với story khác)
            slug = self.ensure_unique_slug(session, base_slug, exclude_folder_hash=folder_hash)

            # Parse datetime
            crawled_at = None
            if metadata.get('crawled_at'):
                try:
                    crawled_at = datetime.strptime(metadata['crawled_at'], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass

            metadata_updated_at = None
            if metadata.get('metadata_updated_at'):
                try:
                    metadata_updated_at = datetime.strptime(metadata['metadata_updated_at'], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass

            # Đếm số chapter files
            if sync_chapters:
                chapter_files = self.get_chapter_files(story_folder)
                crawled_chapters_count = len(chapter_files)
            else:
                chapter_files = []
                crawled_chapters_count = self.count_chapter_files(story_folder)
            status_value = self._normalize_story_status(metadata.get('status', ''), story_folder)

            if existing_story:
                # Cập nhật thông tin story
                existing_story.title = metadata.get('title', '')
                existing_story.slug = slug  # Update slug (bỏ dấu với unidecode)
                existing_story.url = metadata.get('url', '')
                existing_story.site_key = metadata.get('site_key', metadata.get('source', ''))
                existing_story.author = metadata.get('author', '')
                existing_story.description = metadata.get('description', '')
                existing_story.cover = metadata.get('cover', '')
                existing_story.status = status_value
                existing_story.crawled_by = metadata.get('crawled_by', '')
                existing_story.rating_value = metadata.get('rating_value')
                existing_story.rating_count = metadata.get('rating_count')
                existing_story.total_chapters_on_site = metadata.get('total_chapters_on_site', 0)
                existing_story.crawled_chapters = crawled_chapters_count
                existing_story.crawled_at = crawled_at
                existing_story.metadata_updated_at = metadata_updated_at
                existing_story.folder_path = story_folder
                existing_story.folder_path_hash = folder_hash  # Use pre-computed hash
                existing_story.genre_folder = genre_folder
                existing_story.skip_crawl = metadata.get('skip_crawl', False)
                existing_story.skip_reason = metadata.get('skip_reason', '')
                existing_story.synced_at = datetime.utcnow()

                story = existing_story
                logger.debug(f"[DB-Sync] Cập nhật story: {story.title}")
            else:
                # Tạo mới story
                story = Story(
                    title=metadata.get('title', ''),
                    slug=slug,
                    url=metadata.get('url', ''),
                    site_key=metadata.get('site_key', metadata.get('source', '')),
                    author=metadata.get('author', ''),
                    description=metadata.get('description', ''),
                    cover=metadata.get('cover', ''),
                    status=status_value,
                    crawled_by=metadata.get('crawled_by', ''),
                    rating_value=metadata.get('rating_value'),
                    rating_count=metadata.get('rating_count'),
                    total_chapters_on_site=metadata.get('total_chapters_on_site', 0),
                    crawled_chapters=crawled_chapters_count,
                    crawled_at=crawled_at,
                    metadata_updated_at=metadata_updated_at,
                    folder_path=story_folder,
                    folder_path_hash=folder_hash,  # Use pre-computed hash
                    genre_folder=genre_folder,
                    skip_crawl=metadata.get('skip_crawl', False),
                    skip_reason=metadata.get('skip_reason', ''),
                    synced_at=datetime.utcnow()
                )
                session.add(story)
                session.flush()  # Để lấy story.id
                logger.info(f"[DB-Sync] Tạo mới story: {story.title}")
                self.stats["total_stories_synced"] += 1

            # Sync categories
            categories_data = metadata.get('categories', [])
            if categories_data:
                # FIX: Thay vì DELETE all + INSERT new (gây duplicate error với batch commits)
                # Dùng approach: lấy existing categories, chỉ add mới những cái chưa có

                # Lấy existing story_categories
                existing_story_cats = session.query(StoryCategory).filter_by(story_id=story.id).all()
                existing_category_ids = {sc.category_id for sc in existing_story_cats}

                seen_category_ids = set()
                seen_category_slugs = set()
                new_category_ids = set()

                for cat_data in categories_data:
                    if isinstance(cat_data, dict):
                        cat_name = cat_data.get('name', '')
                        cat_url = cat_data.get('url', '')
                    else:
                        cat_name = str(cat_data)
                        cat_url = ''

                    if cat_name:
                        cat_slug = self.slugify(cat_name)
                        # Bỏ qua trùng lặp trong metadata để tránh lỗi unique
                        if cat_slug in seen_category_slugs:
                            continue
                        category = self.get_or_create_category(session, cat_name, cat_url)
                        if category and category.id not in seen_category_ids:
                            seen_category_slugs.add(cat_slug or category.slug)
                            seen_category_ids.add(category.id)
                            new_category_ids.add(category.id)

                            # Chỉ add nếu chưa tồn tại
                            if category.id not in existing_category_ids:
                                story_category = StoryCategory(
                                    story_id=story.id,
                                    category_id=category.id
                                )
                                session.add(story_category)

                # Xóa categories không còn trong metadata (optional - có thể bỏ qua)
                # for story_cat in existing_story_cats:
                #     if story_cat.category_id not in new_category_ids:
                #         session.delete(story_cat)

            # Sync sources
            sources_data = metadata.get('sources', [])
            if sources_data:
                # FIX: Same as categories - check existing instead of DELETE + INSERT
                existing_sources = session.query(StorySource).filter_by(story_id=story.id).all()
                existing_source_hashes = {src.url_hash for src in existing_sources}

                seen_source_hashes = set()
                for source_data in sources_data:
                    if isinstance(source_data, dict):
                        url = source_data.get('url', '')
                        url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
                        if url_hash in seen_source_hashes:
                            continue
                        seen_source_hashes.add(url_hash)

                        # Only add if not exists
                        if url_hash not in existing_source_hashes:
                            source = StorySource(
                                story_id=story.id,
                                url=url,
                                url_hash=url_hash,
                                site_key=source_data.get('site_key', ''),
                                is_primary=source_data.get('is_primary', False)
                            )
                            session.add(source)
                            existing_source_hashes.add(url_hash)

            # Sync chapters
            if sync_chapters:
                self.sync_chapters(session, story, chapter_files, story_folder)

            # Commit sau mỗi story (only if do_commit=True)
            if do_commit:
                self._commit_with_retry(session)

            return True

        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi sync story {story_folder}: {e}", exc_info=True)
            session.rollback()
            self.stats["total_errors"] += 1
            return False

    def _ensure_story_record(
        self,
        session,
        story_folder: str,
        genre_folder: str,
    ) -> Story | None:
        """Ensure story exists without forcing a full chapter sync."""
        folder_hash = self.hash_folder_path(story_folder)
        story = session.query(Story).filter_by(folder_path_hash=folder_hash).first()
        if story:
            return story
        self._sync_story_internal(
            session,
            story_folder,
            genre_folder,
            do_commit=False,
            sync_chapters=False,
        )
        return session.query(Story).filter_by(folder_path_hash=folder_hash).first()

    def _upsert_chapter_mysql(
        self,
        session,
        story_id: int,
        chapter_number: int,
        chapter_title: str,
        chapter_path: str,
        chapter_file: str,
        content: str | None,
        word_count: int,
        *,
        max_attempts: int = 3,
        base_delay: float = 1.0,
    ) -> bool:
        """Insert/update chapter in MySQL using ON DUPLICATE KEY UPDATE.

        Retries lock wait/deadlock errors (1205/1213) with backoff to avoid
        dropping chapters when the database is under contention.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                now = datetime.utcnow()
                insert_stmt = mysql_insert(Chapter).values(
                    story_id=story_id,
                    chapter_number=chapter_number,
                    title=chapter_title,
                    file_path=chapter_path,
                    file_name=chapter_file,
                    content=content,
                    word_count=word_count,
                    synced_at=now,
                )

                update_fields = {
                    "chapter_number": chapter_number,  # FIX: Update chapter_number from metadata
                    "title": chapter_title,
                    "file_path": chapter_path,
                    "file_name": chapter_file,
                    "word_count": word_count,
                    "synced_at": now,
                }

                if content is not None:
                    update_fields["content"] = content

                result = session.execute(insert_stmt.on_duplicate_key_update(**update_fields))
                return result.rowcount == 1
            except OperationalError as e:
                err_args = getattr(getattr(e, "orig", None), "args", [])
                err_code = err_args[0] if err_args else None
                if err_code in (1205, 1213) and attempt < max_attempts:
                    session.rollback()
                    delay = base_delay * attempt
                    logger.warning(
                        f"[DB-Sync] Lock wait/deadlock (code={err_code}) khi upsert chapter, "
                        f"thử lại {attempt}/{max_attempts} sau {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue
                session.rollback()
                raise

    def sync_chapters(self, session, story: Story, chapter_files: list[str], story_folder: str):
        """Đồng bộ các chapter của một truyện"""
        synced_count = 0
        dialect_name = session.bind.dialect.name if session.bind else ""
        is_mysql = dialect_name == "mysql"
        chapter_title_map, chapter_num_map, file_num_map = self.load_chapter_meta_titles(story_folder)

        for chapter_file in chapter_files:
            try:
                # FIX: Priority to metadata chapter_number, then semantic, then file prefix
                chapter_number = file_num_map.get(chapter_file)
                if chapter_number is None:
                    # Try parsing semantic chapter number from filename (chuong-XXX)
                    chapter_number = self.parse_semantic_chapter_number(chapter_file)
                if chapter_number is None:
                    # Final fallback: parse from file prefix (0001, 0002...)
                    chapter_number = self.parse_chapter_number(chapter_file)
                # Thử lookup theo full filename trước
                chapter_title = chapter_title_map.get(chapter_file)
                # Nếu không có, thử lookup theo pattern (phần sau _)
                if not chapter_title and '_' in chapter_file:
                    pattern = chapter_file.split('_', 1)[1]
                    chapter_title = chapter_title_map.get(pattern)
                
                # Nếu vẫn không có, thử lookup theo số chương trong tên file (semantic)
                if not chapter_title:
                    semantic_num = self.parse_semantic_chapter_number(chapter_file)
                    if semantic_num is not None:
                        chapter_title = chapter_num_map.get(semantic_num)

                # Fallback: parse từ filename (mất dấu)
                if not chapter_title:
                    chapter_title = self.parse_chapter_title(chapter_file)
                    if story_folder not in self._missing_title_warned:
                        logger.warning(
                            "[DB-Sync] Không tìm thấy title trong metadata cho %s, "
                            "fallback sang parse filename (có thể mất dấu)",
                            chapter_file,
                        )
                        self._missing_title_warned.add(story_folder)
                    else:
                        logger.debug(
                            "[DB-Sync] Missing title in metadata for %s; using filename fallback.",
                            chapter_file,
                        )
                chapter_path = os.path.join(story_folder, chapter_file)

                # Đọc content nếu được bật
                content = self.read_chapter_content(chapter_path) if self.sync_content else None

                # Tính word count
                word_count = len(content.split()) if content else 0

                if is_mysql:
                    was_inserted = self._upsert_chapter_mysql(
                        session=session,
                        story_id=story.id,
                        chapter_number=chapter_number,
                        chapter_title=chapter_title,
                        chapter_path=chapter_path,
                        chapter_file=chapter_file,
                        content=content,
                        word_count=word_count,
                    )
                    if was_inserted:
                        synced_count += 1
                else:
                    # Kiểm tra chapter đã tồn tại
                    existing_chapter = session.query(Chapter).filter_by(
                        story_id=story.id,
                        chapter_number=chapter_number
                    ).first()

                    if existing_chapter:
                        # Cập nhật chapter
                        existing_chapter.title = chapter_title
                        existing_chapter.file_path = chapter_path
                        existing_chapter.file_name = chapter_file
                        if self.sync_content:
                            existing_chapter.content = content
                        existing_chapter.word_count = word_count
                        existing_chapter.synced_at = datetime.utcnow()
                    else:
                        # Tạo mới chapter
                        chapter = Chapter(
                            story_id=story.id,
                            chapter_number=chapter_number,
                            title=chapter_title,
                            file_path=chapter_path,
                            file_name=chapter_file,
                            content=content,
                            word_count=word_count,
                            synced_at=datetime.utcnow()
                        )
                        session.add(chapter)
                        synced_count += 1

            except Exception as e:
                logger.error(f"[DB-Sync] Lỗi sync chapter {chapter_file}: {e}")
                self.stats["total_errors"] += 1

        if synced_count > 0:
            self.stats["total_chapters_synced"] += synced_count
            logger.debug(f"[DB-Sync] Synced {synced_count} chapters cho story: {story.title}")

    def scan_completed_stories(self, modified_since: float = 0.0) -> list[tuple]:
        """Quét thư mục completed_stories và trả về danh sách (genre_folder, story_folder)

        Args:
            modified_since: Unix timestamp - only return stories with metadata.json
                           modified after this time. If 0, return all stories.
        Returns:
            List of (genre_folder_name, full_story_path) tuples
        """
        stories = []
        total_scanned = 0
        skipped_not_modified = 0

        if not os.path.exists(self.completed_folder):
            logger.warning(f"[DB-Sync] Thư mục completed_stories không tồn tại: {self.completed_folder}")
            return stories

        try:
            # Quét các thư mục thể loại (Tiên Hiệp, etc.)
            for genre_folder in os.listdir(self.completed_folder):
                genre_path = os.path.join(self.completed_folder, genre_folder)

                if not os.path.isdir(genre_path):
                    continue

                # Skip folder "Unknown" (case-insensitive) để tránh sync stories chưa được health check fix
                if genre_folder.lower() == "unknown":
                    unknown_count = len(os.listdir(genre_path))
                    if unknown_count > 0:
                        logger.info(f"[DB-Sync] Bỏ qua folder 'Unknown' (chứa {unknown_count} items)")
                    continue

                # Quét các thư mục truyện trong thể loại
                for story_folder in os.listdir(genre_path):
                    story_path = os.path.join(genre_path, story_folder)

                    if not os.path.isdir(story_path):
                        continue

                    # Kiểm tra có metadata.json không
                    metadata_path = os.path.join(story_path, "metadata.json")
                    if not os.path.exists(metadata_path):
                        continue

                    total_scanned += 1

                    # OPTIMIZATION: Incremental sync - check modification time
                    if modified_since > 0:
                        try:
                            mtime = os.path.getmtime(metadata_path)
                            if mtime <= modified_since:
                                skipped_not_modified += 1
                                continue
                        except OSError:
                            pass  # If we can't get mtime, include the story

                    stories.append((genre_folder, story_path))

            if modified_since > 0:
                logger.info(f"[DB-Sync] Incremental scan: {len(stories)} modified / {total_scanned} total (skipped {skipped_not_modified} unchanged)")
            else:
                logger.info(f"[DB-Sync] Full scan: Tìm thấy {len(stories)} truyện để sync")

        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi quét thư mục completed_stories: {e}")

        return stories

    def run_sync_cycle(self):
        """Chạy một chu kỳ đồng bộ"""
        self._cycle_count += 1

        # Determine if this should be a full sync or incremental
        do_full_sync = (
            not self.incremental_sync or  # Incremental disabled
            self._last_sync_time == 0.0 or  # First run
            (self._cycle_count % self.full_sync_every_n_cycles == 0)  # Periodic full sync
        )

        sync_mode = "FULL" if do_full_sync else "INCREMENTAL"
        sync_chapters = do_full_sync or self.sync_chapters_on_incremental
        logger.info(f"[DB-Sync] ===== BẮT ĐẦU CHU KỲ ĐỒNG BỘ (#{self._cycle_count}, {sync_mode}) =====")
        logger.info(f"[DB-Sync] Content sync: {'ENABLED' if self.sync_content else 'DISABLED'}")
        logger.info(f"[DB-Sync] Sync chapters: {'ENABLED' if sync_chapters else 'DISABLED'}")
        logger.info(f"[DB-Sync] Batch commit interval: {self.batch_commit_interval} stories")

        # Reset stats
        self.stats = {
            "total_stories_scanned": 0,
            "total_stories_synced": 0,
            "total_chapters_synced": 0,
            "total_errors": 0
        }

        sync_started_at = datetime.utcnow()
        cycle_start_time = time.time()

        # Tạo sync log
        with db_manager.get_session() as session:
            sync_log = SyncLog(
                sync_started_at=sync_started_at,
                status='running'
            )
            session.add(sync_log)
            session.commit()
            sync_log_id = sync_log.id

        try:
            # Quét thư mục completed_stories (incremental or full)
            modified_since = 0.0 if do_full_sync else self._last_sync_time
            stories_to_sync = self.scan_completed_stories(modified_since=modified_since)
            self.stats["total_stories_scanned"] = len(stories_to_sync)

            # FIX ISSUE #4: Batch commits for better performance
            # Sync từng truyện với batch commits
            with db_manager.get_session() as session:
                for idx, (genre_folder, story_folder) in enumerate(stories_to_sync, 1):
                    try:
                        # Sync story (without individual commit)
                        self.sync_story_no_commit(
                            session,
                            story_folder,
                            genre_folder,
                            sync_chapters=sync_chapters,
                        )

                        # Batch commit every N stories
                        if idx % self.batch_commit_interval == 0:
                            self._commit_with_retry(session)
                            logger.debug(f"[DB-Sync] Batch committed {idx} stories")
                    except Exception as e:
                        logger.error(f"[DB-Sync] Error syncing {story_folder}: {e}")
                        self.stats["total_errors"] += 1
                        session.rollback()

                # Final commit for remaining stories
                if len(stories_to_sync) % self.batch_commit_interval != 0:
                    self._commit_with_retry(session)

            # Cập nhật sync log
            with db_manager.get_session() as session:
                sync_log = session.query(SyncLog).filter_by(id=sync_log_id).first()
                if sync_log:
                    sync_log.sync_completed_at = datetime.utcnow()
                    sync_log.total_stories_scanned = self.stats["total_stories_scanned"]
                    sync_log.total_stories_synced = self.stats["total_stories_synced"]
                    sync_log.total_chapters_synced = self.stats["total_chapters_synced"]
                    sync_log.total_errors = self.stats["total_errors"]
                    sync_log.status = 'completed'
                    session.commit()

            # Update last sync time for incremental sync
            self._last_sync_time = cycle_start_time

            elapsed = time.time() - cycle_start_time
            logger.info(
                f"[DB-Sync] ===== HOÀN THÀNH CHU KỲ ĐỒNG BỘ ({sync_mode}) ===== "
                f"Stories: {self.stats['total_stories_synced']}/{self.stats['total_stories_scanned']}, "
                f"Chapters: {self.stats['total_chapters_synced']}, "
                f"Errors: {self.stats['total_errors']}, "
                f"Time: {elapsed:.1f}s"
            )

        except Exception as e:
            logger.error(f"[DB-Sync] Lỗi trong chu kỳ đồng bộ: {e}", exc_info=True)

            # Cập nhật sync log với lỗi
            with db_manager.get_session() as session:
                sync_log = session.query(SyncLog).filter_by(id=sync_log_id).first()
                if sync_log:
                    sync_log.sync_completed_at = datetime.utcnow()
                    sync_log.status = 'failed'
                    sync_log.error_message = str(e)
                    sync_log.total_stories_scanned = self.stats["total_stories_scanned"]
                    sync_log.total_stories_synced = self.stats["total_stories_synced"]
                    sync_log.total_chapters_synced = self.stats["total_chapters_synced"]
                    sync_log.total_errors = self.stats["total_errors"]
                    session.commit()

    async def _periodic_scan_loop(self):
        """Loop quét định kỳ (full/incremental scan)"""
        # Chạy sync lần đầu trong thread để không block event loop (Kafka consumer)
        await asyncio.to_thread(self.run_sync_cycle)

        while True:
            try:
                logger.info(f"[DB-Sync] Chờ {self.sync_interval} giây cho chu kỳ tiếp theo...")
                await asyncio.sleep(self.sync_interval)
                # Run sync in thread pool to avoid blocking async loop
                await asyncio.to_thread(self.run_sync_cycle)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[DB-Sync] Lỗi không mong đợi: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def run(self):
        """Chạy worker liên tục (Dual mode: Event-driven + Periodic)"""
        logger.info("[DB-Sync] Database Sync Worker đã khởi động!")
        logger.info(f"[DB-Sync] Completed folder: {self.completed_folder}")
        logger.info(f"[DB-Sync] Sync interval: {self.sync_interval} giây")
        logger.info(f"[DB-Sync] Sync content: {self.sync_content}")
        logger.info(f"[DB-Sync] Incremental sync: {self.incremental_sync}")
        if self.incremental_sync:
            logger.info(f"[DB-Sync] Full sync every {self.full_sync_every_n_cycles} cycles ({self.full_sync_every_n_cycles * self.sync_interval // 60} minutes)")

        # Khởi tạo database với retry để chờ MySQL/MariaDB sẵn sàng
        max_attempts = int(os.getenv("DB_INIT_RETRIES", "5"))
        backoff = float(os.getenv("DB_INIT_BACKOFF", "2.0"))
        for attempt in range(1, max_attempts + 1):
            try:
                db_manager.initialize()
                db_manager.create_tables()
                break
            except OperationalError as e:
                logger.warning(
                    "[DB-Sync] Kết nối DB thất bại (attempt %d/%d): %s",
                    attempt,
                    max_attempts,
                    e,
                )
                if attempt == max_attempts:
                    raise
                await asyncio.sleep(backoff * attempt)
            except Exception:
                logger.exception(
                    "[DB-Sync] Lỗi khi khởi tạo DB (attempt %d/%d)",
                    attempt,
                    max_attempts,
                )
                if attempt == max_attempts:
                    raise
                await asyncio.sleep(backoff * attempt)

        # Run both consumers concurrently
        await asyncio.gather(
            self.start_kafka_consumer(),
            self._periodic_scan_loop()
        )


def main():
    """Entry point"""
    worker = DatabaseSyncWorker()
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()

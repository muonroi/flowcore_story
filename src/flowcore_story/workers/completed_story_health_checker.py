"""
Completed Story Health Checker Worker
======================================
Worker để kiểm tra và sửa chữa các vấn đề với truyện trong thư mục completed_stories:
1. Kiểm tra metadata đầy đủ (title, author, description, categories)
2. Fix unknown category bằng cách re-fetch từ site
3. Kiểm tra chapters có đủ không (so với expected_total)
4. Re-fetch chapters nếu thật sự thiếu

Logic tham khảo từ move_story_to_completed và evaluate_chapter_state
"""

import asyncio
import json
import os
import re
from datetime import datetime
from typing import Any

from unidecode import unidecode

from flowcore_story.adapters import get_adapter_for_site
from flowcore_story.utils.io_utils import safe_write_file, safe_write_json
from flowcore_story.utils.logger import logger


class CompletedStoryHealthChecker:
    """Worker kiểm tra sức khỏe của các truyện đã hoàn thành"""

    def __init__(self):
        self.completed_folder = os.getenv("COMPLETED_FOLDER", "/app/completed_stories")
        self.check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "3600"))  # 1 giờ
        self.batch_size = int(os.getenv("HEALTH_CHECK_BATCH_SIZE", "50"))
        self.enable_refetch = os.getenv("HEALTH_CHECK_ENABLE_REFETCH", "true").lower() == "true"
        self.log_sample_limit = int(os.getenv("HEALTH_CHECK_LOG_SAMPLE_LIMIT", "20"))

        # Stats
        self.stats = {
            "total_stories_scanned": 0,
            "stories_with_unknown_category": 0,
            "stories_with_missing_metadata": 0,
            "stories_with_missing_chapters": 0,
            "stories_with_chapter_meta_mismatch": 0,
            "chapter_metadata_regenerated": 0,
            "stories_relocated": 0,
            "stories_fixed": 0,
            "stories_failed": 0,
        }

        # State file để track progress
        self.state_file = os.path.join(os.getenv("STATE_FOLDER", "/app/state"), "health_checker_state.json")
        self.load_state()
        self._reset_log_samples()

    def _reset_log_samples(self) -> None:
        limit = max(self.log_sample_limit, 0)
        self._log_samples_remaining = {
            "unknown_category": limit,
            "missing_metadata": limit,
            "missing_chapters": limit,
        }
        self._log_suppressed = set()

    def _should_log_sample(self, key: str, label: str) -> bool:
        if self.log_sample_limit <= 0:
            return False
        remaining = self._log_samples_remaining.get(key, 0)
        if remaining <= 0:
            if key not in self._log_suppressed:
                logger.info(
                    f"[HealthCheck] Đã vượt giới hạn log cho {label}, "
                    "sẽ ẩn các cảnh báo còn lại trong lần quét này."
                )
                self._log_suppressed.add(key)
            return False
        self._log_samples_remaining[key] = remaining - 1
        return True

    def load_state(self):
        """Load state từ file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, encoding='utf-8') as f:
                    self.state = json.load(f)
            else:
                self.state = {
                    "last_check_time": None,
                    "checked_stories": [],
                    "failed_stories": []
                }
        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi load state: {e}")
            self.state = {
                "last_check_time": None,
                "checked_stories": [],
                "failed_stories": []
            }

    def save_state(self):
        """Save state ra file"""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi save state: {e}")

    def read_metadata(self, story_folder: str) -> dict[str, Any] | None:
        """Đọc metadata.json từ thư mục truyện"""
        metadata_path = os.path.join(story_folder, "metadata.json")
        if not os.path.exists(metadata_path):
            logger.warning(f"[HealthCheck] Không tìm thấy metadata.json: {metadata_path}")
            return None

        try:
            with open(metadata_path, encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi đọc metadata.json từ {metadata_path}: {e}")
            return None

    def _derive_story_url_from_chapter_url(self, chapter_url: str) -> str:
        """Derive story URL by stripping chapter suffix from a chapter URL."""
        if not chapter_url:
            return ""
        base_url = chapter_url.split("#", 1)[0].split("?", 1)[0]
        base_url = re.sub(
            r"/(?:chuong|chapter)-[^/]+(?:\\.html?)?/?$",
            "",
            base_url,
            flags=re.IGNORECASE,
        )
        return base_url.rstrip("/")

    def bootstrap_metadata_from_chapter_header(
        self,
        story_folder: str,
        genre_folder: str,
    ) -> dict[str, Any] | None:
        """Build minimal metadata.json from chapter header lines if missing."""
        try:
            chapter_files = sorted(
                file_name
                for file_name in os.listdir(story_folder)
                if file_name.endswith(".txt")
            )
        except Exception as exc:
            logger.warning(f"[HealthCheck] Không đọc được chapter files: {story_folder} ({exc})")
            return None

        if not chapter_files:
            return None

        first_chapter_path = os.path.join(story_folder, chapter_files[0])
        source_url = ""
        story_title = ""
        category_line = ""

        try:
            with open(first_chapter_path, encoding="utf-8") as f:
                for _ in range(10):
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    match = re.match(r"^(Nguon|Nguồn):\\s*(.+)$", line, re.IGNORECASE)
                    if match:
                        source_url = match.group(2).strip()
                        continue
                    match = re.match(r"^(Truyen|Truyện):\\s*(.+)$", line, re.IGNORECASE)
                    if match:
                        story_title = match.group(2).strip()
                        continue
                    match = re.match(r"^(The loai|Thể loại):\\s*(.+)$", line, re.IGNORECASE)
                    if match:
                        category_line = match.group(2).strip()
                        continue
        except Exception as exc:
            logger.warning(f"[HealthCheck] Không đọc được chapter header: {first_chapter_path} ({exc})")
            return None

        story_url = self._derive_story_url_from_chapter_url(source_url) or source_url
        site_key = self._extract_site_key_from_url(story_url or source_url)

        categories = []
        if category_line:
            for raw_cat in re.split(r"[,/|]+", category_line):
                cat = raw_cat.strip()
                if cat:
                    categories.append({"name": cat})
        elif genre_folder:
            categories.append({"name": genre_folder})

        if not story_title:
            folder_name = os.path.basename(story_folder)
            story_title = folder_name.replace("-", " ").replace("_", " ").title().strip()

        total_chapters = self.count_chapter_files(story_folder)
        source_value = site_key
        if not source_value and story_url:
            try:
                from urllib.parse import urlparse

                source_value = urlparse(story_url).netloc or ""
            except Exception:
                source_value = ""

        sources = []
        if story_url:
            sources.append(
                {
                    "url": story_url,
                    "site_key": site_key or "",
                    "priority": 100,
                    "total_chapters": total_chapters,
                }
            )

        return {
            "title": story_title,
            "url": story_url,
            "total_chapters_on_site": total_chapters,
            "description": "",
            "author": "",
            "cover": "",
            "categories": categories,
            "sources": sources,
            "skip_crawl": False,
            "site_key": site_key or "",
            "source": source_value or "",
        }

    def has_unknown_category(self, metadata: dict[str, Any]) -> bool:
        """Kiểm tra xem story có unknown category không"""
        categories = metadata.get('categories', [])

        if not categories:
            return True

        # Check for "Unknown" in category names
        for cat in categories:
            if isinstance(cat, dict):
                cat_name = cat.get('name', '').lower()
                if 'unknown' in cat_name or not cat_name:
                    return True
            elif isinstance(cat, str):
                if 'unknown' in cat.lower() or not cat:
                    return True

        return False

    def has_missing_metadata(self, metadata: dict[str, Any]) -> tuple[bool, list[str]]:
        """Kiểm tra xem metadata có thiếu các trường required không"""
        require_author = os.getenv("HEALTH_CHECK_REQUIRE_AUTHOR", "false").lower() == "true"
        required_fields = ['title', 'description', 'categories', 'source']
        if require_author:
            required_fields.append('author')
        missing = []

        for field in required_fields:
            value = metadata.get(field)
            if not value:
                missing.append(field)
            elif isinstance(value, str) and not value.strip():
                missing.append(field)
            elif isinstance(value, list) and len(value) == 0:
                missing.append(field)

        return (len(missing) > 0, missing)

    def count_chapter_files(self, story_folder: str) -> int:
        """Đếm số chapter files (.txt) trong thư mục"""
        try:
            files = [f for f in os.listdir(story_folder) if f.endswith('.txt')]
            return len(files)
        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi đếm chapter files: {e}")
            return 0

    def has_missing_chapters(self, metadata: dict[str, Any], story_folder: str) -> tuple[bool, int, int]:
        """Kiểm tra xem có thiếu chapters không

        Returns:
            (has_missing, actual_count, expected_count)
        """
        expected = metadata.get('total_chapters_on_site', 0)
        if not expected or expected <= 0:
            return (False, 0, 0)

        actual = self.count_chapter_files(story_folder)

        # Cho phép sai lệch 1-2 chapter (có thể là dead chapters)
        tolerance = 2
        has_missing = (actual + tolerance) < expected

        return (has_missing, actual, expected)

    async def refetch_story_metadata(self, metadata: dict[str, Any], story_folder: str) -> dict[str, Any] | None:
        """Re-fetch metadata từ site để fix unknown category và missing fields"""
        site_key = metadata.get('site_key') or metadata.get('source')
        story_url = metadata.get('url')
        story_title = metadata.get('title', 'Unknown')

        if not site_key or not story_url:
            logger.error(f"[HealthCheck] Thiếu site_key hoặc url để re-fetch: {story_folder}")
            return None

        try:
            logger.info(f"[HealthCheck] Re-fetching metadata cho '{story_title}' từ {story_url}")

            adapter = get_adapter_for_site(site_key)
            if not adapter:
                logger.error(f"[HealthCheck] Không tìm thấy adapter cho site '{site_key}'")
                return None

            # Fetch story details từ site
            details = await adapter.get_story_details(story_url, story_title)

            if not details:
                logger.error(f"[HealthCheck] Không lấy được details cho '{story_title}'")
                return None

            # Merge với metadata hiện tại (giữ lại thông tin cũ, chỉ update fields mới)
            updated_metadata = metadata.copy()

            # Update các fields quan trọng
            if details.get('title'):
                updated_metadata['title'] = details['title']
            if details.get('author'):
                updated_metadata['author'] = details['author']
            if details.get('description'):
                updated_metadata['description'] = details['description']
            if details.get('categories') or details.get('genres_full'):
                updated_metadata['categories'] = details.get('categories') or details.get('genres_full', [])
            if details.get('cover'):
                updated_metadata['cover'] = details['cover']
            if details.get('status'):
                updated_metadata['status'] = details['status']
            if details.get('total_chapters_on_site'):
                updated_metadata['total_chapters_on_site'] = details['total_chapters_on_site']

            # Update URL with ID (important for truyencom)
            if details.get('url'):
                updated_metadata['url'] = details['url']

            # Update sources with correct URL
            if details.get('sources'):
                updated_metadata['sources'] = details['sources']

            # Update timestamp
            updated_metadata['metadata_updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated_metadata['health_check_fixed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            logger.info(f"[HealthCheck] ✓ Đã re-fetch metadata cho '{story_title}'")
            return updated_metadata

        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi re-fetch metadata cho '{story_title}': {e}")
            return None

    async def refetch_missing_chapters(
        self,
        metadata: dict[str, Any],
        story_folder: str,
        actual_count: int,
        expected_count: int
    ) -> bool:
        """Re-fetch các chapters còn thiếu

        Improvements (2025-12-14):
        - Site-specific delays to avoid anti-bot
        - max_batches to limit AJAX requests for long stories
        - Retry logic for transient failures
        """
        site_key = metadata.get('site_key') or metadata.get('source')
        story_url = metadata.get('url')
        story_title = metadata.get('title', 'Unknown')

        if not site_key or not story_url:
            logger.error(f"[HealthCheck] Thiếu site_key hoặc url để re-fetch chapters: {story_folder}")
            return False

        # Site-specific delays to avoid anti-bot (2025-12-14)
        site_delays = {
            'xtruyen': 2.0,      # XTruyen cần delay cao hơn
            'tangthuvien': 3.0,  # TangThuVien rất strict
            'truyencom': 1.0,    # TruyenCom ít strict hơn
        }
        chapter_delay = site_delays.get(site_key, 1.0)

        try:
            logger.info(
                f"[HealthCheck] Re-fetching chapters cho '{story_title}': "
                f"có {actual_count}/{expected_count} chapters (delay={chapter_delay}s)"
            )

            adapter = get_adapter_for_site(site_key)
            if not adapter:
                logger.error(f"[HealthCheck] Không tìm thấy adapter cho site '{site_key}'")
                return False

            # Calculate max_batches to avoid fetching all chapters at once (2025-12-14)
            # For xtruyen: chapters come in batches of 200, so calculate needed batches
            missing_count = expected_count - actual_count
            chapters_per_batch = getattr(adapter, '_chapter_list_batch', 200)
            max_batches = max(1, (missing_count + chapters_per_batch - 1) // chapters_per_batch)
            # Cap at 5 batches to avoid overloading
            max_batches = min(max_batches, 5)

            # Get chapter list từ site with max_batches limit
            chapters = await adapter.get_chapter_list(
                story_url=story_url,
                story_title=story_title,
                site_key=site_key,
                total_chapters=expected_count,
                max_batches=max_batches  # Limit AJAX requests (2025-12-14)
            )

            if not chapters:
                logger.warning(f"[HealthCheck] Không lấy được chapter list cho '{story_title}'")
                return False

            # FIX 2026-01-13: Update total_chapters_on_site với giá trị thực tế từ site
            actual_total_on_site = len(chapters)
            if actual_total_on_site != expected_count:
                logger.info(
                    f"[HealthCheck] Cập nhật total_chapters_on_site: {expected_count} -> {actual_total_on_site}"
                )
                metadata['total_chapters_on_site'] = actual_total_on_site
                # Save updated metadata
                metadata_path = os.path.join(story_folder, "metadata.json")
                await safe_write_json(metadata_path, metadata)

            # Lấy danh sách chapters hiện có
            existing_files = set()
            try:
                for filename in os.listdir(story_folder):
                    if filename.endswith('.txt'):
                        existing_files.add(filename)
            except Exception as e:
                logger.error(f"[HealthCheck] Lỗi đọc thư mục: {e}")
                return False

            # FIX 2026-01-13: Check if we already have all available chapters
            if len(existing_files) >= actual_total_on_site:
                logger.info(
                    f"[HealthCheck] ✓ Đã có đủ chapters ({len(existing_files)}/{actual_total_on_site}), "
                    f"không cần fetch thêm"
                )
                return True  # Success - no need to fetch more

            # Fetch các chapters còn thiếu
            fetched_count = 0
            for idx, chapter in enumerate(chapters, start=1):
                chapter_url = chapter.get('url')
                chapter_title = chapter.get('title', f'Chương {idx}')

                # Tạo filename theo format: 0001_chapter-title.txt
                filename = f"{idx:04d}_{self.slugify(chapter_title)}.txt"
                chapter_path = os.path.join(story_folder, filename)

                # Skip nếu chapter đã tồn tại
                if filename in existing_files:
                    continue

                if not chapter_url:
                    logger.warning(f"[HealthCheck] Chapter {idx} thiếu URL")
                    continue

                # Fetch chapter content with retry logic (2025-12-14)
                max_retries = 2
                for retry in range(max_retries + 1):
                    try:
                        content = await adapter.get_chapter_content(
                            chapter_url=chapter_url,
                            chapter_title=chapter_title,
                            site_key=site_key
                        )

                        if content:
                            await safe_write_file(chapter_path, content)
                            fetched_count += 1
                            logger.info(f"[HealthCheck] ✓ Fetched chapter {idx}/{len(chapters)}: {chapter_title}")
                            break  # Success, exit retry loop
                        elif content == "":
                            # Empty content = broken chapter on website, don't retry (2025-12-14)
                            logger.warning(f"[HealthCheck] Chapter {idx} is EMPTY/BROKEN on website, skipping")
                            break
                        else:
                            # None = possible anti-bot, retry
                            if retry < max_retries:
                                logger.warning(f"[HealthCheck] Chapter {idx} returned None, retry {retry+1}/{max_retries}")
                                await asyncio.sleep(chapter_delay * 2)  # Extra delay before retry
                            else:
                                logger.warning(f"[HealthCheck] Chapter {idx} failed after {max_retries} retries")

                    except Exception as e:
                        if retry < max_retries:
                            logger.warning(f"[HealthCheck] Error fetching chapter {idx}, retry {retry+1}/{max_retries}: {e}")
                            await asyncio.sleep(chapter_delay * 2)
                        else:
                            logger.error(f"[HealthCheck] Lỗi fetch chapter {idx} after {max_retries} retries: {e}")
                        continue

                # Site-specific delay to avoid anti-bot (2025-12-14)
                await asyncio.sleep(chapter_delay)

            logger.info(f"[HealthCheck] ✓ Đã re-fetch {fetched_count} chapters cho '{story_title}'")
            return fetched_count > 0

        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi re-fetch chapters cho '{story_title}': {e}")
            return False

    def slugify(self, text: str) -> str:
        """Tạo slug từ text cho filename (bỏ dấu tiếng Việt với unidecode)"""
        if not text:
            return "unknown"

        # Remove Vietnamese diacritics using unidecode
        slug = unidecode(text)
        # Convert to lowercase and strip
        slug = slug.lower().strip()
        # Replace spaces with hyphens
        slug = slug.replace(" ", "-")
        # Remove special characters (keep only alphanumeric and hyphens)
        slug = re.sub(r'[^\w\s-]', '', slug)
        # Replace whitespace with hyphens
        slug = re.sub(r'[\s]+', '-', slug)
        # Remove consecutive hyphens
        slug = re.sub(r'-+', '-', slug)
        # Strip hyphens from ends
        slug = slug.strip("-")

        # Giới hạn độ dài
        if len(slug) > 100:
            slug = slug[:100].rstrip("-")

        return slug or "unknown"

    def safe_folder_name(self, name: str) -> str:
        """Sanitize tên thư mục để tránh ký tự không hợp lệ trên FS."""
        cleaned = "".join(ch if ch.isalnum() or ch in " -_." else "-" for ch in name)
        cleaned = cleaned.strip().strip(".")
        return cleaned or "Unknown"

    def get_expected_genre(self, metadata: dict[str, Any], current_genre: str) -> str | None:
        """Xác định genre folder mong muốn từ metadata (ưu tiên metadata, sau đó category đầu tiên)."""
        # Metadata có thể có key genre_folder hoặc genre
        for key in ("genre_folder", "genre"):
            val = metadata.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        # Lấy category đầu tiên
        categories = metadata.get("categories") or []
        if categories:
            first = categories[0]
            if isinstance(first, dict):
                name = first.get("name") or first.get("title") or ""
            else:
                name = str(first)
            if name.strip():
                return name.strip()
        # Nếu không xác định được thì giữ nguyên
        return current_genre

    def auto_fix_source_field(self, metadata: dict[str, Any]) -> bool:
        """Auto-populate field 'source' từ 'site_key' hoặc 'sources' nếu thiếu

        Returns:
            True nếu đã fix được, False nếu không có gì để fix
        """
        if metadata.get('source'):
            return False  # Đã có rồi

        # Try to get from site_key
        if metadata.get('site_key'):
            metadata['source'] = metadata['site_key']
            logger.info(f"[HealthCheck] Auto-fixed source field from site_key: {metadata['site_key']}")
            return True

        # Try to get from sources array
        sources = metadata.get('sources', [])
        if sources and len(sources) > 0:
            first_source = sources[0]
            if isinstance(first_source, dict) and first_source.get('site_key'):
                metadata['source'] = first_source['site_key']
                logger.info(f"[HealthCheck] Auto-fixed source field from sources[0]: {first_source['site_key']}")
                return True

        logger.warning("[HealthCheck] Cannot auto-fix source field - no site_key or sources found")
        return False

    def _extract_site_key_from_url(self, url: str) -> str | None:
        """Extract site_key từ URL

        Examples:
            https://truyencom.com/story/... -> truyencom
            https://xtruyen.vn/truyen/... -> xtruyen
            https://truyen.tangthuvien.vn/... -> tangthuvien
        """
        if not url:
            return None

        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            # Map domains to site_key
            if 'truyencom.com' in domain:
                return 'truyencom'
            elif 'xtruyen.vn' in domain:
                return 'xtruyen'
            elif 'truyenfullmoi' in domain:
                return 'truyenfullmoi'
            elif 'metruyenful' in domain:
                return 'metruyenful'
            elif 'tangthuvien' in domain:
                return 'tangthuvien'
            elif 'wuxiaworld' in domain:
                return 'wuxiaworld'

            return None
        except:
            return None

    def auto_fix_site_key_field(self, metadata: dict[str, Any]) -> bool:
        """Auto-populate field 'site_key' nếu thiếu

        Priority:
            1. Nếu đã có site_key -> skip
            2. Lấy từ source field
            3. Lấy từ sources[0].site_key
            4. Infer từ URL

        Returns:
            True nếu đã fix được, False nếu không có gì để fix
        """
        if metadata.get('site_key'):
            return False  # Đã có rồi

        # Try from source field
        if metadata.get('source'):
            metadata['site_key'] = metadata['source']
            logger.info(f"[HealthCheck] Auto-fixed site_key field from source: {metadata['source']}")
            return True

        # Try from sources array
        sources = metadata.get('sources', [])
        if sources and len(sources) > 0:
            first_source = sources[0]
            if isinstance(first_source, dict) and first_source.get('site_key'):
                metadata['site_key'] = first_source['site_key']
                logger.info(f"[HealthCheck] Auto-fixed site_key field from sources[0]: {first_source['site_key']}")
                return True
            elif isinstance(first_source, dict) and first_source.get('url'):
                # Infer from URL in sources
                site_key = self._extract_site_key_from_url(first_source['url'])
                if site_key:
                    metadata['site_key'] = site_key
                    logger.info(f"[HealthCheck] Auto-fixed site_key field from sources URL: {site_key}")
                    return True

        # Try to infer from main URL
        url = metadata.get('url')
        if url:
            site_key = self._extract_site_key_from_url(url)
            if site_key:
                metadata['site_key'] = site_key
                logger.info(f"[HealthCheck] Auto-fixed site_key field from main URL: {site_key}")
                return True

        logger.warning("[HealthCheck] Cannot auto-fix site_key field - no source found")
        return False

    def _extract_semantic_chapter_number(self, filename: str) -> int | None:
        """Extract semantic chapter number từ filename (e.g., chuong-894)"""
        import re
        match = re.search(r'chuong-(\d+)', filename, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None

    def _build_title_maps_from_metadata(self, metadata: dict[str, Any]) -> tuple[dict[int, str], dict[str, str]]:
        """
        Build title maps từ metadata.json chapters array.
        Returns: (titles_by_chapter_num, titles_by_url_pattern)
        """
        import re
        titles_by_num: dict[int, str] = {}
        titles_by_url: dict[str, str] = {}

        chapters = metadata.get("chapters", [])
        for ch in chapters:
            if not isinstance(ch, dict):
                continue

            title = ch.get("title", "")
            url = ch.get("url", "")

            # Extract chapter number from title: "Chương XXX: Title"
            match = re.search(r'(?:Chương|Chuong|chương|chuong)\s*(\d+)', title, re.IGNORECASE)
            if match:
                ch_num = int(match.group(1))
                titles_by_num[ch_num] = title

            # Extract pattern from URL for matching
            if url:
                url_parts = url.rstrip('/').split('/')
                if url_parts:
                    pattern = url_parts[-1].replace('.html', '')  # e.g., "chuong-76"
                    titles_by_url[pattern] = title

        return titles_by_num, titles_by_url

    def _generate_chapter_title(
        self,
        filename: str,
        semantic_num: int | None,
        titles_by_num: dict[int, str],
        titles_by_url: dict[str, str]
    ) -> str:
        """Generate title cho chapter từ các nguồn, ưu tiên title có dấu từ metadata"""
        import re

        # 1. Try exact chapter number match from metadata
        if semantic_num and semantic_num in titles_by_num:
            return titles_by_num[semantic_num]

        # 2. Try URL pattern match
        if semantic_num:
            pattern = f"chuong-{semantic_num}"
            if pattern in titles_by_url:
                return titles_by_url[pattern]

        # 3. Fallback: Generate from filename (mất dấu nhưng vẫn readable)
        # Extract slug from filename: "0820_chuong-894-dong-can-sinh.txt" -> "dong-can-sinh"
        slug_match = re.search(r'chuong-\d+-(.+)\.txt$', filename, re.IGNORECASE)
        if slug_match:
            slug = slug_match.group(1)
            title_text = ' '.join(word.capitalize() for word in slug.replace('-', ' ').split())
            if semantic_num:
                return f"Chương {semantic_num}: {title_text}"
            return title_text

        # 4. Last resort
        if semantic_num:
            return f"Chương {semantic_num}"

        return "Unknown Chapter"

    def generate_metadata_chapter(self, story_folder: str, metadata: dict[str, Any]) -> dict[str, Any] | None:
        """Tạo chapter_metadata.json từ chapter files trong thư mục.

        Logic cải tiến:
        1. Scan actual .txt files để lấy danh sách files thực
        2. Extract semantic chapter number từ filename (chuong-XXX)
        3. Match với metadata.json chapters array để lấy title có dấu
        4. ALSO check existing chapter_metadata.json để preserve original titles
        5. Fallback sang parse từ filename nếu không tìm được

        Returns:
            List of chapter entries hoặc None nếu không tạo được
        """
        import re

        try:
            # Build title maps từ metadata.json chapters (nếu có)
            titles_by_num, titles_by_url = self._build_title_maps_from_metadata(metadata)

            # ENHANCEMENT: Also build title maps from existing chapter_metadata.json
            # This preserves original titles even when metadata.json is missing chapters
            existing_titles_by_file = {}
            existing_titles_by_index = {}
            chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
            if os.path.exists(chapter_meta_path):
                try:
                    with open(chapter_meta_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)

                    # Handle both list and dict formats
                    if isinstance(existing_data, dict):
                        existing_chapters = existing_data.get("chapters") or existing_data.get("items") or []
                    elif isinstance(existing_data, list):
                        existing_chapters = existing_data
                    else:
                        existing_chapters = []

                    # Build lookups
                    for ch in existing_chapters:
                        if not isinstance(ch, dict):
                            continue

                        title = ch.get("title")
                        if not title:
                            continue

                        # Map by filename
                        filename = ch.get('file') or ch.get('filename')
                        if filename:
                            existing_titles_by_file[filename] = title

                        # Map by index
                        idx = ch.get('index')
                        if idx:
                            existing_titles_by_index[idx] = title

                    logger.debug(f"[HealthCheck] Loaded {len(existing_titles_by_file)} existing titles from chapter_metadata.json")
                except Exception as e:
                    logger.debug(f"[HealthCheck] Could not load existing chapter_metadata.json: {e}")

            # Scan all .txt files
            chapter_files = []
            for filename in sorted(os.listdir(story_folder)):
                if not filename.endswith('.txt'):
                    continue

                # Extract index (prefix before _)
                parts = filename.split('_', 1)
                if len(parts) < 1:
                    continue

                try:
                    index = int(parts[0])
                except ValueError:
                    continue

                # Extract semantic chapter number
                semantic_num = self._extract_semantic_chapter_number(filename)

                # Generate title with priority order:
                # 1. Existing chapter_metadata.json (preserves original titles with diacritics)
                # 2. metadata.json chapters array
                # 3. Fallback: parse from filename
                title = None

                # Priority 1: Check existing chapter_metadata.json by filename
                if filename in existing_titles_by_file:
                    title = existing_titles_by_file[filename]
                # Priority 2: Check existing chapter_metadata.json by index
                elif index in existing_titles_by_index:
                    title = existing_titles_by_index[index]
                # Priority 3: Check metadata.json
                else:
                    title = self._generate_chapter_title(filename, semantic_num, titles_by_num, titles_by_url)

                # Try to find URL from metadata chapters
                url = ""
                if semantic_num:
                    for ch in metadata.get("chapters", []):
                        ch_url = ch.get("url", "")
                        if f"chuong-{semantic_num}" in ch_url or f"chapter-{semantic_num}" in ch_url:
                            url = ch_url
                            break

                chapter_files.append({
                    "index": index,
                    "chapter_number": semantic_num,
                    "title": title,
                    "url": url,
                    "file": filename
                })

            if not chapter_files:
                logger.warning(f"[HealthCheck] No chapter files found in {story_folder}")
                return None

            # Sort by index
            chapter_files.sort(key=lambda x: x['index'])

            # Build chapter_metadata.json structure (array format để khớp với existing)
            chapters_list = [
                {
                    "index": ch["index"],
                    "chapter_number": ch["chapter_number"],
                    "title": ch["title"],
                    "url": ch["url"],
                    "file": ch["file"]
                }
                for ch in chapter_files
            ]

            # DEBUG: Log first 3 chapters to verify chapter_number is included
            if chapters_list:
                logger.info(f"[HealthCheck][DEBUG] Sample chapters_list: {chapters_list[:3]}")

            logger.info(
                f"[HealthCheck] Generated chapter_metadata with {len(chapters_list)} chapters "
                f"(matched {len([c for c in chapter_files if c['url']])} URLs)"
            )
            return chapters_list

        except Exception as e:
            logger.error(f"[HealthCheck] Error generating chapter_metadata: {e}")
            return None

    def check_chapter_metadata_mismatch(self, story_folder: str) -> tuple[bool, int]:
        """
        Kiểm tra xem chapter_metadata.json có khớp với actual files không.

        Returns:
            (has_mismatch, mismatch_count)
        """
        import re

        chapter_meta_path = os.path.join(story_folder, "chapter_metadata.json")
        if not os.path.exists(chapter_meta_path):
            chapter_meta_path = os.path.join(story_folder, "metadata-chapter.json")

        if not os.path.exists(chapter_meta_path):
            return False, 0  # No metadata to check

        try:
            # Load chapter metadata
            with open(chapter_meta_path, encoding="utf-8") as f:
                data = json.load(f)

            # Handle both formats
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("chapters") or data.get("items") or []
            else:
                return False, 0

            # Build map: index -> expected_file
            expected_files = {}
            for item in items:
                if isinstance(item, dict):
                    idx = item.get("index")
                    file_name = item.get("file") or item.get("filename")
                    if idx is not None and file_name:
                        expected_files[idx] = file_name

            # Scan actual files
            actual_files = {}
            for filename in os.listdir(story_folder):
                if not filename.endswith('.txt'):
                    continue
                parts = filename.split('_', 1)
                if parts:
                    try:
                        idx = int(parts[0])
                        actual_files[idx] = filename
                    except ValueError:
                        continue

            # Count mismatches
            mismatch_count = 0
            all_indices = set(expected_files.keys()) | set(actual_files.keys())

            for idx in all_indices:
                expected = expected_files.get(idx)
                actual = actual_files.get(idx)

                if expected and actual and expected != actual:
                    mismatch_count += 1
                elif expected and not actual:
                    mismatch_count += 1  # Missing file
                elif actual and not expected:
                    mismatch_count += 1  # Missing in metadata

            # Consider mismatch if > 5% files don't match
            threshold = max(5, len(actual_files) * 0.05)
            has_mismatch = mismatch_count > threshold

            return has_mismatch, mismatch_count

        except Exception as e:
            logger.warning(f"[HealthCheck] Error checking chapter metadata mismatch: {e}")
            return False, 0

    def relocate_if_wrong_genre(self, story_folder: str, genre_folder: str, metadata: dict[str, Any]) -> str:
        """Nếu thư mục đang ở sai genre (ví dụ Unknown) thì di chuyển sang genre đúng."""
        expected_genre = self.get_expected_genre(metadata, genre_folder)
        if not expected_genre:
            return story_folder

        # So sánh không phân biệt hoa thường và bỏ khoảng trắng dư
        norm_current = genre_folder.strip().lower()
        norm_expected = expected_genre.strip().lower()

        if norm_current == norm_expected:
            return story_folder

        # Nếu thư mục hiện tại là Unknown/Unknow hoặc khác hoàn toàn, tiến hành move
        target_genre_name = self.safe_folder_name(expected_genre)
        target_genre_path = os.path.join(self.completed_folder, target_genre_name)
        os.makedirs(target_genre_path, exist_ok=True)

        story_name = os.path.basename(story_folder)
        target_story_path = os.path.join(target_genre_path, story_name)

        # Tránh đè lên thư mục đã tồn tại
        if os.path.exists(target_story_path):
            suffix = 1
            while os.path.exists(f"{target_story_path}-dup{suffix}"):
                suffix += 1
            target_story_path = f"{target_story_path}-dup{suffix}"

        logger.info(
            f"[HealthCheck] Di chuyển story '{metadata.get('title', story_name)}' "
            f"từ genre '{genre_folder}' -> '{target_genre_name}'"
        )
        os.rename(story_folder, target_story_path)
        self.stats["stories_relocated"] += 1
        # Cập nhật metadata ghi nhận genre_folder mong muốn
        metadata["genre_folder"] = target_genre_name
        try:
            with open(os.path.join(target_story_path, "metadata.json"), "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[HealthCheck] Không ghi được metadata sau khi move: {e}")

        return target_story_path

    async def check_and_fix_story(self, story_folder: str) -> bool:
        """Kiểm tra và fix một story

        Returns:
            True nếu story OK hoặc đã fix thành công, False nếu có lỗi
        """
        try:
            self.stats["total_stories_scanned"] += 1

            # Read metadata
            genre_folder = os.path.basename(os.path.dirname(story_folder))
            metadata = self.read_metadata(story_folder)
            if not metadata:
                metadata = self.bootstrap_metadata_from_chapter_header(story_folder, genre_folder)
                if not metadata:
                    logger.warning(f"[HealthCheck] Skip story không có metadata: {story_folder}")
                    return False
                metadata_path = os.path.join(story_folder, "metadata.json")
                await safe_write_json(metadata_path, metadata)
                logger.info(f"[HealthCheck] Đã tạo metadata.json từ chapter header: {story_folder}")

            story_title = metadata.get('title', os.path.basename(story_folder))
            needs_fix = False
            needs_save = False
            fix_actions = []
            wrong_genre = "unknown" in genre_folder.lower()

            # 0. Auto-fix source field if missing (quick fix không cần refetch)
            if self.auto_fix_source_field(metadata):
                needs_save = True
                fix_actions.append("auto_fixed_source")

            # 0.1 Auto-fix site_key field if missing (FIX 2025-12-23)
            if self.auto_fix_site_key_field(metadata):
                needs_save = True
                fix_actions.append("auto_fixed_site_key")

            # 0.1 Check and generate/regenerate chapter metadata file
            # Support both naming conventions: metadata-chapter.json and chapter_metadata.json
            metadata_chapter_path = os.path.join(story_folder, "metadata-chapter.json")
            chapter_metadata_path = os.path.join(story_folder, "chapter_metadata.json")

            # Check if either file exists
            has_chapter_metadata = os.path.exists(metadata_chapter_path) or os.path.exists(chapter_metadata_path)

            # Check for mismatch between metadata and actual files
            has_mismatch, mismatch_count = False, 0
            if has_chapter_metadata:
                has_mismatch, mismatch_count = self.check_chapter_metadata_mismatch(story_folder)

            if not has_chapter_metadata:
                logger.info(f"[HealthCheck] '{story_title}' thiếu chapter metadata file, đang tạo...")
                regenerate_reason = "missing"
            elif has_mismatch:
                self.stats["stories_with_chapter_meta_mismatch"] += 1
                logger.info(
                    f"[HealthCheck] '{story_title}' có {mismatch_count} chapters không khớp metadata, "
                    f"đang regenerate..."
                )
                regenerate_reason = f"mismatch:{mismatch_count}"
            else:
                regenerate_reason = None

            if regenerate_reason:
                metadata_chapter = self.generate_metadata_chapter(story_folder, metadata)
                if metadata_chapter:
                    # Use chapter_metadata.json as default (matching existing convention)
                    # Backup old file if exists
                    if has_chapter_metadata and os.path.exists(chapter_metadata_path):
                        backup_path = chapter_metadata_path + ".bak"
                        try:
                            import shutil
                            shutil.copy2(chapter_metadata_path, backup_path)
                        except Exception:
                            pass

                    await safe_write_json(chapter_metadata_path, metadata_chapter)
                    fix_actions.append(f"regenerated_chapter_metadata:{regenerate_reason}")
                    self.stats["chapter_metadata_regenerated"] += 1
                    logger.info(f"[HealthCheck] ✓ Đã tạo/cập nhật chapter_metadata.json cho '{story_title}'")
                else:
                    logger.warning(f"[HealthCheck] ✗ Không tạo được chapter_metadata.json cho '{story_title}'")

            # 1. Check unknown category
            if self.has_unknown_category(metadata):
                self.stats["stories_with_unknown_category"] += 1
                needs_fix = True
                fix_actions.append("unknown_category")
                if self._should_log_sample("unknown_category", "unknown category"):
                    logger.warning(f"[HealthCheck] '{story_title}' có unknown category")

            # 2. Check missing metadata
            has_missing, missing_fields = self.has_missing_metadata(metadata)
            if has_missing:
                self.stats["stories_with_missing_metadata"] += 1
                needs_fix = True
                fix_actions.append(f"missing_metadata:{','.join(missing_fields)}")
                if self._should_log_sample("missing_metadata", "missing metadata"):
                    logger.warning(f"[HealthCheck] '{story_title}' thiếu metadata: {missing_fields}")

            # 3. Check missing chapters
            has_missing_chaps, actual, expected = self.has_missing_chapters(metadata, story_folder)
            if has_missing_chaps:
                self.stats["stories_with_missing_chapters"] += 1
                needs_fix = True
                fix_actions.append(f"missing_chapters:{actual}/{expected}")
                if self._should_log_sample("missing_chapters", "missing chapters"):
                    logger.warning(f"[HealthCheck] '{story_title}' thiếu chapters: {actual}/{expected}")

            # Save metadata if auto-fixes were applied
            if needs_save:
                metadata_path = os.path.join(story_folder, "metadata.json")
                await safe_write_json(metadata_path, metadata)
                logger.info(f"[HealthCheck] ✓ Đã lưu auto-fixes cho '{story_title}'")

            # Fix if needed
            if needs_fix and self.enable_refetch:
                logger.info(f"[HealthCheck] Bắt đầu fix '{story_title}': {fix_actions}")

                # Fix metadata (unknown category + missing fields)
                if "unknown_category" in str(fix_actions) or "missing_metadata" in str(fix_actions) or wrong_genre:
                    updated_metadata = await self.refetch_story_metadata(metadata, story_folder)
                    if updated_metadata:
                        # Save updated metadata
                        metadata_path = os.path.join(story_folder, "metadata.json")
                        await safe_write_json(metadata_path, updated_metadata)
                        metadata = updated_metadata
                        logger.info(f"[HealthCheck] ✓ Đã fix metadata cho '{story_title}'")
                    else:
                        logger.error(f"[HealthCheck] ✗ Không fix được metadata cho '{story_title}'")
                        self.stats["stories_failed"] += 1
                        return False

                # Relocate sau khi đã có metadata mới (tránh move sai)
                new_story_folder = self.relocate_if_wrong_genre(story_folder, genre_folder, metadata)
                if new_story_folder != story_folder:
                    story_folder = new_story_folder
                    genre_folder = os.path.basename(os.path.dirname(story_folder))
                    fix_actions.append("relocated_genre")

                # Fix missing chapters
                if has_missing_chaps:
                    success = await self.refetch_missing_chapters(metadata, story_folder, actual, expected)
                    if success:
                        logger.info(f"[HealthCheck] ✓ Đã fix chapters cho '{story_title}'")
                    else:
                        logger.warning(f"[HealthCheck] ✗ Không fix được chapters cho '{story_title}'")
                        self.stats["stories_failed"] += 1
                        return False

                self.stats["stories_fixed"] += 1
                return True

            elif needs_fix:
                logger.info(f"[HealthCheck] Story '{story_title}' cần fix nhưng HEALTH_CHECK_ENABLE_REFETCH=false")
                # Even if refetch disabled, still count auto-fixes as success
                if fix_actions and ("auto_fixed_source" in fix_actions or "generated_metadata_chapter" in fix_actions):
                    self.stats["stories_fixed"] += 1
                    return True
                return False

            # Story is healthy (nhưng nếu đang ở genre Unknown/unknown-uncategorized thì vẫn thử relocate nhẹ)
            new_story_folder = self.relocate_if_wrong_genre(story_folder, genre_folder, metadata)
            if new_story_folder != story_folder:
                self.stats["stories_relocated"] += 1

            # Count auto-fixes as successful
            if fix_actions:
                self.stats["stories_fixed"] += 1

            return True

        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi check story {story_folder}: {e}")
            self.stats["stories_failed"] += 1
            return False

    async def scan_completed_stories(self):
        """Quét tất cả stories trong completed_stories"""
        try:
            if not os.path.exists(self.completed_folder):
                logger.warning(f"[HealthCheck] Thư mục completed không tồn tại: {self.completed_folder}")
                return

            logger.info(f"[HealthCheck] Bắt đầu quét {self.completed_folder}")

            # Get all genre folders
            genre_folders = []
            for item in os.listdir(self.completed_folder):
                genre_path = os.path.join(self.completed_folder, item)
                if os.path.isdir(genre_path):
                    genre_folders.append((item, genre_path))

            logger.info(f"[HealthCheck] Tìm thấy {len(genre_folders)} genre folders")

            # Scan each genre folder
            for genre_name, genre_path in genre_folders:
                logger.info(f"[HealthCheck] Scanning genre: {genre_name}")

                try:
                    story_folders = []
                    for item in os.listdir(genre_path):
                        story_path = os.path.join(genre_path, item)
                        if os.path.isdir(story_path):
                            story_folders.append(story_path)

                    logger.info(f"[HealthCheck] Genre '{genre_name}': {len(story_folders)} stories")

                    # Process stories in batches
                    for i in range(0, len(story_folders), self.batch_size):
                        batch = story_folders[i:i+self.batch_size]

                        # Process batch concurrently
                        tasks = [self.check_and_fix_story(story_folder) for story_folder in batch]
                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        # Log results
                        success_count = sum(1 for r in results if r is True)
                        logger.info(
                            f"[HealthCheck] Batch {i//self.batch_size + 1}: "
                            f"{success_count}/{len(batch)} OK"
                        )

                        # Sleep between batches
                        if i + self.batch_size < len(story_folders):
                            await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"[HealthCheck] Lỗi scan genre {genre_name}: {e}")
                    continue

            # Update state
            self.state["last_check_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_state()

            # Log final stats
            logger.info("[HealthCheck] === STATS ===")
            logger.info(f"[HealthCheck] Tổng stories quét: {self.stats['total_stories_scanned']}")
            logger.info(f"[HealthCheck] Stories có unknown category: {self.stats['stories_with_unknown_category']}")
            logger.info(f"[HealthCheck] Stories thiếu metadata: {self.stats['stories_with_missing_metadata']}")
            logger.info(f"[HealthCheck] Stories thiếu chapters: {self.stats['stories_with_missing_chapters']}")
            logger.info(f"[HealthCheck] Stories có chapter metadata mismatch: {self.stats['stories_with_chapter_meta_mismatch']}")
            logger.info(f"[HealthCheck] Chapter metadata đã regenerate: {self.stats['chapter_metadata_regenerated']}")
            logger.info(f"[HealthCheck] Stories đã fix: {self.stats['stories_fixed']}")
            logger.info(f"[HealthCheck] Stories lỗi: {self.stats['stories_failed']}")

        except Exception as e:
            logger.error(f"[HealthCheck] Lỗi scan completed_stories: {e}")

    async def run(self):
        """Main loop"""
        logger.info("[HealthCheck] Worker started")
        logger.info(f"[HealthCheck] Completed folder: {self.completed_folder}")
        logger.info(f"[HealthCheck] Check interval: {self.check_interval}s")
        logger.info(f"[HealthCheck] Batch size: {self.batch_size}")
        logger.info(f"[HealthCheck] Enable refetch: {self.enable_refetch}")

        while True:
            try:
                # Reset stats
                self.stats = {
                    "total_stories_scanned": 0,
                    "stories_with_unknown_category": 0,
                    "stories_with_missing_metadata": 0,
                    "stories_with_missing_chapters": 0,
                    "stories_with_chapter_meta_mismatch": 0,
                    "chapter_metadata_regenerated": 0,
                    "stories_relocated": 0,
                    "stories_fixed": 0,
                    "stories_failed": 0,
                }
                self._reset_log_samples()

                # Scan all completed stories
                await self.scan_completed_stories()

                # Wait for next check
                logger.info(f"[HealthCheck] Chờ {self.check_interval}s cho lần check tiếp theo...")
                await asyncio.sleep(self.check_interval)

            except KeyboardInterrupt:
                logger.info("[HealthCheck] Worker stopped by user")
                break
            except Exception as e:
                logger.error(f"[HealthCheck] Lỗi trong main loop: {e}")
                await asyncio.sleep(60)


async def main():
    """Entry point"""
    worker = CompletedStoryHealthChecker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

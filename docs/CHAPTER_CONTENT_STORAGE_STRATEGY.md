# Chapter Content Storage Strategy

## Overview

StoryFlow hỗ trợ 2 chiến lược lưu trữ nội dung chapter vào database:

1. **File-based Storage (Recommended)** - Chỉ lưu metadata, content đọc từ file
2. **Database Storage** - Lưu toàn bộ content vào database

## Configuration

Sử dụng biến môi trường `DB_SYNC_CONTENT` trong `config/env/database.env`:

```env
# false: Chỉ lưu metadata, tiết kiệm storage (RECOMMENDED)
# true: Lưu toàn bộ content vào DB, query nhanh hơn nhưng tốn storage
DB_SYNC_CONTENT=false
```

## Strategy Comparison

### File-based Storage (DB_SYNC_CONTENT=false)

**Ưu điểm:**
- ✅ Tiết kiệm rất nhiều storage trên database
- ✅ Database backup nhẹ và nhanh
- ✅ Dễ dàng thay đổi/sửa content trực tiếp từ file
- ✅ Phù hợp với hệ thống có nhiều truyện (hàng chục nghìn chapter)

**Nhược điểm:**
- ❌ Query content phải đọc từ file system (chậm hơn một chút)
- ❌ Cần đảm bảo file system luôn available
- ❌ Phải maintain path integrity giữa DB và file

**Database Schema:**
```sql
chapters:
  - id
  - story_id
  - chapter_number
  - title
  - content: NULL           # Không lưu content
  - file_path: "/app/..."   # Path tới file .txt
  - file_name: "0001_..."   # Tên file
  - word_count
```

### Database Storage (DB_SYNC_CONTENT=true)

**Ưu điểm:**
- ✅ Query content trực tiếp từ DB (nhanh hơn)
- ✅ Không phụ thuộc vào file system
- ✅ Full-text search dễ dàng hơn (có thể index TEXT column)
- ✅ Dễ dàng backup/restore toàn bộ từ DB dump

**Nhược điểm:**
- ❌ Tốn rất nhiều storage (mỗi chapter ~10-50KB)
- ❌ Database size phình to rất nhanh (có thể lên hàng chục GB)
- ❌ Backup/restore database chậm và tốn bandwidth
- ❌ Chi phí hosting database cao hơn

**Database Schema:**
```sql
chapters:
  - id
  - story_id
  - chapter_number
  - title
  - content: TEXT           # Lưu toàn bộ nội dung
  - file_path: "/app/..."   # Path tới file (backup)
  - file_name: "0001_..."   # Tên file (backup)
  - word_count
```

## Storage Size Estimation

Giả sử hệ thống có:
- 1,000 truyện
- Trung bình 500 chapter/truyện
- Trung bình 3,000 từ/chapter (~15KB UTF-8)

### File-based Storage
```
Metadata only: 1,000 stories × 500 chapters × 200 bytes = ~100 MB
Content files: 1,000 stories × 500 chapters × 15 KB = ~7.5 GB (on filesystem)
Total DB size: ~100 MB
```

### Database Storage
```
Metadata + Content: 1,000 stories × 500 chapters × 15.2 KB = ~7.6 GB
Total DB size: ~7.6 GB (76x larger than file-based!)
```

## UTF-8 Encoding Handling

Cả 2 chiến lược đều xử lý tiếng Việt đúng cách:

### Reading Files (Line 148-153)
```python
def read_chapter_content(self, chapter_path: str) -> Optional[str]:
    """Đọc nội dung chapter từ file"""
    try:
        with open(chapter_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        logger.error(f"[DB-Sync] Lỗi đọc chapter content: {e}")
        return None
```

### Database Models (models.py Line 88)
```python
content = Column(Text)  # SQLAlchemy Text tự động handle UTF-8
```

### MySQL Configuration
Đảm bảo database sử dụng UTF-8:
```sql
CREATE DATABASE storyflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

## Implementation Details

### Sync Worker Logic (database_sync_worker.py)

```python
def __init__(self):
    # Line 36: Đọc config
    self.sync_content = os.getenv("DB_SYNC_CONTENT", "false").lower() == "true"

def sync_chapters(self, session, story, chapter_files, story_folder):
    for chapter_file in chapter_files:
        # Line 439: Đọc content nếu được bật
        content = self.read_chapter_content(chapter_path) if self.sync_content else None

        # Line 442: Tính word count
        word_count = len(content.split()) if content else 0

        if is_mysql:
            # Line 445-454: MySQL upsert với/không content
            self._upsert_chapter_mysql(
                session=session,
                story_id=story.id,
                chapter_number=chapter_number,
                title=chapter_title,
                chapter_path=chapter_path,
                chapter_file=chapter_file,
                content=content,  # None hoặc full content
                word_count=word_count,
            )
        else:
            # Line 458-486: PostgreSQL/MSSQL insert/update
            if existing_chapter:
                if self.sync_content:
                    existing_chapter.content = content  # Chỉ update nếu bật
            else:
                chapter = Chapter(
                    content=content,  # None hoặc full content
                    ...
                )
```

## Recommendation

**Dùng File-based Storage (DB_SYNC_CONTENT=false)** vì:

1. **Scalability**: Hệ thống có thể scale lên hàng chục nghìn truyện mà không lo database phình to
2. **Cost**: Chi phí database hosting thấp hơn rất nhiều
3. **Flexibility**: Dễ dàng migrate content giữa các storage backend
4. **Performance**: File system caching của OS rất hiệu quả

**Chỉ dùng Database Storage nếu:**
- Cần full-text search trên content
- Không có file system persistence (container ephemeral storage)
- Performance query content là priority số 1

## Migration Guide

### Chuyển từ Database → File-based

```python
# Export content từ DB ra files
from storyflow_core.database.connection import db_manager
from storyflow_core.database.models import Chapter

with db_manager.get_session() as session:
    chapters = session.query(Chapter).filter(Chapter.content.isnot(None)).all()
    for chapter in chapters:
        with open(chapter.file_path, 'w', encoding='utf-8') as f:
            f.write(chapter.content)

        # Clear content từ DB
        chapter.content = None
    session.commit()
```

### Chuyển từ File-based → Database

```bash
# Chỉ cần set env var và restart sync worker
export DB_SYNC_CONTENT=true
docker-compose restart database-sync-worker
```

Sync worker sẽ tự động đọc content từ file và sync vào database ở chu kỳ tiếp theo.

## Monitoring

Kiểm tra storage usage:

```sql
-- MySQL: Kiểm tra kích thước bảng chapters
SELECT
    table_name,
    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
FROM information_schema.TABLES
WHERE table_schema = 'storyflow' AND table_name = 'chapters';

-- Kiểm tra content NULL ratio
SELECT
    COUNT(*) as total_chapters,
    SUM(CASE WHEN content IS NULL THEN 1 ELSE 0 END) as null_content,
    ROUND(SUM(CASE WHEN content IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
FROM chapters;
```

## Summary

| Aspect | File-based | Database |
|--------|-----------|----------|
| Storage efficiency | ⭐⭐⭐⭐⭐ | ⭐ |
| Query performance | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Scalability | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| Cost | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| Full-text search | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| Backup/restore | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| **Recommended** | ✅ YES | ❌ NO |

"""
Database models cho StoryFlow
Hỗ trợ: MySQL, PostgreSQL, MS SQL Server
"""
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, event
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Story(Base):
    """Bảng lưu thông tin truyện"""
    __tablename__ = 'stories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(500), nullable=False, index=True)
    # MySQL with UTF8MB4: max index key = 3072 bytes, 191 chars = 764 bytes
    slug = Column(String(191), unique=True, nullable=False, index=True)
    url = Column(String(1000))
    site_key = Column(String(100), index=True)

    # Thông tin tác giả và mô tả
    author = Column(String(500), index=True)
    # MySQL: MEDIUMTEXT (16MB max), PostgreSQL/MSSQL: TEXT (unlimited)
    description = Column(Text().with_variant(MEDIUMTEXT, 'mysql'))
    cover = Column(String(1000))

    # Thông tin trạng thái
    status = Column(String(100))  # "Đang ra", "Hoàn thành", etc.
    crawled_by = Column(String(100))

    # Rating
    rating_value = Column(Float)
    rating_count = Column(Integer)

    # Số chương
    total_chapters_on_site = Column(Integer, default=0)
    crawled_chapters = Column(Integer, default=0)

    # Timestamps
    crawled_at = Column(DateTime)
    metadata_updated_at = Column(DateTime)
    synced_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # File system path
    # MySQL UTF8MB4: unique index limited to 768 bytes = 191 chars (768/4)
    # Use hash column for unique constraint on long paths
    folder_path = Column(String(1000), nullable=False)
    folder_path_hash = Column(String(64), unique=True, nullable=False)  # SHA256 hash for unique constraint
    genre_folder = Column(String(200), index=True)  # Thư mục thể loại (Tiên Hiệp, etc.)

    # Skip flag
    skip_crawl = Column(Boolean, default=False)
    skip_reason = Column(String(500))

    # Relationships
    chapters = relationship("Chapter", back_populates="story", cascade="all, delete-orphan")
    categories = relationship("StoryCategory", back_populates="story", cascade="all, delete-orphan")
    sources = relationship("StorySource", back_populates="story", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('idx_site_status', 'site_key', 'status'),
        Index('idx_genre_folder', 'genre_folder'),
        # MySQL index length must stay under 3072 bytes; use prefix indexes
        Index('idx_story_url_prefix', 'url', mysql_length=191),
        Index('idx_story_folder_path_prefix', 'folder_path', mysql_length=191),
        # Ensure UTF8MB4 for MySQL to support Vietnamese characters
        # Enable InnoDB Row Compression for MEDIUMTEXT columns
        {
            'mysql_charset': 'utf8mb4',
            'mysql_collate': 'utf8mb4_unicode_ci',
            'mysql_row_format': 'COMPRESSED',
            'mysql_key_block_size': '8'
        }
    )


class Chapter(Base):
    """Bảng lưu thông tin chapter"""
    __tablename__ = 'chapters'

    id = Column(Integer, primary_key=True, autoincrement=True)
    story_id = Column(Integer, ForeignKey('stories.id', ondelete='CASCADE'), nullable=False, index=True)

    # Thông tin chapter
    chapter_number = Column(Integer, nullable=False)  # Số thứ tự chapter (1, 2, 3...)
    title = Column(String(500), nullable=False)
    url = Column(String(1000))

    # Nội dung
    # MySQL: MEDIUMTEXT (16MB max), PostgreSQL/MSSQL: TEXT (unlimited)
    content = Column(Text().with_variant(MEDIUMTEXT, 'mysql'))  # Có thể lưu toàn bộ nội dung hoặc null nếu chỉ lưu file
    file_path = Column(String(1000))  # Path tới file .txt
    file_name = Column(String(500))  # Tên file (ví dụ: 0001_chuong-1.txt)

    # Word count
    word_count = Column(Integer)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    synced_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship
    story = relationship("Story", back_populates="chapters")

    # Constraints và indexes
    __table_args__ = (
        UniqueConstraint('story_id', 'chapter_number', name='uq_story_chapter'),
        Index('idx_story_chapter', 'story_id', 'chapter_number'),
        # Index for duplicate detection in cleanup worker
        Index('idx_story_title', 'story_id', 'title', mysql_length={'title': 191}),
        # Ensure UTF8MB4 for MySQL to support Vietnamese characters
        # Enable InnoDB Row Compression for MEDIUMTEXT columns
        {
            'mysql_charset': 'utf8mb4',
            'mysql_collate': 'utf8mb4_unicode_ci',
            'mysql_row_format': 'COMPRESSED',
            'mysql_key_block_size': '8'
        }
    )


class Category(Base):
    """Bảng lưu danh mục/thể loại"""
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), unique=True, nullable=False, index=True)
    slug = Column(String(200), unique=True, nullable=False, index=True)
    url = Column(String(1000))

    # Relationships
    stories = relationship("StoryCategory", back_populates="category", cascade="all, delete-orphan")


class StoryCategory(Base):
    """Bảng trung gian giữa Story và Category (many-to-many)"""
    __tablename__ = 'story_categories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    story_id = Column(Integer, ForeignKey('stories.id', ondelete='CASCADE'), nullable=False, index=True)
    category_id = Column(Integer, ForeignKey('categories.id', ondelete='CASCADE'), nullable=False, index=True)

    # Relationships
    story = relationship("Story", back_populates="categories")
    category = relationship("Category", back_populates="stories")

    # Constraint
    __table_args__ = (
        UniqueConstraint('story_id', 'category_id', name='uq_story_category'),
    )


class StorySource(Base):
    """Bảng lưu các nguồn của truyện"""
    __tablename__ = 'story_sources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    story_id = Column(Integer, ForeignKey('stories.id', ondelete='CASCADE'), nullable=False, index=True)

    url = Column(String(1000), nullable=False)
    url_hash = Column(String(64), nullable=False)  # SHA256 hash for unique constraint
    site_key = Column(String(100), nullable=False, index=True)
    is_primary = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationship
    story = relationship("Story", back_populates="sources")

    # Constraint - use hash for uniqueness to avoid key length limit
    __table_args__ = (
        UniqueConstraint('story_id', 'url_hash', name='uq_story_source'),
    )


class SyncLog(Base):
    """Bảng log quá trình sync"""
    __tablename__ = 'sync_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_started_at = Column(DateTime, default=datetime.utcnow)
    sync_completed_at = Column(DateTime)

    total_stories_scanned = Column(Integer, default=0)
    total_stories_synced = Column(Integer, default=0)
    total_chapters_synced = Column(Integer, default=0)
    total_errors = Column(Integer, default=0)

    status = Column(String(50))  # 'running', 'completed', 'failed'
    error_message = Column(Text)

    # Index
    __table_args__ = (
        Index('idx_sync_started', 'sync_started_at'),
    )



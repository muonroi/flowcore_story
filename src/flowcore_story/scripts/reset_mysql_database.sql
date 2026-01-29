-- Script to reset MySQL database with proper UTF8MB4 encoding
-- Run this to fix Vietnamese character issues

-- Drop all tables
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS `sync_logs`;
DROP TABLE IF EXISTS `story_sources`;
DROP TABLE IF EXISTS `story_categories`;
DROP TABLE IF EXISTS `chapters`;
DROP TABLE IF EXISTS `stories`;
DROP TABLE IF EXISTS `categories`;
SET FOREIGN_KEY_CHECKS = 1;

-- Ensure database uses UTF8MB4
ALTER DATABASE storyflow CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

-- Tables will be recreated by SQLAlchemy with UTF8MB4 encoding
-- via the models.py __table_args__ configuration

"""
Utility script to clean Unicode-broken rows or fully reset database tables.

Use cases:
- Remove only rows containing the replacement character "�" (likely decode failures).
- Truncate all sync tables to resync from scratch.
"""
import argparse

from sqlalchemy import text

from flowcore_story.database.connection import db_manager
from flowcore_story.database.models import Category, Chapter, Story
from flowcore_story.utils.logger import logger


def delete_corrupt_rows() -> None:
    """Delete rows that contain the Unicode replacement character."""
    with db_manager.get_session() as session:
        corrupt_chapters = session.query(Chapter).filter(
            (Chapter.title.contains("�")) |
            (Chapter.file_name.contains("�")) |
            (Chapter.content.contains("�"))
        ).all()

        corrupt_stories = session.query(Story).filter(
            (Story.title.contains("�")) |
            (Story.description.contains("�")) |
            (Story.author.contains("�"))
        ).all()

        corrupt_categories = session.query(Category).filter(
            (Category.name.contains("�")) |
            (Category.slug.contains("�"))
        ).all()

        logger.info(f"[Cleanup] Deleting {len(corrupt_chapters)} corrupt chapters")
        for row in corrupt_chapters:
            session.delete(row)

        logger.info(f"[Cleanup] Deleting {len(corrupt_stories)} corrupt stories")
        for row in corrupt_stories:
            session.delete(row)

        logger.info(f"[Cleanup] Deleting {len(corrupt_categories)} corrupt categories")
        for row in corrupt_categories:
            session.delete(row)

        session.commit()
        logger.info("[Cleanup] Done deleting corrupt rows.")


def truncate_all_tables() -> None:
    """Truncate all sync-related tables to resync from scratch."""
    tables: list[str] = [
        "story_categories",
        "story_sources",
        "chapters",
        "stories",
        "categories",
        "sync_logs",
    ]

    with db_manager.get_session() as session:
        logger.warning("[Cleanup] TRUNCATE ALL TABLES - FK checks will be disabled temporarily.")
        session.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for table in tables:
            logger.info(f"[Cleanup] Truncating {table}")
            session.execute(text(f"TRUNCATE TABLE {table}"))
        session.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        session.commit()
        logger.info("[Cleanup] Truncate complete.")


def main():
    parser = argparse.ArgumentParser(description="Cleanup Unicode errors or reset DB for Storyflow.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--delete-corrupt", action="store_true", help="Delete rows containing the Unicode replacement char (�).")
    group.add_argument("--truncate-all", action="store_true", help="Truncate all sync tables to resync from scratch.")

    args = parser.parse_args()

    db_manager.initialize()

    if args.delete_corrupt:
        delete_corrupt_rows()
    elif args.truncate_all:
        truncate_all_tables()


if __name__ == "__main__":
    main()

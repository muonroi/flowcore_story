
import json
import os

from flowcore_story.config.config import COMPLETED_FOLDER, DATA_FOLDER
from flowcore_story.utils.chapter_utils import count_dead_chapters
from flowcore_story.utils.logger import logger


def _read_story_metadata(story_path: str, status: str, genre_from_folder: str | None = None) -> dict | None:
    """Reads metadata.json from a story path and returns a structured dict."""
    metadata_path = os.path.join(story_path, "metadata.json")
    if not os.path.exists(metadata_path):
        return None

    try:
        with open(metadata_path, encoding='utf-8') as f:
            metadata = json.load(f)

        genre = genre_from_folder
        if not genre:
            categories = metadata.get('categories', [])
            if categories and isinstance(categories, list) and len(categories) > 0 and categories[0].get('name'):
                genre = categories[0]['name']
            else:
                genre = 'Unknown'

        try:
            txt_files = [f for f in os.listdir(story_path) if f.endswith('.txt')]
            crawled_chapters = len(txt_files)
        except FileNotFoundError:
            crawled_chapters = 0

        return {
            'title': metadata.get('title', os.path.basename(story_path)),
            'path': story_path,
            'status': status,
            'genre': genre,
            'total_chapters': metadata.get('total_chapters_on_site', 0),
            'crawled_chapters': crawled_chapters,
            'url': metadata.get('url', ''),
            'is_skipped': metadata.get('skip_crawl', False),
            'skip_reason': metadata.get('skip_reason', '')
        }
    except Exception as e:
        logger.error(f"[Analyzer] Failed to read/parse metadata for {story_path}: {e}")
        return None

def get_all_stories() -> list[dict]:
    """Scans DATA_FOLDER and COMPLETED_FOLDER to get a list of all stories."""
    all_stories: list[dict] = []
    processed_paths = set()

    if os.path.isdir(COMPLETED_FOLDER):
        for genre_name in os.listdir(COMPLETED_FOLDER):
            genre_path = os.path.join(COMPLETED_FOLDER, genre_name)
            if not os.path.isdir(genre_path):
                continue
            for story_slug in os.listdir(genre_path):
                story_path = os.path.join(genre_path, story_slug)
                if os.path.isdir(story_path):
                    story_info = _read_story_metadata(story_path, status='completed', genre_from_folder=genre_name)
                    if story_info:
                        all_stories.append(story_info)
                        processed_paths.add(os.path.abspath(story_path))

    if os.path.isdir(DATA_FOLDER):
        for story_slug in os.listdir(DATA_FOLDER):
            story_path = os.path.join(DATA_FOLDER, story_slug)
            if os.path.isdir(story_path) and os.path.abspath(story_path) not in processed_paths:
                story_info = _read_story_metadata(story_path, status='ongoing')
                if story_info:
                    all_stories.append(story_info)

    logger.info(f"[Analyzer] Scanned and found {len(all_stories)} stories in total.")
    return all_stories

def get_disk_usage(path: str) -> int:
    """Calculates the total disk usage for a given path in bytes."""
    total_size = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def get_health_stats(stories: list[dict]) -> dict:
    """Analyzes a list of stories to get health statistics."""
    skipped_stories = [s for s in stories if s.get('is_skipped')]

    total_dead_chapters = 0
    stories_with_dead_chapters = []
    for story in stories:
        dead_count = count_dead_chapters(story['path'])
        if dead_count > 0:
            total_dead_chapters += dead_count
            stories_with_dead_chapters.append({'title': story['title'], 'dead_count': dead_count})

    return {
        'skipped_stories': skipped_stories,
        'total_dead_chapters': total_dead_chapters,
        'stories_with_dead_chapters': stories_with_dead_chapters
    }

"""
Parser module for quykiep.com (Next.js-based site)

Extracts data from __NEXT_DATA__ JSON and HTML content.
"""
import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

_DEFAULT_PARSER = "html.parser"
BASE_URL = "https://quykiep.com"


def extract_next_data(html_content: str) -> dict | None:
    """Extract __NEXT_DATA__ JSON from Next.js page."""
    match = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html_content,
        re.DOTALL,
    )
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    script = soup.select_one('script#__NEXT_DATA__')
    if script and script.string:
        try:
            return json.loads(script.string)
        except json.JSONDecodeError:
            pass
    return None


def parse_genres(html_content: str, base_url: str) -> list[dict[str, str]]:
    """Parse genres from /the-loai page.

    Returns list of genre dicts with 'name' and 'url'.
    """
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    anchors = soup.find_all('a', href=True)
    base_netloc = urlparse(base_url).netloc
    pattern = re.compile(r'^/truyen-([a-z0-9-]+)-ln/?$', re.IGNORECASE)

    genres: list[dict[str, str]] = []
    seen: set[str] = set()

    def add_genre(name: str, url: str) -> None:
        cleaned_name = name.strip()
        if not cleaned_name:
            return
        key = cleaned_name.lower()
        if key in seen:
            return
        seen.add(key)
        genres.append({'name': cleaned_name, 'url': url})

    def is_menu_link(tag: Any) -> bool:
        for parent in tag.parents:
            if parent.name in ('nav', 'menu'):
                return True
            class_attr = ' '.join(parent.get('class', [])).lower()
            if any(token in class_attr for token in ('menu', 'nav', 'navbar', 'category', 'categories', 'genre', 'genres')):
                return True
            id_attr = (parent.get('id') or '').lower()
            if any(token in id_attr for token in ('menu', 'nav', 'navbar', 'category', 'categories', 'genre', 'genres')):
                return True
        return False

    for anchor in anchors:
        href = anchor.get('href', '').strip()
        if not href or href.startswith('#') or href.lower().startswith('javascript:'):
            continue

        parsed = urlparse(href)
        if parsed.netloc and parsed.netloc != base_netloc:
            continue

        path = parsed.path or ''
        slug_match = pattern.match(path)
        menu_link = is_menu_link(anchor)
        if not slug_match and not menu_link:
            continue

        name = anchor.get_text(strip=True)
        if not name:
            for attr in ('title', 'aria-label', 'data-title'):
                name = (anchor.get(attr) or '').strip()
                if name:
                    break
        if not name and slug_match:
            name = slug_match.group(1).replace('-', ' ').title()

        full_url = urljoin(base_url, href)
        add_genre(name, full_url)

    fallback_genres = [
        {'name': 'Tiên Hiệp', 'url': f"{base_url}/truyen-tien-hiep-ln"},
        {'name': 'Huyền Huyễn', 'url': f"{base_url}/truyen-huyen-huyen-ln"},
        {'name': 'Khoa Huyễn', 'url': f"{base_url}/truyen-khoa-huyen-ln"},
        {'name': 'Võng Du', 'url': f"{base_url}/truyen-vong-du-ln"},
        {'name': 'Đô Thị', 'url': f"{base_url}/truyen-do-thi-ln"},
        {'name': 'Đồng Nhân', 'url': f"{base_url}/truyen-dong-nhan-ln"},
        {'name': 'Dã Sử', 'url': f"{base_url}/truyen-da-su-ln"},
        {'name': 'Cạnh Kỹ', 'url': f"{base_url}/truyen-canh-ky-ln"},
        {'name': 'Huyễn Nghi', 'url': f"{base_url}/truyen-huyen-nghi-ln"},
        {'name': 'Kiếm Hiệp', 'url': f"{base_url}/truyen-kiem-hiep-ln"},
        {'name': 'Kỳ Ảo', 'url': f"{base_url}/truyen-ky-ao-ln"},
        {'name': 'Truyện Teen', 'url': f"{base_url}/truyen-teen-ln"},
        {'name': 'Truyện Full', 'url': f"{base_url}/truyen-full-ds"},
        {'name': 'Truyện Hot', 'url': f"{base_url}/truyen-hot-ds"},
    ]

    for genre in fallback_genres:
        add_genre(genre['name'], genre['url'])

    return genres


def parse_story_list(html_content: str, base_url: str) -> tuple[list[dict[str, str]], int]:
    """Parse story list from genre/listing page.

    Returns:
        Tuple of (stories, max_page)
        - stories: List of story dicts with 'title', 'url', 'cover', 'total_chapters'
        - max_page: Maximum page number for pagination
    """
    next_data = extract_next_data(html_content)
    if not next_data:
        return [], 0

    page_props = next_data.get('props', {}).get('pageProps', {})

    data = page_props.get('data', [])
    total = page_props.get('total', 0)

    stories: list[dict[str, str]] = []

    for item in data:
        if not isinstance(item, dict):
            continue

        name = item.get('name')
        slug = item.get('slug')

        if not name or not slug:
            continue

        cover_url = item.get('coverUrl', '')
        if cover_url and not cover_url.startswith('http'):
            cover_url = urljoin(base_url, cover_url)

        stories.append({
            'title': name,
            'url': f"{base_url}/truyen/{slug}",
            'cover': cover_url,
            'total_chapters': str(item.get('chapterCount', 0)),
        })

    # Calculate max pages (assuming 18 items per page based on observed data)
    items_per_page = len(data) if data else 18
    max_page = (total + items_per_page - 1) // items_per_page if total > 0 else 1

    return stories, max_page


def parse_story_info(html_content: str, base_url: str) -> dict[str, Any]:
    """Parse story detail page to extract metadata.

    Returns dict with:
        - title, author, description, cover, categories, status
        - total_chapters, first_chapter_slug, last_chapter_slug
        - book_id (for internal use)
    """
    next_data = extract_next_data(html_content)
    if not next_data:
        return {'_parse_error': 'no_next_data'}

    page_props = next_data.get('props', {}).get('pageProps', {})
    book = page_props.get('book', {})

    if not book:
        return {'_parse_error': 'no_book_data'}

    info: dict[str, Any] = {}

    # Title
    info['title'] = book.get('name', '')
    info['slug'] = book.get('slug', '')

    # Author
    author_data = book.get('author', {})
    if isinstance(author_data, dict):
        info['author'] = author_data.get('name', '')
    else:
        info['author'] = str(author_data) if author_data else ''

    # Description (HTML)
    intro = book.get('introduction', '')
    if intro:
        # Clean HTML
        soup = BeautifulSoup(intro, _DEFAULT_PARSER)
        info['description'] = soup.get_text(strip=True)
    else:
        info['description'] = ''

    # Cover image
    cover_url = book.get('coverUrl', '')
    if cover_url and not cover_url.startswith('http'):
        cover_url = urljoin(base_url, cover_url)
    info['cover'] = cover_url
    info['cover_image'] = cover_url

    # Genres/Categories
    genres_data = book.get('genres', [])
    categories = []
    for g in genres_data:
        if isinstance(g, dict):
            name = g.get('name', '')
            if name:
                categories.append(name)
        elif isinstance(g, str):
            categories.append(g)
    info['genres'] = categories
    info['categories'] = categories

    # Tags
    tags_data = book.get('tags', [])
    tags = []
    for t in tags_data:
        if isinstance(t, dict):
            name = t.get('name', '')
            if name:
                tags.append(name)
        elif isinstance(t, str):
            tags.append(t)
    info['tags'] = tags

    # Status
    state = book.get('state', 0)
    if state == 0:
        info['status'] = 'ongoing'
    elif state == 1:
        info['status'] = 'completed'
    else:
        info['status'] = 'unknown'

    # Chapter info
    info['total_chapters'] = book.get('chapterCount', 0)
    info['total_chapters_on_site'] = info['total_chapters']
    info['first_chapter_slug'] = book.get('firstChapterSlug', '')
    info['last_chapter_slug'] = book.get('lastChapterSlug', '')
    info['book_id'] = book.get('bookId', '')

    # TASK 2: Standardize fields for AI training
    info['view_count'] = book.get('viewCount', 0)
    info['rating_star'] = book.get('star', 0)
    info['rating_count'] = book.get('starCount', 0)

    # Map categories to standard format
    if 'categories' in info and info['categories']:
        # Convert list of strings to list of dicts
        if info['categories'] and isinstance(info['categories'][0], str):
            info['categories'] = [{'name': cat} for cat in info['categories']]

    # Add compatibility fields
    book_id = info.get('book_id')
    if book_id:
        info['manga_id'] = book_id  # Alias for compatibility
        info['post_id'] = book_id   # Alias for compatibility
        info['story_id'] = book_id  # Alias for compatibility

    # Add source field
    info['source'] = 'quykiep.com'

    # ajax_nonce (Quykiep doesn't use this, set to None)
    info['ajax_nonce'] = None

    return info


def parse_chapter_list_page(html_content: str, story_slug: str, base_url: str) -> tuple[list[dict[str, str]], int, int]:
    """Parse chapter list from /truyen/{slug}/danh-sach-chuong page.

    Returns:
        Tuple of (chapters, total_chapters, current_page)
    """
    next_data = extract_next_data(html_content)
    if not next_data:
        return [], 0, 1

    page_props = next_data.get('props', {}).get('pageProps', {})

    chapter_list = page_props.get('chapterList', [])
    total = page_props.get('total', 0)
    page_index = page_props.get('pageIndex', 1)

    chapters: list[dict[str, str]] = []

    for item in chapter_list:
        if not isinstance(item, dict):
            continue

        name = item.get('name', '')
        slug = item.get('slug', '')

        if not slug:
            continue

        # Extract chapter number from name or slug
        import re
        chapter_num_match = re.search(r'Chương\s*(\d+)', name, re.IGNORECASE)
        if chapter_num_match:
            chapter_number = chapter_num_match.group(1)
        else:
            # Try from slug: chuong-1-title
            slug_match = re.search(r'chuong-(\d+)', slug)
            chapter_number = slug_match.group(1) if slug_match else '0'

        chapters.append({
            'title': name.strip(),
            'url': f"{base_url}/truyen/{story_slug}/{slug}",
            'chapter_number': chapter_number,
        })

    return chapters, total, page_index


def parse_chapter_content(html_content: str) -> str | None:
    """Extract chapter content from chapter page.

    For quykiep.com, content is in __NEXT_DATA__ -> pageProps.chapter.content

    Returns:
        str: Chapter text content if found
        None: If content not found or anti-bot detected
    """
    # First, try to extract from __NEXT_DATA__ (preferred method)
    next_data = extract_next_data(html_content)
    if next_data:
        page_props = next_data.get('props', {}).get('pageProps', {})
        chapter = page_props.get('chapter', {})

        if isinstance(chapter, dict):
            content = chapter.get('content', '')

            # Fallback to other data fields if content is empty
            if not content or not content.strip():
                for fallback_key in ['data', 'data1', 'data2', 'data3']:
                    val = chapter.get(fallback_key)
                    if val and isinstance(val, str) and len(val.strip()) > 100:
                        content = val
                        break

            if content and isinstance(content, str):
                # Content is already clean text from API or HTML fragment
                # Just clean up any extra whitespace
                lines = [line.strip() for line in content.splitlines()]
                lines = [line for line in lines if line]
                cleaned = '\n\n'.join(lines)

                if len(cleaned) >= 50:
                    return cleaned

    # Fallback: Try HTML parsing
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    # Check for anti-bot signs
    page_text = soup.get_text()
    antibot_signs = ['Just a moment', 'Enable JavaScript', 'Cloudflare', 'captcha']
    for sign in antibot_signs:
        if sign.lower() in page_text.lower():
            return None

    # Try various selectors
    content_elem = None
    for selector in ['#chapter-content', '.chapter-content', '.reading-content', 'article .prose']:
        content_elem = soup.select_one(selector)
        if content_elem:
            break

    if not content_elem:
        return None

    # Clean content
    for elem in content_elem.select('script, style, .ads, .advertisement, iframe'):
        elem.decompose()

    text = content_elem.get_text(separator='\n')
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    content = '\n\n'.join(lines)

    if len(content) < 50:
        return None

    return content


def parse_chapter_info_from_page(html_content: str) -> dict[str, Any]:
    """Extract chapter metadata from chapter page."""
    next_data = extract_next_data(html_content)
    if not next_data:
        return {}

    page_props = next_data.get('props', {}).get('pageProps', {})
    chapter = page_props.get('chapter', {})

    if not chapter or not isinstance(chapter, dict):
        # Extract from URL query
        query = next_data.get('query', {})
        return {
            'story_slug': query.get('book_slug', ''),
            'chapter_slug': query.get('chapter_slug', ''),
        }

    return {
        'title': chapter.get('name', ''),
        'slug': chapter.get('slug', ''),
        'chapter_number': chapter.get('chapterIndex', 0),
    }

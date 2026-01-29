import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

_DEFAULT_PARSER = "html.parser"


def parse_genres(html_content: str, base_url: str) -> list[dict[str, str]]:
    """Parse homepage HTML to extract genre/category links from truyencom.com"""
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    genres: list[dict[str, str]] = []
    seen: set[str] = set()

    # 1. Try to extract genres from JavaScript variable (Preferred method to get IDs)
    # Pattern: genres = JSON.parse('{"1":{"name":"Ti\u00ean Hi\u1ec7p","slug":"tien-hiep"},...}');
    script_content = None
    for script in soup.find_all('script'):
        if script.string and 'genres = JSON.parse' in script.string:
            script_content = script.string
            break
    
    if script_content:
        try:
            match = re.search(r"genres\s*=\s*JSON\.parse\('(.+?)'\);", script_content)
            if match:
                json_str = match.group(1)
                # The string inside might be unicode escaped (e.g. \u00ea)
                # json.loads handles this automatically
                data = json.loads(json_str)
                
                for gid, info in data.items():
                    name = info.get('name')
                    slug = info.get('slug')
                    
                    if name and slug:
                        # Create a synthetic URL that includes the ID
                        # This allows the adapter to use the API later
                        # Format: https://truyencom.com/the-loai/{id}/{slug}/
                        # Note: Real site URL is /truyen-{slug}/ but we need ID for API
                        url = f"{base_url.rstrip('/')}/the-loai/{gid}/{slug}/"
                        
                        key = url.lower()
                        if key not in seen:
                            seen.add(key)
                            genres.append({'name': name, 'url': url})
                            
                if genres:
                    return genres
        except Exception as e:
            # Log error if needed, but fall through to HTML parsing
            pass

    # 2. Fallback: HTML parsing (Gets 'Full' links mostly)
    # Find the genre dropdown menu in navigation
    genre_dropdown = soup.select_one('li.dropdown .dropdown-menu.multi-column')

    if genre_dropdown:
        # Get all genre links from the multi-column dropdown
        anchors = genre_dropdown.select('a[href*="/truyen-"][href*="/full/"]')
    else:
        # Fallback: find any genre links
        anchors = soup.select('a[href*="/truyen-"][href*="/full/"]')

    for anchor in anchors:
        name = anchor.get_text(strip=True)
        href = anchor.get('href')

        if not name or not href:
            continue

        # Skip non-genre links
        if 'Full' not in name:
            continue

        # Clean up the name
        name = name.replace(' Full', '').strip()

        url = urljoin(base_url, href)
        key = url.lower()

        if key in seen:
            continue

        seen.add(key)
        genres.append({'name': name, 'url': url})

    return genres


def parse_genre_api_response(json_content: str, base_url: str) -> tuple[list[dict[str, str]], int]:
    """Parse API response for genre stories listing.
    Endpoint: /api/list/{cat_id}/{type}/{page}/{limit}
    """
    try:
        data = json.loads(json_content)
    except json.JSONDecodeError:
        return [], 0

    if not isinstance(data, dict):
        return [], 0

    items = data.get('items', [])
    # API returns total count, assume limit is 25 as per adapter config
    total_items = 0
    try:
        total_items = int(data.get('total', 0))
    except (ValueError, TypeError):
        pass
    
    # Calculate max page based on limit 25 (default used in adapter)
    max_page = (total_items + 24) // 25 if total_items > 0 else 1

    stories: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
            
        story_id = item.get('storyID')
        alias = item.get('alias')
        title = item.get('title')
        author = item.get('author')
        
        if not story_id or not title:
            continue
            
        # Construct URL: https://truyencom.com/{alias}.{id}/
        url = f"{base_url.rstrip('/')}/{alias}.{story_id}/"
        
        story = {
            'title': title,
            'url': url,
        }
        if author:
            story['author'] = author
            
        stories.append(story)
        
    return stories, max_page


def parse_story_list(html_content: str, base_url: str) -> tuple[list[dict[str, str]], int]:
    """Parse a genre page to obtain stories and pagination info"""
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    stories: list[dict[str, str]] = []

    # Find story items in the list
    list_container = soup.select_one('div.list.list-truyen')

    if list_container:
        story_rows = list_container.select('div.row[itemtype*="Book"]')

        for row in story_rows:
            # Get the title and link
            title_elem = row.select_one('h3.truyen-title a[href]')

            if not title_elem:
                continue

            title = title_elem.get('title') or title_elem.get_text(strip=True)
            href = title_elem.get('href')

            if not title or not href:
                continue

            stories.append({
                'title': title.strip(),
                'url': urljoin(base_url, href),
            })

    # Extract pagination info
    max_page = 1
    pagination = soup.select('ul.pagination a[href*="trang-"]')

    for link in pagination:
        href = link.get('href', '')
        # Extract page number from URL like /truyen-kiem-hiep/full/trang-2/
        match = re.search(r'/trang-(\d+)/', href)
        if match:
            page_num = int(match.group(1))
            max_page = max(max_page, page_num)

    return stories, max_page


def parse_story_info(html_content: str, base_url: str) -> dict[str, Any]:
    """Parse story detail page to extract metadata and chapter list"""
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    info: dict[str, Any] = {}

    # Early validation: Check if this is a valid story page
    # NOTE: Must check BEFORE decomposing head, because BeautifulSoup may incorrectly
    # place body elements inside <head> for malformed HTML, causing them to be deleted
    has_story_structure = bool(soup.select('.info'))
    canonical = soup.select_one('link[rel="canonical"]')
    is_homepage = canonical and canonical.get('href', '').strip('/').endswith('truyencom.com')

    if not has_story_structure or is_homepage:
        return {'_parse_error': 'deleted_or_invalid_page'}

    # Extract critical fields BEFORE decomposing, to avoid BeautifulSoup parser bugs
    # with malformed HTML that places body elements inside <head>

    # Get story title
    title_elem = (
        soup.select_one('h1.title[itemprop="name"]') or
        soup.select_one('.col-info-desc h3.title') or
        soup.select_one('h1.title') or
        soup.select_one('h1')
    )

    if title_elem:
        title_text = title_elem.get_text(strip=True)
        # Avoid generic titles
        if title_text.lower() == "truyện" and soup.select_one('h3.title'):
            title_text = soup.select_one('h3.title').get_text(strip=True)
        info['title'] = title_text
    else:
        return {'_parse_error': 'missing_title'}

    # Get author
    author_elem = soup.select_one('a[itemprop="author"]') or soup.select_one('.info a[href*="/tac-gia/"]')
    if author_elem:
        info['author'] = author_elem.get_text(strip=True)

    # Get genres (deduplicate)
    genre_elems = soup.select('a[itemprop="genre"]')
    if genre_elems:
        genres = [g.get_text(strip=True) for g in genre_elems]
        info['genres'] = list(dict.fromkeys(genres))

    # Get status
    status_text = None
    for div in soup.select('.info div'):
        div_text = div.get_text()
        if 'Trạng thái:' in div_text:
            status_span = div.select_one('span')
            if status_span:
                status_text = status_span.get_text(strip=True)
                break

    if not status_text:
        # Fallback search
        status_tag = soup.find(string=re.compile(r'Trạng thái|Tình trạng', re.IGNORECASE))
        if status_tag and status_tag.parent:
            status_text = status_tag.parent.get_text(strip=True).replace('Trạng thái:', '').replace('Tình trạng:', '').strip()

    if status_text:
        info['status'] = status_text
    else:
        info['status'] = "Đang ra"

    # Now safe to remove noise for parsing remaining fields
    for hidden in soup(['script', 'style']):
        hidden.decompose()

    # Get description
    desc_elem = soup.select_one('div.desc-text[itemprop="description"]') or soup.select_one('.desc-text')
    if desc_elem:
        info['description'] = desc_elem.get_text('\n', strip=True)

    # Get cover image
    cover_elem = soup.select_one('div.books img[itemprop="image"]') or soup.select_one('.books img')
    if cover_elem:
        info['cover_image'] = cover_elem.get('src') or cover_elem.get('data-pc') or cover_elem.get('data-src')

    # TASK 2: Parse Rating & Views
    # Views extraction
    view_match = re.search(r'Lượt\s*xem:\s*([\d\.,]+)', html_content, re.IGNORECASE)
    if view_match:
        view_str = view_match.group(1).replace('.', '').replace(',', '')
        try:
            info['view_count'] = int(view_str)
        except ValueError:
            pass

    rating_container = soup.select_one('div.rate div[itemprop="aggregateRating"]') or soup.select_one('.rate')
    if rating_container:
        rating_val = rating_container.select_one('[itemprop="ratingValue"]')
        rating_cnt = rating_container.select_one('[itemprop="ratingCount"]')
        
        if rating_val:
            try:
                info['rating_star'] = float(rating_val.get_text(strip=True))
            except (ValueError, TypeError):
                pass
        
        if rating_cnt:
            try:
                info['rating_count'] = int(rating_cnt.get_text(strip=True))
            except (ValueError, TypeError):
                pass

    # Extract story ID from page
    story_id = None
    # Story ID often in script tags (even if we decomposed them, we can search in original html_content)
    id_match = re.search(r'storyID\s*[=:]\s*(\d+)', html_content)
    if id_match:
        story_id = id_match.group(1)

    if not story_id:
        first_chapter = soup.select_one('ul.list-chapter li a[href]')
        if first_chapter:
            href = first_chapter.get('href', '')
            match = re.search(r'\.(\d+)/', href)
            if match:
                story_id = match.group(1)

    if story_id:
        info['story_id'] = story_id
        info['manga_id'] = story_id  # Alias for compatibility
        info['post_id'] = story_id   # Alias for compatibility

    # Add source field
    info['source'] = 'truyencom.com'

    # ajax_nonce (TruyenCom doesn't use this, set to None)
    info['ajax_nonce'] = None

    return info


def parse_chapter_list(html_content: str, base_url: str) -> list[dict[str, str]]:
    """Parse chapter list from story detail page"""
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    chapters: list[dict[str, str]] = []

    # Find chapter list
    chapter_lists = soup.select('ul.list-chapter')

    for ul in chapter_lists:
        chapter_items = ul.select('li a[href]')

        for link in chapter_items:
            title = link.get('title') or link.get_text(strip=True)
            href = link.get('href')

            if not href:
                continue

            # Clean up title
            if ' - ' in title:
                # Remove story name prefix
                parts = title.split(' - ', 1)
                if len(parts) > 1:
                    title = parts[1]

            chapters.append({
                'title': title.strip(),
                'url': urljoin(base_url, href),
            })

    return chapters


def _build_story_base_url(
    story_url: str,
    story_slug: str | None = None,
    story_id: str | None = None,
) -> str | None:
    parsed = urlparse(story_url or "")
    if parsed.scheme and parsed.netloc:
        origin = f"{parsed.scheme}://{parsed.netloc}"
    else:
        origin = "https://truyencom.com"

    path_segment = parsed.path.strip("/") if parsed.path else ""

    if path_segment:
        match = re.match(r"^(.+)\.(\d+)$", path_segment)
        if match:
            # Story detail uses slug.id, but chapter URLs are slug-only
            path_segment = match.group(1)
        else:
            path_segment = path_segment
    elif story_slug:
        path_segment = story_slug
    elif story_id:
        # Last resort: use id alone
        path_segment = story_id

    if not path_segment:
        return None

    return f"{origin}/{path_segment.strip('/')}".rstrip("/")


def parse_chapter_list_api(
    json_content: str,
    story_url: str,
    story_slug: str | None = None,
    story_id: str | None = None,
) -> list[dict[str, str]]:
    """Parse chapter list from API response"""
    try:
        data = json.loads(json_content)
    except json.JSONDecodeError:
        return []

    chapters: list[dict[str, str]] = []

    if not isinstance(data, dict):
        return []

    items = data.get('items', [])

    if not isinstance(items, list):
        return []

    base_story_url = _build_story_base_url(story_url, story_slug, story_id)

    for item in items:
        if not isinstance(item, dict):
            continue

        chapter_id = item.get('chapterID')
        chapter_name = item.get('chapter_name')

        if not chapter_id or not chapter_name:
            continue

        # Build chapter URL
        # Pattern: https://truyencom.com/{story-slug}/chuong-{number}.html
        # Extract number from chapter name
        chapter_num_match = re.search(r'Chương\s+(\d+)', chapter_name, re.IGNORECASE)
        if chapter_num_match:
            suffix = chapter_num_match.group(1)
        else:
            suffix = str(chapter_id)

        if base_story_url:
            chapter_url = f"{base_story_url}/chuong-{suffix}.html"
        else:
            chapter_url = f"https://truyencom.com/chuong-{suffix}.html"

        chapters.append({
            'title': chapter_name,
            'url': chapter_url,
            'chapter_id': str(chapter_id),
        })

    return chapters


def parse_chapter_content(html_content: str) -> str | None:
    """Extract chapter content from chapter page

    Returns:
        str: Chapter content if found and valid
        "": If chapter exists but content is empty/broken
        None: If parsing failed (missing div or anti-bot)

    Note: Caller can treat empty string as a terminal "broken chapter" signal
    """
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    # Find the chapter content div
    content_div = soup.select_one('div#chapter-c.chapter-c')

    if not content_div:
        return None

    # Check for Cloudflare anti-bot content
    if "Just a moment...Enable JavaScript and cookies to continue" in content_div.get_text():
        # Log this explicitly for better visibility
        from flowcore_story.utils.logger import logger
        logger.info("[TruyenCom] Cloudflare anti-bot page detected, skipping content extraction.")
        return None  # Indicate that valid content was not found

    # Remove ads and scripts
    for elem in content_div.select('div.ads, script, style'):
        elem.decompose()

    # Get the content
    content_html = content_div.decode_contents()

    if not content_html:
        # FIX: Return empty string for empty div (not None)
        # This allows caller to distinguish empty content from parsing errors
        from flowcore_story.utils.logger import logger
        logger.warning("[TruyenCom] Chapter content div exists but is EMPTY (broken chapter on website)")
        return ""  # Empty content (broken chapter)

    # Clean up the content
    content_html = content_html.strip()

    if not content_html:
        from flowcore_story.utils.logger import logger
        logger.warning("[TruyenCom] Chapter content is empty after cleaning (broken chapter)")
        return ""  # Empty after cleaning

    # Replace multiple <br> tags with double line breaks
    content_html = re.sub(r'(<br\s*/?>\s*){2,}', '\n\n', content_html, flags=re.IGNORECASE)

    # Convert to text while preserving some structure
    temp_soup = BeautifulSoup(content_html, _DEFAULT_PARSER)

    # Replace <br> with newlines
    for br in temp_soup.find_all('br'):
        br.replace_with('\n')

    text_content = temp_soup.get_text()

    # Clean up whitespace
    lines = [line.strip() for line in text_content.splitlines()]
    lines = [line for line in lines if line]

    final_content = '\n\n'.join(lines)

    # FIX: Check if content is suspiciously short (likely empty/broken)
    if len(final_content) < 50:
        from flowcore_story.utils.logger import logger
        if "Không cover được nội dung" in final_content:
             logger.warning(f"[TruyenCom] Chapter content BROKEN (Source Error): '{final_content}'")
        else:
             logger.warning(f"[TruyenCom] Chapter content very short ({len(final_content)} chars), likely broken: '{final_content[:100]}'")
        return ""  # Too short, likely broken

    return final_content


def extract_story_slug(story_url: str) -> str | None:
    """Extract story slug from story URL
    Example: https://truyencom.com/vo-dich-thien-ha.663/ -> vo-dich-thien-ha
    Example: https://truyencom.com/vo-dich-thien-ha/ -> vo-dich-thien-ha
    """
    # Prefer slug.id pattern
    match = re.search(r"/([^/]+)\.\d+/?$", story_url)
    if match:
        return match.group(1)

    # Fallback: slug-only pattern
    match = re.search(r"/([^/]+)/?$", story_url)
    if match:
        return match.group(1)

    return None


def extract_story_id(story_url: str) -> str | None:
    """Extract story ID from story URL
    Example: https://truyencom.com/vo-dich-thien-ha.663/ -> 663
    """
    match = re.search(r'\.(\d+)/?$', story_url)
    if match:
        return match.group(1)
    return None


def parse_search_results(html_content: str, base_url: str) -> list[dict[str, str]]:
    """Parse search results page from truyencom.com.

    Args:
        html_content: HTML content of search results page
        base_url: Base URL of the site

    Returns:
        List of story dicts with 'title', 'url', 'slug', 'id'

    Example:
        Input: Search page for "tiên nghịch"
        Output: [{'title': 'Tiên Nghịch', 'url': 'https://truyencom.com/tien-nghich.2/',
                  'slug': 'tien-nghich', 'id': '2'}]
    """
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    results: list[dict[str, str]] = []

    # Find all story items in search results
    # Based on the HTML structure: <div class="row" itemscope itemtype="Book">
    story_rows = soup.select('div.list.list-truyen div.row[itemscope]')

    for row in story_rows:
        # Get the title link: <h3 class="truyen-title"><a href="...">Title</a></h3>
        title_elem = row.select_one('h3.truyen-title a[href]')

        if not title_elem:
            continue

        title = title_elem.get_text(strip=True)
        href = title_elem.get('href')

        if not title or not href:
            continue

        url = urljoin(base_url, href)

        # Extract slug and ID from URL
        # Example: https://truyencom.com/tien-nghich.2/ -> slug=tien-nghich, id=2
        parsed = urlparse(url)
        path = parsed.path.strip('/')

        # Extract slug and ID using regex: {slug}.{id}
        match = re.match(r'^(.+)\.(\d+)$', path)

        if match:
            slug = match.group(1)
            story_id = match.group(2)

            results.append({
                'title': title,
                'url': url,
                'slug': slug,
                'id': story_id,
                'slug_with_id': f"{slug}.{story_id}"
            })

    return results

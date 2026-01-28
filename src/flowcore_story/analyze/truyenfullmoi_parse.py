"""Parsing functions for truyenfullmoi.com

This module provides HTML parsing functions to extract data from
truyenfullmoi.com pages including genres, stories, chapters, and content.
"""

import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from flowcore.utils.logger import logger


def parse_genres(html: str, base_url: str) -> list[dict[str, str]]:
    """Parse genre list from homepage

    Args:
        html: Homepage HTML content
        base_url: Base URL of the site

    Returns:
        List of genre dicts with 'name', 'url', 'slug'
    """
    soup = BeautifulSoup(html, 'html.parser')
    genres = []
    seen_urls = set()

    # Strategy 1: Parse all links with /truyen- pattern
    # Genre links are scattered throughout page, not just in nav
    genre_links = soup.select('a[href*="/truyen-"]')

    for link in genre_links:
        href = link.get('href', '')
        text = link.get_text(strip=True)

        if not href or not text:
            continue

        # Extract path from URL (handle both full URLs and relative paths)
        parsed = urlparse(href)
        path = parsed.path if parsed.path else href

        # Filter to only genre pages: /truyen-{slug}/ (not story pages)
        # Valid patterns: /truyen-do-thi/ or /truyen-do-thi/trang-2/
        # Invalid: /truyen-do-thi/story-name.123/, /tag/truyen-vip
        if not re.match(r'/truyen-[^/]+/?(?:trang-\d+/?)?$', path):
            continue

        full_url = urljoin(base_url, href)

        # Avoid duplicates
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Extract slug from URL path
        # Pattern: /truyen-{slug}/ or /truyen-{slug}/trang-N/
        slug = None
        match = re.search(r'/truyen-([^/]+)', path)
        if match:
            slug = match.group(1)

        if slug:
            genres.append({
                'name': text,
                'url': full_url,
                'slug': slug,
            })

    # Strategy 2: Also check menu/category links
    if not genres:
        menu_links = soup.select('.menu a, .category-list a, .genre-list a')
        for link in menu_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)

            if '/truyen-' not in href or not text:
                continue

            full_url = urljoin(base_url, href)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            match = re.search(r'/truyen-([^/]+)', href)
            if match:
                genres.append({
                    'name': text,
                    'url': full_url,
                    'slug': match.group(1),
                })

    logger.debug(f"[truyenfullmoi] Parsed {len(genres)} genres from homepage")
    return genres


def parse_story_list(html: str, base_url: str) -> tuple[list[dict[str, str]], int]:
    """Parse story list from genre page

    Args:
        html: Genre page HTML content
        base_url: Base URL of the site

    Returns:
        Tuple of (stories, max_page)
        - stories: List of story dicts
        - max_page: Maximum page number from pagination
    """
    soup = BeautifulSoup(html, 'html.parser')
    stories = []
    seen_urls = set()

    # Find story containers
    # Pattern: .list .row structure
    story_rows = soup.select('.list .row')

    for row in story_rows:
        # Find story link
        link = row.select_one('h3 a, a[href*="/"][href$="/"]')
        if not link:
            continue

        href = link.get('href', '')
        title = link.get_text(strip=True)

        if not href or not title:
            continue

        full_url = urljoin(base_url, href)

        # Avoid duplicates
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Extract story ID from URL pattern: /slug.ID/
        story_id = None
        match = re.search(r'\.(\d+)/?$', href)
        if match:
            story_id = match.group(1)

        # Extract additional info from row
        author = None
        chapter_count = None
        status = None

        # Status labels
        if row.select_one('.label-full'):
            status = 'full'
        elif row.select_one('.label-hot'):
            status = 'hot'
        elif row.select_one('.label-new'):
            status = 'new'

        # Try to find author (text near "Tác giả" or similar)
        row_text = row.get_text()
        author_match = re.search(r'(?:Tác giả|Author):\s*([^\n]+)', row_text)
        if author_match:
            author = author_match.group(1).strip()

        # Try to find chapter count
        chapter_match = re.search(r'(\d+)\s*(?:chương|chapters?)', row_text, re.IGNORECASE)
        if chapter_match:
            chapter_count = chapter_match.group(1)

        stories.append({
            'title': title,
            'url': full_url,
            'story_id': story_id,
            'author': author,
            'chapter_count': chapter_count,
            'status': status,
        })

    # Extract pagination
    max_page = 1
    pagination_links = soup.select('a[href*="/trang-"]')
    pages = []

    for link in pagination_links:
        href = link.get('href', '')
        match = re.search(r'/trang-(\d+)/', href)
        if match:
            pages.append(int(match.group(1)))

    if pages:
        max_page = max(pages)

    # Also check for "Cuối" (last) link which might have the max page
    last_link = soup.select_one('a:contains("Cuối"), a:contains("Last")')
    if last_link:
        href = last_link.get('href', '')
        match = re.search(r'/trang-(\d+)/', href)
        if match:
            last_page = int(match.group(1))
            max_page = max(max_page, last_page)

    logger.debug(f"[truyenfullmoi] Parsed {len(stories)} stories, max_page={max_page}")
    return stories, max_page


def parse_story_info(html: str, base_url: str) -> dict[str, Any]:
    """Parse story metadata from story detail page

    Args:
        html: Story detail page HTML content
        base_url: Base URL of the site

    Returns:
        Dict with story metadata including:
        - title, author, story_id, story_alias
        - genres, status, cover_image, description, rating
        - Or {'_parse_error': 'error_type'} on critical failure
    """
    soup = BeautifulSoup(html, 'html.parser')
    details: dict[str, Any] = {}

    # Check if page redirected to homepage (deleted story)
    canonical = soup.select_one('link[rel="canonical"]')
    if canonical:
        canonical_url = canonical.get('href', '')
        parsed = urlparse(canonical_url)
        if parsed.path in ('/', ''):
            logger.warning("[truyenfullmoi] Story page redirects to homepage (deleted)")
            return {'_parse_error': 'deleted_or_invalid_page'}

    # Extract story ID and alias from inline JavaScript
    # Pattern: storyID=1779; storyAlias='van-co-de-nhat-than'
    scripts = soup.find_all('script')
    for script in scripts:
        script_text = script.string or ''

        story_id_match = re.search(r'storyID\s*=\s*(\d+)', script_text)
        if story_id_match:
            details['story_id'] = story_id_match.group(1)

        alias_match = re.search(r"storyAlias\s*=\s*['\"]([^'\"]+)['\"]", script_text)
        if alias_match:
            details['story_alias'] = alias_match.group(1)

    # Remove script and style elements to avoid false matches in text search
    # This prevents extracting CSS content as status if "Status" keyword appears in CSS
    for element in soup(['script', 'style']):
        element.decompose()

    # Extract title
    # Strategy 1: Specific container h3 (most accurate for truyenfullmoi)
    # Strategy 2: itemprop="name" inside story info
    # Strategy 3: Breadcrumb h1 (fallback)
    title_tag = (
        soup.select_one('.col-info-desc h3.title') or 
        soup.select_one('h3.title[itemprop="name"]') or
        soup.select_one('.info-holder + .title') or
        soup.select_one('h1') or 
        soup.select_one('h2.title') or 
        soup.select_one('.story-title')
    )
    
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # If we accidentally picked up "Truyện" from breadcrumb, try harder
        if title_text.lower() == "truyện" or len(title_text) < 2:
             # Try to find the second [itemprop="name"] which is usually the story title
             all_names = soup.select('[itemprop="name"]')
             for name_tag in all_names:
                 text = name_tag.get_text(strip=True)
                 if text.lower() != "truyện" and len(text) > 2:
                     title_text = text
                     break
        details['title'] = title_text
    else:
        logger.error("[truyenfullmoi] Failed to parse story title")
        return {'_parse_error': 'missing_title'}

    # Extract author
    # Strategy 1: Use schema.org itemprop
    author_tag = soup.select_one('[itemprop="author"]')
    if author_tag:
        details['author'] = author_tag.get_text(strip=True)
    else:
        # Strategy 2: Text search fallback
        author_node = soup.find(string=re.compile(r'Tác giả|Author', re.IGNORECASE))
        if author_node:
            parent = author_node.parent
            if parent:
                author_text = parent.get_text(strip=True)
                # Remove label
                author_text = re.sub(r'Tác giả:\s*', '', author_text, flags=re.IGNORECASE)
                author_text = re.sub(r'Author:\s*', '', author_text, flags=re.IGNORECASE)
                if author_text:
                    details['author'] = author_text.strip()

    # Extract genres
    # Strategy 1: Use schema.org itemprop (Most accurate)
    genres = []
    genre_tags = soup.select('[itemprop="genre"]')
    for tag in genre_tags:
        genres.append(tag.get_text(strip=True))

    # Strategy 2: Look for specific info block if itemprop missing
    if not genres:
        info_block = soup.select_one('.info')
        if info_block:
            # Find line with "Thể loại"
            cat_label = info_block.find(string=re.compile(r'Thể loại', re.IGNORECASE))
            if cat_label and cat_label.parent:
                # Get all links in this parent (or parent's parent if label is wrapped)
                container = cat_label.parent.parent if cat_label.parent.name != 'div' else cat_label.parent
                for link in container.select('a'):
                    genres.append(link.get_text(strip=True))

    details['genres'] = list(dict.fromkeys(genres))  # Remove duplicates preserving order

    # Extract status
    status_text = None
    info_block = soup.select_one('.info')
    
    if info_block:
        # Strategy 1: Look inside .info div
        status_label = info_block.find(string=re.compile(r'Trạng thái|Tình trạng|Status', re.IGNORECASE))
        if status_label:
            parent = status_label.parent
            if parent:
                # Get the value following the label
                # Try sibling first
                sibling = status_label.next_sibling
                if sibling and isinstance(sibling, str):
                    status_text = sibling.strip(': ')
                
                if not status_text or len(status_text) < 2:
                    # Look for span siblings
                    next_span = parent.find_next_sibling('span') or parent.select_one('span')
                    if next_span:
                        status_text = next_span.get_text(strip=True)
                
                # Fallback: get text of the whole parent and remove label
                if not status_text or len(status_text) < 2:
                    raw_text = parent.get_text(separator=' ', strip=True)
                    status_text = re.sub(r'(Trạng thái|Tình trạng|Status)\s*:?', '', raw_text, flags=re.IGNORECASE).strip()

    # Strategy 2: Global search (fallback)
    if not status_text or len(status_text) < 2:
        # We already decomposed script/style tags at the beginning
        status_tag = soup.find(string=re.compile(r'Trạng thái|Tình trạng|Status', re.IGNORECASE))
        if status_tag:
            parent = status_tag.parent
            if parent:
                text = parent.get_text(strip=True)
                status_text = re.sub(r'(Trạng thái|Tình trạng|Status)\s*:?', '', text, flags=re.IGNORECASE).strip()

    # Final check for "Hoàn thành" / "Đang ra" keywords if still no status
    if not status_text:
        if soup.find(string=re.compile(r'Hoàn thành|Full', re.IGNORECASE)):
            status_text = "Hoàn thành"
        elif soup.find(string=re.compile(r'Đang ra|Truyện đang ra', re.IGNORECASE)):
            status_text = "Đang ra"

    if status_text:
        # Sanity check: ensure we didn't pick up something crazy
        if len(status_text) > 50:
            status_text = status_text[:50].split('\n')[0].strip()
        details['status'] = status_text
    else:
        details['status'] = "Đang ra" # Default fallback

    # Extract cover image
    cover_img = soup.select_one('[itemprop="image"], .book img, img[src*="cover"]')
    if cover_img:
        cover_src = cover_img.get('src', '') or cover_img.get('data-src', '')
        if not cover_src and cover_img.name == 'meta':
            cover_src = cover_img.get('content', '')
            
        if cover_src:
            details['cover_image'] = urljoin(base_url, cover_src)

    # Extract description
    desc_container = soup.select_one('.desc-text, [itemprop="description"], .story-desc, .description')
    if desc_container:
        # Remove "Bạn đang đọc review..." header often found in truyenfullmoi
        for h2 in desc_container.select('h2'):
            if "bạn đang đọc" in h2.get_text().lower():
                h2.decompose()
        
        details['description'] = desc_container.get_text('\n', strip=True)

    # Extract rating & view count
    # Views: search for "Lượt xem: 123"
    view_match = re.search(r'Lượt\s*xem:\s*(\d+)', html, re.IGNORECASE)
    if view_match:
        details['view_count'] = int(view_match.group(1))

    # Rating: schema.org
    rating_val = soup.select_one('[itemprop="ratingValue"]')
    rating_cnt = soup.select_one('[itemprop="ratingCount"]')
    
    if rating_val:
        try:
            details['rating_star'] = float(rating_val.get_text(strip=True))
            details['rating'] = details['rating_star']
        except (ValueError, TypeError):
            pass
    
    if rating_cnt:
        try:
            details['rating_count'] = int(rating_cnt.get_text(strip=True))
        except (ValueError, TypeError):
            pass

    if 'rating_star' not in details:
        rating_match = re.search(r'(\d+\.?\d*)\s*/\s*10', html)
        if rating_match:
            details['rating_star'] = float(rating_match.group(1))
            details['rating'] = details['rating_star']

    # Map genres to categories for compatibility
    if 'genres' in details and not details.get('categories'):
        details['categories'] = [{'name': g} for g in details['genres']] if details['genres'] else []

    # Add compatibility fields
    story_id = details.get('story_id')
    if story_id:
        details['manga_id'] = story_id  # Alias for compatibility
        details['post_id'] = story_id   # Alias for compatibility

    # Add source field
    details['source'] = 'truyenfullmoi.com'

    # ajax_nonce (TruyenFullMoi doesn't use this, set to None)
    details['ajax_nonce'] = None

    return details


def parse_chapter_list(html: str, base_url: str) -> list[dict[str, str]]:
    """Parse chapter list from story detail page (one page worth)

    Args:
        html: Story detail page HTML
        base_url: Base URL of the site

    Returns:
        List of chapter dicts with 'title', 'url', 'chapter_number'

    Note:
        This extracts ONE page of chapters (typically 50).
        The adapter must handle pagination to get all chapters.
    """
    soup = BeautifulSoup(html, 'html.parser')
    chapters = []
    seen_urls = set()

    # Find chapter links
    # Pattern: /story-slug/chuong-{number}.html
    chapter_links = soup.select('a[href*="/chuong-"]')

    for link in chapter_links:
        href = link.get('href', '')
        chapter_title = link.get_text(strip=True)

        if not href or not chapter_title:
            continue

        full_url = urljoin(base_url, href)

        # Avoid duplicates
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Extract chapter number from URL
        # Pattern: /chuong-123.html
        chapter_num = None
        match = re.search(r'/chuong-(\d+)', href)
        if match:
            chapter_num = match.group(1)

        # If not in URL, try to extract from title
        # Pattern: "Chương 123: Title"
        if not chapter_num:
            title_match = re.search(r'Chương\s+(\d+)', chapter_title, re.IGNORECASE)
            if title_match:
                chapter_num = title_match.group(1)

        chapters.append({
            'title': chapter_title,
            'url': full_url,
            'chapter_number': chapter_num,
        })

    logger.debug(f"[truyenfullmoi] Parsed {len(chapters)} chapters from page")
    return chapters


def parse_chapter_content(html: str) -> str:
    """Parse chapter content, removing ads

    Args:
        html: Chapter page HTML content

    Returns:
        Clean HTML content (to be converted to text later)
        Returns "" if verified empty chapter
        Returns None if parsing failed or anti-bot detected
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Strategy: Try common content selectors first
    content_selectors = [
        '#chapter-content',
        '.chapter-content',
        '#content',
        '.content',
        'div[id*="content"]',
        'div[class*="content"]',
        '#chapter-c',
        '.chapter-c',
    ]

    content_div = None
    for selector in content_selectors:
        content_div = soup.select_one(selector)
        if content_div:
            logger.debug(f"[truyenfullmoi] Found content using selector: {selector}")
            break

    # Fallback: Find largest div with substantial text
    if not content_div:
        all_divs = soup.find_all('div')
        max_text_len = 0
        candidate = None

        for div in all_divs:
            # Get text length (rough estimate of content)
            text = div.get_text(strip=True)
            text_len = len(text)

            # Must have substantial text (more than navigation/headers)
            if text_len > max_text_len and text_len > 200:
                # Avoid navigation/header divs
                div_classes = ' '.join(div.get('class', []))
                div_id = div.get('id', '')

                # Skip known non-content areas
                if any(skip in div_classes.lower() for skip in ['nav', 'menu', 'header', 'footer', 'sidebar']):
                    continue
                if any(skip in div_id.lower() for skip in ['nav', 'menu', 'header', 'footer', 'sidebar']):
                    continue

                max_text_len = text_len
                candidate = div

        if candidate:
            content_div = candidate
            logger.debug(f"[truyenfullmoi] Found content using fallback (largest div)")

    if not content_div:
        logger.warning("[truyenfullmoi] Could not find content div")
        return None

    # Remove ads and unwanted elements
    for unwanted in content_div.select('[class*="ads"], [id*="ads"], .adsbygoogle, script, style, iframe, noscript'):
        unwanted.decompose()

    # Also remove common ad patterns
    for ad_div in content_div.find_all(['div', 'ins'], class_=re.compile(r'ad', re.IGNORECASE)):
        ad_div.decompose()

    # Get clean HTML content
    # We return HTML (not text) because chapter_utils will convert to text
    content_html = str(content_div)

    # Get text for validation
    content_text = content_div.get_text(separator=' ', strip=True)

    if not content_text:
        logger.warning("[truyenfullmoi] Content div is empty after ad removal")
        return ""

    # Check for anti-bot indicators
    if len(content_text) < 100:
        # Very short content, might be anti-bot or broken page
        if any(indicator in content_text.lower() for indicator in [
            'cloudflare', 'checking your browser', 'enable javascript',
            'please wait', 'bot protection'
        ]):
            logger.warning("[truyenfullmoi] Anti-bot protection detected")
            return None

    logger.debug(f"[truyenfullmoi] Extracted content: {len(content_text)} chars")
    return content_html


def extract_story_slug(url: str) -> str:
    """Extract story slug from chapter or story URL

    Args:
        url: Story or chapter URL

    Returns:
        Story slug (string)

    Examples:
        '/van-co-de-nhat-than.1779/' -> 'van-co-de-nhat-than'
        '/van-co-de-nhat-than/chuong-1.html' -> 'van-co-de-nhat-than'
        'https://truyenfullmoi.com/story.123/' -> 'story'
    """
    parsed = urlparse(url)
    path = parsed.path.strip('/')

    # Split by /
    parts = path.split('/')

    # Find the story segment
    # Pattern: story-slug.ID or story-slug
    for part in parts:
        # Skip empty parts
        if not part:
            continue

        # Skip known non-story patterns
        if part in ('chuong', 'truyen', 'trang'):
            continue
        if part.startswith('chuong-'):
            continue
        if part.endswith('.html'):
            continue

        # This should be the story slug
        # Remove .ID suffix if present
        slug = re.sub(r'\.\d+$', '', part)
        return slug

    # Fallback: use last meaningful part
    if parts:
        slug = parts[0]
        slug = re.sub(r'\.\d+$', '', slug)
        return slug

    return ""

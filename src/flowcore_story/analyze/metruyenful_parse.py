"""Parsing functions for metruyenful.com

This module provides HTML parsing functions to extract data from
metruyenful.com pages including genres, stories, chapters, and content.
"""

import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from flowcore_story.utils.logger import logger


_NON_GENRE_SLUGS = {
    'truyen-moi',
    'truyen-hot',
    'truyen-full',
    'truyen-convert',
    'blog',
    'tin-tuc',
    'hot',
}

_SKIP_SLUG_PARTS = {
    'page',
    'chuong',
    'chapter',
    'tag',
    'category',
    'the-loai',
    'genre',
}


def _is_non_genre(name: str, url: str, slug: str) -> bool:
    name_lower = (name or '').lower()
    url_lower = (url or '').lower()
    slug_lower = (slug or '').lower()

    if slug_lower in _NON_GENRE_SLUGS:
        return True

    if any(token in slug_lower for token in _NON_GENRE_SLUGS):
        return True

    if any(token in name_lower for token in _NON_GENRE_SLUGS):
        return True

    if 'blog' in url_lower:
        return True

    return False


def parse_genres(html: str, base_url: str) -> list[dict[str, str]]:
    """Parse genre list from homepage dropdown menus.

    Args:
        html: Homepage HTML content
        base_url: Base URL of the site

    Returns:
        List of genre dicts with keys: 'name', 'url', 'slug'

    Notes:
        - Sources: '.dropdown-menu a', '.multi-column a'
        - Filters out non-genre links such as "truyen-moi", "truyen-hot", "blog"
        - Deduplicates by full URL
    """
    if not html:
        logger.warning("[metruyenful] Empty HTML provided to parse_genres")
        return []

    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        logger.exception("[metruyenful] Failed to parse HTML in parse_genres")
        return []

    genres: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    links = soup.select('.dropdown-menu a, .multi-column a')
    base_netloc = urlparse(base_url).netloc

    for link in links:
        try:
            href = (link.get('href') or '').strip()
            name = link.get_text(strip=True)

            if not href or not name:
                continue

            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)

            if base_netloc and parsed.netloc and parsed.netloc != base_netloc:
                continue

            path = parsed.path.strip('/')
            if not path or '/' in path:
                continue

            slug = path

            if _is_non_genre(name, full_url, slug):
                continue

            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            genres.append({
                'name': name,
                'url': full_url,
                'slug': slug,
            })
        except Exception:
            logger.exception("[metruyenful] Error while parsing a genre link")

    logger.debug(f"[metruyenful] Parsed {len(genres)} genres from homepage")
    return genres


def parse_story_list(html: str, base_url: str) -> tuple[list[dict[str, str]], int]:
    """Parse story list from genre page.

    Args:
        html: Genre page HTML content
        base_url: Base URL of the site

    Returns:
        Tuple of (stories, max_page)
        - stories: List of dicts with 'title', 'url', 'slug', 'story_id'
        - max_page: Maximum page number from pagination

    Notes:
        - Story links are derived from the parent anchors of img.cover elements.
        - Story URL pattern: '/{story-slug}/' (no numeric ID).
        - Pagination pattern: '/{genre}/page/{N}'.
    """
    if not html:
        logger.warning("[metruyenful] Empty HTML provided to parse_story_list")
        return [], 1

    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        logger.exception("[metruyenful] Failed to parse HTML in parse_story_list")
        return [], 1

    stories: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    cover_images = soup.select('img.cover')
    for img in cover_images:
        try:
            link = img.find_parent('a')
            if not link:
                continue

            href = (link.get('href') or '').strip()
            if not href:
                continue

            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            path = parsed.path.strip('/')

            if not path:
                continue

            if '/page/' in parsed.path:
                continue

            path_parts = [part for part in path.split('/') if part]
            if len(path_parts) > 1:
                tail = path_parts[1].lower()
                if tail.startswith('chuong') or tail.startswith('chapter'):
                    continue

            title = img.get('alt') or img.get('title') or link.get_text(strip=True)
            if not title:
                continue

            slug = extract_story_slug(full_url)
            if not slug:
                continue

            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            stories.append({
                'title': title,
                'url': full_url,
                'slug': slug,
                'story_id': None,
            })
        except Exception:
            logger.exception("[metruyenful] Error while parsing a story item")

    max_page = 1
    try:
        pagination_links = soup.select('a[href*="/page/"]')
        pages = []

        for link in pagination_links:
            href = link.get('href', '')
            match = re.search(r'/page/(\d+)', href)
            if match:
                pages.append(int(match.group(1)))

        last_link = soup.select_one('a[rel="last"], a[title="Cuối"], a:contains("Cuối"), a:contains("Last")')
        if last_link:
            href = last_link.get('href', '')
            match = re.search(r'/page/(\d+)', href)
            if match:
                pages.append(int(match.group(1)))

        if pages:
            max_page = max(pages)
    except Exception:
        logger.exception("[metruyenful] Error while parsing pagination")

    logger.debug(f"[metruyenful] Parsed {len(stories)} stories, max_page={max_page}")
    return stories, max_page


def parse_story_info(html: str, base_url: str) -> dict[str, Any]:
    """Parse story metadata from a WordPress story detail page.

    Args:
        html: Story detail page HTML content
        base_url: Base URL of the site

    Returns:
        Dict with story metadata including:
        - title, slug, author, genres, status, cover_image, description
        - Or {'_parse_error': 'error_type'} on critical failure

    Notes:
        - Uses WordPress selectors: '.entry-title', '[itemprop="name"]', '.post-title'.
        - Avoids numeric IDs; slug is derived from canonical or OG URL.
    """
    if not html:
        logger.warning("[metruyenful] Empty HTML provided to parse_story_info")
        return {'_parse_error': 'empty_html'}

    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        logger.exception("[metruyenful] Failed to parse HTML in parse_story_info")
        return {'_parse_error': 'invalid_html'}

    details: dict[str, Any] = {}

    try:
        canonical = soup.select_one('link[rel="canonical"]')
        if canonical:
            canonical_url = canonical.get('href', '')
            parsed = urlparse(canonical_url)
            if parsed.path in ('', '/'):
                logger.warning("[metruyenful] Story page redirects to homepage (deleted)")
                return {'_parse_error': 'deleted_or_invalid_page'}
            details['slug'] = extract_story_slug(canonical_url)

        og_url = soup.select_one('meta[property="og:url"]')
        if og_url and not details.get('slug'):
            details['slug'] = extract_story_slug(og_url.get('content', ''))
    except Exception:
        logger.exception("[metruyenful] Error while parsing canonical/OG URL")

    # Extract title BEFORE decomposing head to avoid BeautifulSoup parser bugs
    # with malformed HTML that places body elements inside <head>
    # Strategy 1: itemprop="name" (best)
    # Strategy 2: .entry-title, .post-title
    # Strategy 3: Breadcrumb link (often too generic, needs check)
    title_tag = (
        soup.select_one('[itemprop="name"]') or
        soup.select_one('.entry-title') or
        soup.select_one('.post-title') or
        soup.select_one('h1')
    )

    if title_tag:
        title_text = title_tag.get_text(strip=True)
        # Avoid generic breadcrumb titles like "Truyện" or "Home"
        if title_text.lower() in ("truyện", "home") or len(title_text) < 2:
            # Look for h1 or other itemprop="name"
            all_names = soup.select('[itemprop="name"]')
            for name_tag in all_names:
                text = name_tag.get_text(strip=True)
                if text.lower() not in ("truyện", "home") and len(text) > 2:
                    title_text = text
                    break
            # Fallback to h1 if still generic
            if title_text.lower() in ("truyện", "home"):
                other_title = soup.find('h1') or soup.select_one('h2.title')
                if other_title:
                    title_text = other_title.get_text(strip=True)
        details['title'] = title_text
    else:
        logger.error("[metruyenful] Failed to parse story title")
        return {'_parse_error': 'missing_title'}

    # Now safe to remove noise for parsing remaining fields
    for hidden in soup(['script', 'style']):
        hidden.decompose()

    author = None
    try:
        author_selectors = [
            '.author a',
            '.post-author a',
            '[itemprop="author"] a',
            '[itemprop="author"]',
            '.posted-by a',
        ]
        for selector in author_selectors:
            tag = soup.select_one(selector)
            if tag:
                author = tag.get_text(strip=True)
                if author:
                    break

        if not author:
            author_tag = soup.find(string=re.compile(r'Tác giả|Author', re.IGNORECASE))
            if author_tag and author_tag.parent:
                author_text = author_tag.parent.get_text(" ", strip=True)
                author_text = re.sub(r'Tác giả:\s*', '', author_text, flags=re.IGNORECASE)
                author_text = re.sub(r'Author:\s*', '', author_text, flags=re.IGNORECASE)
                author = author_text.strip() if author_text else None

        if author:
            details['author'] = author
    except Exception:
        logger.exception("[metruyenful] Error while parsing author")

    try:
        genres: list[str] = []
        genre_selectors = [
            'a[itemprop="genre"]',
            '[itemprop="genre"] a',
            '.category a',
            '.post-categories a',
            '.tags a',
            '.tagcloud a',
            'a[rel="tag"]',
            'a[rel="category tag"]',
        ]
        for selector in genre_selectors:
            for link in soup.select(selector):
                text = link.get_text(strip=True)
                href = link.get('href', '')
                if not text or len(text) > 50:
                    continue
                slug = extract_story_slug(href)
                if _is_non_genre(text, href, slug):
                    continue
                genres.append(text)

        details['genres'] = list(dict.fromkeys(genres))
    except Exception:
        logger.exception("[metruyenful] Error while parsing genres")
        details.setdefault('genres', [])

    try:
        status = None
        status_selectors = [
            '.status',
            '.story-status',
            '.post-status',
            '.meta .status',
        ]
        for selector in status_selectors:
            tag = soup.select_one(selector)
            if tag:
                status = tag.get_text(strip=True)
                if status:
                    break

        if not status:
            status_tag = soup.find(string=re.compile(r'Tình trạng|Status|Hoàn thành|Đang ra', re.IGNORECASE))
            if status_tag and status_tag.parent:
                status_text = status_tag.parent.get_text(" ", strip=True)
                status_text = re.sub(r'Tình trạng:\s*', '', status_text, flags=re.IGNORECASE)
                status_text = re.sub(r'Status:\s*', '', status_text, flags=re.IGNORECASE)
                status = status_text.strip() if status_text else None

        if status:
            # Sanity check
            if len(status) > 50:
                status = status[:50].split('\n')[0].strip()
            details['status'] = status
        else:
            details['status'] = "Đang ra"
    except Exception:
        logger.exception("[metruyenful] Error while parsing status")

    try:
        cover_url = None
        cover_selectors = [
            'img.wp-post-image',
            '.featured-image img',
            '.post-thumbnail img',
            '[itemprop="image"] img',
            '[itemprop="image"]',
        ]
        for selector in cover_selectors:
            tag = soup.select_one(selector)
            if tag:
                cover_url = tag.get('src') or tag.get('content')
                if cover_url:
                    break

        if not cover_url:
            og_image = soup.select_one('meta[property="og:image"]')
            if og_image:
                cover_url = og_image.get('content')

        if cover_url:
            details['cover_image'] = urljoin(base_url, cover_url)
    except Exception:
        logger.exception("[metruyenful] Error while parsing cover image")

    try:
        desc_selectors = [
            '[itemprop="description"]',
            '.summary',
            '.description',
            '.entry-content',
            '.post-content',
        ]
        description = None
        for selector in desc_selectors:
            tag = soup.select_one(selector)
            if tag:
                description = tag.get_text("\n", strip=True)
                if description:
                    break

        if description:
            description = re.sub(r'^Giới thiệu\s*:?\s*', '', description, flags=re.IGNORECASE)
            details['description'] = description
    except Exception:
        logger.exception("[metruyenful] Error while parsing description")

    # TASK 2: Parse Rating & Views based on real HTML structure
    try:
        # Views: look for "&nbsp;Lượt Xem: 776 <br>"
        view_match = re.search(r'Lượt\s*Xem:\s*([\d\.,]+)', html, re.IGNORECASE)
        if view_match:
            view_str = view_match.group(1).replace('.', '').replace(',', '')
            details['view_count'] = int(view_str)
        
        # Rating: Look for Schema.org microdata (most reliable)
        rating_val = soup.select_one('[itemprop="ratingValue"]')
        rating_cnt = soup.select_one('[itemprop="ratingCount"]') or soup.select_one('[itemprop="reviewCount"]')
        
        if rating_val:
            try:
                details['rating_star'] = float(rating_val.get_text(strip=True))
            except (ValueError, TypeError):
                pass
        
        if rating_cnt:
            try:
                details['rating_count'] = int(rating_cnt.get_text(strip=True))
            except (ValueError, TypeError):
                pass
            
        # Fallback to regex if schema missing
        if 'rating_star' not in details:
            # Match patterns like "8.5/10 từ 123 lượt"
            rating_match = re.search(r'(\d+(?:\.\d+)?)\s*/\s*10\s*từ\s*(\d+)\s*lượt', soup.get_text(), re.IGNORECASE)
            if rating_match:
                details['rating_star'] = float(rating_match.group(1))
                details['rating_count'] = int(rating_match.group(2))
    except Exception:
        pass

    # Map genres to categories for compatibility
    if 'genres' in details and not details.get('categories'):
        details['categories'] = [{'name': g} for g in details['genres']] if details['genres'] else []

    # Add compatibility fields
    # MetruyenFul doesn't use numeric IDs, so set to None
    details['manga_id'] = None
    details['post_id'] = None
    details['story_id'] = None

    # Add source field
    details['source'] = 'metruyenful.com'

    # ajax_nonce (MetruyenFul doesn't use this, set to None)
    details['ajax_nonce'] = None

    return details


def parse_chapter_list(html: str, base_url: str) -> list[dict[str, str]]:
    """Parse chapter list from story detail page (one page worth).

    Args:
        html: Story detail page HTML
        base_url: Base URL of the site

    Returns:
        List of chapter dicts with 'title', 'url', 'chapter_number'

    Notes:
        - Uses WordPress chapter list selectors with fallbacks.
        - Chapter number is derived from URL or title when possible.
    """
    if not html:
        logger.warning("[metruyenful] Empty HTML provided to parse_chapter_list")
        return []

    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        logger.exception("[metruyenful] Failed to parse HTML in parse_chapter_list")
        return []

    chapters: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    link_selectors = [
        '.chapter-list a',
        '.list-chapter a',
        '#list-chapter a',
        '.wp-manga-chapter a',
        '.chapters a',
        'a[href*="-chuong-"]',
        'a[href*="-chapter-"]',
        'a[href*="/chuong"]',
        'a[href*="/chapter"]',
    ]

    links: list[Any] = []
    for selector in link_selectors:
        links.extend(soup.select(selector))

    # Get story slug to filter out non-chapter links
    story_slug = None
    canonical = soup.select_one('link[rel="canonical"]')
    if canonical:
        story_slug = extract_story_slug(canonical.get('href', ''))

    for link in links:
        try:
            href = (link.get('href') or '').strip()
            title = link.get_text(strip=True)

            if not href or not title:
                continue

            full_url = urljoin(base_url, href)

            if full_url in seen_urls:
                continue
            
            # Filter out story URL or URLs that don't look like chapters
            if story_slug and full_url.rstrip('/').endswith(story_slug):
                continue

            # Require either a detected chapter number or a URL that looks like a chapter
            # Pattern: story-slug-chuong-1.html or chuong-1/
            url_match = re.search(r'(?:chuong|chapter)-?(\d+(?:\.\d+)?)', href, re.IGNORECASE)
            
            is_chapter_url = False
            if url_match:
                chapter_num = url_match.group(1)
                is_chapter_url = True
            elif '/chuong' in href.lower() or '/chapter' in href.lower():
                is_chapter_url = True
            
            # Final verification: Title should usually contain "Chương" or "Chapter"
            # or the URL should have a numeric part at the end
            if not is_chapter_url:
                if any(token in title.lower() for token in ['chương', 'chapter']):
                    is_chapter_url = True
            
            # Avoid pagination links disguised as chapters (text like "1", "2"...)
            if title.isdigit() and len(title) < 4:
                is_chapter_url = False

            if not is_chapter_url:
                continue

            if not chapter_num:
                title_match = re.search(r'(?:Chương|Chapter)\s*(\d+(?:\.\d+)?)', title, re.IGNORECASE)
                if title_match:
                    chapter_num = title_match.group(1)

            chapters.append({
                'title': title,
                'url': full_url,
                'chapter_number': chapter_num,
            })
        except Exception:
            logger.exception("[metruyenful] Error while parsing a chapter link")

    logger.debug(f"[metruyenful] Parsed {len(chapters)} chapters from page")
    return chapters


def parse_chapter_content(html: str) -> str:
    """Parse chapter content from WordPress chapter page.

    Args:
        html: Chapter page HTML content

    Returns:
        Clean HTML content string, "" for verified empty chapters,
        or None if parsing failed.

    Notes:
        - Selectors: '.entry-content', '.post-content', '[itemprop="articleBody"]'
        - Removes ads: '.adsbygoogle', '[class*="ads"]', '[id*="ads"]'
    """
    if not html:
        logger.warning("[metruyenful] Empty HTML provided to parse_chapter_content")
        return None

    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        logger.exception("[metruyenful] Failed to parse HTML in parse_chapter_content")
        return None

    content_selectors = [
        '#chapter-c',  # Found by investigation
        '.chapter-c',  # Found by investigation
        '.entry-content',
        '.post-content',
        '[itemprop="articleBody"]',
        'article .content',
        '.single-content',
    ]

    content_div = None
    for selector in content_selectors:
        content_div = soup.select_one(selector)
        if content_div:
            logger.debug(f"[metruyenful] Found content using selector: {selector}")
            break

    if not content_div:
        logger.warning("[metruyenful] Could not find chapter content container")
        return None

    try:
        for unwanted in content_div.select(
            '.adsbygoogle, [class*="ads"], [id*="ads"], script, style, iframe, noscript'
        ):
            unwanted.decompose()

        for nav in content_div.select('.chapter-nav, .post-navigation, .nav-links'):
            nav.decompose()

        for ad_div in content_div.find_all(['div', 'ins'], class_=re.compile(r'ad', re.IGNORECASE)):
            ad_div.decompose()
    except Exception:
        logger.exception("[metruyenful] Error while cleaning chapter content")

    content_html = str(content_div)
    content_text = content_div.get_text(separator=' ', strip=True)

    if not content_text:
        logger.warning("[metruyenful] Chapter content empty after cleaning")
        return ""

    if len(content_text) < 100:
        if any(indicator in content_text.lower() for indicator in [
            'cloudflare',
            'checking your browser',
            'enable javascript',
            'please wait',
            'bot protection',
        ]):
            logger.warning("[metruyenful] Anti-bot protection detected in content")
            return None

    logger.debug(f"[metruyenful] Extracted content: {len(content_text)} chars")
    return content_html


def extract_story_slug(url: str) -> str:
    """Extract story slug from story or chapter URL.

    Args:
        url: Story or chapter URL

    Returns:
        Story slug string (empty if not found)

    Examples:
        'https://metruyenful.com/story-slug/' -> 'story-slug'
        'https://metruyenful.com/story-slug/chuong-1/' -> 'story-slug'
        'https://metruyenful.com/story-slug/chapter-2/' -> 'story-slug'
    """
    try:
        parsed = urlparse(url)
        path = parsed.path.strip('/')
    except Exception:
        logger.exception("[metruyenful] Failed to parse URL in extract_story_slug")
        return ""

    if not path:
        return ""

    parts = [part for part in path.split('/') if part]

    for part in parts:
        normalized = part.lower()

        if normalized in _SKIP_SLUG_PARTS:
            continue

        if normalized.startswith('page'):
            continue

        if normalized.startswith('chuong') or normalized.startswith('chapter'):
            continue

        cleaned = re.sub(r'\.html$', '', part, flags=re.IGNORECASE)
        cleaned = re.sub(r'\.\d+$', '', cleaned)

        if cleaned.lower().startswith('chuong') or cleaned.lower().startswith('chapter'):
            continue

        if cleaned.isdigit():
            continue

        return cleaned

    return ""

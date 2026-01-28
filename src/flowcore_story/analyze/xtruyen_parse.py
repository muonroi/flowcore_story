import base64
import binascii
import json
import re
import zlib
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

_DEFAULT_PARSER = "html.parser"


_BASE64_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r'base64\s*=\s*(?P<quote>["\'`])(?P<data>[A-Za-z0-9+/=\s]+?)(?P=quote)', re.IGNORECASE | re.DOTALL),
    re.compile(r'data-base64\s*=\s*(?P<quote>["\'`])(?P<data>[A-Za-z0-9+/=\s]+?)(?P=quote)', re.IGNORECASE | re.DOTALL),
    re.compile(r'"base64"\s*:\s*(?P<quote>["\'`])(?P<data>[A-Za-z0-9+/=\s]+?)(?P=quote)', re.IGNORECASE | re.DOTALL),
)
_DOUBLE_BREAK_PATTERN = re.compile(r'(?:&nbsp;)*(?:\s*<br\s*/?>\s*){2,}', re.IGNORECASE)
_BASE64_FALLBACK_PATTERN = re.compile(
    r'(?P<quote>["\'`])(?P<data>[A-Za-z0-9+/=\s]{80,})(?P=quote)',
    re.DOTALL,
)
_DATA_X_PATTERN = re.compile(
    r'data_x\s*=\s*(?P<quote>["\'`])(?P<data>[A-Za-z0-9_=\-]+)(?P=quote)',
    re.IGNORECASE,
)
_DATA_X_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_"
_BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_AJAX_NONCE_PATTERNS = (
    re.compile(r'ajax_nonce"\s*:\s*"([^"]+)"', re.IGNORECASE),
    re.compile(r'"nonce"\s*:\s*"([^"]+)"', re.IGNORECASE),
    re.compile(r'data-nonce\s*=\s*"([^"]+)"', re.IGNORECASE),
    re.compile(r'"security"\s*:\s*"([^"]+)"', re.IGNORECASE),
)
_MANGA_ID_PATTERN = re.compile(r'"manga_id"\s*:\s*"?(?P<id>[0-9]+)"?', re.IGNORECASE)
_POST_ID_FALLBACK_SELECTORS: tuple[str, ...] = (
    'body[data-post-id]', # New: try body attribute
    '[data-post-id]',
    '[data-postid]',
    '[data-post]',
    '[data-manga-id]',
    '[data-manga]',
    '[data-id]', 
    '#manga-chapters-holder',
    '.manga-action',
    '.wp-manga-action-button',
    'input.rating-post-id',
)
_POST_ID_REGEX = re.compile(r'"post_id"\s*:\s*"?(?P<id>[0-9]+)"?', re.IGNORECASE)
_SHORTLINK_REGEX = re.compile(r'shortlink[\'"]\s*href=[\'"].*?p=(\d+)[\'"]', re.IGNORECASE)


def _decode_base64_string(encoded: str) -> str | None:
    normalized = re.sub(r'\s+', '', encoded)
    if not normalized:
        return None
    try:
        payload = base64.b64decode(normalized)
    except (binascii.Error, ValueError):
        return None

    for window in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
        try:
            inflated = zlib.decompress(payload, window)
        except zlib.error:
            continue
        try:
            return inflated.decode('utf-8', errors='ignore')
        except UnicodeDecodeError:
            return inflated.decode('utf-8', errors='ignore')
    return None


def _extract_base64_payload(script_text: str) -> str | None:
    for pattern in _BASE64_PATTERNS:
        match = pattern.search(script_text)
        if not match:
            continue
        decoded = _decode_base64_string(match.group('data'))
        if decoded:
            return decoded

    literals = _BASE64_FALLBACK_PATTERN.findall(script_text)
    for _, candidate in sorted(literals, key=lambda item: len(item[1]), reverse=True):
        decoded = _decode_base64_string(candidate)
        if decoded:
            return decoded
    return None


def _decode_data_x_payload(encoded: str) -> str | None:
    if not encoded:
        return None
    translated = []
    for ch in encoded:
        idx = _DATA_X_ALPHABET.find(ch)
        if idx == -1:
            # Keep unknown chars as-is to preserve padding or separators.
            translated.append(ch)
        else:
            translated.append(_BASE64_ALPHABET[idx])
    normalized = ''.join(translated)
    pad = '=' * (-len(normalized) % 4)
    try:
        raw = base64.b64decode(normalized + pad)
    except (binascii.Error, ValueError):
        return None

    for window in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
        try:
            inflated = zlib.decompress(raw, window)
        except zlib.error:
            continue
        try:
            return inflated.decode('utf-8', errors='ignore')
        except UnicodeDecodeError:
            return inflated.decode('utf-8', errors='ignore')
    return None


def _sanitize_content_fragment(raw_html: str) -> str:
    normalized = _DOUBLE_BREAK_PATTERN.sub('<br><br>', raw_html.strip())
    if not normalized:
        return ''
    wrapper = BeautifulSoup(f'<div id="__payload_root__">{normalized}</div>', _DEFAULT_PARSER)
    root = wrapper.select_one('#__payload_root__')
    if not root:
        return normalized

    for selector in ('div.ads', 'div[data-cl-spot]', 'script', 'style'):
        for tag in root.select(selector):
            tag.decompose()

    for anchor in root.select('a[href="/hdsd/xaudio.php"]'):
        container = anchor.find_parent('div')
        if container:
            container.decompose()
        else:
            anchor.decompose()

    cleaned = root.decode_contents().strip()
    return cleaned


def _clean_text_blocks(html_fragment: str) -> str:
    fragment_soup = BeautifulSoup(html_fragment, _DEFAULT_PARSER)
    for tag in fragment_soup.select('script, style'):
        tag.decompose()
    for br in fragment_soup.find_all('br'):
        br.replace_with('\n')
    text = fragment_soup.get_text(separator='\n')
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return '\n'.join(lines)


def parse_genres(html_content: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    genres: list[dict[str, str]] = []
    seen: set[str] = set()

    menu = soup.select_one('li#menu-item-787939')
    anchors = menu.select('ul.sub-menu a[href]') if menu else soup.select('li.menu-item a[href*="/theloai/"]')

    for anchor in anchors:
        name = anchor.get_text(strip=True)
        href = anchor.get('href')
        if not name or not href:
            continue
        url = urljoin(base_url, href)
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        genres.append({'name': name, 'url': url})

    return genres


def parse_story_list(html_content: str, base_url: str) -> tuple[list[dict[str, str]], int]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    stories: list[dict[str, str]] = []

    # Strategy 1: Standard Madara List/Search (c-tabs-item__content)
    items = soup.select('div.c-tabs-item__content')
    
    # Strategy 2: Popular Items (fallback)
    if not items:
        items = soup.select('div.popular-item-wrap')

    # Strategy 3: Badge Position (fallback)
    if not items:
        items = soup.select('div.badge-pos-1')

    for item in items:
        # Try multiple selectors for title link
        link = (
            item.select_one('h3.h4 a[href]') or 
            item.select_one('.post-title h3 a[href]') or
            item.select_one('.post-title a[href]') or
            item.select_one('h5.widget-title a[href]')
        )
        
        if not link:
            continue
            
        title = link.get('title') or link.get_text(strip=True)
        href = link.get('href')
        
        if not title or not href:
            continue
            
        stories.append({
            'title': title.strip(),
            'url': urljoin(base_url, href),
        })

    max_page = 1
    for a in soup.select('ul.pagination a.page-link[href], .wp-pagenavi a.page[href]'):
        text = a.get_text(strip=True)
        href = a.get('href', '')
        page_num: int | None = None
        if text.isdigit():
            page_num = int(text)
        elif 'page=' in href:
            try:
                page_num = int(href.split('page=')[-1])
            except ValueError:
                page_num = None
        if page_num:
            max_page = max(max_page, page_num)

    return stories, max_page


def _extract_digits(value: str | None) -> str | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        digits = str(int(value))
        return digits if digits.isdigit() else None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return text
    match = re.search(r'(\d+)', text)
    return match.group(1) if match else None


def _extract_post_id(
    body_classes: list[str],
    soup: BeautifulSoup | None = None,
    html_content: str | None = None,
) -> str | None:
    for cls in body_classes:
        if cls.startswith('postid-'):
            digits = _extract_digits(cls.split('postid-')[-1])
            if digits:
                return digits

    if soup:
        body_data_id = soup.body and soup.body.get('data-post-id')
        if body_data_id:
            digits = _extract_digits(body_data_id)
            if digits: return digits

        for selector in _POST_ID_FALLBACK_SELECTORS:
            nodes = soup.select(selector)
            if not nodes:
                continue
            for node in nodes:
                for attr in ('data-post-id', 'data-postid', 'data-post', 'data-id', 'data-manga', 'data-manga-id', 'value'):
                    digits = _extract_digits(node.get(attr))
                    if digits:
                        return digits

    if html_content:
        match_sl = _SHORTLINK_REGEX.search(html_content)
        if match_sl:
            return match_sl.group(1)
            
        match = _POST_ID_REGEX.search(html_content)
        if match:
            return match.group('id')
    return None


def _extract_chapter_ranges(soup: BeautifulSoup) -> list[dict[str, int]]:
    ranges: list[dict[str, int]] = []

    def _parse_int(value: str | None) -> int | None:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.isdigit():
                return int(stripped)
        return None

    chapter_groups = soup.select('ul.version-chap > li.has-child')
    for item in chapter_groups:
        anchor = item.select_one('a.has-child')
        label = anchor.get_text(" ", strip=True) if anchor else item.get_text(" ", strip=True)
        label_numbers = [
            int(num)
            for num in re.findall(r'(?:^|[^0-9])(\d+)(?=[^0-9]|$)', label)
        ]

        start: int | None = label_numbers[0] if label_numbers else None
        end: int | None = None
        if label_numbers:
            end = label_numbers[1] if len(label_numbers) >= 2 else label_numbers[0]

        data_start_candidates = [
            _parse_int(item.get('data-from')),
            _parse_int(item.get('data-start')),
        ]
        data_end_candidates = [
            _parse_int(item.get('data-to')),
            _parse_int(item.get('data-end')),
        ]

        value_attr = item.get('data-value')
        value_numbers: list[int] = []
        if isinstance(value_attr, str) and value_attr:
            value_numbers = [int(num) for num in re.findall(r'(\d+)', value_attr)]
            if value_numbers:
                data_start_candidates.append(value_numbers[0])
                if len(value_numbers) >= 2:
                    data_end_candidates.append(value_numbers[1])
                else:
                    data_end_candidates.append(value_numbers[0])

        if start is None:
            for candidate in data_start_candidates:
                if candidate is not None:
                    start = candidate
                    break
        if end is None:
            for candidate in data_end_candidates:
                if candidate is not None:
                    end = candidate
                    break

        for candidate in data_start_candidates:
            if candidate is None or start is None:
                continue
            if abs(candidate - start) > 5:
                start = candidate
                break

        for candidate in data_end_candidates:
            if candidate is None or end is None:
                continue
            if end - candidate > 5:
                end = candidate
                break

        if start is None:
            continue
        if end is None:
            end = start
        if end < start:
            start, end = end, start

        if ranges and ranges[-1]['from'] == start and ranges[-1]['to'] == end:
            continue
        ranges.append({'from': start, 'to': end})
    return ranges


def parse_story_info(html_content: str, base_url: str = "") -> dict[str, Any]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    # Remove noise early
    for hidden in soup(['script', 'style', 'head']):
        hidden.decompose()

    # Extract title
    title_tag = (
        soup.select_one('.post-title h1') or 
        soup.select_one('h1.post-title') or 
        soup.select_one('h1.entry-title') or
        soup.select_one('h1')
    )
    
    title = title_tag.get_text(strip=True) if title_tag else None
    
    # Author extraction with fallback for non-link text
    author_tag = soup.select_one('.author-content a, .author-name a')
    if not author_tag:
        author_tag = soup.select_one('.author-content, .author-name')
    
    cover_tag = soup.select_one('.summary_image img, .tab-summary img, .summary_image a img')

    description_block = soup.select_one('.description-summary .summary__content')
    description = ''
    if description_block:
        raw_text = description_block.get_text(separator='\n')
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        description = '\n'.join(lines)

    # Genres extraction with multiple strategies
    genres = []
    seen_genre_names = set()
    
    # Strategy 1: Standard Madara selectors for Genres and Tags
    genre_anchors = soup.select('.genres-content a[href], .manga-genre a[href], a[itemprop="genre"], .tags-content a[href], .wp-manga-tags-list a[href]')
    
    # Strategy 2: Search by heading
    post_items = soup.select('.post-content_item')
    for item in post_items:
        heading = item.select_one('.summary-heading h3, .summary-heading h5, .summary-heading')
        if heading:
            text = heading.get_text().lower()
            if 'thể loại' in text or 'genre' in text or 'tag' in text or 'từ khóa' in text:
                content = item.select_one('.summary-content')
                if content:
                    genre_anchors.extend(content.select('a[href]'))

    for a in genre_anchors:
        name = a.get_text(strip=True)
        name = name.strip(' ,').strip()
        if name and name.lower() not in seen_genre_names:
            genres.append({
                'name': name,
                'url': urljoin(base_url, a.get('href'))
            })
            seen_genre_names.add(name.lower())

    # Status extraction
    status_text = None
    for item in post_items:
        heading = item.select_one('.summary-heading h3, .summary-heading h5, .summary-heading')
        if heading and ('tình trạng' in heading.get_text().lower() or 'status' in heading.get_text().lower()):
            content = item.select_one('.summary-content')
            if content:
                status_text = content.get_text(strip=True)
                break
    
    if not status_text:
        # Fallback search
        status_tag = soup.find(string=re.compile(r'Tình trạng|Status', re.IGNORECASE))
        if status_tag and status_tag.parent:
            status_text = status_tag.parent.get_text(strip=True).replace('Tình trạng:', '').replace('Status:', '').strip()

    if not status_text:
        status_text = "Đang ra"

    body = soup.body or soup
    body_classes = body.get('class', []) if hasattr(body, 'get') else []
    post_id = _extract_post_id(body_classes, soup=soup, html_content=html_content)

    # Extract manga_id from HTML content
    manga_id = None
    if html_content:
        match = _MANGA_ID_PATTERN.search(html_content)
        if match:
            manga_id = match.group('id')

    # Fallback to post_id if manga_id not found
    if not manga_id:
        manga_id = post_id

    chapters = parse_chapter_list(html_content, base_url)

    total_chapters = None
    # Use full content for total chapters as we decomposed soup
    total_chapters_match = re.search(r'"data-last"\s*:\s*"?(?:Chương\s+)?(\d+)"?', html_content)
    if total_chapters_match:
        total_chapters = int(total_chapters_match.group(1))

    if total_chapters is None:
        total_chapters = len(chapters) or None

    # TASK 2: Parse Rating & Views
    rating_value = None
    rating_count = None
    view_count = None
    
    # Try to extract views from summary items
    for item in post_items:
        heading = item.select_one('.summary-heading h3, .summary-heading h5, .summary-heading')
        if heading and 'lượt xem' in heading.get_text().lower():
            content = item.select_one('.summary-content')
            if content:
                view_text = content.get_text(strip=True)
                view_count_str = _extract_digits(view_text)
                if view_count_str:
                    view_count = int(view_count_str)

    # Rating from page content (Madara specific)
    rating_star_tag = soup.select_one('#averagerating')
    if rating_star_tag:
        try:
            rating_value = float(rating_star_tag.get_text(strip=True))
        except (ValueError, TypeError):
            pass

    rating_count_tag = soup.select_one('#countrating')
    if rating_count_tag:
        try:
            rating_count = int(rating_count_tag.get_text(strip=True))
        except (ValueError, TypeError):
            pass

    # Extract ajax_nonce from HTML content
    ajax_nonce = None
    if html_content:
        for pattern in _AJAX_NONCE_PATTERNS:
            match = pattern.search(html_content)
            if match:
                ajax_nonce = match.group(1)
                break

    # Set source
    source = "xtruyen.vn"

    # Standardize field names for output
    details = {
        'title': title,
        'author': author_tag.get_text(strip=True) if author_tag else None,
        'description': description,
        'post_id': post_id,
        'manga_id': manga_id,
        'status': status_text,
        'categories': genres,
        'genres_full': genres,
        'cover': cover_tag.get('src') if cover_tag and cover_tag.get('src') else None,
        'chapters': chapters,
        'total_chapters_on_site': total_chapters,
        'rating_value': rating_value,
        'rating_star': rating_value,
        'rating_count': rating_count,
        'view_count': view_count,
        'source': source,
        'ajax_nonce': ajax_nonce,
    }
    return details


def _detect_chapter_number(label: str) -> int | None:
    if not label:
        return None
    match = re.search(r'(?:chuong|chapter)[^0-9]*([0-9]+)', label, re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.search(r'([0-9]+)', label)
    if match:
        return int(match.group(1))
    return None


def parse_chapter_list(html_content: str, base_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    anchors = []
    chapter_list_container = soup.select_one('ul.version-chap')
    if chapter_list_container:
        anchors = chapter_list_container.select('li a[href]')

    if not anchors:
        anchors = soup.select('ul.main li.wp-manga-chapter a[href]')
        if not anchors:
            anchors = soup.select('li.wp-manga-chapter a[href]')

    chapters: list[dict[str, str]] = []
    chapter_numbers: list[int] = []
    for a in anchors:
        href = a.get('href')
        if href and href.strip().startswith('javascript:'):
            continue

        title = a.get_text(strip=True)
        if not title or not href:
            continue

        number = _detect_chapter_number(title)
        if number is not None:
            chapter_numbers.append(number)

        chapters.append({
            'title': title,
            'url': urljoin(base_url, href),
            'chapter_number': number,
        })

    if len(chapter_numbers) >= 2:
        first, last = chapter_numbers[0], chapter_numbers[-1]
        if first > last:
            chapters.reverse()
    else:
        chapters.reverse()

    return chapters


def parse_chapter_content(html_content: str) -> dict[str, str | None] | None:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    title_tag = soup.select_one('h2') or soup.select_one('h1#chapter-heading')
    title = title_tag.get_text(strip=True) if title_tag else None

    content_html: str | None = None

    def _resolve_payload_from_script(script_tag) -> str | None:
        if not script_tag:
            return None

        data_attr = script_tag.get('data-base64')
        if data_attr:
            decoded = _decode_base64_string(data_attr)
            if decoded:
                return decoded

        script_text = script_tag.string or script_tag.get_text()
        if script_text:
            payload_text = _extract_base64_payload(script_text)
            if payload_text:
                return payload_text
        return None

    script = soup.select_one('script#decompress-script')
    payload = _resolve_payload_from_script(script)

    if not payload:
        for candidate in soup.find_all('script'):
            if candidate is script:
                continue
            marker = candidate.string or candidate.get_text()
            if not marker:
                continue
            lowered = marker.lower()
            if 'base64' not in lowered and 'pako' not in lowered:
                continue
            payload = _resolve_payload_from_script(candidate)
            if payload:
                break

    if payload:
        sanitized = _sanitize_content_fragment(payload)
        if sanitized:
            lower = sanitized.lower()
            if '<p' in lower:
                content_html = sanitized
            elif '<br' in lower:
                parts = [
                    segment.strip()
                    for segment in re.split(r'<br\s*/?>', sanitized)
                    if segment.strip()
                ]
                if parts:
                    content_html = ''.join(f'<p>{part}</p>' for part in parts)
            else:
                normalized = _clean_text_blocks(sanitized)
                if normalized:
                    parts = [line for line in normalized.split('\n') if line.strip()]
                    content_html = ''.join(f'<p>{part}</p>' for part in parts) or None

    if not content_html:
        for candidate in soup.find_all('script'):
            marker = candidate.string or candidate.get_text()
            if not marker or 'data_x' not in marker:
                continue
            match = _DATA_X_PATTERN.search(marker)
            if not match:
                continue
            decoded = _decode_data_x_payload(match.group('data'))
            if not decoded:
                continue
            sanitized = _sanitize_content_fragment(decoded)
            if sanitized:
                content_html = sanitized
            break

    if not content_html:
        content_div = soup.select_one('#chapter-reading-content')
        if content_div:
            extracted = content_div.decode_contents().strip()
            if extracted:
                content_html = extracted
            else:
                content_html = ""

    return {'title': title, 'content': content_html}

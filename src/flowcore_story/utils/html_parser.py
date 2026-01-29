import json
import os
from urllib.parse import urlparse

import chardet
from bs4 import BeautifulSoup

from flowcore_story.config.config import AI_PROFILES_PATH, BASE_URLS, HEADER_RE, PATTERN_FILE, SITE_SELECTORS
from flowcore_story.utils.chapter_utils import slugify_title
from flowcore_story.utils.cleaner import clean_chapter_content
from flowcore_story.utils.io_utils import filter_lines_by_patterns, load_patterns
from flowcore_story.utils.logger import logger

BLACKLIST_PATTERNS = load_patterns(PATTERN_FILE)


def extract_chapter_content(
    html: str,
    site_key: str,
    chapter_title: str = None,  # type: ignore
    patterns: list = None,  # type: ignore
) -> str:
    if patterns is None:
        patterns = load_patterns(PATTERN_FILE)

    debug_prefix = f"[DEBUG][{site_key}][{chapter_title}]"

    # Detect encoding if needed
    try:
        if isinstance(html, bytes):
            detected = chardet.detect(html)
            logger.info(f"{debug_prefix} Detected encoding: {detected}")
            html = html.decode(detected.get('encoding') or 'utf-8', errors='replace')
    except Exception as e:
        logger.error(f"{debug_prefix} Error when detect/decode encoding: {e}")

    # Parse and locate chapter container
    soup = BeautifulSoup(html, "html.parser")
    chapter_div = None

    # Try id-based heuristic first (chapter-c variants)
    for div in soup.find_all('div'):
        div_id = div.get('id')  # type: ignore
        if div_id:
            logger.warning(f"[DEBUG][{site_key}][{chapter_title}] id: {repr(div_id)}")
            id_clean = (
                div_id.strip().lower().replace('\u200b', '').replace('\xa0', '').replace('\t', '').replace('\n', '')
            )
            if 'chapter-c' in id_clean:
                chapter_div = div
                logger.warning(
                    f"[DEBUG][{site_key}][{chapter_title}] FOUND id: {repr(div_id)} CLEAN: {repr(id_clean)}"
                )
                with open(f'debug_div_{slugify_title(chapter_title)}.html', 'w', encoding='utf-8') as f:  # type: ignore
                    f.write(str(div))
                break

    # Site-specific selector fallback
    if not chapter_div:
        selector_fn = SITE_SELECTORS.get(site_key)
        if selector_fn:
            try:
                chapter_div = selector_fn(soup)
            except Exception as e:
                logger.error(f"{debug_prefix} Error calling selector_fn: {e}")

    # Cached AI content selector fallback (non-blocking): try domain profile if exists
    if not chapter_div:
        try:
            base = BASE_URLS.get(site_key)
            domain = urlparse(base).netloc if base else None
            if domain and AI_PROFILES_PATH:
                profile_path = AI_PROFILES_PATH
                if os.path.exists(profile_path):
                    with open(profile_path, encoding="utf-8") as f:
                        data = json.load(f)
                    prof = data.get(domain)
                    sel = prof.get("content_selector") if isinstance(prof, dict) else None
                    if sel:
                        cand = soup.select_one(sel)
                        if cand:
                            chapter_div = cand
                            logger.warning(f"{debug_prefix} [AI-CACHED] Applied content selector: {sel}")
        except Exception as e:
            logger.warning(f"{debug_prefix} Cached content selector load failed: {e}")

    # As a last resort, dump and return empty
    if not chapter_div:
        fname = f"debug_empty_chapter_{slugify_title(chapter_title) or 'unknown'}.html"
        if not os.path.exists(fname):
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(html)
        logger.error(
            f"{debug_prefix} Could not find chapter content container. Saved HTML to {fname}"
        )
        return ""

    try:
        clean_chapter_content(chapter_div)
    except Exception as e:
        logger.error(f"{debug_prefix} Error when clean_chapter_content: {e}")

    text = chapter_div.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    logger.info(f"{debug_prefix} Line count after strip: {len(lines)}")
    if len(lines) < 3:
        logger.warning(f"{debug_prefix} Very few lines after clean ({len(lines)}). Content may be missing.")

    try:
        cleaned_lines = filter_lines_by_patterns(lines, patterns)
    except Exception as e:
        logger.error(f"{debug_prefix} Error when filter_lines_by_patterns: {e}")
        cleaned_lines = lines

    content = clean_header("\n".join(cleaned_lines)).strip()
    if not content:
        fname = f"debug_empty_chapter_{slugify_title(chapter_title) or 'unknown'}_after_filter.html"
        if not os.path.exists(fname):
            with open(fname, 'w', encoding='utf-8') as f:
                f.write(html)
        logger.error(
            f"{debug_prefix} Chapter content EMPTY after filter/clean. Saved HTML to {fname}"
        )
        return ""

    return content


def clean_header(text: str):
    lines = text.splitlines()
    out = []
    skipping = True
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line:
            continue
        if skipping and HEADER_RE.match(stripped_line):
            continue
        skipping = False
        out.append(line)
    return "\n".join(out).strip()


def get_total_pages_category(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    import re
    pag = soup.select_one('ul.pagination')
    if not pag:
        return 1
    max_page = 1
    for a in pag.find_all('a'):
        if 'Cu?i' in a.get_text():
            title = a.get('title', '')  # type: ignore
            m = re.search(r'trang[- ]?(\d+)', title, re.I)  # type: ignore
            if m:
                num = int(m.group(1))
                if num > max_page:
                    max_page = num
            else:
                href = a.get('href', '')  # type: ignore
                m = re.search(r'/trang-(\d+)', href)  # type: ignore
                if m:
                    num = int(m.group(1))
                    if num > max_page:
                        max_page = num
        elif a.get_text().strip().isdigit():
            num = int(a.get_text().strip())
            if num > max_page:
                max_page = num
    return max_page


def parse_stories_from_category_page(html: str):
    soup = BeautifulSoup(html, "html.parser")
    stories = []
    for row in soup.select('div.row[itemtype="https://schema.org/Book"]'):
        a_tag = row.select_one('.truyen-title a')
        if a_tag and a_tag.has_attr('href'):
            title = a_tag.get_text(strip=True)
            href = a_tag['href']
            stories.append({
                "title": title,
                "url": href,
            })
    return stories


def get_total_pages_metruyen_category(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    pag = soup.select_one('ul.pagination')
    if not pag:
        return 1
    max_page = 1
    for a in pag.find_all('a'):
        data_page = a.get('data-page')  # type: ignore
        if data_page and data_page.isdigit():  # type: ignore
            num = int(data_page)  # type: ignore
            if num > max_page:
                max_page = num
        elif 'Cu?i' in a.get_text() or 'Cu?i' in a.get('title', ''):  # type: ignore
            title = a.get('title')  # type: ignore
            if title and title.isdigit():  # type: ignore
                num = int(title)  # type: ignore
                if num > max_page:
                    max_page = num
            else:
                import re
                m = re.search(r'/page/(\d+)', a.get('href', ''))  # type: ignore
                if m:
                    num = int(m.group(1))
                    if num > max_page:
                        max_page = num
        elif a.get_text().isdigit():
            num = int(a.get_text())
            if num > max_page:
                max_page = num
    return max_page

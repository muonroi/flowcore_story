import html
import json
import re
import unicodedata
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

_DEFAULT_PARSER = "html.parser"


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


_ALLOWED_TITLE_PUNCTUATION = set("-_()[]!?',.:/&")


def _sanitize_vietnamese_title(value: str | None) -> str:
    if not value:
        return ""

    sanitized: list[str] = []
    last_latin_letter = False

    for ch in value:
        if ch.isdigit():
            sanitized.append(ch)
            last_latin_letter = False
            continue
        if ch.isspace():
            sanitized.append(ch)
            last_latin_letter = False
            continue
        if ch in _ALLOWED_TITLE_PUNCTUATION:
            sanitized.append(ch)
            last_latin_letter = False
            continue

        category = unicodedata.category(ch)
        if category.startswith("L"):
            name = unicodedata.name(ch, "")
            if "LATIN" in name:
                sanitized.append(ch)
                last_latin_letter = True
                continue
        if category == "Mn" and last_latin_letter:
            sanitized.append(ch)
            continue

        last_latin_letter = False

    cleaned = re.sub(r"\s+", " ", "".join(sanitized)).strip()
    return cleaned


def _title_from_url(url: str) -> str:
    path = (urlparse(url).path or "").strip("/")
    slug = path.split("/")[-1] if path else ""
    slug = slug.replace("-", " ")
    return re.sub(r"\s+", " ", slug).strip()


def _extract_first_int(text: str | None) -> int | None:
    if not text:
        return None
    normalized = re.sub(r"[^0-9]", "", text)
    return int(normalized) if normalized else None


_PRIVATE_USE_RE = re.compile(
    "[\uE000-\uF8FF\U000F0000-\U000FFFFD\U00100000-\U0010FFFD]"
)
_TRAILING_COUNT_RE = re.compile(r"(?:[\s\(\[\u00A0]*\d+[\s\)\]\u00A0]*)+$")


def _normalize_genre_name(value: str | None) -> str:
    if not value:
        return ""
    cleaned = _PRIVATE_USE_RE.sub("", value)
    cleaned = _normalize_text(cleaned)
    cleaned = _TRAILING_COUNT_RE.sub("", cleaned).strip()
    return cleaned


def parse_genres(html_content: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    results: list[dict[str, str]] = []
    seen: set[str] = set()

    # Define allowed domains for genre URLs
    allowed_domains = {urlparse(base_url).netloc, urlparse("https://m.tangthuvien.net").netloc, urlparse("https://tangthuvien.net").netloc}

    # 1. Mobile categories carousel (/danh-muc) structure (most reliable for mobile base_url)
    for anchor in soup.select(".outer-categories a.category"):
        raw_text = html.unescape(anchor.get_text(" ", strip=True))
        name = _normalize_genre_name(raw_text)
        href = anchor.get("href")
        
        if not name or not href or href.strip().lower().startswith("javascript"):
            continue
        
        url = urljoin(base_url, href)
        # Ensure the genre URL belongs to TangThuVien domain
        if urlparse(url).netloc not in allowed_domains:
            continue

        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append({"name": name, "url": url})
    
    if results:
        return results

    # 2. Desktop version selectors (if not found in mobile specific section, or for desktop base_url)
    desktop_selectors = [
        "header a[href^='/the-loai/']",
        "nav a[href^='/the-loai/']",
        "div.nav-list a[href^='/the-loai/']",
        "div.classify-list a[href*='/the-loai/']",  # Desktop genre grid
        "a[href*='/the-loai/']", # Broad fallback but now with domain check
    ]

    for selector in desktop_selectors:
        for anchor in soup.select(selector):
            raw_text = html.unescape(anchor.get_text(" ", strip=True))
            name = _normalize_genre_name(raw_text)
            href = anchor.get("href")
            if not name or not href:
                continue
            
            url = urljoin(base_url, href)
            # Ensure the genre URL belongs to TangThuVien domain
            if urlparse(url).netloc not in allowed_domains:
                continue

            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            results.append({"name": name, "url": url})
        if results: # If we found genres with a desktop selector, use them
            return results

    # 3. Mobile version fallback - if no desktop genres found, try mobile patterns
    #    This part might have picked up xtruyen links, making it more strict
    mobile_fallback_selectors = [
        "a[href*='/tong-hop?ctg=']",
        "a[href*='ctg=']",
        "a[href*='/danh-muc/']",
    ]

    for selector in mobile_fallback_selectors:
        for anchor in soup.select(selector):
            raw_text = html.unescape(anchor.get_text(" ", strip=True))
            name = _normalize_genre_name(raw_text)
            href = anchor.get("href")
            if not name or not href:
                continue
            
            url = urljoin(base_url, href)
            # Ensure the genre URL belongs to TangThuVien domain
            if urlparse(url).netloc not in allowed_domains:
                continue

            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            results.append({"name": name, "url": url})
        if results:
            return results

    return results


def find_genre_listing_url(html_content: str, base_url: str) -> str | None:
    """Locate the canonical listing url (/tong-hop?ctg=...) for a genre page."""

    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    candidates: list[tuple[int, str]] = []

    for anchor in soup.select("a[href*='/tong-hop']"):
        href = anchor.get("href") or ""
        if "ctg=" not in href:
            continue
        url = urljoin(base_url, href)
        text = _normalize_text(anchor.get_text(" ", strip=True)).lower()

        score = 0
        if "tp=cv" in href:
            score += 3
        if "xem thêm" in text:
            score += 2
        if "rank=" in href or "time=" in href:
            score -= 1
        if "ord=" in href:
            score -= 1

        candidates.append((score, url))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][1]


def parse_story_list(html_content: str, base_url: str) -> tuple[list[dict[str, str]], int]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    stories: list[dict[str, str]] = []
    seen: set[str] = set()

    def _extract_story_from_container(container) -> dict[str, str] | None:
        title_tag = None
        # Prioritize a.name (from table rows), then other common selectors
        for selector in (
            "a.name[href]", # Common in div.update-list table tbody tr
            "a.book-name[href]",
            "a.bookTitle[href]",
            "h3 a[href]",
            "h4 a[href]",
            "a[href]",
        ):
            candidate = container.select_one(selector)
            if not candidate:
                continue
            href_value = candidate.get("href") or ""
            if "/chuong-" in href_value.lower(): # Exclude chapter links
                continue
            if "?read_now=" in href_value.lower() or "read_now=1" in href_value.lower():
                continue
            title_tag = candidate
            break

        if not title_tag:
            return None

        raw_title = _normalize_text(html.unescape(title_tag.get_text(" ", strip=True)))
        if not raw_title:
            return None

        # Strip metadata that may be concatenated with title (mobile site issue)
        # Pattern: "Title Thể loại: Genre Tác giả: Author Lượt xem: Views Tình trạng: Status Số chuơng: Chapters"
        # Keep only the title part before "Thể loại:"
        if "Thể loại:" in raw_title:
            raw_title = raw_title.split("Thể loại:")[0].strip()

        sanitized_title = _sanitize_vietnamese_title(raw_title)
        if sanitized_title.lower() == "doc truyen": # Specific bad title from old parser
            return None
        href = title_tag.get("href")
        if not href:
            return None
        url = urljoin(base_url, href)
        path_lower = (urlparse(url).path or "").rstrip("/").lower()
        
        # Story URLs are /doc-truyen/slug
        if not path_lower.startswith("/doc-truyen/"): # Updated to /doc-truyen/
            return None # Skip if not a valid story URL
            
        if url in seen:
            return None

        title = sanitized_title
        if not title:
            fallback = _title_from_url(url)
            title = _sanitize_vietnamese_title(fallback) or fallback or raw_title

        story: dict[str, str] = {"title": title, "url": url}

        # Latest chapter (find within the same row/container)
        latest_tag = container.select_one("a.section[href*='/chuong-']")
        if latest_tag:
            story["latest_chapter"] = _normalize_text(html.unescape(latest_tag.get_text(" ", strip=True)))
            latest_href = latest_tag.get("href") or ""
            story["latest_chapter_url"] = urljoin(base_url, latest_href)

        # Author (find within the same row/container)
        author_tag = container.select_one("a.author[href*='/tac-gia?author=']")
        if author_tag:
            story["author"] = _normalize_text(html.unescape(author_tag.get_text(" ", strip=True)))

        seen.add(url)
        return story

    # Primary selector for /the-loai/{slug} pages: table rows
    for item in soup.select("div.update-list table tbody tr"):
        story = _extract_story_from_container(item)
        if story:
            stories.append(story)
            
    if not stories: # Fallback to other common list items
        for item in soup.select("div.update-list div.book-wrap, div.update-list li, div.book-list li, div.update-list div.story-card"):
            story = _extract_story_from_container(item)
            if story:
                stories.append(story)
                
    if not stories: # Fallback to even broader link search
        container = soup.select_one("div.update-list") or soup
        for anchor in container.select("a[href*='/doc-truyen/']"):
            href = anchor.get("href") or ""
            if not href or "/chuong-" in href.lower():
                continue
            parent_story = _extract_story_from_container(anchor.parent or container)
            if parent_story:
                stories.append(parent_story)


    max_page = 1
    # Pagination for /the-loai/{slug} pages (observed as numbers in div.page)
    pagination_div = soup.select_one('div.page')
    if pagination_div:
        for anchor in pagination_div.select("a.page-numbers[href], span.page-numbers"):
            href = anchor.get("href") or ""
            text_num = _extract_first_int(anchor.get_text(strip=True))

            page_num = None
            if text_num:
                page_num = text_num
            elif "page/" in href: # Example: /the-loai/huyen-huyen/page/2
                try:
                    match = re.search(r"page/(\d+)", href)
                    if match:
                        page_num = int(match.group(1))
                except ValueError:
                    pass
            
            if page_num:
                max_page = max(max_page, page_num)
    
    # Fallback to old pagination logic (for /tong-hop?ctg=X&page=Y or other formats)
    for anchor in soup.select("a[href*='page=']"):
        href = anchor.get("href") or ""
        match = re.search(r"page=([0-9]+)", href)
        if match:
            try:
                max_page = max(max_page, int(match.group(1)))
            except ValueError:
                continue

    for option in soup.select("select option[value]"):
        value = option.get("value")
        if value and value.isdigit():
            max_page = max(max_page, int(value))

    for element in soup.select("[data-page]"):
        data_page = element.get("data-page")
        if data_page and data_page.isdigit():
            max_page = max(max_page, int(data_page))


    return stories, max_page


def _validate_required_fields(story_data: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate required metadata fields are present and non-empty.

    Returns:
        (is_valid, missing_fields): True if all required fields present, list of missing field names
    """
    required_fields = {
        'title': story_data.get('title'),
        'author': story_data.get('author'),
        'description': story_data.get('description'),
        'categories': story_data.get('categories') or story_data.get('genres_full'),
    }

    missing = []
    for field_name, field_value in required_fields.items():
        if not field_value:
            missing.append(field_name)
        elif isinstance(field_value, str) and not field_value.strip():
            missing.append(field_name)
        elif isinstance(field_value, list) and len(field_value) == 0:
            missing.append(field_name)

    return (len(missing) == 0, missing)


def parse_story_info(html_content: str, base_url: str = "") -> dict[str, Any]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    def _collect_genres(s: BeautifulSoup, current_base: str) -> list[dict[str, str]]:
        genre_tags = s.select("p.tag a.red")
        if not genre_tags:
            # Some pages use other classes or generic anchors for categories
            genre_tags = s.select("div.book-info a[href*='/the-loai/']")
        if not genre_tags:
            genre_tags = s.select("div.crumbs-nav a[href*='/the-loai/']")
        if not genre_tags:
            genre_tags = s.select("a[href*='/the-loai/']")

        genres_local: list[dict[str, str]] = []
        seen_genres: set[str] = set()

        for tag in genre_tags:
            name = _normalize_text(html.unescape(tag.get_text(" ", strip=True)))
            href = tag.get("href")
            if not name or not href:
                continue
            url_val = urljoin(current_base, href)
            key = url_val.lower()
            if key in seen_genres:
                continue
            seen_genres.add(key)
            genres_local.append({"name": name, "url": url_val})

        # Mobile "danh-muc" carousel (owl-stage) fallback - for mobile pages
        if not genres_local:
            for tag in s.select(".owl-stage .owl-item a.category"):
                name = _normalize_genre_name(tag.get_text(" ", strip=True))
                href = (tag.get("href") or "").strip()
                if not name or not href or href.startswith("javascript"):
                    continue
                url_val = urljoin(current_base, href)
                key = url_val.lower()
                if key in seen_genres:
                    continue
                seen_genres.add(key)
                genres_local.append({"name": name, "url": url_val})

        # Fallback: parse meta description for "thể loại X" if anchors are missing
        if not genres_local:
            meta_desc = s.find("meta", attrs={"property": "og:description"}) or s.find(
                "meta", attrs={"name": "description"}
            )
            if meta_desc and meta_desc.get("content"):
                desc = _normalize_text(html.unescape(meta_desc["content"]))
                match = re.search(r"th(?:ể|e)\s*loại\s+([^,.;]+)", desc, flags=re.IGNORECASE)
                if match:
                    name = _normalize_genre_name(match.group(1))
                    if name:
                        genres_local.append({"name": name, "url": None})

        return genres_local

    title_tag = soup.select_one("div.book-info h1")
    title = html.unescape(title_tag.get_text(" ", strip=True)) if title_tag else None
    if title and " - " in title:
        head, tail = title.split(" - ", 1)
        if tail.strip() and set(tail.strip()) <= {"?", "-"}:
            title = head.strip()
    if title:
        title = _normalize_text(title)

    author_tag = soup.select_one("p.tag a.blue")
    author = _normalize_text(html.unescape(author_tag.get_text(" ", strip=True))) if author_tag else None

    status_tag = soup.select_one("p.tag span.blue")
    status = _normalize_text(html.unescape(status_tag.get_text(" ", strip=True))) if status_tag else None

    description_parts: list[str] = []
    for node in soup.select("div.book-info p.intro, p.intro"):
        text = _normalize_text(html.unescape(node.get_text(" ", strip=True)))
        if text:
            description_parts.append(text)
    if not description_parts:
        summary = soup.select_one("div#bookDetail, div.book-detail")
        if summary:
            text = _normalize_text(html.unescape(summary.get_text(" ", strip=True)))
            if text:
                description_parts.append(text)
    description = " ".join(description_parts)

    cover_tag = soup.select_one("div.book-img img[src]")
    cover = urljoin(base_url, cover_tag.get("src", "")) if cover_tag else None

    # Collect genres from current HTML
    genres: list[dict[str, str]] = _collect_genres(soup, base_url)

    story_id = None
    story_meta = soup.find("meta", attrs={"name": "book_detail"})
    if story_meta and story_meta.get("content"):
        story_id = story_meta["content"].strip()
    if not story_id:
        hidden = soup.select_one("input#story_id_hidden[value]")
        if hidden and hidden.get("value"):
            story_id = hidden["value"].strip()

    path_meta = soup.find("meta", attrs={"name": "book_path"})
    story_path = path_meta.get("content").strip() if path_meta and path_meta.get("content") else None

    if title:
        sanitized_title = _sanitize_vietnamese_title(title)
        if not sanitized_title and story_path:
            fallback_url = urljoin(base_url, story_path)
            fallback_title = _title_from_url(fallback_url)
            sanitized_title = _sanitize_vietnamese_title(fallback_title) or fallback_title
        title = sanitized_title or title

    total_chapters = None
    catalog_link = soup.select_one("a#j-bookCatalogPage")
    if catalog_link:
        total_chapters = _extract_first_int(catalog_link.get_text(" ", strip=True))

    inline_chapters: list[dict[str, str]] = []
    catalog_container = soup.select_one("div.catalog-content")
    if catalog_container:
        inline_html = catalog_container.decode_contents()
        if inline_html:
            inline_chapters = parse_chapter_list(inline_html, base_url)

    latest_chapters: list[dict[str, str]] = []
    for volume in soup.select("div.volume-wrap div.volume"):
        heading = volume.find("h3")
        if not heading:
            continue
        heading_text = _normalize_text(html.unescape(heading.get_text(" ", strip=True))).lower()
        if "chương mới nhất" not in heading_text:
            continue
        for anchor in volume.select("ul li a[href]"):
            href = anchor.get("href")
            text = _normalize_text(html.unescape(anchor.get_text(" ", strip=True)))
            if not href or not text:
                continue
            latest_chapters.append({
                "title": text,
                "url": urljoin(base_url, href),
            })
        if latest_chapters:
            break

    rating_value = None
    rating_count = None
    json_ld_tag = soup.find("script", type="application/ld+json")
    if json_ld_tag and json_ld_tag.string:
        try:
            payload = json.loads(json_ld_tag.string)
            rating = payload.get("aggregateRating")
            if isinstance(rating, dict):
                rating_value = rating.get("ratingValue")
                rating_count = rating.get("reviewCount") or rating.get("reviewCount")
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    stats_map = {
        "ULtwOOTH-like": "likes",
        "ULtwOOTH-view": "views",
        "ULtwOOTH-follow": "follows",
        "ULtwOOTH-nomi": "monthly_votes",
    }
    stats: dict[str, int | None] = {}
    for cls, key in stats_map.items():
        span = soup.select_one(f"span.{cls}")
        if not span:
            continue
        stats[key] = _extract_first_int(span.get_text(" ", strip=True))

    # TASK 2: Standardize fields for AI training
    view_count = stats.get("views")
    
    return {
        "title": title,
        "author": author,
        "status": status,
        "description": description,
        "cover": cover,
        "categories": genres,
        "genres_full": genres,
        "chapters": inline_chapters,
        "total_chapters_on_site": total_chapters,
        "story_id": story_id,
        "manga_id": story_id,
        "story_path": story_path,
        "rating_value": rating_value,
        "rating_star": rating_value, # Alias for standardization
        "rating_count": rating_count,
        "view_count": view_count,    # Alias for standardization
        "stats": stats,
        "tags": tags,
        "latest_chapters": latest_chapters,
        "source": "tangthuvien.net",
        "ajax_nonce": None,
        "chapter_ranges": [],
    }


def parse_chapter_list(html_content: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)
    chapters: list[dict[str, str]] = []
    seen: set[str] = set()

    anchor_candidates = soup.select("ul.cf li a[href]")
    if not anchor_candidates:
        anchor_candidates = soup.select("ul li a[href]")

    for anchor in anchor_candidates:
        parent_ul = anchor.find_parent("ul")
        if parent_ul and "pagination" in (parent_ul.get("class") or []):
            continue
        href = anchor.get("href")
        if not href or href.strip().startswith("javascript"):
            continue
        title = _normalize_text(html.unescape(anchor.get_text(" ", strip=True)))
        if not title:
            continue
        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)
        chapters.append({"title": title, "url": url})

    return chapters


def _chapters_to_html_paragraphs(parts: list[str]) -> str | None:
    lines: list[str] = []
    for part in parts:
        for segment in re.split(r"\r?\n+", part):
            cleaned = segment.strip()
            if cleaned:
                lines.append(f"<p>{html.escape(cleaned)}</p>")
    return "".join(lines) if lines else None


def parse_chapter_content(html_content: str) -> dict[str, str | None] | None:
    soup = BeautifulSoup(html_content, _DEFAULT_PARSER)

    title_tag = soup.select_one("div.chapter h2") or soup.select_one("h2") or soup.select_one("div.chapter h1")
    title = _normalize_text(html.unescape(title_tag.get_text(" ", strip=True))) if title_tag else None

    content_container = soup.select_one("div.chapter-c-content")
    if content_container:
        for selector in [
            ".left-control",
            ".panel-box",
            ".panel-setting",
            ".panel-catalog",
            ".chapter-comment",
            "script",
            "style",
        ]:
            for tag in content_container.select(selector):
                tag.decompose()

    parts: list[str] = []
    if content_container:
        for box in content_container.select("div.box-chap"):
            text = box.get_text("\n", strip=True)
            if text:
                parts.append(text)
        if not parts:
            text = content_container.get_text("\n", strip=True)
            if text:
                parts.append(text)

    content_html = _chapters_to_html_paragraphs(parts) if parts else None

    if not content_html:
        fallback = soup.select_one("div#chapter-content, div.chapter-content")
        if fallback:
            text = fallback.get_text("\n", strip=True)
            if text:
                content_html = _chapters_to_html_paragraphs([text])

    return {"title": title, "content": content_html}
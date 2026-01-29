#!/usr/bin/env python3
"""
Script khám phá cấu trúc site metruyenful.com để tạo adapter mới.

Script này sẽ:
1. Fetch homepage và phân tích genre list
2. Fetch genre page và phân tích story list + pagination
3. Fetch story detail và phân tích chapter list + metadata
4. Fetch chapter page và phân tích content
5. Output kết quả dưới dạng JSON để AI agents sử dụng

Usage:
    python scripts/explore_metruyenful.py [--output result.json]
"""

import argparse
import asyncio
import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


class MetruyenfulExplorer:
    """Explore metruyenful.com structure"""

    def __init__(self, base_url: str = "https://metruyenful.com"):
        self.base_url = base_url
        self.client: httpx.AsyncClient | None = None
        self.results: dict[str, Any] = {
            "site_key": "metruyenful",
            "base_url": base_url,
            "exploration_results": {},
        }

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def fetch_html(self, url: str) -> tuple[str, str]:
        """Fetch HTML from URL and return (html, final_url)"""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async context manager.")

        response = await self.client.get(url)
        response.raise_for_status()
        return response.text, str(response.url)

    def extract_genres(self, html: str) -> list[dict[str, str]]:
        """Extract genre list from homepage"""
        soup = BeautifulSoup(html, 'html.parser')
        genres = []

        # Strategy 1: Look for genre dropdown menu (metruyenful specific)
        # Based on HTML: <div class="dropdown-menu multi-column"> with genre links
        genre_dropdowns = soup.select('.dropdown-menu a, .multi-column a')

        seen_urls = set()
        for link in genre_dropdowns:
            href = link.get('href', '')
            text = link.get_text(strip=True)

            # Filter out non-genre links (Truyện mới, Truyện hot, Blog, etc.)
            skip_keywords = ['truyện mới', 'truyện hot', 'truyện convert', 'truyện full', 'blog', 'home']
            if text and any(keyword in text.lower() for keyword in skip_keywords):
                continue

            # Accept genre URLs (typically single-word slugs or genre names)
            if href and text and len(text) < 30:  # Genre names are typically short
                full_url = urljoin(self.base_url, href)

                if full_url not in seen_urls and full_url != self.base_url:
                    seen_urls.add(full_url)
                    genres.append({
                        'name': text,
                        'url': full_url,
                        'slug': href.strip('/').split('/')[-1]
                    })

        # Strategy 2: Look for inline JavaScript genres data
        script_tags = soup.find_all('script')
        for script in script_tags:
            script_text = script.string or ''
            if 'genres' in script_text or 'categories' in script_text:
                # Try to extract JSON object
                match = re.search(r'(?:genres|categories)\s*=\s*(\{[^}]+\}|\[[^\]]+\])', script_text)
                if match:
                    try:
                        genres_data = match.group(1)
                        print(f"Found inline genres data: {genres_data[:200]}...")
                    except Exception as e:
                        print(f"Could not parse inline genres: {e}")

        return genres

    def extract_story_list(self, html: str, base_url: str) -> tuple[list[dict[str, str]], dict[str, Any]]:
        """Extract story list and pagination info from genre page"""
        soup = BeautifulSoup(html, 'html.parser')
        stories = []
        pagination_info = {}

        # Try multiple story container patterns
        story_containers = (
            soup.select('.story-list .story-item') or
            soup.select('.list .row') or
            soup.select('.story-grid .item') or
            soup.select('.truyen-list .truyen-item') or
            soup.select('article, .post')
        )

        for container in story_containers:
            # Find story link
            link = container.select_one('a[href*="/truyen"], a.story-link, h3 a, h2 a, a')
            if not link:
                continue

            href = link.get('href', '')
            title = link.get_text(strip=True) or link.get('title', '').strip()

            if not href or not title:
                continue

            full_url = urljoin(base_url, href)

            # Try to extract story ID from URL
            # Common patterns: /truyen-123, /story-123, /123/, .123/
            story_id = None
            for pattern in [r'/truyen-(\d+)', r'/story-(\d+)', r'/(\d+)/', r'\.(\d+)/?$']:
                match = re.search(pattern, href)
                if match:
                    story_id = match.group(1)
                    break

            # Find additional info
            author = None
            chapter_count = None
            status = None

            # Look for author
            author_elem = container.select_one('.author, .story-author, [class*="author"]')
            if author_elem:
                author = author_elem.get_text(strip=True)

            # Look for chapter count
            chapter_elem = container.select_one('.chapter-count, .chapters, [class*="chapter"]')
            if chapter_elem:
                chapter_text = chapter_elem.get_text(strip=True)
                match = re.search(r'(\d+)', chapter_text)
                if match:
                    chapter_count = match.group(1)

            # Look for status
            status_indicators = {
                'full': container.select_one('.label-full, .status-full, [class*="full"]'),
                'hot': container.select_one('.label-hot, .status-hot, [class*="hot"]'),
                'new': container.select_one('.label-new, .status-new, [class*="new"]'),
            }
            for status_type, elem in status_indicators.items():
                if elem:
                    status = status_type
                    break

            stories.append({
                'title': title,
                'url': full_url,
                'story_id': story_id,
                'author': author,
                'chapter_count': chapter_count,
                'status': status,
            })

        # Extract pagination
        pagination_links = soup.select('.pagination a, .paging a, a[href*="page"]')
        pages = []

        for link in pagination_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)

            # Try multiple pagination patterns
            for pattern in [r'[?&]page=(\d+)', r'/page-(\d+)', r'/trang-(\d+)', r'/p(\d+)']:
                match = re.search(pattern, href)
                if match:
                    pages.append(int(match.group(1)))
                    break

            # Also try to extract from link text
            if text.isdigit():
                pages.append(int(text))

        if pages:
            pagination_info = {
                'max_page': max(pages),
                'has_pagination': True,
                'detected_patterns': list(set(re.findall(r'page[=-]?\d+|trang-\d+|p\d+', ' '.join([l.get('href', '') for l in pagination_links])))),
            }
        else:
            pagination_info = {
                'max_page': 1,
                'has_pagination': False,
            }

        return stories, pagination_info

    def extract_story_details(self, html: str, base_url: str) -> dict[str, Any]:
        """Extract story metadata and chapter list from story detail page"""
        soup = BeautifulSoup(html, 'html.parser')
        details: dict[str, Any] = {}

        # Extract story ID and alias from inline JavaScript
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string or ''

            for pattern in [
                (r'storyID\s*=\s*["\']?(\d+)', 'story_id'),
                (r'story[_-]?id\s*[=:]\s*["\']?(\d+)', 'story_id'),
                (r'storyAlias\s*=\s*["\']([^"\']+)', 'story_alias'),
                (r'story[_-]?slug\s*[=:]\s*["\']([^"\']+)', 'story_alias'),
            ]:
                match = re.search(pattern[0], script_text)
                if match:
                    details[pattern[1]] = match.group(1)

        # Extract title
        title_selectors = ['h1', 'h2.title', '.story-title', '.book-title', '[itemprop="name"]']
        for selector in title_selectors:
            title_tag = soup.select_one(selector)
            if title_tag:
                details['title'] = title_tag.get_text(strip=True)
                break

        # Extract author
        author_patterns = [
            (soup.select_one('.author, .story-author, [itemprop="author"]'), None),
            (soup.find(string=re.compile(r'Tác giả|Author')), 'sibling'),
        ]
        for elem, mode in author_patterns:
            if elem:
                if mode == 'sibling':
                    parent = elem.parent
                    if parent:
                        author_text = parent.get_text(strip=True)
                        details['author'] = re.sub(r'Tác giả:?|Author:?', '', author_text).strip()
                else:
                    details['author'] = elem.get_text(strip=True)
                break

        # Extract genres
        genres = []
        genre_links = soup.select('a[href*="/the-loai/"], a[href*="/genre/"], a[href*="/truyen-"], .genre a, .category a')
        for link in genre_links:
            genre_text = link.get_text(strip=True)
            if genre_text and len(genre_text) < 50:
                genres.append(genre_text)
        details['genres'] = list(set(genres))

        # Extract status
        status_selectors = ['.status, .story-status', '[itemprop="bookStatus"]']
        for selector in status_selectors:
            status_tag = soup.select_one(selector)
            if status_tag:
                details['status'] = status_tag.get_text(strip=True)
                break

        # Extract cover image
        cover_selectors = ['img[src*="cover"]', 'img.story-cover', '.book-cover img', '[itemprop="image"]']
        for selector in cover_selectors:
            cover_img = soup.select_one(selector)
            if cover_img:
                details['cover_image'] = urljoin(base_url, cover_img.get('src', ''))
                break

        # Extract description
        desc_selectors = ['.story-desc', '.description', '[itemprop="description"]', '.summary', '.synopsis']
        for selector in desc_selectors:
            desc_container = soup.select_one(selector)
            if desc_container:
                details['description'] = desc_container.get_text(strip=True)
                break

        # Extract chapter list
        chapters = []
        chapter_links = soup.select('a[href*="/chuong"], a[href*="/chapter"], .chapter-list a, .list-chapter a')

        for link in chapter_links:
            href = link.get('href', '')
            chapter_title = link.get_text(strip=True)

            if not href or not chapter_title:
                continue

            full_url = urljoin(base_url, href)

            # Extract chapter number
            chapter_num = None
            for pattern in [r'/chuong-?(\d+)', r'/chapter-?(\d+)', r'/ch-?(\d+)', r'chuong[\s-]*(\d+)']:
                match = re.search(pattern, href.lower())
                if match:
                    chapter_num = match.group(1)
                    break

            chapters.append({
                'title': chapter_title,
                'url': full_url,
                'chapter_number': chapter_num,
            })

        details['chapters'] = chapters
        details['total_chapters'] = len(chapters)

        # Extract chapter list pagination
        chapter_pagination = soup.select('.chapter-pagination a, .list-chapter .pagination a')
        if chapter_pagination:
            pages = []
            for link in chapter_pagination:
                href = link.get('href', '')
                for pattern in [r'page[=-](\d+)', r'/p(\d+)', r'trang-(\d+)']:
                    match = re.search(pattern, href)
                    if match:
                        pages.append(int(match.group(1)))
                        break

            if pages:
                details['chapter_list_pagination'] = {
                    'max_page': max(pages),
                    'chapters_shown_per_page': len(chapters),
                }

        return details

    def extract_chapter_content(self, html: str) -> dict[str, Any]:
        """Extract chapter content and metadata"""
        soup = BeautifulSoup(html, 'html.parser')
        result: dict[str, Any] = {}

        # Extract chapter title
        title_selectors = ['h2', 'h1', '.chapter-title', '.chapter-name']
        for selector in title_selectors:
            title_tag = soup.select_one(selector)
            if title_tag:
                result['title'] = title_tag.get_text(strip=True)
                break

        # Extract breadcrumb
        breadcrumbs = soup.select('.breadcrumb a, nav a, .breadcrumbs a')
        if breadcrumbs:
            result['breadcrumb'] = [a.get_text(strip=True) for a in breadcrumbs]

        # Find main content area
        content_selectors = [
            '#chapter-content',
            '.chapter-content',
            '.content',
            '.chapter-body',
            '.reading-content',
            'div[id*="content"]',
            'div[class*="content"]',
        ]

        content_div = None
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                result['content_selector'] = selector
                break

        if not content_div:
            # Fallback: Find largest div with text
            all_divs = soup.find_all('div')
            max_text_len = 0
            for div in all_divs:
                text = div.get_text(strip=True)
                if len(text) > max_text_len:
                    max_text_len = len(text)
                    content_div = div
            if content_div:
                result['content_selector'] = 'largest_div_fallback'

        if content_div:
            # Remove ads
            for ad in content_div.select('[class*="ads"], [id*="ads"], .adsbygoogle, script, style, iframe'):
                ad.decompose()

            # Get clean text
            content_text = content_div.get_text(separator='\n', strip=True)
            result['content'] = content_text
            result['content_length'] = len(content_text)
            result['content_html_structure'] = str(content_div)[:500]

        # Extract navigation
        nav_links = {}
        nav_patterns = [
            ('prev', ['a[href*="chuong"]:has-text("trước")', 'a.prev-chapter', '.prev a']),
            ('next', ['a[href*="chuong"]:has-text("tiếp")', 'a.next-chapter', '.next a']),
        ]

        for nav_type, selectors in nav_patterns:
            for selector in selectors:
                try:
                    link = soup.select_one(selector)
                    if link:
                        nav_links[nav_type] = link.get('href', '')
                        break
                except:
                    pass

        result['navigation'] = nav_links

        return result

    async def explore_full_site(self) -> dict[str, Any]:
        """Run full exploration of the site"""

        print(f"\n{'='*60}")
        print(f"EXPLORING: {self.base_url}")
        print(f"{'='*60}\n")

        # Step 1: Explore homepage for genres
        print("Step 1: Fetching homepage to extract genres...")
        try:
            homepage_html, _ = await self.fetch_html(self.base_url)
            genres = self.extract_genres(homepage_html)

            self.results['exploration_results']['genres'] = {
                'count': len(genres),
                'list': genres[:10],  # First 10 for review
                'sample': genres[0] if genres else None,
            }
            print(f"  ✓ Found {len(genres)} genres")
        except Exception as e:
            print(f"  ✗ Error fetching homepage: {e}")
            self.results['exploration_results']['genres'] = {'error': str(e)}
            genres = []

        # Step 2: Explore a genre page
        if genres:
            sample_genre = genres[0]
            genre_url = sample_genre['url']

            print(f"\nStep 2: Fetching genre page: {sample_genre['name']}...")
            try:
                genre_html, _ = await self.fetch_html(genre_url)
                stories, pagination = self.extract_story_list(genre_html, self.base_url)

                self.results['exploration_results']['genre_page'] = {
                    'genre_name': sample_genre['name'],
                    'genre_url': genre_url,
                    'stories_found': len(stories),
                    'stories_sample': stories[:5],  # First 5
                    'pagination': pagination,
                }
                print(f"  ✓ Found {len(stories)} stories")
                print(f"  ✓ Pagination: {pagination}")
            except Exception as e:
                print(f"  ✗ Error fetching genre page: {e}")
                self.results['exploration_results']['genre_page'] = {'error': str(e)}
                stories = []

            # Step 3: Explore a story detail page
            if stories:
                sample_story = stories[0]
                story_url = sample_story['url']

                print(f"\nStep 3: Fetching story page: {sample_story['title']}...")
                try:
                    story_html, _ = await self.fetch_html(story_url)
                    story_details = self.extract_story_details(story_html, self.base_url)

                    self.results['exploration_results']['story_page'] = {
                        'story_url': story_url,
                        'story_title': sample_story['title'],
                        'metadata': {
                            k: v for k, v in story_details.items()
                            if k not in ['chapters', 'description']
                        },
                        'chapters_found': story_details.get('total_chapters', 0),
                        'chapters_sample': story_details.get('chapters', [])[:5],
                    }
                    print(f"  ✓ Story ID: {story_details.get('story_id')}")
                    print(f"  ✓ Chapters: {story_details.get('total_chapters', 0)}")
                except Exception as e:
                    print(f"  ✗ Error fetching story page: {e}")
                    self.results['exploration_results']['story_page'] = {'error': str(e)}
                    story_details = {}

                # Step 4: Explore a chapter page
                chapters = story_details.get('chapters', [])
                if chapters:
                    sample_chapter = chapters[0]
                    chapter_url = sample_chapter['url']

                    print(f"\nStep 4: Fetching chapter: {sample_chapter['title']}...")
                    try:
                        chapter_html, _ = await self.fetch_html(chapter_url)
                        chapter_content = self.extract_chapter_content(chapter_html)

                        self.results['exploration_results']['chapter_page'] = {
                            'chapter_url': chapter_url,
                            'chapter_title': sample_chapter['title'],
                            'content_length': chapter_content.get('content_length', 0),
                            'has_content': bool(chapter_content.get('content')),
                            'content_selector': chapter_content.get('content_selector'),
                            'navigation': chapter_content.get('navigation', {}),
                            'breadcrumb': chapter_content.get('breadcrumb', []),
                        }
                        print(f"  ✓ Content length: {chapter_content.get('content_length', 0)} chars")
                        print(f"  ✓ Content selector: {chapter_content.get('content_selector')}")
                        print(f"  ✓ Has navigation: {bool(chapter_content.get('navigation'))}")
                    except Exception as e:
                        print(f"  ✗ Error fetching chapter page: {e}")
                        self.results['exploration_results']['chapter_page'] = {'error': str(e)}

        print(f"\n{'='*60}")
        print("EXPLORATION COMPLETE!")
        print(f"{'='*60}\n")

        return self.results


async def main():
    parser = argparse.ArgumentParser(description="Explore metruyenful.com structure")
    parser.add_argument(
        '--output',
        type=str,
        default='metruyenful_exploration.json',
        help='Output JSON file path'
    )
    parser.add_argument(
        '--base-url',
        type=str,
        default='https://metruyenful.com',
        help='Base URL of the site'
    )

    args = parser.parse_args()

    async with MetruyenfulExplorer(args.base_url) as explorer:
        results = await explorer.explore_full_site()

        # Save to file
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"\n✓ Results saved to: {args.output}")
        print("\nSummary:")
        genres_info = results['exploration_results'].get('genres', {})
        genre_page_info = results['exploration_results'].get('genre_page', {})
        story_page_info = results['exploration_results'].get('story_page', {})

        print(f"  - Genres: {genres_info.get('count', 0)}")
        print(f"  - Stories sampled: {genre_page_info.get('stories_found', 0)}")
        print(f"  - Chapters sampled: {story_page_info.get('chapters_found', 0)}")


if __name__ == '__main__':
    asyncio.run(main())

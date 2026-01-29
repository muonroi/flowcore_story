#!/usr/bin/env python3
"""
Script khám phá cấu trúc site truyenfullmoi.com để tạo adapter mới.

Script này sẽ:
1. Fetch homepage và phân tích genre list
2. Fetch genre page và phân tích story list + pagination
3. Fetch story detail và phân tích chapter list + metadata
4. Fetch chapter page và phân tích content
5. Output kết quả dưới dạng JSON để AI agents sử dụng

Usage:
    python scripts/explore_truyenfullmoi.py [--output result.json]
"""

import argparse
import asyncio
import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


class TruyenFullMoiExplorer:
    """Explore truyenfullmoi.com structure"""

    def __init__(self, base_url: str = "https://truyenfullmoi.com"):
        self.base_url = base_url
        self.client: httpx.AsyncClient | None = None
        self.results: dict[str, Any] = {
            "site_key": "truyenfullmoi",
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

        # Strategy 1: Look for genre menu/navigation
        # Common patterns: .genre-menu, .category-list, nav with genre links
        genre_containers = soup.select('nav a[href*="/truyen-"]')

        for link in genre_containers:
            href = link.get('href', '')
            text = link.get_text(strip=True)

            if href and text and '/truyen-' in href:
                full_url = urljoin(self.base_url, href)
                genres.append({
                    'name': text,
                    'url': full_url,
                    'slug': href.strip('/').split('/')[-1]
                })

        # Strategy 2: Look for inline JavaScript genres data
        # Pattern: genres = {...}
        script_tags = soup.find_all('script')
        for script in script_tags:
            script_text = script.string or ''
            if 'genres' in script_text and '{' in script_text:
                # Try to extract JSON object
                match = re.search(r'genres\s*=\s*(\{[^}]+\})', script_text)
                if match:
                    try:
                        # This is a simplified extraction, may need refinement
                        genres_json = match.group(1)
                        print(f"Found inline genres data: {genres_json[:200]}...")
                    except Exception as e:
                        print(f"Could not parse inline genres: {e}")

        return genres

    def extract_story_list(self, html: str, base_url: str) -> tuple[list[dict[str, str]], dict[str, Any]]:
        """Extract story list and pagination info from genre page"""
        soup = BeautifulSoup(html, 'html.parser')
        stories = []
        pagination_info = {}

        # Find story containers
        # Based on analysis: .list .row structure
        story_rows = soup.select('.list .row')

        for row in story_rows:
            # Find story link
            link = row.select_one('a[href*="/"][href$="/"]')
            if not link:
                continue

            href = link.get('href', '')
            title = link.get_text(strip=True)

            if not href or not title:
                continue

            full_url = urljoin(base_url, href)

            # Extract story ID from URL pattern: /slug.ID/
            story_id = None
            match = re.search(r'\.(\d+)/?$', href)
            if match:
                story_id = match.group(1)

            # Find additional info
            author = None
            chapter_count = None
            status = None

            # Look for author, chapter count, status in row
            text_content = row.get_text()

            # Status labels
            if row.select_one('.label-full'):
                status = 'full'
            elif row.select_one('.label-hot'):
                status = 'hot'
            elif row.select_one('.label-new'):
                status = 'new'

            stories.append({
                'title': title,
                'url': full_url,
                'story_id': story_id,
                'author': author,
                'chapter_count': chapter_count,
                'status': status,
            })

        # Extract pagination
        # Pattern: /truyen-{slug}/trang-{n}/
        pagination_links = soup.select('a[href*="/trang-"]')
        pages = []

        for link in pagination_links:
            href = link.get('href', '')
            match = re.search(r'/trang-(\d+)/', href)
            if match:
                pages.append(int(match.group(1)))

        if pages:
            pagination_info = {
                'max_page': max(pages),
                'has_pagination': True,
                'pattern': '/trang-{page}/',
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
        # Pattern: storyID=1779;storyAlias='van-co-de-nhat-than'
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.string or ''

            story_id_match = re.search(r'storyID\s*=\s*(\d+)', script_text)
            if story_id_match:
                details['story_id'] = story_id_match.group(1)

            alias_match = re.search(r"storyAlias\s*=\s*['\"]([^'\"]+)['\"]", script_text)
            if alias_match:
                details['story_alias'] = alias_match.group(1)

        # Extract title
        title_tag = soup.select_one('h1, h2.title, .story-title')
        if title_tag:
            details['title'] = title_tag.get_text(strip=True)

        # Extract author
        author_tag = soup.find(string=re.compile(r'Tác giả|Author'))
        if author_tag:
            author_parent = author_tag.parent
            if author_parent:
                # Get next sibling or value
                author_text = author_parent.get_text(strip=True)
                details['author'] = author_text.replace('Tác giả:', '').strip()

        # Extract genres
        genres = []
        genre_links = soup.select('a[href*="/truyen-"]')
        for link in genre_links:
            genre_text = link.get_text(strip=True)
            if genre_text and len(genre_text) < 50:  # Avoid long text
                genres.append(genre_text)
        details['genres'] = list(set(genres))  # Remove duplicates

        # Extract status
        status_tag = soup.find(string=re.compile(r'Tình trạng|Status|Đang ra|Hoàn thành'))
        if status_tag:
            details['status'] = status_tag.strip()

        # Extract cover image
        cover_img = soup.select_one('img[src*="cover"]')
        if cover_img:
            details['cover_image'] = urljoin(base_url, cover_img.get('src', ''))

        # Extract description
        desc_container = soup.select_one('.story-desc, .description, [itemprop="description"]')
        if desc_container:
            details['description'] = desc_container.get_text(strip=True)

        # Extract chapter list
        chapters = []
        chapter_links = soup.select('a[href*="/chuong-"]')

        for link in chapter_links:
            href = link.get('href', '')
            chapter_title = link.get_text(strip=True)

            if not href or not chapter_title:
                continue

            full_url = urljoin(base_url, href)

            # Extract chapter number from URL or title
            chapter_num = None
            match = re.search(r'/chuong-(\d+)', href)
            if match:
                chapter_num = match.group(1)

            chapters.append({
                'title': chapter_title,
                'url': full_url,
                'chapter_number': chapter_num,
            })

        details['chapters'] = chapters
        details['total_chapters'] = len(chapters)

        # Extract chapter list pagination
        chapter_pagination = soup.select('.pagination a, .paging a')
        if chapter_pagination:
            pages = []
            for link in chapter_pagination:
                href = link.get('href', '')
                # Pattern may vary: ?page=2, /page-2/, etc.
                match = re.search(r'page[=-](\d+)', href)
                if match:
                    pages.append(int(match.group(1)))

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
        title_tag = soup.select_one('h2, h1, .chapter-title')
        if title_tag:
            result['title'] = title_tag.get_text(strip=True)

        # Extract breadcrumb for story info
        breadcrumbs = soup.select('.breadcrumb a, nav a')
        if breadcrumbs:
            result['breadcrumb'] = [a.get_text(strip=True) for a in breadcrumbs]

        # Find main content area
        # Strategy: Look for large text blocks, exclude ads

        # Try common content selectors
        content_selectors = [
            '#chapter-content',
            '.chapter-content',
            '.content',
            'div[id*="content"]',
            'div[class*="content"]',
        ]

        content_div = None
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
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
            # Remove ads
            for ad in content_div.select('[class*="ads"], [id*="ads"], .adsbygoogle'):
                ad.decompose()

            # Get clean text
            content_text = content_div.get_text(separator='\n', strip=True)
            result['content'] = content_text
            result['content_length'] = len(content_text)
            result['content_html_structure'] = str(content_div)[:500]  # First 500 chars

        # Extract navigation
        nav_links = {}
        prev_link = soup.select_one('a[href*="chuong-"]:has-text("trước"), a.prev-chapter')
        next_link = soup.select_one('a[href*="chuong-"]:has-text("tiếp"), a.next-chapter')

        if prev_link:
            nav_links['prev'] = prev_link.get('href', '')
        if next_link:
            nav_links['next'] = next_link.get('href', '')

        result['navigation'] = nav_links

        return result

    async def explore_full_site(self) -> dict[str, Any]:
        """Run full exploration of the site"""

        print(f"\n{'='*60}")
        print(f"EXPLORING: {self.base_url}")
        print(f"{'='*60}\n")

        # Step 1: Explore homepage for genres
        print("Step 1: Fetching homepage to extract genres...")
        homepage_html, _ = await self.fetch_html(self.base_url)
        genres = self.extract_genres(homepage_html)

        self.results['exploration_results']['genres'] = {
            'count': len(genres),
            'list': genres[:5],  # First 5 for brevity
            'sample': genres[0] if genres else None,
        }
        print(f"  ✓ Found {len(genres)} genres")

        # Step 2: Explore a genre page
        if genres:
            sample_genre = genres[0]
            genre_url = sample_genre['url']

            print(f"\nStep 2: Fetching genre page: {sample_genre['name']}...")
            genre_html, _ = await self.fetch_html(genre_url)
            stories, pagination = self.extract_story_list(genre_html, self.base_url)

            self.results['exploration_results']['genre_page'] = {
                'genre_name': sample_genre['name'],
                'genre_url': genre_url,
                'stories_found': len(stories),
                'stories_sample': stories[:3],  # First 3
                'pagination': pagination,
            }
            print(f"  ✓ Found {len(stories)} stories")
            print(f"  ✓ Pagination: {pagination}")

            # Step 3: Explore a story detail page
            if stories:
                sample_story = stories[0]
                story_url = sample_story['url']

                print(f"\nStep 3: Fetching story page: {sample_story['title']}...")
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
                    'chapters_sample': story_details.get('chapters', [])[:3],
                }
                print(f"  ✓ Story ID: {story_details.get('story_id')}")
                print(f"  ✓ Chapters: {story_details.get('total_chapters', 0)}")

                # Step 4: Explore a chapter page
                chapters = story_details.get('chapters', [])
                if chapters:
                    sample_chapter = chapters[0]
                    chapter_url = sample_chapter['url']

                    print(f"\nStep 4: Fetching chapter: {sample_chapter['title']}...")
                    chapter_html, _ = await self.fetch_html(chapter_url)
                    chapter_content = self.extract_chapter_content(chapter_html)

                    self.results['exploration_results']['chapter_page'] = {
                        'chapter_url': chapter_url,
                        'chapter_title': sample_chapter['title'],
                        'content_length': chapter_content.get('content_length', 0),
                        'has_content': bool(chapter_content.get('content')),
                        'navigation': chapter_content.get('navigation', {}),
                        'breadcrumb': chapter_content.get('breadcrumb', []),
                    }
                    print(f"  ✓ Content length: {chapter_content.get('content_length', 0)} chars")
                    print(f"  ✓ Has navigation: {bool(chapter_content.get('navigation'))}")

        print(f"\n{'='*60}")
        print("EXPLORATION COMPLETE!")
        print(f"{'='*60}\n")

        return self.results


async def main():
    parser = argparse.ArgumentParser(description="Explore truyenfullmoi.com structure")
    parser.add_argument(
        '--output',
        type=str,
        default='truyenfullmoi_exploration.json',
        help='Output JSON file path'
    )
    parser.add_argument(
        '--base-url',
        type=str,
        default='https://truyenfullmoi.com',
        help='Base URL of the site'
    )

    args = parser.parse_args()

    async with TruyenFullMoiExplorer(args.base_url) as explorer:
        results = await explorer.explore_full_site()

        # Save to file
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"\n✓ Results saved to: {args.output}")
        print("\nSummary:")
        print(f"  - Genres: {results['exploration_results'].get('genres', {}).get('count', 0)}")
        print(f"  - Stories sampled: {results['exploration_results'].get('genre_page', {}).get('stories_found', 0)}")
        print(f"  - Chapters sampled: {results['exploration_results'].get('story_page', {}).get('chapters_found', 0)}")


if __name__ == '__main__':
    asyncio.run(main())

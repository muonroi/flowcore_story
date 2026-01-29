#!/usr/bin/env python3
"""Debug chapter HTML structure"""
import asyncio
from flowcore_story.apps.scraper import make_request
from bs4 import BeautifulSoup

async def main():
    url = "https://quykiep.com/truyen/lang-thien-kiem-de/chuong-1"

    response = await make_request(url, site_key="quykiep", method="GET")

    if not response or not response.text:
        print("Failed to fetch")
        return

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    # Save full HTML
    with open('/tmp/chapter_full.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("Saved full HTML to /tmp/chapter_full.html")

    # Check article tag
    article = soup.select_one('article')
    if article:
        print(f"\narticle tag found, children:")
        for i, child in enumerate(article.children):
            if hasattr(child, 'name') and child.name:
                text = child.get_text(strip=True)[:50] if child.get_text(strip=True) else ""
                print(f"  [{i}] <{child.name}> class={child.get('class')} | {text}...")

    # Look for specific content divs
    print("\n--- Looking for content containers ---")
    selectors = [
        'div.prose', 'div.content', 'div.chapter-content',
        'div#chapter-content', 'div.reading-content',
        'div[class*="content"]', 'main article div'
    ]

    for sel in selectors:
        elems = soup.select(sel)
        if elems:
            print(f"\n{sel}: found {len(elems)}")
            for e in elems[:2]:
                text = e.get_text(strip=True)[:100]
                print(f"  class={e.get('class')} | {text}...")

if __name__ == "__main__":
    asyncio.run(main())

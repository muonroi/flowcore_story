import asyncio

from flowcore_story.adapters.xtruyen_adapter import XTruyenAdapter
from flowcore_story.apps.scraper import close_playwright, initialize_scraper
from flowcore_story.utils.challenge_harvester_client import close_challenge_harvester_client


async def main() -> None:
    url = "https://xtruyen.vn/truyen/de-vuong-truy-sung-hoang-hau-song-lai/chuong-75/"
    chapter_title = "Chuong 75"
    adapter = XTruyenAdapter()

    await initialize_scraper(adapter.site_key)
    try:
        content = await adapter.get_chapter_content(url, chapter_title, adapter.site_key)

        if content is None:
            print("RESULT: None (retryable / anti-bot / parse failure)")
        elif content == "":
            print("RESULT: EMPTY (verified)")
        else:
            snippet = content[:160].replace("\n", " ").strip()
            print(f"RESULT: OK length={len(content)} snippet='{snippet}'")
    finally:
        await close_challenge_harvester_client()
        await close_playwright()


if __name__ == "__main__":
    asyncio.run(main())

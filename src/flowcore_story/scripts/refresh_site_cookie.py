import argparse
import asyncio

from flowcore_story.apps.scraper import refresh_site_cookie
from flowcore_story.utils.cookie_manager import get_cookie_header
from flowcore_story.utils.logger import logger


async def _run(
    site_key: str,
    url: str,
    wait_for_selector: str | None,
    close_after: bool,
) -> None:
    header = await refresh_site_cookie(
        site_key,
        url,
        wait_for_selector=wait_for_selector,
        close_after=close_after,
    )
    if header:
        print(header)
    else:
        logger.warning("No cookies captured for %s; check URL or challenge status.", site_key)
        current = get_cookie_header(site_key)
        if current:
            print(current)


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh site cookies via Playwright.")
    parser.add_argument("site_key", help="Site key configured in StoryFlow (e.g. xtruyen).")
    parser.add_argument("url", help="URL to open for refreshing cookies.")
    parser.add_argument(
        "--wait-for-selector",
        dest="wait_for_selector",
        default=None,
        help="Optional CSS selector to wait for after navigation.",
    )
    parser.add_argument(
        "--close",
        action="store_true",
        help="Close the Playwright browser after refreshing cookies.",
    )

    args = parser.parse_args()
    asyncio.run(
        _run(
            args.site_key,
            args.url,
            args.wait_for_selector,
            close_after=args.close,
        )
    )


if __name__ == "__main__":
    main()


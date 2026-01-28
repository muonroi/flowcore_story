from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar
import logging

logger = logging.getLogger(__name__)

class BaseSiteAdapter(ABC):
    """Base contract that all site adapters must follow."""

    #: Identifier used by the plugin loader to register the adapter.
    site_key: ClassVar[str]

    @classmethod
    def get_site_key(cls) -> str:
        """Return the declared ``site_key`` for the adapter."""

        key = getattr(cls, "site_key", "")
        if not isinstance(key, str) or not key:
            raise NotImplementedError(
                f"Adapter {cls.__name__} must define a non-empty `site_key` class attribute."
            )
        return key

    @abstractmethod
    async def get_genres(self):
        pass

    @abstractmethod
    async def get_stories_in_genre(self, genre_url, page=1):
        pass

    @abstractmethod
    async def get_all_stories_from_genre(self, genre_name, genre_url, max_pages=None):
        pass

    @abstractmethod
    async def get_story_details(self, story_url, story_title):
        pass

    @abstractmethod
    async def get_chapter_list(
        self,
        story_url,
        story_title,
        site_key,
        max_pages=None,
        total_chapters=None,
        max_batches=None,
    ):
        pass

    @abstractmethod
    async def get_chapter_content(self, chapter_url, chapter_title, site_key):
        pass

    @abstractmethod
    def extract_chapter_list(
        self, html: str, base_url: str | None = None
    ) -> list[dict[str, str]]:
        """Parse chapter list from raw HTML without fetching."""
        raise NotImplementedError(
            f"Adapter {self.__class__.__name__} must implement extract_chapter_list for sticky worker support."
        )

    @abstractmethod
    def extract_chapter_content(self, html: str, base_url: str | None = None) -> str | None:
        """Parse chapter content from raw HTML without fetching."""
        raise NotImplementedError(
            f"Adapter {self.__class__.__name__} must implement extract_chapter_content for sticky worker support."
        )

    @abstractmethod
    async def get_all_stories_from_genre_with_page_check(
        self,
        genre_name,
        genre_url,
        site_key,
        max_pages=None,
        *,
        page_callback: Callable[[list[dict[str, Any]], int, int | None], Awaitable[None]] | None = None,
        collect: bool = True,
    ):
        pass

    def get_chapters_per_page_hint(self) -> int:
        """Return an estimated number of chapters per page for chapter listings.

        Adapters can override this value to provide a more accurate hint that will
        be used when applying generic crawl limits.
        """
        return 100

    async def fetch_html(
        self,
        url: str,
        method: str = "GET",
        wait_for_selector: str | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout: int = 30,
        **kwargs
    ) -> str | None:
        """Standard method to fetch HTML content using shared infrastructure."""
        from flowcore.apps.scraper import make_request
        try:
            response = await make_request(
                url,
                site_key=self.get_site_key(),
                method=method,
                wait_for_selector=wait_for_selector,
                extra_headers=extra_headers,
                timeout=timeout,
                **kwargs
            )
            if response and getattr(response, "text", None):
                return response.text
        except Exception as e:
            logger.warning(f"[{self.get_site_key()}] Error in fetch_html for {url}: {e}")
        return None

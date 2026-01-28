from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class WorkerSettings:
    """Runtime configuration for crawl workers.

    The previous implementation stored these attributes on a simple class with an
    ``__init__`` method defined inside ``main.py``. Moving the configuration to a
    dedicated module keeps ``main.py`` focused on orchestration logic while also
    making the settings container easier to import in isolation for tests.
    """

    genre_batch_size: int
    genre_async_limit: int
    proxies_file: str
    failed_genres_file: str
    retry_genre_round_limit: int
    retry_sleep_seconds: int

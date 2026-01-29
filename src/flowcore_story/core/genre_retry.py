from __future__ import annotations

import asyncio
import json
import os
import random
from collections.abc import Awaitable, Callable
from typing import Any

from flowcore_story.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.utils.batch_utils import smart_delay
from flowcore_story.utils.logger import logger

ProcessGenreCallable = Callable[
    [
        Any,
        dict[str, Any],
        dict[str, Any],
        BaseSiteAdapter,
        str,
    ],
    Awaitable[None],
]


async def retry_failed_genres(
    adapter: BaseSiteAdapter,
    site_key: str,
    settings,
    shuffle_func: Callable[[], None],
    process_genre: ProcessGenreCallable,
    *,
    client_session_factory: Callable[[], Any] | None = None,
) -> None:
    """Retry failed genres by delegating processing to a callback.

    The logic used to live inside ``main.py`` and imported ``process_genre_with_limit``
    dynamically, which caused circular imports when ``retry_failed_genres`` was used
    from other modules (for example ``kafka_dispatcher``). Extracting the logic here
    keeps retry behaviour isolated and removes the dynamic import.
    """

    round_idx = 0
    while True:
        if not os.path.exists(settings.failed_genres_file):
            break
        with open(settings.failed_genres_file, encoding="utf-8") as f:
            failed_genres = json.load(f)
        if not failed_genres:
            break

        round_idx += 1
        logger.warning(
            "=== [RETRY ROUND %s] Đang retry %s thể loại bị fail... ===",
            round_idx,
            len(failed_genres),
        )

        to_remove = []
        random.shuffle(failed_genres)

        if client_session_factory is None:
            try:
                import aiohttp  # type: ignore
            except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
                raise RuntimeError(
                    "retry_failed_genres requires an aiohttp-like ClientSession"
                ) from exc
            client_session_factory = aiohttp.ClientSession

        async with client_session_factory() as session:
            for genre in failed_genres:
                delay = min(60, 5 * (2 ** genre.get("fail_count", 1)))
                await smart_delay(delay)
                try:
                    await process_genre(session, genre, {}, adapter, site_key)
                except Exception as ex:  # pragma: no cover - defensive logging
                    genre["fail_count"] = genre.get("fail_count", 1) + 1
                    logger.error(
                        "[RETRY] Vẫn lỗi genre: %s: %s",
                        genre.get("name"),
                        ex,
                    )
                else:
                    to_remove.append(genre)
                    logger.info(
                        "[RETRY] Thành công genre: %s",
                        genre.get("name"),
                    )

        if to_remove:
            failed_genres = [g for g in failed_genres if g not in to_remove]
            with open(settings.failed_genres_file, "w", encoding="utf-8") as f:
                json.dump(failed_genres, f, ensure_ascii=False, indent=4)

        if failed_genres:
            if round_idx < settings.retry_genre_round_limit:
                shuffle_func()
                logger.warning(
                    "Còn %s genre fail, bắt đầu vòng retry tiếp theo...",
                    len(failed_genres),
                )
                continue
            shuffle_func()
            genre_names = ", ".join([g.get("name", "unknown") for g in failed_genres])
            logger.error(
                "Sleep %s phút rồi retry lại các genre fail: %s",
                settings.retry_sleep_seconds // 60,
                genre_names,
            )
            await asyncio.sleep(settings.retry_sleep_seconds)
            round_idx = 0
            continue
        logger.info("Tất cả genre fail đã retry thành công.")
        break

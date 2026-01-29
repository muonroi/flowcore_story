"""Utilities for coordinating multi-source story crawling.

This module centralises the logic that was previously scattered between
``main.py`` and ``workers/crawler_missing_chapter.py``. The goal is to make the
behaviour around iterating story sources, loading adapters and invoking the
chapter pipeline consistent for both the full crawl and the missing chapter
crawl flows.

The :class:`MultiSourceStoryCrawler` class exposes a single entry point
(:meth:`crawl_story_until_complete`) which mirrors the previous
``crawl_all_sources_until_full`` function while keeping the implementation
encapsulated and easier to test.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from flowcore_story.adapters.base_site_adapter import BaseSiteAdapter
from flowcore_story.adapters.factory import get_adapter
from flowcore_story.apps.scraper import initialize_scraper
from flowcore_story.config import config as app_config
from flowcore_story.utils.batch_utils import smart_delay
from flowcore_story.utils.chapter_utils import (
    count_dead_chapters,
    crawl_missing_chapters_for_story,
    get_saved_chapters_files,
)
from flowcore_story.utils.domain_rate_limiter import domain_circuit_breaker
from flowcore_story.utils.domain_utils import get_site_key_from_url
from flowcore_story.utils.errors import CrawlError, classify_crawl_exception, is_retryable_error
from flowcore_story.utils.health_monitor import site_health_monitor
from flowcore_story.utils.logger import (
    anti_bot_logger,
    chapter_error_logger,
    logger,
    progress_logger,
)
from flowcore_story.utils.meta_utils import derive_story_slug
from flowcore_story.utils.metrics_tracker import metrics_tracker
from flowcore_story.utils.skip_manager import (
    is_story_skipped,
    load_skipped_stories,
    mark_story_as_skipped,
)
from flowcore_story.utils.state_utils import locked_crawl_state, update_crawl_state_section

DEFAULT_SOURCE_COOLDOWN_SECONDS = 180.0
DEFAULT_SITE_COOLDOWN_SECONDS = 420.0
DEFAULT_STORY_COOLDOWN_SECONDS = 900.0
DEFAULT_COOLDOWN_BACKOFF = 2.0


@dataclass(slots=True)
class SourceFailureState:
    """Keep track of the health of a particular story source."""

    total_failures: int = 0
    permanent_failures: int = 0
    transient_failures: int = 0
    cooldown_until: float = 0.0
    cooldown_expiry_ts: float = 0.0
    last_error: CrawlError = CrawlError.UNKNOWN

    def register_success(self) -> None:
        self.transient_failures = 0
        self.last_error = CrawlError.UNKNOWN
        self.cooldown_until = 0.0
        self.cooldown_expiry_ts = 0.0

    def register_failure(
        self,
        error: CrawlError,
        now: float,
        base_cooldown: float,
        backoff_multiplier: float,
        max_retry: int,
        *,
        wall_time: float | None = None,
    ) -> None:
        self.total_failures += 1
        self.last_error = error
        if is_retryable_error(error):
            self.transient_failures += 1
            delay = base_cooldown * (backoff_multiplier ** (self.transient_failures - 1))
            self.cooldown_until = max(self.cooldown_until, now + delay)
            current_wall = wall_time or time.time()
            self.cooldown_expiry_ts = max(self.cooldown_expiry_ts, current_wall + delay)
        else:
            self.permanent_failures = max(self.permanent_failures, max_retry)
            self.transient_failures = 0
            self.cooldown_until = 0.0
            self.cooldown_expiry_ts = 0.0

    def is_in_cooldown(self, now: float) -> bool:
        return self.cooldown_until > now

    def is_exhausted(self, max_retry: int) -> bool:
        if self.permanent_failures >= max_retry:
            return True
        return self.total_failures >= max_retry and not is_retryable_error(self.last_error)

    def refresh_runtime_cooldown(self, wall_now: float) -> None:
        if self.cooldown_expiry_ts > wall_now:
            remaining = self.cooldown_expiry_ts - wall_now
            self.cooldown_until = time.monotonic() + remaining
        else:
            self.cooldown_until = 0.0
            if self.cooldown_expiry_ts <= wall_now:
                self.cooldown_expiry_ts = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_failures": self.total_failures,
            "permanent_failures": self.permanent_failures,
            "transient_failures": self.transient_failures,
            "cooldown_expiry_ts": self.cooldown_expiry_ts,
            "last_error": self.last_error.value,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> SourceFailureState:
        state = cls()
        try:
            state.total_failures = int(payload.get("total_failures", 0))
        except (TypeError, ValueError):
            state.total_failures = 0
        try:
            state.permanent_failures = int(payload.get("permanent_failures", 0))
        except (TypeError, ValueError):
            state.permanent_failures = 0
        try:
            state.transient_failures = int(payload.get("transient_failures", 0))
        except (TypeError, ValueError):
            state.transient_failures = 0
        try:
            state.cooldown_expiry_ts = float(payload.get("cooldown_expiry_ts", 0.0) or 0.0)
        except (TypeError, ValueError):
            state.cooldown_expiry_ts = 0.0
        raw_error = payload.get("last_error")
        if isinstance(raw_error, str):
            try:
                state.last_error = CrawlError(raw_error)
            except ValueError:
                state.last_error = CrawlError.UNKNOWN
        else:
            state.last_error = CrawlError.UNKNOWN
        return state


@dataclass(slots=True)
class StorySource:
    """Representation of a crawlable source for a story."""

    url: str
    site_key: str
    priority: int = 100


@dataclass(slots=True)
class StoryCrawlRequest:
    """Parameters required to run a multi-source crawl."""

    site_key: str
    session: Any  # ``aiohttp.ClientSession`` in runtime, kept generic for tests
    story_data_item: dict[str, Any]
    current_discovery_genre_data: dict[str, Any]
    story_folder_path: str
    crawl_state: dict[str, Any]
    num_batches: int = 10
    state_file: str | None = None
    adapter: BaseSiteAdapter | None = None
    category_id: str | None = None
    story_registry_id: int | None = None


def _sort_sources(sources: Iterable[dict[str, Any]]) -> list[StorySource]:
    """Normalise and sort raw source dictionaries.

    The logic mirrors the historical implementation in ``main.py`` but returns
    :class:`StorySource` instances to keep the rest of the pipeline strongly
    typed.
    """

    result: list[StorySource] = []
    for raw in sources:
        if not isinstance(raw, dict):
            continue
        url = raw.get("url")
        if not url:
            continue
        site_key = (
            raw.get("site_key")
            or raw.get("site")
            or get_site_key_from_url(url)
        )
        if not site_key:
            continue
        priority = raw.get("priority") if isinstance(raw.get("priority"), int) else 100
        result.append(StorySource(url=url, site_key=site_key, priority=priority))

    return sorted(result, key=lambda s: s.priority)


class MultiSourceStoryCrawler:
    """Coordinate crawling a story across multiple mirrored sources."""

    def __init__(self) -> None:
        self._adapter_cache: dict[str, BaseSiteAdapter] = {}
        self._source_failures: dict[str, SourceFailureState] = {}
        self._site_cooldowns: dict[str, float] = {}
        self._source_cooldown_seconds = float(
            getattr(app_config, "SOURCE_COOLDOWN_SECONDS", DEFAULT_SOURCE_COOLDOWN_SECONDS)
        )
        self._site_cooldown_seconds = float(
            getattr(app_config, "SITE_COOLDOWN_SECONDS", DEFAULT_SITE_COOLDOWN_SECONDS)
        )
        self._story_cooldown_seconds = float(
            getattr(app_config, "STORY_COOLDOWN_SECONDS", DEFAULT_STORY_COOLDOWN_SECONDS)
        )
        self._cooldown_backoff = float(
            getattr(app_config, "COOLDOWN_BACKOFF_MULTIPLIER", DEFAULT_COOLDOWN_BACKOFF)
        )

    def _should_persist_failure(
        self, state: SourceFailureState, now_wall: float, max_retry: int
    ) -> bool:
        if state.cooldown_expiry_ts > now_wall:
            return True
        if state.is_exhausted(max_retry):
            return True
        if state.permanent_failures >= max_retry:
            return True
        if state.total_failures > 0 and state.last_error is not CrawlError.UNKNOWN:
            return True
        return False

    async def _load_persisted_health_state(
        self, request: StoryCrawlRequest, max_retry: int
    ) -> bool:
        async with locked_crawl_state(request.crawl_state) as crawl_state:
            changed = False
            now_wall = time.time()

            persisted_site_cooldowns = crawl_state.get("site_cooldowns")
            if isinstance(persisted_site_cooldowns, dict):
                for site_key, value in list(persisted_site_cooldowns.items()):
                    try:
                        expiry = float(value)
                    except (TypeError, ValueError):
                        persisted_site_cooldowns.pop(site_key, None)
                        changed = True
                        continue
                    if expiry <= now_wall:
                        persisted_site_cooldowns.pop(site_key, None)
                        changed = True
                        continue
                    current = self._site_cooldowns.get(site_key, 0.0)
                    if expiry > current:
                        self._site_cooldowns[site_key] = expiry
                if not persisted_site_cooldowns:
                    crawl_state.pop("site_cooldowns", None)
                    changed = True

            persisted_failures = crawl_state.get("source_failures")
            if isinstance(persisted_failures, dict):
                for url, payload in list(persisted_failures.items()):
                    if not isinstance(payload, dict):
                        persisted_failures.pop(url, None)
                        changed = True
                        continue
                    state = SourceFailureState.from_dict(payload)
                    state.refresh_runtime_cooldown(now_wall)
                    if not self._should_persist_failure(state, now_wall, max_retry):
                        persisted_failures.pop(url, None)
                        changed = True
                        continue
                    self._source_failures[url] = state
                if not persisted_failures:
                    crawl_state.pop("source_failures", None)
                    changed = True

            return changed

    async def _persist_health_state(
        self, request: StoryCrawlRequest, max_retry: int
    ) -> None:
        now_wall = time.time()

        site_cooldowns = {
            site: expiry
            for site, expiry in list(self._site_cooldowns.items())
            if expiry > now_wall
        }
        self._site_cooldowns = site_cooldowns

        failure_payload: dict[str, Any] = {}
        for url, state in list(self._source_failures.items()):
            state.refresh_runtime_cooldown(now_wall)
            if self._should_persist_failure(state, now_wall, max_retry):
                failure_payload[url] = state.to_dict()
            elif state.cooldown_expiry_ts <= 0 and state.last_error is CrawlError.UNKNOWN:
                if (
                    state.total_failures == 0
                    and state.permanent_failures == 0
                    and state.transient_failures == 0
                ):
                    self._source_failures.pop(url, None)

        def _apply(state: dict[str, Any]) -> None:
            if failure_payload:
                state["source_failures"] = failure_payload
            else:
                state.pop("source_failures", None)
            if site_cooldowns:
                state["site_cooldowns"] = site_cooldowns
            else:
                state.pop("site_cooldowns", None)

        if request.state_file:
            await update_crawl_state_section(
                request.crawl_state,
                _apply,
                component="source_health",
                state_file=request.state_file,
                site_key=request.site_key,
                # Avoid forcing a disk write on every chapter; rely on the
                # helper's default debounce window to batch updates.
                debounce=2.0,
            )
        else:
            _apply(request.crawl_state)

    def _resolve_event_identity(
        self, request: StoryCrawlRequest
    ) -> tuple[str, str] | None:
        story = request.story_data_item
        category_candidates = [
            request.category_id,
            request.current_discovery_genre_data.get("category_id"),
            request.current_discovery_genre_data.get("id"),
            request.current_discovery_genre_data.get("slug"),
            request.current_discovery_genre_data.get("slug_id"),
            request.current_discovery_genre_data.get("code"),
            request.current_discovery_genre_data.get("url"),
            request.current_discovery_genre_data.get("name"),
        ]
        category_id: str | None = None
        for candidate in category_candidates:
            if isinstance(candidate, (str, int)) and str(candidate).strip():
                category_id = str(candidate).strip()
                break
        if not category_id:
            category_id = request.site_key

        story_candidates = [
            request.story_registry_id,
            story.get("registry_story_id"),
            story.get("story_id"),
            story.get("id"),
            story.get("slug"),
            story.get("url"),
        ]
        story_id: str | None = None
        for candidate in story_candidates:
            if isinstance(candidate, (str, int)) and str(candidate).strip():
                story_id = str(candidate).strip()
                break
        if not story_id:
            return None

        return category_id, story_id

    def _record_event(
        self,
        request: StoryCrawlRequest,
        event: str,
        **metadata: Any,
    ) -> None:
        identity = self._resolve_event_identity(request)
        if not identity:
            return
        category_id, story_id = identity
        metrics_tracker.record_story_event(
            category_id,
            story_id,
            event,
            metadata={k: v for k, v in metadata.items() if v is not None},
        )

    async def crawl_story_until_complete(
        self, request: StoryCrawlRequest
    ) -> int | None:
        """Attempt to crawl all chapters for ``request.story_data_item``.

        The method keeps the retry semantics and logging from the previous
        implementation while exposing a cleaner surface that can be re-used by
        other workers.
        """

        story = request.story_data_item
        story_slug = derive_story_slug(story.get("title"), story.get("url"))
        total_chapters = story.get("total_chapters_on_site") or story.get("total_chapters")
        if not total_chapters:
            logger.error(f"Không xác định được tổng số chương cho '{story['title']}'")
            metrics_tracker.story_failed(
                story_slug,
                "missing_total_chapters",
                final_status="permanent_fail",
            )
            self._record_event(
                request,
                "failure",
                reason="missing_total_chapters",
                permanent=True,
            )
            return None

        story_url = story.get("url")
        story_cooldowns = request.crawl_state.setdefault("story_cooldowns", {})
        current_ts = time.time()
        if story_url:
            cooldown_until = max(
                float(story.get("_cooldown_until") or 0),
                float(story_cooldowns.get(story_url) or 0),
            )
            if cooldown_until and cooldown_until > current_ts:
                human_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cooldown_until))
                logger.info(
                    f"[COOLDOWN][STORY] '{story['title']}' đang trong thời gian chờ đến {human_ts}, bỏ qua lượt này."
                )
                metrics_tracker.story_on_cooldown(story_slug, cooldown_until)
                story["_cooldown_until"] = cooldown_until
                return None
            story_cooldowns.pop(story_url, None)
            story.pop("_cooldown_until", None)

        sources = _sort_sources(story.get("sources", []))
        if not sources:
            url = story.get("url")
            if url:
                logger.warning(
                    f"[AUTO-FIX] Chưa có sources cho '{story['title']}', auto thêm nguồn chính."
                )
                sources = [StorySource(url=url, site_key=request.site_key, priority=1)]
                story["sources"] = [
                    {"url": source.url, "site_key": source.site_key, "priority": source.priority}
                    for source in sources
                ]
            else:
                logger.error(
                    f"Không xác định được URL cho '{story['title']}', không thể auto-fix sources."
                )
                metrics_tracker.story_failed(
                    story_slug,
                    "missing_sources",
                    final_status="permanent_fail",
                )
                self._record_event(
                    request,
                    "failure",
                    reason="missing_sources",
                    permanent=True,
                )
                return None

        metrics_tracker.story_started(
            story_slug,
            story["title"],
            total_chapters,
            primary_site=request.site_key,
            genre_name=request.current_discovery_genre_data.get("name"),
            genre_url=request.current_discovery_genre_data.get("url"),
            genre_site_key=request.current_discovery_genre_data.get("site_key")
            or request.site_key,
        )
        self._record_event(
            request,
            "start",
            title=story.get("title"),
            total_chapters=total_chapters,
        )
        progress_logger.info(
            f"[START] Bắt đầu crawl '{story['title']}' với {total_chapters} chương (site chính: {request.site_key})."
        )

        if request.adapter and request.site_key not in self._adapter_cache:
            self._adapter_cache[request.site_key] = request.adapter

        max_retry = 3
        if await self._load_persisted_health_state(request, max_retry):
            await self._persist_health_state(request, max_retry)
        retry_round = 0
        applied_chapter_limit: int | None = None

        while True:
            if retry_round > 0:
                self._record_event(
                    request,
                    "retry",
                    round=retry_round,
                )
            current_ts = time.time()
            if is_story_skipped(story):
                logger.warning(
                    f"[SKIP][LOOP] Truyện '{story['title']}' đã bị đánh dấu skip, bỏ qua vòng lặp sources."
                )
                self._record_event(
                    request,
                    "failure",
                    reason="already_skipped",
                    permanent=True,
                )
                break

            files_before = len(get_saved_chapters_files(request.story_folder_path))
            files_after = files_before
            prev_files_after = files_before
            crawled_any = False

            for source in sources:
                state = self._source_failures.setdefault(source.url, SourceFailureState())
                now_monotonic = time.monotonic()
                if state.is_exhausted(max_retry):
                    logger.warning(f"[SKIP] Nguồn {source.url} bị lỗi quá nhiều, bỏ qua.")
                    continue

                site_cooldown_until = self._site_cooldowns.get(source.site_key)
                if site_cooldown_until and site_cooldown_until > current_ts:
                    logger.info(
                        f"[COOLDOWN][SITE] Tạm hoãn crawl {source.site_key} đến {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(site_cooldown_until))}"
                    )
                    continue

                if state.is_in_cooldown(now_monotonic):
                    logger.debug(
                        f"[COOLDOWN][SOURCE] {source.url} đang chờ thêm {state.cooldown_until - now_monotonic:.1f}s trước khi thử lại"
                    )
                    continue

                try:
                    adapter = await self._ensure_adapter(source.site_key)
                    
                    # Quick check for homepage redirect (common for deleted stories)
                    if hasattr(adapter, '_looks_like_homepage'):
                        # Optional: We could do a quick check here if needed
                        pass
                        
                except Exception as ex:  # pragma: no cover - network/IO guard
                    logger.warning(
                        f"[SOURCE] Không lấy được adapter cho site '{source.site_key}' (url={source.url}): {ex}"
                    )
                    error_type = classify_crawl_exception(ex)
                    state.register_failure(
                        error_type,
                        now_monotonic,
                        self._source_cooldown_seconds,
                        self._cooldown_backoff,
                        max_retry,
                        wall_time=time.time(),
                    )
                    site_health_monitor.record_failure(source.site_key, error_type)
                    metrics_tracker.update_story_progress(
                        story_slug,
                        last_source=source.site_key,
                        last_error=error_type.value,
                    )
                    if error_type in {CrawlError.ANTI_BOT, CrawlError.RATE_LIMIT}:
                        self._apply_site_cooldown(source.site_key)
                    await self._persist_health_state(request, max_retry)
                    continue

                try:
                    async with domain_circuit_breaker.limit(source.site_key):
                        chapters = await adapter.get_chapter_list(
                            story_url=source.url,
                            story_title=story["title"],
                            site_key=source.site_key,
                            total_chapters=total_chapters,
                            max_pages=app_config.MAX_CHAPTER_PAGES_TO_CRAWL,
                        )
                except Exception as ex:  # pragma: no cover - adapter/network guard
                    error_type = classify_crawl_exception(ex)
                    state.register_failure(
                        error_type,
                        now_monotonic,
                        self._source_cooldown_seconds,
                        self._cooldown_backoff,
                        max_retry,
                        wall_time=time.time(),
                    )
                    self._log_source_error(source, error_type, ex)
                    site_health_monitor.record_failure(source.site_key, error_type)
                    metrics_tracker.update_story_progress(
                        story_slug,
                        last_source=source.site_key,
                        last_error=error_type.value,
                    )
                    if error_type in {CrawlError.ANTI_BOT, CrawlError.RATE_LIMIT}:
                        self._apply_site_cooldown(source.site_key)
                    await self._persist_health_state(request, max_retry)
                    continue

                state.register_success()
                await self._persist_health_state(request, max_retry)
                site_health_monitor.record_success(source.site_key)
                limit_from_missing = await crawl_missing_chapters_for_story(
                    source.site_key,
                    request.session,
                    chapters,
                    story,
                    request.current_discovery_genre_data,
                    request.story_folder_path,
                    request.crawl_state,
                    num_batches=request.num_batches,
                    state_file=request.state_file,
                    adapter=adapter,
                )

                if isinstance(limit_from_missing, int):
                    applied_chapter_limit = (
                        limit_from_missing
                        if applied_chapter_limit is None
                        else min(applied_chapter_limit, limit_from_missing)
                    )

                load_skipped_stories()
                if is_story_skipped(story):
                    logger.warning(
                        f"[SKIP][AFTER CRAWL] Truyện '{story['title']}' đã bị đánh dấu skip, thoát khỏi vòng lặp sources."
                    )
                    break

                files_after = len(get_saved_chapters_files(request.story_folder_path))
                crawled_any = True
                new_files = max(files_after - prev_files_after, 0)
                prev_files_after = files_after

                dead_chapters = count_dead_chapters(request.story_folder_path)
                expected_total = len(chapters) if chapters else (total_chapters or 0)
                missing = max(expected_total - (files_after + dead_chapters), 0)
                metrics_tracker.update_story_progress(
                    story_slug,
                    crawled_chapters=files_after,
                    missing_chapters=missing,
                    last_source=source.site_key,
                )

                genre_data = request.current_discovery_genre_data
                genre_url = genre_data.get("url")
                genre_site_key = genre_data.get("site_key") or request.site_key

                if genre_url and genre_site_key:
                    progress_percent = 0
                    if expected_total > 0:
                        progress_percent = int((files_after / expected_total) * 100)

                    metrics_tracker.update_genre_story_progress(
                        site_key=genre_site_key,
                        genre_url=genre_url,
                        story_title=story["title"],
                        progress=progress_percent,
                        story_id=request.story_registry_id,
                        story_slug=story_slug,
                    )
                if new_files > 0:
                    progress_logger.info(
                        f"[PROGRESS] '{story['title']}' đã crawl thêm {new_files} chương (còn thiếu {missing})."
                    )

                if (files_after + dead_chapters) >= expected_total:
                    logger.info(
                        f"Đã crawl đủ chương {files_after}/{total_chapters} cho '{story['title']}' (từ nguồn {source.site_key})"
                    )
                    metrics_tracker.story_completed(story_slug)
                    self._record_event(
                        request,
                        "success",
                        source=source.site_key,
                        crawled=files_after,
                    )
                    progress_logger.info(
                        f"[DONE] Hoàn thành '{story['title']}' với {files_after} chương."
                    )
                    if applied_chapter_limit is not None:
                        story["_chapter_limit"] = applied_chapter_limit
                    return applied_chapter_limit

            if not crawled_any or files_after == files_before:
                if self._has_viable_sources(sources, max_retry):
                    cooldown_until = current_ts + self._story_cooldown_seconds
                    story["_cooldown_until"] = cooldown_until
                    if story_url:
                        story_cooldowns[story_url] = cooldown_until
                    human_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cooldown_until))
                    logger.warning(
                        f"[COOLDOWN][STORY] Đặt truyện '{story['title']}' vào hàng chờ đến {human_ts} do lỗi tạm thời từ các nguồn."
                    )
                    metrics_tracker.story_on_cooldown(story_slug, cooldown_until)
                    metrics_tracker.update_story_progress(
                        story_slug,
                        status="cooldown",
                        last_error="temporary_failures",
                        cooldown_until=cooldown_until,
                    )
                    self._record_event(
                        request,
                        "failure",
                        reason="temporary_failures",
                        cooldown_until=cooldown_until,
                    )
                    progress_logger.warning(
                        f"[COOLDOWN] '{story['title']}' tạm nghỉ đến {human_ts} vì lỗi tạm thời."
                    )
                    break

                logger.warning(
                    f"[ALERT] Đã thử hết nguồn nhưng không crawl thêm được chương nào cho '{story['title']}'. Đánh dấu skip và next truyện."
                )
                metrics_tracker.story_failed(
                    story_slug,
                    "exhausted_sources",
                    final_status="permanent_fail",
                )
                self._record_event(
                    request,
                    "failure",
                    reason="exhausted_sources",
                    permanent=True,
                )
                mark_story_as_skipped(story, reason="sources_fail_or_all_chapter_skipped")
                break

            retry_round += 1
            if retry_round >= app_config.RETRY_STORY_ROUND_LIMIT:
                logger.error(
                    f"[FATAL] Vượt quá retry cho truyện {story['title']}, sẽ bỏ qua."
                )
                metrics_tracker.story_failed(
                    story_slug,
                    "retry_limit_exceeded",
                    final_status="permanent_fail",
                )
                self._record_event(
                    request,
                    "failure",
                    reason="retry_limit_exceeded",
                    permanent=True,
                    round=retry_round,
                )
                break

            if retry_round % 20 == 0:
                missing = max(total_chapters - files_after, 0)
                message = (
                    f"[ALERT] Truyện '{story['title']}' còn thiếu {missing} chương sau {retry_round} vòng thử tất cả nguồn."
                )
                logger.error(message)
                chapter_error_logger.error(message)

            await smart_delay()

        if applied_chapter_limit is not None:
            story["_chapter_limit"] = applied_chapter_limit
        return applied_chapter_limit

    async def _ensure_adapter(self, site_key: str) -> BaseSiteAdapter:
        """Return (and cache) an adapter for ``site_key``."""

        if site_key not in self._adapter_cache:
            adapter = get_adapter(site_key)
            await initialize_scraper(site_key)
            self._adapter_cache[site_key] = adapter
        return self._adapter_cache[site_key]

    def _apply_site_cooldown(self, site_key: str) -> None:
        cooldown_until = time.time() + self._site_cooldown_seconds
        self._site_cooldowns[site_key] = cooldown_until
        logger.warning(
            f"[COOLDOWN][SITE] Site '{site_key}' gặp lỗi tạm thời, sẽ thử lại sau {self._site_cooldown_seconds:.0f}s."
        )

    @staticmethod
    def _log_source_error(source: StorySource, error_type: CrawlError, exc: Exception) -> None:
        prefix = "[SOURCE]"
        target_logger = logger
        if error_type in {CrawlError.ANTI_BOT, CrawlError.RATE_LIMIT}:
            target_logger = anti_bot_logger
        elif error_type in {CrawlError.NOT_FOUND, CrawlError.DEAD_LINK}:
            target_logger = chapter_error_logger

        if error_type == CrawlError.NOT_FOUND:
            message = (
                f"{prefix} Nguồn {source.url} trả về 404/không tồn tại. Sẽ không retry nhiều lần."
            )
        elif error_type == CrawlError.ANTI_BOT:
            message = (
                f"{prefix} Bị chặn anti-bot khi crawl {source.url}. Sẽ tạm dừng trước khi retry."
            )
        elif error_type == CrawlError.RATE_LIMIT:
            message = (
                f"{prefix} Gặp giới hạn truy cập khi crawl {source.url}. Sẽ chuyển sang nguồn khác tạm thời."
            )
        else:
            message = f"{prefix} Lỗi '{error_type.value}' khi crawl {source.url}: {exc}"

        target_logger.warning(message)
        if target_logger is not logger:
            logger.warning(message)

    def _has_viable_sources(self, sources: list[StorySource], max_retry: int) -> bool:
        now_ts = time.time()
        for source in sources:
            state = self._source_failures.get(source.url)
            if state is None:
                return True
            if not state.is_exhausted(max_retry):
                return True
            if is_retryable_error(state.last_error) and state.total_failures < max_retry:
                return True
            site_cooldown = self._site_cooldowns.get(source.site_key)
            if site_cooldown and site_cooldown > now_ts:
                return True
        return False


# Singleton used across modules to avoid recreating the cache repeatedly.
multi_source_story_crawler = MultiSourceStoryCrawler()



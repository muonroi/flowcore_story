from __future__ import annotations

import asyncio

from flowcore_story.utils.async_primitives import LoopBoundLock
from flowcore_story.utils.logger import logger
from flowcore_story.workers.crawler_missing_chapter import loop_once_multi_sites

MISSING_BACKGROUND_INTERVAL_SECONDS = 24 * 60 * 60


class MissingBackgroundLoop:
    """Periodic task that re-checks missing chapters for all stories."""

    def __init__(
        self,
        *,
        interval_seconds: int = MISSING_BACKGROUND_INTERVAL_SECONDS,
        force_unskip: bool = False,
    ) -> None:
        self._interval = max(1, interval_seconds)
        self._force_unskip = force_unskip
        self._stop_event: asyncio.Event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError as exc:  # pragma: no cover - defensive guard
            raise RuntimeError(
                "MissingBackgroundLoop.start() requires an active asyncio event loop"
            ) from exc
        self._stop_event.clear()
        logger.info(
            "[MISSING][BACKGROUND] Khởi động luồng kiểm tra chương thiếu định kỳ (mỗi %s giây)",
            self._interval,
        )
        self._task = asyncio.create_task(self._run(), name="missing-background-loop")

    def enable_force_unskip(self) -> None:
        self._force_unskip = True

    async def stop(self) -> None:
        if not self._task:
            return
        logger.info("[MISSING][BACKGROUND] Đang dừng luồng kiểm tra chương thiếu...")
        self._stop_event.set()
        try:
            await self._task
        finally:
            self._task = None
            logger.info("[MISSING][BACKGROUND] Luồng kiểm tra chương thiếu đã dừng.")

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await loop_once_multi_sites(force_unskip=self._force_unskip)
                except Exception as ex:  # pragma: no cover - safety net for background task
                    logger.exception(
                        "[MISSING][BACKGROUND] Lỗi khi chạy vòng kiểm tra chương thiếu: %s",
                        ex,
                    )

                if self._stop_event.is_set():
                    break

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
                except TimeoutError:
                    continue
        except asyncio.CancelledError:  # pragma: no cover - defensive cancellation handling
            logger.info("[MISSING][BACKGROUND] Task bị huỷ.")
        finally:
            self._stop_event.clear()


_missing_loop_lock = LoopBoundLock()
_missing_loop: MissingBackgroundLoop | None = None
_missing_loop_refcount = 0


async def start_missing_background_loop(force_unskip: bool = False) -> None:
    """Ensure the background loop is running. Safe for concurrent callers."""

    global _missing_loop_refcount, _missing_loop

    async with _missing_loop_lock:
        _missing_loop_refcount += 1
        if _missing_loop is None:
            _missing_loop = MissingBackgroundLoop(force_unskip=force_unskip)
            _missing_loop.start()
        elif force_unskip:
            # Nếu đã chạy và có yêu cầu force_unskip mới → cập nhật cờ.
            _missing_loop.enable_force_unskip()


async def stop_missing_background_loop() -> None:
    """Decrease the reference count and stop the loop when no caller remains."""

    global _missing_loop_refcount, _missing_loop

    loop_to_stop: MissingBackgroundLoop | None = None
    async with _missing_loop_lock:
        if _missing_loop_refcount == 0:
            return
        _missing_loop_refcount -= 1
        if _missing_loop_refcount == 0 and _missing_loop is not None:
            loop_to_stop = _missing_loop
            _missing_loop = None

    if loop_to_stop is not None:
        await loop_to_stop.stop()


__all__ = [
    "MissingBackgroundLoop",
    "start_missing_background_loop",
    "stop_missing_background_loop",
]


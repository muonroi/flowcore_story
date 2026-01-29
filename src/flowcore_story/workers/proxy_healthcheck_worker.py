"""Worker chạy health-check proxy định kỳ để blacklist proxy yếu."""

from __future__ import annotations

import argparse
import asyncio
import signal

from flowcore_story.utils.logger import logger
from flowcore_story.utils.proxy_healthcheck import healthcheck_loop


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Chạy vòng health-check proxy và tự động blacklist proxy lỗi."
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Khoảng thời gian giữa các lần kiểm tra (giây).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Chạy đúng một vòng kiểm tra rồi thoát.",
    )
    return parser.parse_args()


async def _run_worker(interval: int, run_once: bool) -> None:
    iterations = 1 if run_once else None
    task = asyncio.create_task(
        healthcheck_loop(interval=interval, iterations=iterations)
    )

    if run_once:
        await task
        return

    stop_event = asyncio.Event()

    def _request_stop() -> None:
        if not stop_event.is_set():
            logger.info("[HEALTHCHECK] Nhận tín hiệu dừng, đang tắt worker proxy health-check.")
            stop_event.set()
            task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:  # pragma: no cover - Windows compatibility
            signal.signal(sig, lambda *_args, _sig=sig: _request_stop())

    try:
        wait_tasks: set[asyncio.Task[object]] = {
            task,
            asyncio.create_task(stop_event.wait()),
        }
        await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for pending in list(wait_tasks):
            if not pending.done():
                pending.cancel()
        for pending in list(wait_tasks):
            try:
                await pending
            except asyncio.CancelledError:
                pass


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(_run_worker(args.interval, args.once))
    except KeyboardInterrupt:  # pragma: no cover - graceful exit
        logger.info("[HEALTHCHECK] Proxy health-check worker dừng bởi KeyboardInterrupt.")


if __name__ == "__main__":
    main()

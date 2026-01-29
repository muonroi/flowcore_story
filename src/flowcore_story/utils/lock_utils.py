"""Utilities for working with inter-process file locks.

The project relies on ``filelock.FileLock`` to coordinate writes to
filesystem-based metadata.  In production environments the lock files can be
left behind when a worker crashes which previously required the
``clean_garbage.py`` helper to be executed manually.  This module provides a
single place to encapsulate the recovery logic so that call sites do not need
to duplicate clean-up code and the behaviour is easy to exercise in tests.

The helpers below add two small but important improvements on top of the
``filelock`` defaults:

* **Stale lock detection** – lock files that have not been touched for a
  configurable amount of time are automatically removed before we try to
  acquire them.  This protects the system from zombie ``.lock`` files which are
  common when processes crash.
* **Retry after clean-up** – if we still time out while acquiring the lock we
  perform a second attempt after purging the stale file.  This mirrors what an
  operator would do manually but without any downtime.

Both behaviours are covered by tests so future regressions will be caught
automatically.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager

from filelock import FileLock, Timeout

from flowcore_story.utils.logger import logger as default_logger

# ``filelock`` itself does not place any limit on how long a lock file may
# exist.  In practice we consider anything older than two minutes to be stale
# because all write operations in the crawler are short lived.
DEFAULT_STALE_LOCK_SECONDS = 120


def is_lock_stale(lock_path: str, *, stale_after: int | None = None) -> bool:
    """Return ``True`` if the lock file appears to be stale.

    ``stale_after`` is expressed in seconds.  When ``None`` we fall back to the
    global default defined in :data:`DEFAULT_STALE_LOCK_SECONDS`.
    """

    ttl = DEFAULT_STALE_LOCK_SECONDS if stale_after is None else stale_after
    if ttl <= 0:
        # Always treat ``ttl`` <= 0 as "never stale" to avoid accidental
        # deletion.
        return False

    try:
        mtime = os.path.getmtime(lock_path)
    except OSError:
        return False
    return (time.time() - mtime) >= ttl


def _purge_stale_lock(lock_path: str, *, stale_after: int | None, log) -> bool:
    if not os.path.exists(lock_path):
        return False
    if not is_lock_stale(lock_path, stale_after=stale_after):
        return False

    try:
        os.remove(lock_path)
        log.warning("Đã phát hiện và xóa lock file quá hạn: %s", lock_path)
        return True
    except OSError as exc:  # pragma: no cover - defensive guard
        log.error("Không thể xóa lock file '%s': %s", lock_path, exc)
        return False


@contextmanager
def robust_file_lock(
    lock_path: str,
    *,
    timeout: float = 60,
    stale_after: int | None = None,
    max_retries: int = 1,
    log=default_logger,
) -> Iterator[FileLock]:
    """Acquire a :class:`~filelock.FileLock` with stale lock recovery.

    The context manager mirrors the behaviour of ``with FileLock(...)`` but it
    additionally removes stale ``.lock`` files and retries the acquisition when
    a timeout occurs.  ``max_retries`` controls how many recovery attempts are
    performed before the original :class:`~filelock.Timeout` is re-raised.
    """

    attempts = 0
    ttl = DEFAULT_STALE_LOCK_SECONDS if stale_after is None else stale_after

    lock = FileLock(lock_path, timeout=timeout)

    while True:
        _purge_stale_lock(lock_path, stale_after=ttl, log=log)
        try:
            lock.acquire(timeout=timeout)
            break
        except Timeout:
            if attempts >= max_retries:
                raise

            attempts += 1
            log.warning(
                "Timeout khi acquire lock %s, thử làm sạch và thử lại (%s/%s)",
                lock_path,
                attempts,
                max_retries,
            )
            _purge_stale_lock(lock_path, stale_after=ttl, log=log)
            lock = FileLock(lock_path, timeout=timeout)

    try:
        yield lock
    finally:
        if lock.is_locked:
            lock.release()


"""Asyncio synchronization primitives that are safe to create at import time."""

from __future__ import annotations

import asyncio
import contextvars
import weakref

__all__ = ["LoopBoundLock", "LoopBoundSemaphore"]


class LoopBoundLock:
    """A lock that lazily binds to the current running event loop.

    ``asyncio`` primitives created at import time are bound to whatever loop is
    current when they are instantiated.  When ``asyncio.run`` later creates a new
    loop these objects cannot be used and raise ``RuntimeError``.  ``LoopBoundLock``
    defers the creation of the underlying :class:`asyncio.Lock` until it is first
    used from within a running loop and keeps a separate instance per loop.
    """

    def __init__(self) -> None:
        self._locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._lock_stack = contextvars.ContextVar(
            f"loopbound_lock_stack_{id(self)}", default=()
        )
        self._lock_tokens = contextvars.ContextVar(
            f"loopbound_lock_tokens_{id(self)}", default=()
        )

    def _get_lock(self) -> asyncio.Lock:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:  # pragma: no cover - defensive guard
            raise RuntimeError("LoopBoundLock requires an active asyncio event loop") from exc

        lock = self._locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[loop] = lock
        return lock

    def clear(self) -> None:
        """Forget cached locks so new ones will be created for future loops."""

        self._locks = weakref.WeakKeyDictionary()

    async def acquire(self) -> bool:
        lock = self._get_lock()
        await lock.acquire()
        stack: tuple[asyncio.Lock, ...] = self._lock_stack.get(())
        tokens: tuple[contextvars.Token[tuple[asyncio.Lock, ...]], ...] = (
            self._lock_tokens.get(())
        )
        token = self._lock_stack.set(stack + (lock,))
        self._lock_tokens.set(tokens + (token,))
        return True

    def release(self) -> None:
        tokens: tuple[contextvars.Token[tuple[asyncio.Lock, ...]], ...] = (
            self._lock_tokens.get(())
        )
        if not tokens:
            raise RuntimeError("LoopBoundLock released too many times")
        token = tokens[-1]
        stack: tuple[asyncio.Lock, ...] = self._lock_stack.get(())
        if not stack:
            raise RuntimeError("LoopBoundLock has no acquired lock to release")
        lock = stack[-1]
        lock.release()
        self._lock_tokens.set(tokens[:-1])
        self._lock_stack.reset(token)

    def locked(self) -> bool:
        stack: tuple[asyncio.Lock, ...] = self._lock_stack.get(())
        if stack:
            return stack[-1].locked()
        return self._get_lock().locked()

    async def __aenter__(self) -> LoopBoundLock:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool | None:
        self.release()
        return None


class LoopBoundSemaphore:
    """A semaphore that creates a per-loop :class:`asyncio.Semaphore` on demand."""

    def __init__(self, value: int = 1) -> None:
        self._initial_value = int(value)
        self._semaphores: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = (
            weakref.WeakKeyDictionary()
        )
        self._semaphore_stack = contextvars.ContextVar(
            f"loopbound_semaphore_stack_{id(self)}", default=()
        )
        self._semaphore_tokens = contextvars.ContextVar(
            f"loopbound_semaphore_tokens_{id(self)}", default=()
        )

    def _get_semaphore(self) -> asyncio.Semaphore:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:  # pragma: no cover - defensive guard
            raise RuntimeError("LoopBoundSemaphore requires an active asyncio event loop") from exc

        semaphore = self._semaphores.get(loop)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self._initial_value)
            self._semaphores[loop] = semaphore
        return semaphore

    def set_value(self, value: int) -> None:
        """Update the semaphore limit for future loops."""

        self._initial_value = int(value)
        self._semaphores = weakref.WeakKeyDictionary()

    def value(self) -> int:
        return self._initial_value

    async def acquire(self) -> bool:
        semaphore = self._get_semaphore()
        await semaphore.acquire()
        stack: tuple[asyncio.Semaphore, ...] = self._semaphore_stack.get(())
        tokens: tuple[tuple[contextvars.Token[tuple[asyncio.Semaphore, ...]], asyncio.Semaphore], ...] = (
            self._semaphore_tokens.get(())
        )
        token = self._semaphore_stack.set(stack + (semaphore,))
        self._semaphore_tokens.set(tokens + ((token, semaphore),))
        return True

    def release(self) -> None:
        tokens: tuple[tuple[contextvars.Token[tuple[asyncio.Semaphore, ...]], asyncio.Semaphore], ...] = (
            self._semaphore_tokens.get(())
        )
        if not tokens:
            raise RuntimeError("LoopBoundSemaphore released too many times")
        token, semaphore = tokens[-1]
        semaphore.release()
        self._semaphore_tokens.set(tokens[:-1])
        self._semaphore_stack.reset(token)

    def locked(self) -> bool:
        stack: tuple[asyncio.Semaphore, ...] = self._semaphore_stack.get(())
        if stack:
            return stack[-1].locked()
        return self._get_semaphore().locked()

    async def __aenter__(self) -> LoopBoundSemaphore:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool | None:
        self.release()
        return None

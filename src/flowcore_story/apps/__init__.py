"""Application entry points for Storyflow core tools."""

import importlib
from typing import Any

__all__ = [
    "main",
    "scraper",
    "telegram_bot",
    "demo_quick_start",
    "verify_bot_setup",
]


def __getattr__(name: str) -> Any:
    if name in __all__:
        return importlib.import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

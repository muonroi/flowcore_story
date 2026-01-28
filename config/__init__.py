"""
Python package facade for configuration helpers.

The original code lives under ``storyflow_core.config`` inside ``src``.  This
module exposes a lightweight facade so legacy imports such as ``config.config``
continue to function.
"""
from __future__ import annotations

import importlib
import sys

_CONFIG_MODULE = importlib.import_module("storyflow_core.config")
_CONFIG_IMPL = importlib.import_module("storyflow_core.config.config")

sys.modules.setdefault("config.config", _CONFIG_IMPL)

for _name in dir(_CONFIG_MODULE):
    if _name.startswith("_"):
        continue
    globals()[_name] = getattr(_CONFIG_MODULE, _name)

__all__ = getattr(_CONFIG_MODULE, "__all__", [])

"""Helpers for loading per-site configuration values."""

from __future__ import annotations

import json
import os
from functools import cache
from typing import Any

SITE_CONFIG_ENV = "SITE_CONFIG_DIR"
DEFAULT_SITE_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config", "sites")


@cache
def load_site_config(site_key: str) -> dict[str, Any]:
    """Load a JSON configuration file for ``site_key`` if it exists."""

    config_dir = os.environ.get(SITE_CONFIG_ENV)
    if not config_dir:
        config_dir = os.path.abspath(DEFAULT_SITE_CONFIG_DIR)
    else:
        config_dir = os.path.abspath(config_dir)

    candidate = os.path.join(config_dir, f"{site_key}.json")
    if not os.path.exists(candidate):
        return {}

    with open(candidate, encoding="utf-8") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in site config '{candidate}': {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"Site config '{candidate}' must contain a JSON object")

    return data


__all__ = ["load_site_config", "SITE_CONFIG_ENV"]

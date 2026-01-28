"""Utility helpers for loading strongly-typed configuration from environment variables."""
from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from dotenv import find_dotenv, load_dotenv


def _ensure_file(path: Path, *, default_contents: str = "") -> None:
    """Create ``path`` with ``default_contents`` if it does not already exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(default_contents, encoding="utf-8")


def _apply_pytest_defaults() -> None:
    """Seed deterministic environment defaults for the test suite."""

    base_dir = Path(tempfile.gettempdir()) / "storyflow_pytest_defaults"
    base_dir.mkdir(parents=True, exist_ok=True)

    folder_defaults = {
        "DATA_FOLDER": base_dir / "data",
        "COMPLETED_FOLDER": base_dir / "completed",
        "BACKUP_FOLDER": base_dir / "backup",
        "STATE_FOLDER": base_dir / "state",
        "LOG_FOLDER": base_dir / "logs",
        "PROXIES_FOLDER": base_dir / "proxies",
    }
    for key, path in folder_defaults.items():
        os.environ.setdefault(key, str(path))

    file_defaults = {
        "SKIPPED_STORIES_FILE": base_dir / "state" / "skipped_stories.json",
        "FAILED_GENRES_FILE": base_dir / "state" / "failed_genres.json",
        "ERROR_CHAPTERS_FILE": base_dir / "state" / "error_chapters.json",
        "MISSING_CHAPTERS_FILE": base_dir / "state" / "missing_chapters.json",
        "BANNED_PROXIES_LOG": base_dir / "logs" / "banned_proxies.log",
        "PATTERN_FILE": base_dir / "config" / "patterns.txt",
        "ANTI_BOT_PATTERN_FILE": base_dir / "config" / "anti_bot_patterns.txt",
        "PROXIES_FILE": base_dir / "proxies" / "proxies.txt",
        "AI_PROFILES_PATH": base_dir / "ai" / "profiles.json",
        "AI_METRICS_PATH": base_dir / "ai" / "metrics.json",
    }

    default_contents = {
        "SKIPPED_STORIES_FILE": "[]\n",
        "FAILED_GENRES_FILE": "[]\n",
        "ERROR_CHAPTERS_FILE": "[]\n",
        "MISSING_CHAPTERS_FILE": "[]\n",
        "BANNED_PROXIES_LOG": "",
        "PATTERN_FILE": "(?i)truyen\n",
        "ANTI_BOT_PATTERN_FILE": "(?i)cloudflare\n",
        "PROXIES_FILE": "# proxies\n",
        "AI_PROFILES_PATH": "{}\n",
        "AI_METRICS_PATH": "{}\n",
    }

    for key, path in file_defaults.items():
        current = os.environ.get(key)
        if current:
            continue
        _ensure_file(path, default_contents=default_contents.get(key, ""))
        os.environ[key] = str(path)

    numeric_defaults = {
        "REQUEST_DELAY": "0.1",
        "TIMEOUT_REQUEST": "30",
        "RETRY_ATTEMPTS": "2",
        "DELAY_ON_RETRY": "0.5",
        "RATE_LIMIT_BACKOFF_SECONDS": "2.0",
        "RATE_LIMIT_BACKOFF_MAX_SECONDS": "12.0",
        "RETRY_GENRE_ROUND_LIMIT": "2",
        "RETRY_SLEEP_SECONDS": "1",
        "RETRY_FAILED_CHAPTERS_PASSES": "2",
        "NUM_CHAPTER_BATCHES": "2",
        "RETRY_STORY_ROUND_LIMIT": "2",
        "MAX_CHAPTER_RETRY": "2",
        "KAFKA_BOOTSTRAP_MAX_RETRIES": "3",
        "KAFKA_BOOTSTRAP_RETRY_DELAY": "1.0",
        "ASYNC_SEMAPHORE_LIMIT": "5",
        "GENRE_ASYNC_LIMIT": "3",
        "GENRE_BATCH_SIZE": "2",
        "STORY_ASYNC_LIMIT": "3",
        "STORY_BATCH_SIZE": "2",
        "AI_PROFILE_TTL_HOURS": "24",
        "AI_TRIM_MAX_BYTES": "4096",
        "MISSING_CRAWL_TIMEOUT_SECONDS": "60",
        "MISSING_CRAWL_TIMEOUT_PER_CHAPTER": "2.0",
        "MISSING_CRAWL_TIMEOUT_MAX": "180",
        "KAFKA_BACKLOG_WARN": "100",
        "KAFKA_BACKLOG_ERROR": "1000",
        "CRAWL_ERROR_RATE_WARN": "0.15",
        "CRAWL_ERROR_RATE_ERROR": "0.3",
        "DEAD_STORY_WARN_THRESHOLD": "5",
        "DEAD_STORY_ERROR_THRESHOLD": "20",
        "DEAD_STORY_REFRESH_SECONDS": "300",
        "PROMETHEUS_PUSH_INTERVAL": "60",
        "CHALLENGE_HARVESTER_TIMEOUT": "45",
    }
    for key, value in numeric_defaults.items():
        os.environ.setdefault(key, value)

    string_defaults = {
        "BASE_XTRUYEN": "https://xtruyen.vn",
        "BASE_TANGTHUVIEN": "https://tangthuvien.net",
        "BASE_TRUYENCOM": "https://truyencom.com",
        "SKIPPED_STORIES_FILE": str(file_defaults["SKIPPED_STORIES_FILE"]),
        "FAILED_GENRES_FILE": str(file_defaults["FAILED_GENRES_FILE"]),
        "ERROR_CHAPTERS_FILE": str(file_defaults["ERROR_CHAPTERS_FILE"]),
        "MISSING_CHAPTERS_FILE": str(file_defaults["MISSING_CHAPTERS_FILE"]),
        "BANNED_PROXIES_LOG": str(file_defaults["BANNED_PROXIES_LOG"]),
        "PATTERN_FILE": str(file_defaults["PATTERN_FILE"]),
        "ANTI_BOT_PATTERN_FILE": str(file_defaults["ANTI_BOT_PATTERN_FILE"]),
        "KAFKA_TOPIC": "storyflow-test",
        "KAFKA_BROKERS": "localhost:9092",
        "KAFKA_GROUP_ID": "storyflow-test",
        "PROXIES_FILE": str(file_defaults["PROXIES_FILE"]),
        "AI_PROFILES_PATH": str(file_defaults["AI_PROFILES_PATH"]),
        "AI_METRICS_PATH": str(file_defaults["AI_METRICS_PATH"]),
        "AI_MODEL": "gpt-3.5-turbo",
        "OPENAI_BASE": "https://api.openai.com/v1",
        "MISSING_WARNING_TOPIC": "storyflow.missing",
        "MISSING_WARNING_GROUP": "storyflow-missing-group",
        "PROMETHEUS_JOB_NAME": "storyflow_crawler",
        "CHALLENGE_HARVESTER_URL": "http://localhost:8099/harvest",
    }
    for key, value in string_defaults.items():
        os.environ.setdefault(key, value)

    os.environ.setdefault("USE_PROXY", "false")
    os.environ.setdefault("CHALLENGE_HARVESTER_ENABLED", "false")


# Load environment variables from dotenv files with test-safety in mind.
# During pytest runs, prefer `.env.example` to avoid polluting tests with
# developer-specific local `.env` settings (e.g. ENABLED_SITE_KEYS overrides).
# In normal execution, prefer the real `.env`, and fall back to `.env.example`.
def _load_dotenv_for_context() -> None:
    is_pytest = (
        "pytest" in sys.modules
        or any(key in os.environ for key in ("PYTEST_CURRENT_TEST", "PYTEST_ADDOPTS", "PYTEST_WORKER"))
    )

    if is_pytest:
        example_env = find_dotenv(".env.example", usecwd=True)
        if example_env:
            load_dotenv(example_env, override=False)
        _apply_pytest_defaults()
        return

    primary_env = find_dotenv(usecwd=True)
    if primary_env:
        load_dotenv(primary_env, override=False)
        return

    # Fallback for environments without a concrete `.env`
    example_env = find_dotenv(".env.example", usecwd=True)
    if example_env:  # pragma: no cover - defensive path
        load_dotenv(example_env, override=False)


_load_dotenv_for_context()

__all__ = [
    "EnvironmentConfigurationError",
    "get_str",
    "get_int",
    "get_float",
    "get_bool",
    "get_list",
]


class EnvironmentConfigurationError(RuntimeError):
    """Raised when the application configuration is invalid or incomplete."""


_T = TypeVar("_T")

_TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "f", "no", "n", "off"}


def _apply_cast(name: str, raw: str, caster: Callable[[str], _T]) -> _T:
    try:
        return caster(raw)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise EnvironmentConfigurationError(
            f"Environment variable {name} has invalid value: {raw!r}"
        ) from exc


def _get_raw(name: str, *, required: bool = False, allow_empty: bool = False) -> str | None:
    """Return the raw environment value while enforcing required/empty rules."""

    value = os.getenv(name)
    if value is None:
        if required:
            raise EnvironmentConfigurationError(
                f"Missing required environment variable: {name}"
            )
        return None

    if not allow_empty:
        value = value.strip()
        if not value:
            if required:
                raise EnvironmentConfigurationError(
                    f"Environment variable {name} must not be empty"
                )
            return None
    return value


def get_str(name: str, *, required: bool = False, allow_empty: bool = False) -> str | None:
    """Return a string environment value or ``None`` when not provided."""

    return _get_raw(name, required=required, allow_empty=allow_empty)


def get_int(name: str, *, required: bool = False) -> int | None:
    """Return an integer parsed from the environment."""

    raw = _get_raw(name, required=required)
    if raw is None:
        return None
    return _apply_cast(name, raw, int)


def get_float(name: str, *, required: bool = False) -> float | None:
    """Return a float parsed from the environment."""

    raw = _get_raw(name, required=required)
    if raw is None:
        return None
    return _apply_cast(name, raw, float)


def get_bool(name: str, *, required: bool = False) -> bool | None:
    """Return a boolean parsed from the environment."""

    raw = _get_raw(name, required=required)
    if raw is None:
        return None

    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise EnvironmentConfigurationError(
        f"Environment variable {name} must be a boolean value (true/false), got {raw!r}"
    )


def get_list(
    name: str,
    *,
    required: bool = False,
    separator: str = ",",
    allow_empty: bool = False,
) -> list[str] | None:
    """Return a list parsed from a separated environment value."""

    raw = _get_raw(name, required=required, allow_empty=allow_empty)
    if raw is None:
        return [] if required and allow_empty else None

    if not raw and not allow_empty:
        return []

    parts = [item.strip() for item in raw.split(separator)]
    return [item for item in parts if item]

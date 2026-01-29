#!/usr/bin/env python3
"""
Utility script to reset the local crawler state.

Removes generated data such as SQLite databases, JSON state files,
logs and downloaded story folders so that the crawler can start fresh.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve()
SRC_ROOT = SCRIPT_PATH.parents[2]
REPO_ROOT = SRC_ROOT.parent

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from flowcore_story.config.env_loader import EnvironmentConfigurationError  # type: ignore  # noqa: E402

try:
    from flowcore_story.config import config as app_config  # type: ignore  # noqa: E402
except EnvironmentConfigurationError as exc:  # pragma: no cover - defensive
    CONFIG_IMPORT_ERROR = exc
    app_config = None
else:
    CONFIG_IMPORT_ERROR = None

DEFAULT_PATHS = {
    "DATA_FOLDER": REPO_ROOT / "truyen_data",
    "COMPLETED_FOLDER": REPO_ROOT / "completed_stories",
    "LOG_FOLDER": REPO_ROOT / "logs",
    "STATE_FOLDER": REPO_ROOT / "state",
}
DEFAULT_SQLITE = {
    "CATEGORY_SNAPSHOT_DB_PATH": DEFAULT_PATHS["STATE_FOLDER"] / "category_snapshot.db",
    "METADATA_DB_PATH": DEFAULT_PATHS["STATE_FOLDER"] / "metadata.db",
}


def _get_setting_path(name: str, fallback: Path) -> Path:
    raw = os.environ.get(name)
    if raw:
        return Path(raw).expanduser()
    if app_config is not None:
        value = getattr(app_config, name, None)
        if value:
            return Path(str(value)).expanduser()
    return fallback


def _ensure_directory(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    path.mkdir(parents=True, exist_ok=True)


def _remove_path(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _clear_directory(path: Path, *, dry_run: bool) -> list[Path]:
    removed: list[Path] = []
    if not path.exists():
        _ensure_directory(path, dry_run=dry_run)
        return removed

    for child in path.iterdir():
        removed.append(child)
        _remove_path(child, dry_run=dry_run)

    _ensure_directory(path, dry_run=dry_run)
    return removed


def _remove_glob(path: Path, patterns: Sequence[str], *, dry_run: bool) -> list[Path]:
    removed: list[Path] = []
    if not path.exists():
        return removed
    for pattern in patterns:
        for candidate in path.glob(pattern):
            removed.append(candidate)
            _remove_path(candidate, dry_run=dry_run)
    return removed


def _remove_sqlite_bundle(path: Path, *, dry_run: bool) -> list[Path]:
    removed: list[Path] = []
    base = Path(path)
    if base.suffix:
        base_stem = str(base)
    else:
        base_stem = str(base)
    extras = ("", "-wal", "-shm", "-journal")
    seen: set[Path] = set()
    for extra in extras:
        candidate = Path(base_stem + extra)
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            removed.append(candidate)
            _remove_path(candidate, dry_run=dry_run)
    return removed


def _format_paths(paths: Iterable[Path]) -> list[str]:
    formatted: list[str] = []
    for path in paths:
        try:
            formatted.append(str(path.relative_to(REPO_ROOT)))
        except ValueError:
            formatted.append(str(path))
    return formatted


@dataclass
class CleanupTask:
    name: str
    runner: Callable[[bool], list[Path]]


def _build_tasks() -> list[CleanupTask]:
    tasks: list[CleanupTask] = []

    data_dir = _get_setting_path("DATA_FOLDER", DEFAULT_PATHS["DATA_FOLDER"])
    completed_dir = _get_setting_path(
        "COMPLETED_FOLDER", DEFAULT_PATHS["COMPLETED_FOLDER"]
    )
    log_dir = _get_setting_path("LOG_FOLDER", DEFAULT_PATHS["LOG_FOLDER"])
    state_dir = _get_setting_path("STATE_FOLDER", DEFAULT_PATHS["STATE_FOLDER"])

    tasks.append(
        CleanupTask(
            "DATA_FOLDER contents",
            lambda dry_run: _clear_directory(data_dir, dry_run=dry_run),
        )
    )
    tasks.append(
        CleanupTask(
            "COMPLETED_FOLDER contents",
            lambda dry_run: _clear_directory(completed_dir, dry_run=dry_run),
        )
    )
    tasks.append(
        CleanupTask(
            "LOG_FOLDER contents",
            lambda dry_run: _clear_directory(log_dir, dry_run=dry_run),
        )
    )
    tasks.append(
        CleanupTask(
            "state JSON files",
            lambda dry_run: _remove_glob(state_dir, ("*.json",), dry_run=dry_run),
        )
    )

    sqlite_paths = [
        (
            "category snapshot SQLite bundle",
            _get_setting_path(
                "CATEGORY_SNAPSHOT_DB_PATH", DEFAULT_SQLITE["CATEGORY_SNAPSHOT_DB_PATH"]
            ),
        ),
        (
            "metadata SQLite bundle",
            _get_setting_path(
                "METADATA_DB_PATH", DEFAULT_SQLITE["METADATA_DB_PATH"]
            ),
        ),
    ]
    seen_sqlite: set[Path] = set()
    for label, sqlite_path in sqlite_paths:
        if not sqlite_path:
            continue
        resolved = sqlite_path.resolve(strict=False)
        if resolved in seen_sqlite:
            continue
        seen_sqlite.add(resolved)
        tasks.append(
            CleanupTask(
                label,
                lambda dry_run, target=resolved: _remove_sqlite_bundle(
                    target, dry_run=dry_run
                ),
            )
        )

    return tasks


def confirm(prompt: str) -> bool:
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return False
    return answer in {"y", "yes"}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reset crawler state by removing generated data files."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt and proceed immediately.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without removing anything.",
    )
    args = parser.parse_args()

    tasks = _build_tasks()

    previews = [(task.name, task.runner(True)) for task in tasks]
    total_paths = sum(len(paths) for _, paths in previews)

    if total_paths == 0:
        print("Nothing to clean up. All target locations are already empty.")
        return 0

    if CONFIG_IMPORT_ERROR is not None:
        print(
            "Warning: environment configuration is incomplete "
            f"({CONFIG_IMPORT_ERROR}). Falling back to default paths."
        )

    print("The following items are scheduled for cleanup:")
    preview_limit = 20
    for name, paths in previews:
        if not paths:
            continue
        print(f"- {name}:")
        formatted_paths = _format_paths(paths)
        display = formatted_paths[:preview_limit]
        for formatted in display:
            print(f"    {formatted}")
        remaining = len(formatted_paths) - len(display)
        if remaining > 0:
            print(f"    ... and {remaining} more")

    if args.dry_run:
        print("\nDry run complete. No files were removed.")
        return 0

    if not args.yes:
        if not confirm("\nProceed with deletion? [y/N]: "):
            print("Aborted. No changes were made.")
            return 0

    print()
    removed_total = 0
    for task in tasks:
        removed = task.runner(False)
        if not removed:
            continue
        removed_total += len(removed)
        print(f"Cleared {task.name} ({len(removed)} item(s)).")

    print(f"\nCleanup finished. Removed {removed_total} item(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

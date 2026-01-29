"""Cleanup directories with concatenated metadata in their names.

These directories were created due to a bug in tangthuvien mobile parser
that included metadata (genre, author, views, status, chapters) in the title.

This script will:
1. Find all directories matching the malformed pattern
2. Remove them so they can be re-crawled with correct names
"""
import os
import shutil
import sys
from pathlib import Path

# Pattern to detect malformed directories:
# - Contains "tac-gia-" (author)
# - Contains "luot-xem-" (views)
# - Contains "tinh-trang-" (status)
# - Contains "so-chuong-" (chapters)
MALFORMED_PATTERNS = [
    "tac-gia-",
    "luot-xem-",
    "tinh-trang-",
    "so-chuong-",
]


def is_malformed_directory(dirname: str) -> bool:
    """Check if directory name contains concatenated metadata."""
    # Must contain at least 3 of the 4 patterns to be considered malformed
    matches = sum(1 for pattern in MALFORMED_PATTERNS if pattern in dirname.lower())
    return matches >= 3


def find_malformed_directories(truyen_data_dir: str) -> list[str]:
    """Find all directories with malformed names."""
    if not os.path.exists(truyen_data_dir):
        print(f"[ERROR] Directory not found: {truyen_data_dir}")
        return []

    malformed_dirs = []

    for item in os.listdir(truyen_data_dir):
        full_path = os.path.join(truyen_data_dir, item)

        # Only check directories
        if not os.path.isdir(full_path):
            continue

        # Check if name matches malformed pattern
        if is_malformed_directory(item):
            malformed_dirs.append(full_path)

    return malformed_dirs


def cleanup_directories(directories: list[str], dry_run: bool = True) -> None:
    """Remove malformed directories."""
    if not directories:
        print("[OK] No malformed directories found")
        return

    print(f"\n{'=' * 80}")
    print(f"Found {len(directories)} malformed directories")
    print(f"{'=' * 80}\n")

    for i, dir_path in enumerate(directories, 1):
        dirname = os.path.basename(dir_path)

        # Show first 100 chars of dirname
        display_name = dirname if len(dirname) <= 100 else dirname[:97] + "..."

        print(f"{i:3d}. {display_name}")

        if not dry_run:
            try:
                shutil.rmtree(dir_path)
                print(f"     [REMOVED]")
            except Exception as e:
                print(f"     [ERROR] Failed to remove: {e}")

    print(f"\n{'=' * 80}")
    if dry_run:
        print(f"DRY RUN - No directories were removed")
        print(f"Run with --execute to actually remove these directories")
    else:
        print(f"CLEANUP COMPLETE - {len(directories)} directories removed")
    print(f"{'=' * 80}\n")


def main():
    """Main function."""
    # Get truyen_data directory
    truyen_data_dir = os.getenv("TRUYEN_DATA_DIR", "/app/truyen_data")

    # Check for --execute flag
    dry_run = "--execute" not in sys.argv

    if dry_run:
        print("[DRY RUN MODE] Use --execute to actually remove directories\n")
    else:
        print("[EXECUTE MODE] Directories will be PERMANENTLY REMOVED\n")

    print(f"Scanning directory: {truyen_data_dir}\n")

    # Find malformed directories
    malformed_dirs = find_malformed_directories(truyen_data_dir)

    # Cleanup
    cleanup_directories(malformed_dirs, dry_run=dry_run)


if __name__ == "__main__":
    main()

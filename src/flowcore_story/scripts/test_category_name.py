#!/usr/bin/env python3
"""
Quick sanity check for chapter_utils.get_category_name.

Run inside container:
  python3 /app/scripts/test_category_name.py
"""

import os
import sys
from pathlib import Path

# Allow running from repo root or container (/app)
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for candidate in (ROOT, SRC):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from flowcore_story.utils.chapter_utils import get_category_name


def run():
    # Dict categories with name populated
    assert get_category_name({"categories": [{"name": "Tien hiep"}]}, {}) == "Tien hiep"

    # Legacy string categories should be accepted
    assert get_category_name({"categories": ["Kiem hiep", "Huyen ao"]}, {}) == "Kiem hiep"

    # Skip empty names and use next valid one
    assert get_category_name({"categories": [{"name": ""}, {"name": "Do thi"}]}, {}) == "Do thi"

    # Fallback to discovery genre when story data lacks categories
    assert get_category_name({}, {"name": "Backup genre"}) == "Backup genre"

    # If discovery genre is a plain string, return it
    assert get_category_name({}, "Chu de khac") == "Chu de khac"

    print("All get_category_name checks passed")


if __name__ == "__main__":
    run()

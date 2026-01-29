#!/bin/bash
set -e

# Script to reset state for specific sites (xtruyen, tangthuvien)
# Preserves truyencom

echo "=============================================="
echo "RESETTING DATA FOR: xtruyen, tangthuvien"
echo "PRESERVING DATA FOR: truyencom"
echo "=============================================="

# 1. Stop the crawler to prevent race conditions
echo "[1/4] Stopping crawler-producer..."
cd docker
docker compose stop crawler-producer
cd ..

# 2. Clean Database (PostgreSQL)
echo "[2/4] Cleaning PostgreSQL data for target sites..."
# We use PGPASSWORD and psql to execute deletions
export PGPASSWORD=storyflow_secret
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="storyflow"
DB_NAME="storyflow_core"

# List of tables to clean by site_key
TABLES=(
    "crawl_states"
    "genre_progress"
    "genre_queue_metadata"
    "site_genre_overview"
    "story_progress"
    "story_queue"
)

for TABLE in "${TABLES[@]}"; do
    echo "  - Cleaning table: $TABLE"
    COL="site_key"
    if [ "$TABLE" == "story_progress" ]; then
        COL="primary_site"
    fi
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "DELETE FROM $TABLE WHERE $COL IN ('xtruyen', 'tangthuvien');"
done

# 3. Clean Local Files
echo "[3/4] Cleaning local data directories..."

# Clean truyen_data
echo "  - Removing truyen_data for sites..."
rm -rf truyen_data/xtruyen
rm -rf truyen_data/tangthuvien

# Clean completed_stories
echo "  - Removing completed_stories for sites..."
rm -rf completed_stories/xtruyen
rm -rf completed_stories/tangthuvien

# Clean state files (careful here)
# skipped_stories.json is a JSON object. We can use python to manipulate it or just leave it.
# Ideally we should remove keys 'xtruyen' and 'tangthuvien' from it.
echo "  - Cleaning skipped_stories.json..."
if [ -f "state/skipped_stories.json" ]; then
    python3 -c "
import json
import os

path = 'state/skipped_stories.json'
if os.path.exists(path):
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        changed = False
        for site in ['xtruyen', 'tangthuvien']:
            if site in data:
                del data[site]
                changed = True
        
        if changed:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            print('    ✓ Removed target sites from skipped_stories.json')
        else:
            print('    - No target sites found in skipped_stories.json')
    except Exception as e:
        print(f'    ⚠ Error processing skipped_stories.json: {e}')
"
fi

# 4. Restart Crawler
echo "[4/4] Restarting crawler-producer..."
cd docker
docker compose up -d crawler-producer
cd ..

echo ""
echo "=============================================="
echo "RESET COMPLETE!"
echo "xtruyen and tangthuvien have been reset."
echo "truyencom data is preserved."
echo "=============================================="

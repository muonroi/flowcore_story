#!/bin/bash

# Script to clean all StoryFlow data and reset the system
# This will remove StoryFlow-specific databases, state files, logs, and crawled data
# Host services (Kafka, PostgreSQL, MariaDB, MongoDB) will NOT be stopped - only their data will be cleaned

set -e

HOST_CLEAN=0
HOST_CLEAN_WARN=0
COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-storyflow-core}
if [ "${1:-}" = "--host" ]; then
    HOST_CLEAN=1
    shift
fi

echo "=========================================="
echo "STORYFLOW CORE - CLEAN ALL DATA"
echo "=========================================="
echo ""
echo "WARNING: This will delete:"
echo "  - All StoryFlow Docker containers, volumes, and images"
echo "  - All SQLite databases (cookies, metadata, category snapshots)"
echo "  - All state files (state/)"
echo "  - All crawled stories (completed_stories/)"
echo "  - All temporary data (truyen_data/)"
echo "  - All logs (logs/)"
if [ "$HOST_CLEAN" -eq 1 ]; then
  echo "  - StoryFlow data in host services (PostgreSQL, MariaDB, MongoDB, Kafka topics)"
  echo "  - Note: Host services will KEEP RUNNING, only StoryFlow data will be cleaned"
fi
echo ""
echo "PRESERVED DATA:"
echo "  ✓ All stories in queue (PostgreSQL story_queue - status reset to 'pending')"
echo "  ✓ Genre planning metadata (PostgreSQL genre_queue_metadata)"
echo "  ✓ Other databases/services on host machine (untouched)"
echo ""
read -p "Are you sure? (yes/no): " confirm

# Trim whitespace from input
confirm=$(echo "$confirm" | xargs)

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Step 1: Stopping and removing StoryFlow Docker containers..."
cd docker
compose_files=()
for f in docker-compose.yml docker-compose.external-infra.yml docker-compose.local.yml; do
    [ -f "$f" ] && compose_files+=("-f" "$f")
done
docker compose -p "$COMPOSE_PROJECT_NAME" "${compose_files[@]}" down -v --remove-orphans || true
cd ..

echo ""
echo "Step 2: Removing StoryFlow Docker volumes..."
# Only remove StoryFlow-specific volumes (legacy prefix docker_, new prefix storyflow-core_)
static_vols=(
    docker_postgres-data
    docker_mysql-data
    docker_mongodb-data
    docker_mongodb-config
    docker_loki-data
    docker_grafana-data
    docker_promtail-positions
    "${COMPOSE_PROJECT_NAME}_postgres-data"
    "${COMPOSE_PROJECT_NAME}_mysql-data"
    "${COMPOSE_PROJECT_NAME}_mongodb-data"
    "${COMPOSE_PROJECT_NAME}_mongodb-config"
    "${COMPOSE_PROJECT_NAME}_loki-data"
    "${COMPOSE_PROJECT_NAME}_grafana-data"
    "${COMPOSE_PROJECT_NAME}_promtail-positions"
)

# Also catch legacy/pending volumes that match our known suffixes
detected_vols=$(docker volume ls -q | grep -E "^(docker|${COMPOSE_PROJECT_NAME})_(postgres-data|mysql-data|mongodb-data|mongodb-config|loki-data|grafana-data|promtail-positions)$" || true)

for vol in "${static_vols[@]}"; do
    docker volume rm "$vol" 2>/dev/null && echo "  - removed $vol" || echo "  - $vol volume not found"
done

if [ -n "$detected_vols" ]; then
    echo "  - Removing detected StoryFlow volumes:"
    echo "$detected_vols" | while read -r v; do
        docker volume rm "$v" 2>/dev/null && echo "    • removed $v" || true
    done
fi

echo ""
echo "Step 3: Removing StoryFlow Docker images..."
# Remove only StoryFlow-related images
storyflow_images=$(docker images --filter "reference=*storyflow*" -q || true)
muonroi_images=$(docker images --filter "reference=muonroi/storyflow-core*" -q || true)

if [ -n "$storyflow_images" ]; then
    echo "$storyflow_images" | xargs docker rmi -f 2>/dev/null && echo "  - removed StoryFlow images" || echo "  - no StoryFlow images to remove"
fi

if [ -n "$muonroi_images" ]; then
    echo "$muonroi_images" | xargs docker rmi -f 2>/dev/null && echo "  - removed muonroi/storyflow-core images" || echo "  - no muonroi images to remove"
fi

if [ "$HOST_CLEAN" -eq 1 ]; then
    echo ""
    echo "Step 4: Cleaning StoryFlow data from host services..."
    echo "  Note: Services will keep running, only StoryFlow-specific data will be cleaned"

    # Clean PostgreSQL - only StoryFlow database
    echo ""
    echo "  [PostgreSQL] Cleaning StoryFlow data..."
    if systemctl is-active --quiet postgresql; then
        # Truncate all tables except queue metadata (story_queue, genre_queue_metadata)
        dynamic_tables=$(sudo -u postgres psql -d storyflow_core -Atc "
            SELECT string_agg(quote_ident(tablename), ',')
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename NOT IN ('story_queue', 'genre_queue_metadata');
        " 2>/dev/null | tr -d '[:space:]')

        if [ -n "$dynamic_tables" ]; then
            if sudo -u postgres psql -d storyflow_core -c "TRUNCATE TABLE ${dynamic_tables} RESTART IDENTITY CASCADE;"; then
                echo "    ✓ Truncated state tables (except queue tables)"
            else
                echo "    ✗ Failed to truncate some PostgreSQL tables"
                HOST_CLEAN_WARN=1
            fi
        else
            echo "    ✓ No non-queue tables found to truncate (possibly clean already)"
        fi

        # Reset all stories in queue to 'pending' status (allow re-crawl)
        sudo -u postgres psql -d storyflow_core -c "
        UPDATE story_queue SET status = 'pending' WHERE status != 'pending';
        " 2>/dev/null && echo "    ✓ Reset all stories to 'pending' status (ready for re-crawl)" || echo "    ✗ story_queue table not found"

    echo "    ✓ PostgreSQL cleaned (other databases untouched)"
    else
        echo "    ⚠ PostgreSQL is not running, skipping"
        HOST_CLEAN_WARN=1
    fi

    # Clean MariaDB - only StoryFlow database
    echo ""
    echo "  [MariaDB] Cleaning StoryFlow database..."
    MARIADB_CLEANED=0
    if systemctl is-active --quiet mariadb; then
        # Drop and recreate storyflow database
        if sudo mysql -e "DROP DATABASE IF EXISTS storyflow; CREATE DATABASE storyflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"; then
            echo "    ✓ Database 'storyflow' recreated"
            MARIADB_CLEANED=1
        else
            echo "    ✗ Failed to clean MariaDB 'storyflow' database (see error above)"
            HOST_CLEAN_WARN=1
        fi

        if [ "$MARIADB_CLEANED" -eq 1 ]; then
            echo "    ✓ MariaDB cleaned (other databases untouched)"
        else
            echo "    ⚠ MariaDB not cleaned; please check credentials/permissions"
        fi
    else
        echo "    ⚠ MariaDB is not running, skipping"
        HOST_CLEAN_WARN=1
    fi

    # Clean MongoDB - only StoryFlow database
    echo ""
    echo "  [MongoDB] Cleaning StoryFlow database..."
    if systemctl is-active --quiet mongod; then
        # Drop storyflow database
        mongosh --quiet --eval "use storyflow; db.dropDatabase();" 2>/dev/null && echo "    ✓ Database 'storyflow' dropped" || echo "    ✗ Failed to clean database"
        echo "    ✓ MongoDB cleaned (other databases untouched)"
    else
        echo "    ⚠ MongoDB is not running, skipping"
    fi

    # Clean Kafka topics - only StoryFlow topics
    echo ""
    echo "  [Kafka] Cleaning StoryFlow topics..."
    if systemctl is-active --quiet kafka; then
        # Delete StoryFlow topics (they will be recreated by kafka-init-users)
        for topic in storyflow.crawl storyflow.crawl.progress storyflow.missing.warning storyflow.crawl.dlq; do
            kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null && echo "    ✓ Deleted topic: $topic" || echo "    ⚠ Topic $topic not found or already deleted"
        done
        echo "    ✓ Kafka cleaned (other topics untouched)"
    else
        echo "    ⚠ Kafka is not running, skipping"
    fi
fi

echo ""
echo "Step 5: Cleaning state directory..."
rm -rf state/*
mkdir -p state
echo "  - state/ cleaned"

echo ""
echo "Step 6: Cleaning completed_stories directory..."
rm -rf completed_stories/*
mkdir -p completed_stories
echo "  - completed_stories/ cleaned"

echo ""
echo "Step 7: Cleaning truyen_data directory..."
rm -rf truyen_data/*
mkdir -p truyen_data
echo "  - truyen_data/ cleaned"

echo ""
echo "Step 8: Cleaning logs directory..."
rm -rf logs/*
mkdir -p logs
echo "  - logs/ cleaned"

echo ""
echo "Step 9: Cleaning backup directories..."
rm -rf backup/*
rm -rf backups/*
mkdir -p backup
mkdir -p backups
echo "  - backup/ and backups/ cleaned"

echo ""
echo "Step 10: Cleaning SQLite databases..."
# Clean cookies database
rm -f src/storyflow_core/config/cookies.sqlite3
rm -f src/storyflow_core/config/cookies.sqlite3-shm
rm -f src/storyflow_core/config/cookies.sqlite3-wal
echo "  - cookies.sqlite3 cleaned"

# Clean state SQLite databases
rm -f state/category_snapshots.sqlite3
rm -f state/metadata.sqlite3
rm -f state/storyflow.db
rm -f state/*.sqlite3
rm -f state/*.db
rm -f state/*.sqlite3-shm
rm -f state/*.sqlite3-wal
echo "  - SQLite databases in state/ cleaned"

echo ""
echo "=========================================="
echo "CLEANUP COMPLETE!"
echo "=========================================="
echo ""
if [ "$HOST_CLEAN" -eq 1 ]; then
    echo "✓ Docker containers, volumes, and images removed"
    if [ "$HOST_CLEAN_WARN" -eq 0 ]; then
        echo "✓ Host services data cleaned (PostgreSQL, MariaDB, MongoDB, Kafka topics)"
    else
        echo "⚠ Host services cleanup completed with warnings (see logs above)"
    fi
    echo "✓ Host services are still running"
    echo "✓ File system cleaned (state, logs, completed_stories, truyen_data)"
    echo "✓ All stories in queue reset to 'pending' status"
    echo ""
    echo "You can now start fresh with:"
    echo "  ./scripts/up-local.sh"
    echo "  or manually:"
    echo "  cd docker && docker-compose -f docker-compose.yml -f docker-compose.external-infra.yml -f docker-compose.local.yml up -d --build"
else
    echo "✓ Docker containers, volumes, and images removed"
    echo "✓ File system cleaned (state, logs, completed_stories, truyen_data)"
    echo ""
    echo "Note: Use --host flag to also clean host services data:"
    echo "  ./scripts/clean-all.sh --host"
    echo ""
    echo "You can now start fresh with:"
    echo "  cd docker && docker-compose up -d"
fi
echo ""

#!/bin/bash
# Smart Backup Script using Restic
# Features: Incremental, Deduplication, Stream Processing

set -e

# ================= CONFIGURATION =================
# Repository location (Local folder or S3/MinIO)
# Example local: /backups
# Example S3: s3:s3.amazonaws.com/my-bucket
RESTIC_REPOSITORY="${RESTIC_REPOSITORY:-/backups}"
RESTIC_PASSWORD="${RESTIC_PASSWORD:-storyflow_secret_backup_key}"

# Retention Policy (Giữ lại bao nhiêu bản backup)
KEEP_DAILY=7
KEEP_WEEKLY=4
KEEP_MONTHLY=6

# Paths to backup
DATA_DIRS=(
    "/app/truyen_data"
    "/app/completed_stories"
    "/app/state"
)

# DB Config (Auto detected from env if running in docker network, else override)
DB_HOST="${DB_HOST:-host.docker.internal}"
DB_USER="${DB_USER:-storyflow}"
DB_PASS="${DB_PASSWORD:-change-me}"
DB_NAME="${DB_NAME:-storyflow}"

# =================================================

export RESTIC_REPOSITORY
export RESTIC_PASSWORD

# 1. Initialize Repo if not exists
if ! restic snapshots > /dev/null 2>&1; then
    echo "[Backup] Initializing Restic Repository at $RESTIC_REPOSITORY..."
    restic init
fi

echo "========================================== "
echo "STARTING SMART BACKUP - $(date)"
echo "========================================== "

# 2. Backup Files (Incremental)
echo "[Backup] 📂 Backing up Static Files..."
# --verbose=1: Show summary only
# --exclude: Skip logs and cache
restic backup "${DATA_DIRS[@]}" \
    --tag filesystem \
    --exclude "**/.cache" \
    --exclude "**/*.log" \
    --exclude "**/__pycache__" \
    --verbose=1

# 3. Backup Database (Stream Mode)
# Dump trực tiếp vào Restic (stdin) -> Không tốn dung lượng ổ cứng cho file .sql tạm
echo "[Backup] 🗄️ Backing up Database (Stream)..."

# Check if we can connect to DB
if command -v mysqldump &> /dev/null; then
    mysqldump -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" --single-transaction --quick "$DB_NAME" \
    | restic backup --stdin --stdin-filename "dump.sql" --tag database
    echo "[Backup] Database backup completed."
else
    echo "[Backup] ⚠️ mysqldump not found. Skipping DB backup."
fi

# 4. Prune Old Backups (Dọn dẹp)
echo "[Backup] 🧹 Pruning old snapshots..."
restic forget \
    --keep-daily $KEEP_DAILY \
    --keep-weekly $KEEP_WEEKLY \
    --keep-monthly $KEEP_MONTHLY \
    --prune

echo "========================================== "
echo "BACKUP COMPLETED SUCCESSFULLY"
echo "Check stats: restic stats"
echo "========================================== "

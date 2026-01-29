#!/bin/bash
#
# StoryFlow Core - Restore Script
# Restore from backup
#
# Usage:
#   ./scripts/restore.sh /path/to/backup/dir
#   ./scripts/restore.sh /path/to/backup/dir --db-only
#   ./scripts/restore.sh /path/to/backup/dir --files-only
#

set -e

# ============================================
# CONFIGURATION
# ============================================

COMPLETED_STORIES="/home/storyflow-core/completed_stories"
STATE_FILES="/home/storyflow-core/state"

# Database credentials (from config/env/database.env)
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-storyflow}"
MYSQL_PASS="${MYSQL_PASS:-change-me}"
MYSQL_DB="${MYSQL_DB:-storyflow}"

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-storyflow}"
POSTGRES_PASS="${POSTGRES_PASS:-storyflow_secret}"
POSTGRES_DB="${POSTGRES_DB:-storyflow_core}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# ============================================
# FUNCTIONS
# ============================================

log_info() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

restore_mysql() {
    local dump_file=$(ls ${BACKUP_DIR}/mysql_*.sql.gz 2>/dev/null | head -1)

    if [ -z "$dump_file" ]; then
        log_warn "No MySQL backup found"
        return 1
    fi

    log_info "Restoring MySQL from: ${dump_file}"

    # Stop containers that use MySQL
    log_info "Stopping dependent containers..."
    docker stop database-sync-worker crawler-producer docker-crawler-consumer-1 crawler-consumer-missing health-checker 2>/dev/null || true

    # Restore
    gunzip -c "${dump_file}" | MYSQL_PWD="${MYSQL_PASS}" mysql \
        -h "${MYSQL_HOST}" \
        -P "${MYSQL_PORT}" \
        -u "${MYSQL_USER}" \
        "${MYSQL_DB}"

    log_info "MySQL restore completed"
}

restore_postgres() {
    local dump_file=$(ls ${BACKUP_DIR}/postgres_*.sql.gz 2>/dev/null | head -1)

    if [ -z "$dump_file" ]; then
        log_warn "No PostgreSQL backup found"
        return 1
    fi

    log_info "Restoring PostgreSQL from: ${dump_file}"

    # Stop containers
    log_info "Stopping dependent containers..."
    docker stop crawler-producer docker-crawler-consumer-1 crawler-consumer-missing 2>/dev/null || true

    # Drop and recreate database
    PGPASSWORD="${POSTGRES_PASS}" psql \
        -h "${POSTGRES_HOST}" \
        -p "${POSTGRES_PORT}" \
        -U "${POSTGRES_USER}" \
        -d postgres \
        -c "DROP DATABASE IF EXISTS ${POSTGRES_DB}; CREATE DATABASE ${POSTGRES_DB};" 2>/dev/null || true

    # Restore
    gunzip -c "${dump_file}" | PGPASSWORD="${POSTGRES_PASS}" psql \
        -h "${POSTGRES_HOST}" \
        -p "${POSTGRES_PORT}" \
        -U "${POSTGRES_USER}" \
        -d "${POSTGRES_DB}" \
        --no-password

    log_info "PostgreSQL restore completed"
}

restore_completed_stories() {
    local tar_file="${BACKUP_DIR}/completed_stories.tar.gz"

    if [ ! -f "$tar_file" ]; then
        log_warn "No completed_stories backup found"
        return 1
    fi

    log_info "Restoring completed_stories from: ${tar_file}"

    # Create backup of current data
    if [ -d "${COMPLETED_STORIES}" ]; then
        local backup_name="${COMPLETED_STORIES}.pre-restore.$(date +%Y%m%d_%H%M%S)"
        log_info "Backing up current data to: ${backup_name}"
        mv "${COMPLETED_STORIES}" "${backup_name}"
    fi

    # Extract
    tar -xzf "${tar_file}" -C "$(dirname ${COMPLETED_STORIES})"

    log_info "Completed stories restore completed"
}

restore_state_files() {
    local tar_file="${BACKUP_DIR}/state_files.tar.gz"

    if [ ! -f "$tar_file" ]; then
        log_warn "No state_files backup found"
        return 1
    fi

    log_info "Restoring state_files from: ${tar_file}"

    # Create backup of current state
    if [ -d "${STATE_FILES}" ]; then
        mv "${STATE_FILES}" "${STATE_FILES}.pre-restore.$(date +%Y%m%d_%H%M%S)"
    fi

    # Extract
    tar -xzf "${tar_file}" -C "$(dirname ${STATE_FILES})"

    log_info "State files restore completed"
}

# ============================================
# MAIN
# ============================================

BACKUP_DIR="$1"
DO_DB=true
DO_FILES=true

if [ -z "$BACKUP_DIR" ]; then
    log_error "Usage: $0 /path/to/backup/dir [--db-only|--files-only]"
    exit 1
fi

if [ ! -d "$BACKUP_DIR" ]; then
    log_error "Backup directory not found: $BACKUP_DIR"
    exit 1
fi

# Parse additional arguments
shift
while [[ $# -gt 0 ]]; do
    case $1 in
        --db-only)
            DO_FILES=false
            shift
            ;;
        --files-only)
            DO_DB=false
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

log_info "============================================"
log_info "StoryFlow Core Restore"
log_info "Backup source: ${BACKUP_DIR}"
log_info "============================================"

# Show manifest if exists
if [ -f "${BACKUP_DIR}/manifest.json" ]; then
    log_info "Backup manifest:"
    cat "${BACKUP_DIR}/manifest.json"
    echo ""
fi

# Confirmation
read -p "Are you sure you want to restore? This will overwrite current data. (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    log_info "Restore cancelled"
    exit 0
fi

# Perform restore
if [ "$DO_DB" = true ]; then
    restore_mysql || log_warn "MySQL restore failed"
    restore_postgres || log_warn "PostgreSQL restore failed"
fi

if [ "$DO_FILES" = true ]; then
    restore_completed_stories || log_warn "Completed stories restore failed"
    restore_state_files || log_warn "State files restore failed"
fi

# Restart containers
log_info "Restarting containers..."
/home/storyflow-core/scripts/up-local.sh

log_info "============================================"
log_info "Restore completed!"
log_info "============================================"

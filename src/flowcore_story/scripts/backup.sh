#!/bin/bash
#
# StoryFlow Core - Backup Script
# Automated backup for 24/7 operation
#
# Usage:
#   ./scripts/backup.sh              # Full backup
#   ./scripts/backup.sh --db-only    # Database only
#   ./scripts/backup.sh --files-only # Files only
#
# Recommended cron schedule:
#   0 */6 * * * /home/storyflow-core/scripts/backup.sh >> /var/log/storyflow-backup.log 2>&1
#

set -e

# ============================================
# CONFIGURATION
# ============================================

# Backup destination
BACKUP_ROOT="${BACKUP_ROOT:-/home/storyflow-backups}"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="${BACKUP_ROOT}/${DATE}"

# Source paths
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

# Retention policy (days)
RETENTION_DAYS="${RETENTION_DAYS:-7}"
MAX_BACKUPS="${MAX_BACKUPS:-28}"

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

check_disk_space() {
    local required_gb=$1
    local available_gb=$(df -BG "${BACKUP_ROOT}" | tail -1 | awk '{print $4}' | sed 's/G//')

    if [ "$available_gb" -lt "$required_gb" ]; then
        log_error "Insufficient disk space. Required: ${required_gb}GB, Available: ${available_gb}GB"
        return 1
    fi
    log_info "Disk space OK: ${available_gb}GB available"
    return 0
}

backup_mysql() {
    log_info "Backing up MySQL/MariaDB database..."

    local dump_file="${BACKUP_DIR}/mysql_${MYSQL_DB}.sql.gz"

    MYSQL_PWD="${MYSQL_PASS}" mysqldump \
        -h "${MYSQL_HOST}" \
        -P "${MYSQL_PORT}" \
        -u "${MYSQL_USER}" \
        --single-transaction \
        --quick \
        --lock-tables=false \
        --routines \
        --triggers \
        "${MYSQL_DB}" | gzip > "${dump_file}"

    local size=$(du -h "${dump_file}" | cut -f1)
    log_info "MySQL backup completed: ${dump_file} (${size})"
}

backup_postgres() {
    log_info "Backing up PostgreSQL database..."

    local dump_file="${BACKUP_DIR}/postgres_${POSTGRES_DB}.sql.gz"

    PGPASSWORD="${POSTGRES_PASS}" pg_dump \
        -h "${POSTGRES_HOST}" \
        -p "${POSTGRES_PORT}" \
        -U "${POSTGRES_USER}" \
        -d "${POSTGRES_DB}" \
        --no-password \
        | gzip > "${dump_file}"

    local size=$(du -h "${dump_file}" | cut -f1)
    log_info "PostgreSQL backup completed: ${dump_file} (${size})"
}

backup_completed_stories() {
    log_info "Backing up completed_stories folder..."

    local tar_file="${BACKUP_DIR}/completed_stories.tar.gz"

    # Use incremental backup with --newer if previous backup exists
    local latest_backup=$(ls -1d ${BACKUP_ROOT}/*/completed_stories.tar.gz 2>/dev/null | tail -1)

    if [ -n "$latest_backup" ] && [ "$INCREMENTAL" = "true" ]; then
        log_info "Creating incremental backup since $(basename $(dirname $latest_backup))"
        tar -czf "${tar_file}" \
            --newer="$(stat -c %y $latest_backup)" \
            -C "$(dirname ${COMPLETED_STORIES})" \
            "$(basename ${COMPLETED_STORIES})" 2>/dev/null || true
    else
        log_info "Creating full backup of completed_stories"
        tar -czf "${tar_file}" \
            -C "$(dirname ${COMPLETED_STORIES})" \
            "$(basename ${COMPLETED_STORIES})"
    fi

    local size=$(du -h "${tar_file}" | cut -f1)
    log_info "Completed stories backup: ${tar_file} (${size})"
}

backup_state_files() {
    log_info "Backing up state files..."

    local tar_file="${BACKUP_DIR}/state_files.tar.gz"

    if [ -d "${STATE_FILES}" ]; then
        tar -czf "${tar_file}" \
            -C "$(dirname ${STATE_FILES})" \
            "$(basename ${STATE_FILES})"

        local size=$(du -h "${tar_file}" | cut -f1)
        log_info "State files backup: ${tar_file} (${size})"
    else
        log_warn "State directory not found: ${STATE_FILES}"
    fi
}

cleanup_old_backups() {
    log_info "Cleaning up old backups (keeping last ${MAX_BACKUPS})..."

    local backup_count=$(ls -1d ${BACKUP_ROOT}/2* 2>/dev/null | wc -l)

    if [ "$backup_count" -gt "$MAX_BACKUPS" ]; then
        local to_delete=$((backup_count - MAX_BACKUPS))
        log_info "Removing ${to_delete} old backup(s)"

        ls -1d ${BACKUP_ROOT}/2* | head -${to_delete} | while read dir; do
            log_info "Deleting: ${dir}"
            rm -rf "${dir}"
        done
    fi

    # Also remove backups older than RETENTION_DAYS
    find "${BACKUP_ROOT}" -maxdepth 1 -type d -mtime +${RETENTION_DAYS} -name "2*" -exec rm -rf {} \; 2>/dev/null || true
}

create_backup_manifest() {
    log_info "Creating backup manifest..."

    cat > "${BACKUP_DIR}/manifest.json" << EOF
{
    "backup_date": "$(date -Iseconds)",
    "backup_type": "${BACKUP_TYPE}",
    "hostname": "$(hostname)",
    "storyflow_version": "$(git -C /home/storyflow-core describe --tags 2>/dev/null || echo 'unknown')",
    "files": {
        "mysql": "$(ls ${BACKUP_DIR}/mysql_*.sql.gz 2>/dev/null | xargs -I {} basename {} || echo 'N/A')",
        "postgres": "$(ls ${BACKUP_DIR}/postgres_*.sql.gz 2>/dev/null | xargs -I {} basename {} || echo 'N/A')",
        "completed_stories": "$(ls ${BACKUP_DIR}/completed_stories.tar.gz 2>/dev/null && echo 'completed_stories.tar.gz' || echo 'N/A')",
        "state_files": "$(ls ${BACKUP_DIR}/state_files.tar.gz 2>/dev/null && echo 'state_files.tar.gz' || echo 'N/A')"
    },
    "sizes": {
        "mysql": "$(du -h ${BACKUP_DIR}/mysql_*.sql.gz 2>/dev/null | cut -f1 || echo 'N/A')",
        "postgres": "$(du -h ${BACKUP_DIR}/postgres_*.sql.gz 2>/dev/null | cut -f1 || echo 'N/A')",
        "completed_stories": "$(du -h ${BACKUP_DIR}/completed_stories.tar.gz 2>/dev/null | cut -f1 || echo 'N/A')",
        "state_files": "$(du -h ${BACKUP_DIR}/state_files.tar.gz 2>/dev/null | cut -f1 || echo 'N/A')"
    },
    "total_size": "$(du -sh ${BACKUP_DIR} | cut -f1)"
}
EOF

    log_info "Manifest created: ${BACKUP_DIR}/manifest.json"
}

# ============================================
# MAIN
# ============================================

BACKUP_TYPE="full"
DO_DB=true
DO_FILES=true
INCREMENTAL=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --db-only)
            DO_FILES=false
            BACKUP_TYPE="db-only"
            shift
            ;;
        --files-only)
            DO_DB=false
            BACKUP_TYPE="files-only"
            shift
            ;;
        --incremental)
            INCREMENTAL=true
            BACKUP_TYPE="incremental"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

log_info "============================================"
log_info "StoryFlow Core Backup - ${BACKUP_TYPE}"
log_info "============================================"

# Create backup directory
mkdir -p "${BACKUP_DIR}"
log_info "Backup directory: ${BACKUP_DIR}"

# Check disk space (require at least 10GB free)
check_disk_space 10 || exit 1

# Perform backups
if [ "$DO_DB" = true ]; then
    backup_mysql || log_warn "MySQL backup failed"
    backup_postgres || log_warn "PostgreSQL backup failed"
fi

if [ "$DO_FILES" = true ]; then
    backup_completed_stories || log_warn "Completed stories backup failed"
    backup_state_files || log_warn "State files backup failed"
fi

# Create manifest
create_backup_manifest

# Cleanup old backups
cleanup_old_backups

# Summary
TOTAL_SIZE=$(du -sh "${BACKUP_DIR}" | cut -f1)
log_info "============================================"
log_info "Backup completed successfully!"
log_info "Location: ${BACKUP_DIR}"
log_info "Total size: ${TOTAL_SIZE}"
log_info "============================================"

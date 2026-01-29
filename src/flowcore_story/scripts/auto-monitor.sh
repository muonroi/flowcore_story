#!/bin/bash
# ================================================================
# STORYFLOW AUTO MONITOR - Chạy 24/7 để đảm bảo stability
# ================================================================
# Sử dụng: ./scripts/auto-monitor.sh
# Hoặc chạy trong crontab: */5 * * * * /home/storyflow-core/scripts/auto-monitor.sh
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_FILE="${PROJECT_DIR}/logs/auto-monitor.log"
DOCKER_COMPOSE_CMD="docker compose -f ${PROJECT_DIR}/docker/docker-compose.yml -f ${PROJECT_DIR}/docker/docker-compose.external-infra.yml -f ${PROJECT_DIR}/docker/docker-compose.local.yml"

# Đảm bảo thư mục logs tồn tại
mkdir -p "${PROJECT_DIR}/logs"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Kiểm tra container unhealthy
check_unhealthy() {
    local unhealthy=$(docker ps --filter "health=unhealthy" --format "{{.Names}}" 2>/dev/null | grep -E "crawler|health|challenge|database" || true)
    echo "$unhealthy"
}

# Kiểm tra container exited
check_exited() {
    local exited=$(docker ps -a --filter "status=exited" --format "{{.Names}}" 2>/dev/null | grep -E "crawler|health|challenge|database" || true)
    echo "$exited"
}

# Restart container cụ thể
restart_container() {
    local container=$1
    log "RESTART: Đang restart $container..."
    docker restart "$container" 2>/dev/null || true
    sleep 5

    # Kiểm tra sau restart
    local status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
    if [ "$status" = "healthy" ] || [ "$status" = "unknown" ]; then
        log "SUCCESS: $container đã restart thành công (status: $status)"
        return 0
    else
        log "WARNING: $container vẫn unhealthy sau restart"
        return 1
    fi
}

# Kiểm tra Kafka backlog
check_kafka_backlog() {
    local backlog=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --group storyflow --describe 2>/dev/null | awk 'NR>1 {sum += $5} END {print sum+0}' || echo "0")
    if [ "$backlog" -gt 10000 ]; then
        log "WARNING: Kafka backlog cao: $backlog messages"
    fi
}

# Kiểm tra disk usage
check_disk_usage() {
    local usage=$(df -h "${PROJECT_DIR}" | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ "$usage" -gt 85 ]; then
        log "WARNING: Disk usage cao: ${usage}%"
        # Auto cleanup old logs
        find "${PROJECT_DIR}/logs" -name "*.log" -mtime +7 -delete 2>/dev/null || true
        log "INFO: Đã cleanup logs cũ hơn 7 ngày"
    fi
}

# Kiểm tra memory usage của containers
check_memory_usage() {
    local high_mem=$(docker stats --no-stream --format "{{.Name}}: {{.MemPerc}}" 2>/dev/null | awk -F': ' '{gsub(/%/,"",$2); if($2>80) print $1}' || true)
    if [ -n "$high_mem" ]; then
        log "WARNING: High memory usage containers: $high_mem"
    fi
}

# Main monitoring loop
main() {
    log "=========================================="
    log "AUTO MONITOR CHECK STARTED"
    log "=========================================="

    # 1. Kiểm tra containers unhealthy
    local unhealthy=$(check_unhealthy)
    if [ -n "$unhealthy" ]; then
        log "ALERT: Unhealthy containers found: $unhealthy"
        for container in $unhealthy; do
            restart_container "$container"
        done
    else
        log "OK: Tất cả containers healthy"
    fi

    # 2. Kiểm tra containers exited
    local exited=$(check_exited)
    if [ -n "$exited" ]; then
        log "ALERT: Exited containers found: $exited"
        for container in $exited; do
            log "RESTART: Starting exited container $container..."
            docker start "$container" 2>/dev/null || true
        done
    fi

    # 3. Kiểm tra disk usage
    check_disk_usage

    # 4. Kiểm tra memory usage
    check_memory_usage

    log "AUTO MONITOR CHECK COMPLETED"
    log ""
}

# Run main
main "$@"

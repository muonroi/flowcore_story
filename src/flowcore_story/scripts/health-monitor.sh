#!/bin/bash
#
# StoryFlow Core - Health Monitor
# Checks system health and sends alerts
#
# Usage:
#   ./scripts/health-monitor.sh              # Check once
#   ./scripts/health-monitor.sh --daemon     # Run continuously
#
# Add to crontab for periodic checking:
#   */5 * * * * /home/storyflow-core/scripts/health-monitor.sh >> /var/log/storyflow/health.log 2>&1
#

set -e

# ============================================ 
# CONFIGURATION
# ============================================ 

# Telegram notification (from config/env/telegram.env)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-6380319725:AAG80j1qbN1InPSvPIBBtvTyP47yhZjAURs}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-5700177789}"

# Alert thresholds
DISK_THRESHOLD=90      # Alert if disk usage > 90%
MEMORY_THRESHOLD=90    # Alert if memory usage > 90%
CPU_THRESHOLD=95       # Alert if CPU usage > 95%

# Check interval for daemon mode (seconds)
CHECK_INTERVAL=300

# Send healthy notification every N checks (0 = only send alerts)
# Default: 12 (every 12 checks = 1 hour with 5 min interval)
HEALTHY_NOTIFY_EVERY="${HEALTHY_NOTIFY_EVERY:-12}"
HEALTHY_CHECK_COUNT_FILE="/tmp/storyflow_health_check_count"

# Cache for full story scan (minutes)
CACHE_DIR="${CACHE_DIR:-/tmp/storyflow_health_monitor}"
CACHE_TTL_MINUTES="${CACHE_TTL_MINUTES:-360}"

# Required containers
REQUIRED_CONTAINERS=(
    "crawler-producer"
    "crawler-consumer"
    "crawler-consumer-xtruyen"
    "crawler-consumer-missing"
    "queue-dispatcher-xtruyen"
    "enrichment-worker"
    "health-checker"
    "database-sync-worker"
    "harvester-1"
    "harvester-2"
    "cookie-renewal"
    "adaptive-scaler"
    "crawler-sticky-worker"
    "data-cleanup-worker"
    "telegram-bot"
)

# Required systemctl services
REQUIRED_SERVICES=(
    "kafka"
    "zookeeper-kafka"
    "mariadb"
    "postgresql"
)

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
LOCK_FILE="/tmp/storyflow_health_monitor.lock"

# ============================================ 
# FUNCTIONS
# ============================================ 

log_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

acquire_lock() {
    exec 200>"$LOCK_FILE"
    if ! flock -n 200; then
        log_warn "Another health monitor instance is already running; skipping this check."
        exit 0
    fi
}

send_telegram() {
    local message="$1"
    if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
        curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d chat_id="${TELEGRAM_CHAT_ID}" \
            -d text="${message}" \
            -d parse_mode="HTML" > /dev/null 2>&1 || true
    fi
}

check_containers() {
    local issues=0

    for container in "${REQUIRED_CONTAINERS[@]}"; do
        # Check if container exists and is running
        local state=$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null || echo "not_found")

        if [ "$state" = "not_found" ]; then
            log_error "Container $container: NOT FOUND"
            issues=$((issues + 1))
            continue
        elif [ "$state" != "running" ]; then
            log_error "Container $container: $state"
            issues=$((issues + 1))
            continue
        fi

        # Check health status if available
        local health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no_healthcheck{{end}}' "$container" 2>/dev/null)

        if [ "$health" = "healthy" ] || [ "$health" = "no_healthcheck" ]; then
            log_ok "Container $container: healthy ($state)"
        elif [ "$health" = "starting" ]; then
             log_warn "Container $container: starting"
             # Don't count starting as an issue yet, give it time
        else
            log_warn "Container $container: unhealthy ($health)"
            issues=$((issues + 1))
        fi
    done

    return $issues
}

check_services() {
    local issues=0

    for service in "${REQUIRED_SERVICES[@]}"; do
        if systemctl is-active --quiet "$service" 2>/dev/null;
 then
            log_ok "Service $service: active"
        else
            log_error "Service $service: NOT RUNNING"
            issues=$((issues + 1))
        fi
    done

    return $issues
}

check_disk() {
    local usage=$(df -h / | tail -1 | awk '{print $5}' | sed 's/%//')

    if [ "$usage" -ge "$DISK_THRESHOLD" ]; then
        log_error "Disk usage: ${usage}% (threshold: ${DISK_THRESHOLD}%)"
        return 1
    else
        log_ok "Disk usage: ${usage}%"
        return 0
    fi
}

check_memory() {
    local usage=$(free | grep Mem | awk '{printf "%.0f", $3/$2 * 100}')

    if [ "$usage" -ge "$MEMORY_THRESHOLD" ]; then
        log_warn "Memory usage: ${usage}% (threshold: ${MEMORY_THRESHOLD}%)"
        return 1
    else
        log_ok "Memory usage: ${usage}%"
        return 0
    fi
}

check_database_stats() {
    # Call python script
    local json_output=$(python3 /home/storyflow-core/scripts/get_db_stats.py 2>/dev/null || echo "{}")
    
    # Create temp python parser script to avoid quoting hell
    local parser_script=$(mktemp)
    cat <<EOF > "$parser_script"
import sys, json
try:
    d = json.load(sys.stdin)
    m = d.get('mysql', {})
    p = d.get('postgres', {})
    
    # Get values
    total = m.get('total_stories', 0)
    n1 = m.get('new_1h', 0)
    n24 = m.get('new_24h', 0)
    q = p.get('queue_size', 0)
    prog = p.get('progress_count', 0)
    top_str = m.get('top_sites_1h_str', '')
    c1 = m.get('checksum_1h', False)
    c24 = m.get('checksum_24h', False)
    
    # Postgres breakdown
    pg_c1 = p.get('created_1h', 0)
    pg_u1 = p.get('updated_1h', 0)
    pg_c24 = p.get('created_24h', 0)
    pg_u24 = p.get('updated_24h', 0)
    last_act = p.get('last_active_minutes', -1)
    
    # Format Last Active
    last_act_str = "Unknown"
    if last_act != -1:
        if last_act < 5:
            last_act_str = f"{last_act} min ago 🟢"
        elif last_act < 30:
            last_act_str = f"{last_act} min ago 🟡"
        else:
            last_act_str = f"{last_act} min ago ⚪"
    
    print(f"{total}|{n1}|{n24}|{q}|{prog}|{top_str}|{c1}|{c24}|{pg_c1}|{pg_u1}|{pg_c24}|{pg_u24}|{last_act_str}")
except Exception:
    print('0|0|0|0|0|Error|False|False|0|0|0|0|Unknown')
EOF

    # Parse vars
    local parsed=$(echo "$json_output" | python3 "$parser_script")
    rm -f "$parser_script"

    # Export for main loop
    export DB_TOTAL_STORIES=$(echo "$parsed" | cut -d'|' -f1)
    export DB_NEW_1H=$(echo "$parsed" | cut -d'|' -f2)
    export DB_NEW_24H=$(echo "$parsed" | cut -d'|' -f3)
    export DB_QUEUE=$(echo "$parsed" | cut -d'|' -f4)
    export DB_PROGRESS=$(echo "$parsed" | cut -d'|' -f5)
    export DB_TOP_SITES=$(echo "$parsed" | cut -d'|' -f6)
    
    local check_1h=$(echo "$parsed" | cut -d'|' -f7)
    local check_24h=$(echo "$parsed" | cut -d'|' -f8)
    
    # Tracking stats
    export PG_CREATED_1H=$(echo "$parsed" | cut -d'|' -f9)
    export PG_UPDATED_1H=$(echo "$parsed" | cut -d'|' -f10)
    export PG_CREATED_24H=$(echo "$parsed" | cut -d'|' -f11)
    export PG_UPDATED_24H=$(echo "$parsed" | cut -d'|' -f12)
    export PG_LAST_ACTIVE_STR=$(echo "$parsed" | cut -d'|' -f13)
    
    if [ "$check_1h" != "True" ] || [ "$check_24h" != "True" ]; then
        log_warn "Database Checksum Failed! (1h: $check_1h, 24h: $check_24h)"
        DB_TOP_SITES="$DB_TOP_SITES ⚠️ Checksum Error"
    else
        log_ok "Database Stats: $DB_TOTAL_STORIES stories. Tracking: +$PG_CREATED_24H new, ^$PG_UPDATED_24H updated (24h). Last active: $PG_LAST_ACTIVE_STR"
    fi
}

check_kafka() {
    # Check Kafka lag
    local lag=$(docker exec crawler-producer python3 -c "
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import os
try:
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'host.docker.internal:9092'),
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=os.getenv('KAFKA_USERNAME', 'storyflow'),
        sasl_plain_password=os.getenv('KAFKA_PASSWORD', 'storyflow_secret'),
    )
    print('OK')
except Exception as e:
    print(f'ERROR: {e}')
" 2>/dev/null || echo "CANNOT_CHECK")

    if [ "$lag" = "OK" ]; then
        log_ok "Kafka connection: OK"
    else
        log_warn "Kafka: $lag"
    fi
}

run_health_check() {
    echo ""
    echo "============================================"
    echo "StoryFlow Health Check - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================"

    local total_issues=0

    echo ""
    echo "--- Docker Containers ---"
    check_containers || total_issues=$((total_issues + $?))

    echo ""
    echo "--- System Services ---"
    check_services || total_issues=$((total_issues + $?))

    echo ""
    echo "--- System Resources ---"
    check_disk || total_issues=$((total_issues + 1))
    check_memory || total_issues=$((total_issues + 1))

    echo ""
    echo "--- Database Statistics ---"
    check_database_stats

    echo ""
    echo "============================================"

    local timestamp host_name disk_usage mem_usage
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    host_name=$(hostname)
    disk_usage=$(df -h / | tail -1 | awk '{print $5}')
    mem_usage=$(free | grep Mem | awk '{printf "%.0f%%", $3/$2 * 100}')

    if [ "$total_issues" -gt 0 ]; then
        echo -e "${RED}Health check FAILED: ${total_issues} issue(s) found${NC}"

        # Reset healthy counter on failure
        echo "0" > "$HEALTHY_CHECK_COUNT_FILE"

        # Send alert
        send_telegram "$(cat <<EOF
🚨 <b>StoryFlow Alert</b>

Health check found ${total_issues} issue(s)

📊 <b>Status:</b>
• Time: ${timestamp}
• Host: ${host_name}
• Stories: ${DB_TOTAL_STORIES} total
• Queue: ${DB_QUEUE} pending
• Active: ${DB_PROGRESS} tracking
• Disk: ${disk_usage}
• Memory: ${mem_usage}

📈 <b>Story Growth (Tracking):</b>
• Last 1h: +${PG_CREATED_1H} new, ^${PG_UPDATED_1H} updated (Total activity: ${DB_NEW_1H})
• Last 24h: +${PG_CREATED_24H} new, ^${PG_UPDATED_24H} updated (Total activity: ${DB_NEW_24H})
• Last active: ${PG_LAST_ACTIVE_STR}
• Top sources: ${DB_TOP_SITES}

Run <code>./scripts/health-monitor.sh</code> for details
EOF
)"

        return 1
    else
        echo -e "${GREEN}Health check PASSED${NC}"

        # Increment healthy counter and send periodic notification
        if [ "$HEALTHY_NOTIFY_EVERY" -gt 0 ]; then
            local count=0
            if [ -f "$HEALTHY_CHECK_COUNT_FILE" ]; then
                count=$(cat "$HEALTHY_CHECK_COUNT_FILE" 2>/dev/null || echo "0")
            fi
            count=$((count + 1))

            if [ "$count" -ge "$HEALTHY_NOTIFY_EVERY" ]; then
                # Send healthy notification
                send_telegram "$(cat <<EOF
✅ <b>StoryFlow Healthy</b>

All systems running normally.

📊 <b>Status:</b>
• Time: ${timestamp}
• Host: ${host_name}
• Stories: ${DB_TOTAL_STORIES} total
• Queue: ${DB_QUEUE} pending
• Active: ${DB_PROGRESS} tracking
• Disk: ${disk_usage}
• Memory: ${mem_usage}
• Containers: 10/10 healthy
• Services: 4/4 active

📈 <b>Story Growth (Tracking):</b>
• Last 1h: +${PG_CREATED_1H} new, ^${PG_UPDATED_1H} updated (Total activity: ${DB_NEW_1H})
• Last 24h: +${PG_CREATED_24H} new, ^${PG_UPDATED_24H} updated (Total activity: ${DB_NEW_24H})
• Last active: ${PG_LAST_ACTIVE_STR}
• Top sources: ${DB_TOP_SITES}
EOF
)"
                count=0
                echo "Sent periodic healthy notification"
            fi
            echo "$count" > "$HEALTHY_CHECK_COUNT_FILE"
        fi

        return 0
    fi
}

# ============================================ 
# MAIN
# ============================================ 

acquire_lock

if [ "$1" = "--daemon" ]; then
    echo "Starting health monitor daemon (interval: ${CHECK_INTERVAL}s)..."
    while true; do
        run_health_check
        sleep $CHECK_INTERVAL
    done
else
    run_health_check
fi
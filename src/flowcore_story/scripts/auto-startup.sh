#!/bin/bash
# StoryFlow Core Auto-Startup Script
# Runs on system boot to start all services
# Safe to run alongside other systems on this host

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

LOG_FILE="/home/storyflow-core/logs/auto-startup.log"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "StoryFlow Core Auto-Startup Starting..."
log "=========================================="

# Change to storyflow directory
cd /home/storyflow-core

# Wait for infrastructure services to be ready
log "Waiting for infrastructure services..."
sleep 5

# Check and wait for critical services
RETRIES=0
MAX_RETRIES=60 # Wait up to 5 minutes
while [ $RETRIES -lt $MAX_RETRIES ]; do
    ZOOKEEPER_OK=false
    KAFKA_OK=false
    
    # Check Zookeeper (required for Kafka)
    # Check port 2181 (client)
    if netstat -tlnp 2>/dev/null | grep -q ":2181.*LISTEN"; then
        ZOOKEEPER_OK=true
    else
        # Try to start if not running
        log "Attempting to start Zookeeper..."
        sudo systemctl start zookeeper-kafka || true
    fi

    # Check Kafka
    # Check port 9092
    if netstat -tlnp 2>/dev/null | grep -q ":9092.*LISTEN"; then
        KAFKA_OK=true
    else
         # Try to start if not running, but ONLY if ZK is ready
         if [ "$ZOOKEEPER_OK" = true ]; then
            # Check for Cluster ID mismatch error
            if grep -q "InconsistentClusterIdException" /opt/kafka/logs/server.log 2>/dev/null; then
                log "⚠ DETECTED KAFKA CLUSTER ID MISMATCH! Fixing automatically..."
                sudo systemctl stop kafka
                
                # Get log dir from config
                KAFKA_LOG_DIR=$(grep "^log.dirs" /opt/kafka/config/server.properties | cut -d'=' -f2)
                if [ -f "$KAFKA_LOG_DIR/meta.properties" ]; then
                    log "Removing old meta.properties from $KAFKA_LOG_DIR"
                    rm -f "$KAFKA_LOG_DIR/meta.properties"
                fi
                
                # Clear the error from log (truncate file to avoid re-triggering)
                echo "" > /opt/kafka/logs/server.log
                
                log "Restarting Kafka after fix..."
                sudo systemctl start kafka || true
                sleep 5
            else
                log "Attempting to start Kafka..."
                sudo systemctl start kafka || true
            fi
         fi
    fi

    if [ "$ZOOKEEPER_OK" = true ] && [ "$KAFKA_OK" = true ]; then
        log "✓ Kafka and Zookeeper are ready"
        break
    fi

    log "Waiting for Kafka/Zookeeper... (ZK:$ZOOKEEPER_OK Kafka:$KAFKA_OK) - Retry $((RETRIES+1))/$MAX_RETRIES"
    sleep 5
    RETRIES=$((RETRIES+1))
done

if [ $RETRIES -eq $MAX_RETRIES ]; then
    log "ERROR: Infrastructure services failed to start. Aborting."
    exit 1
fi

# Additional wait for Kafka to be fully initialized
log "Waiting additional 5s for Kafka stability..."
sleep 5

# Verify MySQL database permissions (Keep this as it's useful one-time setup)
log "Verifying MySQL database permissions..."
HOSTNAME=$(hostname -f)
if ! mysql -u root -e "SELECT COUNT(*) FROM mysql.user WHERE user='storyflow' AND host='$HOSTNAME'" 2>/dev/null | grep -q "1"; then
    log "Granting MySQL permissions for 'storyflow'@'$HOSTNAME'..."
    mysql -u root -e "GRANT ALL PRIVILEGES ON storyflow.* TO 'storyflow'@'$HOSTNAME' IDENTIFIED BY '3b4Kv4iE4ZCtnbT3UNU7nR97'; FLUSH PRIVILEGES;" 2>/dev/null || true
fi

# Kill any existing autoscale manager
log "Cleaning up existing autoscale manager..."
pkill -f autoscale_manager.py || true
sleep 2

# Start StoryFlow containers using up-local.sh which handles the docker-compose logic
log "Starting StoryFlow containers..."
./scripts/up-local.sh >> "$LOG_FILE" 2>&1

log "=========================================="
log "StoryFlow Core Auto-Startup Completed!"
log "=========================================="
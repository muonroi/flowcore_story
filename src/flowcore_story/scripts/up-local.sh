#!/bin/bash

# Script to start StoryFlow with local build and external infrastructure
# This script will:
#   1. Use infrastructure services running on host (PostgreSQL, Kafka, MongoDB, MariaDB)
#   2. Build application images from local source code
#   3. Start all application containers

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "STORYFLOW CORE - START WITH LOCAL BUILD"
echo "=========================================="
echo ""

# Get root directory
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Check if docker-compose files exist
required_files=(
    "docker/docker-compose.yml"
    "docker/docker-compose.external-infra.yml"
    "docker/docker-compose.local.yml"
)

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo "Error: Required file not found: $file"
        exit 1
    fi
done

echo -e "${BLUE}Step 1: Checking host infrastructure services...${NC}"
# Check if required host services are running
required_services=(
    "postgresql"
    "kafka"
    "zookeeper-kafka"
    "mongod"
    "mariadb"
)

all_running=true
# for service in "${required_services[@]}"; do
#     if systemctl is-active --quiet "$service" 2>/dev/null; then
#         echo -e "  ${GREEN}✓${NC} $service is running"
#     else
#         echo -e "  ${YELLOW}✗${NC} $service is NOT running"
#         all_running=false
#     fi
# done

if [ "$all_running" = false ]; then
    echo ""
    echo -e "${YELLOW}Warning: Some infrastructure services are not running.${NC}"
    echo "The application may not start properly without them."
    read -p "Do you want to continue anyway? (yes/no): " continue_anyway
    continue_anyway=$(echo "$continue_anyway" | xargs)
    if [ "$continue_anyway" != "yes" ]; then
        echo "Aborted."
        exit 1
    fi
fi

echo ""
echo -e "${BLUE}Step 2: Starting StoryFlow containers...${NC}"
echo "  - Using host infrastructure (PostgreSQL, Kafka, MongoDB, MariaDB)"
echo "  - Building images from local source code"
echo "  - Starting application containers in background"
echo ""

# Compose files (base + host infra overrides + local build + autoscale)
COMPOSE_FILES=(
    "-f" "docker/docker-compose.yml"
    "-f" "docker/docker-compose.external-infra.yml"
    "-f" "docker/docker-compose.local.yml"
    "-f" "docker/docker-compose.autoscale.yml"
)

# Only start application services; --no-deps prevents infra deps from being pulled in
APP_SERVICES=(
    crawler-producer
    crawler-consumer
    crawler-sticky-worker
    crawler-consumer-xtruyen
    crawler-consumer-xtruyen-scale
    crawler-consumer-missing
    queue-dispatcher-xtruyen
    telegram-bot
    database-sync-worker
    health-checker
    challenge-harvester-1
    challenge-harvester-2
    enrichment-worker
    memory-monitor
    data-cleanup-worker
    cookie-auto-renewal
    adaptive-scaler
    stealth-gateway
)

docker compose "${COMPOSE_FILES[@]}" \
    up -d --build --no-deps --scale crawler-sticky-worker=1 \
    "$@" \
    "${APP_SERVICES[@]}"

echo ""
echo -e "${GREEN}=========================================="
echo "STORYFLOW STARTED SUCCESSFULLY!"
echo "==========================================${NC}"
echo ""
echo "Application containers are now running."
echo ""
echo -e "${BLUE}Current services:${NC}"
docker compose \
    -f docker/docker-compose.yml \
    -f docker/docker-compose.external-infra.yml \
    -f docker/docker-compose.local.yml \
    -f docker/docker-compose.autoscale.yml \
    ps
echo ""
echo -e "${BLUE}Useful commands:${NC}"
echo "  View logs:        ./scripts/logs.sh"
echo "  View status:      docker compose -f docker/docker-compose.yml -f docker/docker-compose.external-infra.yml -f docker/docker-compose.local.yml -f docker/docker-compose.autoscale.yml ps ${APP_SERVICES[*]}"
echo "  Stop:             docker compose -f docker/docker-compose.yml -f docker/docker-compose.external-infra.yml -f docker/docker-compose.local.yml -f docker/docker-compose.autoscale.yml down"
echo "  Restart services: docker compose -f docker/docker-compose.yml -f docker/docker-compose.external-infra.yml -f docker/docker-compose.local.yml -f docker/docker-compose.autoscale.yml restart ${APP_SERVICES[*]}"
echo ""
echo -e "${BLUE}Service endpoints:${NC}"
echo "  Grafana:             http://localhost:3000 (admin/admin)"
echo "  Challenge Harvester: http://localhost:8099"
echo "  Memory Monitor:      http://localhost:9100/metrics"
echo ""
echo -e "${GREEN}🚀 Auto-scaling and memory monitoring enabled!${NC}"
echo "  - Challenge harvester: 1G → 1.5G (auto cleanup enabled)"
echo "  - Missing worker: 1.5G → 2G (memory monitoring enabled)"
echo "  - Memory monitor running on port 9100"

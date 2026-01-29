#!/bin/bash

###############################################################################
# StoryFlow Core - Health Check Script
#
# Checks the health status of all services
#
# Usage:
#   ./scripts/health-check.sh
###############################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo -e "${BLUE}======================================"
echo -e "StoryFlow Core - Health Check"
echo -e "======================================${NC}"
echo ""

cd "$PROJECT_DIR/docker"

# Check if any containers are running
if ! docker compose ps --quiet | grep -q .; then
    echo -e "${RED}[ERROR]${NC} No services are running!"
    exit 1
fi

# Get service status
echo -e "${BLUE}[INFO]${NC} Service Status:"
echo ""

docker compose ps --format "table {{.Service}}\t{{.Status}}\t{{.Health}}"

echo ""
echo -e "${BLUE}[INFO]${NC} Service Health Details:"
echo ""

# Check each service health
SERVICES=$(docker compose ps --services)
UNHEALTHY_COUNT=0

for service in $SERVICES; do
    HEALTH=$(docker compose ps "$service" --format "{{.Health}}" 2>/dev/null || echo "N/A")
    STATUS=$(docker compose ps "$service" --format "{{.Status}}" 2>/dev/null || echo "N/A")

    if [[ "$HEALTH" == "healthy" ]] || [[ "$STATUS" == *"Up"* && "$HEALTH" == "" ]]; then
        echo -e "  ${GREEN}✓${NC} $service: healthy"
    elif [[ "$HEALTH" == "starting" ]]; then
        echo -e "  ${YELLOW}⟳${NC} $service: starting"
    else
        echo -e "  ${RED}✗${NC} $service: unhealthy or down"
        UNHEALTHY_COUNT=$((UNHEALTHY_COUNT + 1))
    fi
done

echo ""

# Check critical endpoints
echo -e "${BLUE}[INFO]${NC} Checking Critical Endpoints:"
echo ""

# Check Kafka
if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list &>/dev/null; then
    echo -e "  ${GREEN}✓${NC} Kafka: accessible"
else
    echo -e "  ${RED}✗${NC} Kafka: not accessible"
    UNHEALTHY_COUNT=$((UNHEALTHY_COUNT + 1))
fi

# Check Dashboard
if curl -f http://localhost:8080/health &>/dev/null; then
    echo -e "  ${GREEN}✓${NC} Dashboard: accessible (http://localhost:8080)"
else
    echo -e "  ${YELLOW}⟳${NC} Dashboard: not accessible or not running"
fi

# Check Grafana
if curl -f http://localhost:3000/api/health &>/dev/null; then
    echo -e "  ${GREEN}✓${NC} Grafana: accessible (http://localhost:3000)"
else
    echo -e "  ${YELLOW}⟳${NC} Grafana: not accessible or not running"
fi

echo ""
echo -e "${BLUE}======================================"

if [ $UNHEALTHY_COUNT -eq 0 ]; then
    echo -e "${GREEN}All critical services are healthy!${NC}"
    echo -e "${BLUE}======================================"
    exit 0
else
    echo -e "${RED}Found $UNHEALTHY_COUNT unhealthy services!${NC}"
    echo -e "${BLUE}======================================"
    exit 1
fi

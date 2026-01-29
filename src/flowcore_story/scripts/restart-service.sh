#!/bin/bash

# Script to restart/deploy a single service worker
# Usage: ./scripts/restart-service.sh <service-name> [--rebuild]
#
# Examples:
#   ./scripts/restart-service.sh health-checker              # Quick restart
#   ./scripts/restart-service.sh health-checker --rebuild    # Rebuild and restart
#   ./scripts/restart-service.sh crawler-consumer-xtruyen -r # Rebuild (short form)

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get root directory
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Compose files (same as up-local.sh)
COMPOSE_FILES=(
    "-f" "docker/docker-compose.yml"
    "-f" "docker/docker-compose.external-infra.yml"
    "-f" "docker/docker-compose.local.yml"
    "-f" "docker/docker-compose.autoscale.yml"
)

# Available application services
AVAILABLE_SERVICES=(
    "crawler-producer"
    "crawler-consumer"
    "crawler-consumer-xtruyen"
    "crawler-consumer-xtruyen-scale"
    "crawler-consumer-missing"
    "queue-dispatcher-xtruyen"
    "telegram-bot"
    "database-sync-worker"
    "health-checker"
    "challenge-harvester-1"
    "challenge-harvester-2"
    "stealth-gateway"
    "enrichment-worker"
    "memory-monitor"
    "data-cleanup-worker"
    "adaptive-scaler"
    "crawler-sticky-worker"
)

# Function to show usage
usage() {
    echo -e "${GREEN}StoryFlow Service Restart Tool${NC}"
    echo ""
    echo "Usage: $0 <service-name> [--rebuild|-r]"
    echo ""
    echo "Options:"
    echo "  --rebuild, -r, --build    Rebuild Docker image before restarting"
    echo "  --help, -h                Show this help message"
    echo ""
    echo "Available services:"
    for svc in "${AVAILABLE_SERVICES[@]}"; do
        echo "  - $svc"
    done
    echo ""
    echo "Examples:"
    echo "  $0 health-checker              # Quick restart (no rebuild)"
    echo "  $0 health-checker --rebuild    # Rebuild and restart"
    echo "  $0 crawler-consumer-xtruyen -r # Rebuild (short form)"
    echo ""
}

# Function to check if service is valid
is_valid_service() {
    local service=$1
    for svc in "${AVAILABLE_SERVICES[@]}"; do
        if [ "$svc" = "$service" ]; then
            return 0
        fi
    done
    return 1
}

# Parse arguments
SERVICE_NAME=""
REBUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            usage
            exit 0
            ;;
        --rebuild|-r|--build)
            REBUILD=true
            shift
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            exit 1
            ;;
        *)
            if [ -z "$SERVICE_NAME" ]; then
                SERVICE_NAME=$1
            else
                echo -e "${RED}Error: Multiple service names provided${NC}"
                usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate service name
if [ -z "$SERVICE_NAME" ]; then
    echo -e "${RED}Error: Service name is required${NC}"
    echo ""
    usage
    exit 1
fi

if ! is_valid_service "$SERVICE_NAME"; then
    echo -e "${RED}Error: Invalid service name: $SERVICE_NAME${NC}"
    echo ""
    echo "Available services:"
    for svc in "${AVAILABLE_SERVICES[@]}"; do
        echo "  - $svc"
    done
    exit 1
fi

# Main logic
echo "=========================================="
echo "RESTART SERVICE: $SERVICE_NAME"
echo "=========================================="
echo ""

if [ "$REBUILD" = true ]; then
    echo -e "${BLUE}Mode: Rebuild and restart${NC}"
    echo -e "${YELLOW}Building new Docker image...${NC}"
    echo ""

    docker compose "${COMPOSE_FILES[@]}" \
        up -d --build --no-deps \
        "$SERVICE_NAME"

    echo ""
    echo -e "${GREEN}✓ Service rebuilt and restarted successfully${NC}"
else
    echo -e "${BLUE}Mode: Quick restart (no rebuild)${NC}"
    echo -e "${YELLOW}Restarting service...${NC}"
    echo ""

    docker compose "${COMPOSE_FILES[@]}" \
        restart "$SERVICE_NAME"

    echo ""
    echo -e "${GREEN}✓ Service restarted successfully${NC}"
fi

echo ""
echo -e "${BLUE}Service status:${NC}"
docker compose "${COMPOSE_FILES[@]}" ps "$SERVICE_NAME"

echo ""
echo -e "${BLUE}Useful commands:${NC}"
echo "  View logs:        docker compose -f docker/docker-compose.yml -f docker/docker-compose.external-infra.yml -f docker/docker-compose.local.yml -f docker/docker-compose.autoscale.yml logs -f $SERVICE_NAME"
echo "  Stop service:     docker compose -f docker/docker-compose.yml -f docker/docker-compose.external-infra.yml -f docker/docker-compose.local.yml -f docker/docker-compose.autoscale.yml stop $SERVICE_NAME"
echo "  View all status:  docker compose -f docker/docker-compose.yml -f docker/docker-compose.external-infra.yml -f docker/docker-compose.local.yml -f docker/docker-compose.autoscale.yml ps"
echo ""
echo -e "${YELLOW}Tip: Use './scripts/logs.sh $SERVICE_NAME' for quick log viewing${NC}"
echo ""

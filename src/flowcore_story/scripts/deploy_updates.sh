#!/bin/bash
# Script to deploy code updates with minimal downtime

set -e

echo "=========================================="
echo "Deploying StoryFlow Code Updates"
echo "=========================================="

cd "$(dirname "$0")/../docker" || exit 1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "\n${YELLOW}Step 1: Stopping all workers...${NC}"
docker-compose stop \
  challenge-harvester \
  crawler-producer \
  crawler-consumer \
  crawler-consumer-missing \
  database-sync-worker \
  health-checker

echo -e "\n${GREEN}✓ Workers stopped${NC}"

echo -e "\n${YELLOW}Step 2: Waiting for graceful shutdown...${NC}"
sleep 5

echo -e "\n${YELLOW}Step 3: Starting services in order...${NC}"

# Start challenge harvester first (other services depend on it)
echo -e "${YELLOW}  → Starting challenge-harvester...${NC}"
docker-compose up -d challenge-harvester
sleep 15

echo -e "${YELLOW}  → Starting crawler-producer...${NC}"
docker-compose up -d crawler-producer
sleep 10

echo -e "${YELLOW}  → Starting crawler-consumers...${NC}"
docker-compose up -d crawler-consumer crawler-consumer-missing
sleep 5

echo -e "${YELLOW}  → Starting database-sync-worker...${NC}"
docker-compose up -d database-sync-worker
sleep 5

echo -e "${YELLOW}  → Starting health-checker...${NC}"
docker-compose up -d health-checker
sleep 5

echo -e "\n${YELLOW}Step 4: Checking services health...${NC}"
docker-compose ps

echo -e "\n${YELLOW}Step 5: Waiting for all services to be healthy...${NC}"
sleep 20

# Check health status
UNHEALTHY=$(docker-compose ps | grep -c "(unhealthy)" || true)
if [ "$UNHEALTHY" -gt 0 ]; then
    echo -e "${RED}⚠ Warning: $UNHEALTHY service(s) are unhealthy${NC}"
    echo -e "${YELLOW}Check logs with: docker-compose logs -f <service-name>${NC}"
else
    echo -e "${GREEN}✓ All services are healthy!${NC}"
fi

echo -e "\n=========================================="
echo -e "${GREEN}✓ Deployment completed successfully!${NC}"
echo -e "=========================================="

echo -e "\n${YELLOW}Useful commands:${NC}"
echo "  View health-checker logs:  docker-compose logs -f health-checker"
echo "  View crawler logs:         docker-compose logs -f crawler-producer"
echo "  View all logs:             docker-compose logs -f"
echo "  Check service status:      docker-compose ps"
echo ""

#!/bin/bash

###############################################################################
# StoryFlow Core - Update Services Script
#
# Quick script to pull latest images and restart specific services
#
# Usage:
#   ./scripts/update-services.sh [service1] [service2] ...
#
# Examples:
#   ./scripts/update-services.sh crawler-producer
#   ./scripts/update-services.sh crawler-consumer crawler-producer
#   ./scripts/update-services.sh  # Updates all services
###############################################################################

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${STORYFLOW_CORE_IMAGE:-muonroii/storyflow-core:latest}"

echo -e "${BLUE}[INFO]${NC} Pulling latest image: $IMAGE"
docker pull "$IMAGE"

cd "$PROJECT_DIR/docker"

if [ $# -eq 0 ]; then
    # Update all services
    echo -e "${BLUE}[INFO]${NC} Updating all services..."
    docker compose up -d --force-recreate
else
    # Update specific services
    echo -e "${BLUE}[INFO]${NC} Updating services: $@"
    docker compose up -d --force-recreate "$@"
fi

echo -e "${GREEN}[SUCCESS]${NC} Services updated successfully!"
docker compose ps

#!/bin/bash

###############################################################################
# StoryFlow Core - Logs Viewer Script
#
# View logs from services
#
# Usage:
#   ./scripts/logs.sh [service] [options]
#
# Examples:
#   ./scripts/logs.sh crawler-producer
#   ./scripts/logs.sh crawler-consumer -f
#   ./scripts/logs.sh  # Shows all logs
###############################################################################

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_DIR/docker"

if [ $# -eq 0 ]; then
    # Show all logs
    docker compose logs --tail=100 -f
else
    # Show specific service logs
    docker compose logs "$@"
fi

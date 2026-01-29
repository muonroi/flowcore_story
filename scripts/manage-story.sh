#!/bin/bash
# Story Domain Management Script

set -e
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMMAND=$1
shift || true

case "$COMMAND" in
    up)
        echo "Starting Story Domain..."
        docker compose up -d "$@"
        ;;
    down)
        echo "Stopping Story Domain..."
        docker compose down "$@"
        ;;
    restart)
        echo "Restarting Story Domain..."
        docker compose restart "$@"
        ;;
    logs)
        docker compose logs -f "$@"
        ;;
    *)
        echo "Usage: $0 {up|down|restart|logs}"
        exit 1
        ;;
esac

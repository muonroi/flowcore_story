#!/usr/bin/env bash
set -euo pipefail

# Bring up core services using host-managed infra (Kafka/Postgres/MySQL/Mongo)
# and local source code (bind mount). Infra containers are disabled unless
# you pass --profile infra explicitly.
#
# Usage: ./scripts/up-local-host-infra.sh [docker-compose args...]
# Example: ./scripts/up-local-host-infra.sh --force-recreate

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Shift container ports off host defaults to avoid clashing with host services
export MONGO_PORT="${MONGO_PORT:-27018}"
export MYSQL_HOST_PORT="${MYSQL_HOST_PORT:-3308}"

COMPOSE_FILES=(
  "-f" "docker/docker-compose.yml"
  "-f" "docker/docker-compose.external-infra.yml"
  "-f" "docker/docker-compose.local-src.yml"
)

# Skip stub infra containers entirely (host provides these services)
SCALE_ARGS=(
  "--scale" "kafka=0"
  "--scale" "kafka-init-users=0"
  "--scale" "zookeeper=0"
  "--scale" "mongodb=0"
  "--scale" "storyflow-mongodb=0"
  "--scale" "mysql=0"
  "--scale" "storyflow-mysql=0"
  "--scale" "storyflow-postgres=0"
)

echo "[up-local-host-infra] Starting stack against host-managed infra..."
docker compose "${COMPOSE_FILES[@]}" up -d "${SCALE_ARGS[@]}" "$@"

echo "[up-local-host-infra] Done. Current services (infra containers skipped unless --profile infra):"
docker compose "${COMPOSE_FILES[@]}" ps

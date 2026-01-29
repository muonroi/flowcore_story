#!/usr/bin/env bash
set -euo pipefail

# Bootstrap default SCRAM users and ACLs for the Storyflow stack.
# The script assumes Docker Compose v2 (docker compose) and that the
# Kafka service is reachable on the secure EXTERNAL listener.

BROKER="${KAFKA_BROKER:-localhost:9092}"
ADMIN_USER="${KAFKA_SUPER_USER:-admin}"
ADMIN_PASSWORD="${KAFKA_SUPER_PASSWORD:-${KAFKA_SASL_PASSWORD:-change-me}}"
CLIENT_USER="${KAFKA_CLIENT_USER:-storyflow-client}"
CLIENT_PASSWORD="${KAFKA_CLIENT_PASSWORD:-storyflow-client-secret}"
SCRAM_MECHANISM="SCRAM-SHA-512"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to execute Kafka CLI commands" >&2
  exit 1
fi

compose() {
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
  else
    docker compose "$@"
  fi
}

exec_kafka() {
  compose exec -T kafka "$@"
}

bootstrap_user() {
  local user="$1"
  local password="$2"
  echo "Configuring SCRAM credentials for user '${user}'"
  exec_kafka kafka-configs \
    --bootstrap-server "${BROKER}" \
    --alter \
    --add-config "${SCRAM_MECHANISM}=[password=${password}]" \
    --entity-type users \
    --entity-name "${user}"
}

create_acl() {
  local user="$1"
  shift
  exec_kafka kafka-acls --bootstrap-server "${BROKER}" --add --force --allow-principal "User:${user}" "$@"
}

bootstrap_user "${ADMIN_USER}" "${ADMIN_PASSWORD}"
bootstrap_user "${CLIENT_USER}" "${CLIENT_PASSWORD}"

# Grant the admin full control
echo "Granting super user permissions to '${ADMIN_USER}'"
create_acl "${ADMIN_USER}" --operation All --resource-pattern-type Literal --topic '*' --group '*' --cluster

# Grant the Storyflow client read/write permissions on project topics
echo "Granting client topic access to '${CLIENT_USER}'"
create_acl "${CLIENT_USER}" --operation Read --operation Write --operation Create \
  --resource-pattern-type Prefixed --topic storyflow-
create_acl "${CLIENT_USER}" --operation Read --resource-pattern-type Prefixed --group storyflow-

echo "Kafka ACL bootstrap complete. Distribute the client credentials securely."

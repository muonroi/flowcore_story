#!/bin/bash
# Script to add src folder mount to docker-compose.yml for hot reload

set -e

COMPOSE_FILE="/home/storyflow-core/docker/docker-compose.yml"
BACKUP_FILE="/home/storyflow-core/docker/docker-compose.yml.backup-src-mount"

echo "=========================================="
echo "Add src mount to docker-compose.yml"
echo "=========================================="
echo

# Check if already added
if grep -q "../src:/app/src" "$COMPOSE_FILE"; then
    echo "✅ src mount already added!"
    exit 0
fi

# Backup
echo "💾 Creating backup: $BACKUP_FILE"
cp "$COMPOSE_FILE" "$BACKUP_FILE"

# Add src mount to crawler-consumer volumes
echo "📝 Adding src mount to crawler-consumer..."
sed -i '/crawler-consumer:/,/volumes:/{
    /volumes:/,/restart:/{
        /- \.\.\/proxies:\/app\/proxies/a\      - ../src:/app/src
    }
}' "$COMPOSE_FILE"

# Add src mount to crawler-consumer-missing volumes
echo "📝 Adding src mount to crawler-consumer-missing..."
sed -i '/crawler-consumer-missing:/,/volumes:/{
    /volumes:/,/restart:/{
        /- \.\.\/proxies:\/app\/proxies/a\      - ../src:/app/src
    }
}' "$COMPOSE_FILE"

# Add src mount to crawler-producer volumes (need to find volumes section first)
echo "📝 Adding src mount to crawler-producer..."
# Find the crawler-producer section and add mount after proxies line
sed -i '/crawler-producer:/,/restart:/{
    /- \.\.\/proxies:\/app\/proxies/a\      - ../src:/app/src
}' "$COMPOSE_FILE"

echo
echo "✅ Successfully added src mounts!"
echo
echo "Changes made:"
echo "  • Added '../src:/app/src' to crawler-consumer volumes"
echo "  • Added '../src:/app/src' to crawler-consumer-missing volumes"
echo "  • Added '../src:/app/src' to crawler-producer volumes"
echo
echo "This enables HOT RELOAD - code changes on host apply immediately!"
echo
echo "To restore original:"
echo "  cp $BACKUP_FILE $COMPOSE_FILE"
echo
echo "⚠️  You need to restart containers:"
echo "  cd /home/storyflow-core/docker"
echo "  docker-compose down"
echo "  docker-compose up -d crawler-producer crawler-consumer crawler-consumer-missing"
echo

#!/bin/bash
# Script to restart all worker containers with updated code

set -e

echo "=================================="
echo "Restarting StoryFlow Workers"
echo "=================================="

cd "$(dirname "$0")/../docker" || exit 1

echo ""
echo "Step 1: Stopping workers..."
echo "----------------------------"
docker-compose stop crawler-producer crawler-consumer crawler-consumer-xtruyen crawler-consumer-missing queue-dispatcher-xtruyen database-sync-worker challenge-harvester health-checker enrichment-worker telegram-bot

echo ""
echo "Step 2: Building images with new code..."
echo "----------------------------"
docker-compose build challenge-harvester crawler-producer crawler-consumer crawler-consumer-xtruyen queue-dispatcher-xtruyen database-sync-worker health-checker enrichment-worker telegram-bot

echo ""
echo "Step 3: Starting workers..."
echo "----------------------------"
# Start in order: challenge-harvester first, then producer, then consumers, then workers
docker-compose up -d challenge-harvester
sleep 10
docker-compose up -d crawler-producer
sleep 10
docker-compose up -d crawler-consumer crawler-consumer-missing crawler-consumer-xtruyen queue-dispatcher-xtruyen enrichment-worker
sleep 5
docker-compose up -d database-sync-worker health-checker telegram-bot

echo ""
echo "Step 4: Checking health status..."
echo "----------------------------"
sleep 15
docker-compose ps

echo ""
echo "=================================="
echo "✓ Workers restarted successfully!"
echo "=================================="
echo ""
echo "To view logs:"
echo "  docker-compose logs -f health-checker"
echo "  docker-compose logs -f crawler-producer"
echo "  docker-compose logs -f database-sync-worker"

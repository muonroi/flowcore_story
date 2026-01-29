#!/bin/bash

###############################################################################
# StoryFlow Core - Deployment Script
#
# This script handles deployment of StoryFlow Core services on the server.
# It pulls the latest Docker images and restarts all services.
#
# Usage:
#   ./scripts/deploy.sh [IMAGE_TAG]
#
# Examples:
#   ./scripts/deploy.sh latest
#   ./scripts/deploy.sh develop
#   ./scripts/deploy.sh v1.0.0
#   ./scripts/deploy.sh main-abc1234
###############################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DOCKER_IMAGE="${DOCKER_IMAGE:-muonroii/storyflow-core}"
IMAGE_TAG="${1:-latest}"
COMPOSE_FILE="docker/docker-compose.yml"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running on server
check_environment() {
    log_info "Checking environment..."

    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    # Check if Docker Compose is installed
    if ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi

    log_success "Environment check passed"
}

# Backup current state
backup_current_state() {
    log_info "Creating backup of current state..."

    BACKUP_DIR="$PROJECT_DIR/backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"

    # Backup environment files
    if [ -f "$PROJECT_DIR/config/env/common.env" ]; then
        cp -r "$PROJECT_DIR/config/env" "$BACKUP_DIR/" || true
    fi

    # Backup docker-compose.yml
    if [ -f "$PROJECT_DIR/$COMPOSE_FILE" ]; then
        cp "$PROJECT_DIR/$COMPOSE_FILE" "$BACKUP_DIR/" || true
    fi

    # Export current container states
    cd "$PROJECT_DIR/docker"
    docker compose ps > "$BACKUP_DIR/container_states.txt" 2>&1 || true

    log_success "Backup created at: $BACKUP_DIR"
}

# Pull latest Docker image
pull_image() {
    log_info "Pulling Docker image: $DOCKER_IMAGE:$IMAGE_TAG"

    if docker pull "$DOCKER_IMAGE:$IMAGE_TAG"; then
        log_success "Image pulled successfully"
    else
        log_error "Failed to pull image: $DOCKER_IMAGE:$IMAGE_TAG"
        exit 1
    fi
}

# Update environment variable
update_env() {
    log_info "Updating environment variables..."

    # Export image tag for docker-compose
    export STORYFLOW_CORE_IMAGE="$DOCKER_IMAGE:$IMAGE_TAG"

    # Update .env file if it exists
    if [ -f "$PROJECT_DIR/.env" ]; then
        if grep -q "STORYFLOW_CORE_IMAGE=" "$PROJECT_DIR/.env"; then
            sed -i.bak "s|STORYFLOW_CORE_IMAGE=.*|STORYFLOW_CORE_IMAGE=$DOCKER_IMAGE:$IMAGE_TAG|" "$PROJECT_DIR/.env"
        else
            echo "STORYFLOW_CORE_IMAGE=$DOCKER_IMAGE:$IMAGE_TAG" >> "$PROJECT_DIR/.env"
        fi
        log_success "Environment variables updated"
    else
        log_warning ".env file not found, using exported variable only"
    fi
}

# Stop current services
stop_services() {
    log_info "Stopping current services..."

    cd "$PROJECT_DIR/docker"

    if docker compose ps --quiet | grep -q .; then
        docker compose down --remove-orphans
        log_success "Services stopped"
    else
        log_info "No running services to stop"
    fi
}

# Start services
start_services() {
    log_info "Starting services with image: $DOCKER_IMAGE:$IMAGE_TAG"

    cd "$PROJECT_DIR/docker"
    export STORYFLOW_CORE_IMAGE="$DOCKER_IMAGE:$IMAGE_TAG"

    docker compose up -d

    log_success "Services started"
}

# Wait for services to be healthy
wait_for_services() {
    log_info "Waiting for services to be healthy..."

    local max_attempts=30
    local attempt=0

    cd "$PROJECT_DIR/docker"

    while [ $attempt -lt $max_attempts ]; do
        attempt=$((attempt + 1))

        # Check if critical services are running
        local running_services=$(docker compose ps --filter "status=running" --quiet | wc -l)

        if [ "$running_services" -gt 0 ]; then
            log_success "Services are running"
            return 0
        fi

        log_info "Attempt $attempt/$max_attempts - Waiting..."
        sleep 2
    done

    log_error "Services failed to start within expected time"
    return 1
}

# Show service status
show_status() {
    log_info "Service status:"

    cd "$PROJECT_DIR/docker"
    docker compose ps

    echo ""
    log_info "Recent logs from main services:"
    docker compose logs --tail=20 crawler-producer crawler-consumer || true
}

# Cleanup old images
cleanup_old_images() {
    log_info "Cleaning up old Docker images..."

    # Remove dangling images
    docker image prune -f

    log_success "Cleanup completed"
}

# Rollback to previous version
rollback() {
    log_error "Deployment failed. Rolling back..."

    # Find the most recent backup
    LATEST_BACKUP=$(ls -td "$PROJECT_DIR/backups"/*/ 2>/dev/null | head -1)

    if [ -n "$LATEST_BACKUP" ]; then
        log_info "Restoring from backup: $LATEST_BACKUP"

        cd "$PROJECT_DIR/docker"
        docker compose down --remove-orphans

        # Restore environment files if they exist
        if [ -d "$LATEST_BACKUP/env" ]; then
            cp -r "$LATEST_BACKUP/env/"* "$PROJECT_DIR/config/env/" || true
        fi

        # Start services with old configuration
        docker compose up -d

        log_warning "Rolled back to previous version"
    else
        log_error "No backup found for rollback"
    fi
}

# Main deployment flow
main() {
    log_info "======================================"
    log_info "StoryFlow Core Deployment"
    log_info "Image: $DOCKER_IMAGE:$IMAGE_TAG"
    log_info "======================================"
    echo ""

    # Set error handler
    trap 'rollback' ERR

    check_environment
    backup_current_state
    pull_image
    update_env
    stop_services
    start_services

    # Wait for services to be healthy
    if wait_for_services; then
        show_status
        cleanup_old_images

        echo ""
        log_success "======================================"
        log_success "Deployment completed successfully!"
        log_success "======================================"
    else
        log_error "Deployment verification failed"
        rollback
        exit 1
    fi
}

# Run main function
main "$@"

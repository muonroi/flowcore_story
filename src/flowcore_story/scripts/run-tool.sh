#!/bin/bash

# Script to run debug tools inside Docker containers
# Usage: ./scripts/run-tool.sh <tool-name> [args...]
#
# Examples:
#   ./scripts/run-tool.sh debug_site_parse xtruyen
#   ./scripts/run-tool.sh inspect_site_data
#   ./scripts/run-tool.sh test_harvester_api_v2
#   ./scripts/run-tool.sh kafka/analyze
#   ./scripts/run-tool.sh --container crawler-consumer debug_site_parse

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default container (most tools run in crawler-consumer-xtruyen)
DEFAULT_CONTAINER="crawler-consumer-xtruyen"
CONTAINER="$DEFAULT_CONTAINER"

# Get root directory
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Available containers
AVAILABLE_CONTAINERS=(
    "crawler-consumer"
    "crawler-consumer-xtruyen"
    "crawler-consumer-xtruyen-scale"
    "crawler-consumer-missing"
    "crawler-producer"
    "database-sync-worker"
    "health-checker"
    "challenge-harvester"
    "enrichment-worker"
    "telegram-bot"
)

# Function to show usage
usage() {
    echo -e "${GREEN}StoryFlow Debug Tool Runner${NC}"
    echo ""
    echo "Usage: $0 [options] <tool-name> [tool-args...]"
    echo ""
    echo "Options:"
    echo "  --container, -c <name>    Specify container to run tool in (default: $DEFAULT_CONTAINER)"
    echo "  --list, -l                List available tools"
    echo "  --help, -h                Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Run site parser debug tool"
    echo "  $0 debug_site_parse xtruyen"
    echo ""
    echo "  # Inspect site data"
    echo "  $0 inspect_site_data"
    echo ""
    echo "  # Test harvester API"
    echo "  $0 test_harvester_api_v2"
    echo ""
    echo "  # Run Kafka debug tools (shortcuts)"
    echo "  $0 kafka/analyze"
    echo "  $0 kafka/peek"
    echo "  $0 kafka/duplicates"
    echo ""
    echo "  # Run in specific container"
    echo "  $0 --container crawler-consumer debug_site_parse"
    echo ""
    echo -e "${YELLOW}Note:${NC} For Kafka tools, you can also use: ${BLUE}tools/run_kafka_debug.sh${NC}"
    echo ""
}

# Function to list available tools
list_tools() {
    echo -e "${GREEN}Available Debug Tools:${NC}"
    echo ""

    echo -e "${BLUE}Site-specific Debug Tools:${NC}"
    ls -1 tools/debug_*.py 2>/dev/null | sed 's/tools\//  /' | sed 's/.py$//' || echo "  (none found)"

    echo ""
    echo -e "${BLUE}Test Tools:${NC}"
    ls -1 tools/test_*.py 2>/dev/null | sed 's/tools\//  /' | sed 's/.py$//' || echo "  (none found)"

    echo ""
    echo -e "${BLUE}Inspection Tools:${NC}"
    ls -1 tools/inspect_*.py 2>/dev/null | sed 's/tools\//  /' | sed 's/.py$//' || echo "  (none found)"
    ls -1 tools/check_*.py 2>/dev/null | sed 's/tools\//  /' | sed 's/.py$//' || echo "  (none found)"
    ls -1 tools/query_*.py 2>/dev/null | sed 's/tools\//  /' | sed 's/.py$//' || echo "  (none found)"
    ls -1 tools/verify_*.py 2>/dev/null | sed 's/tools\//  /' | sed 's/.py$//' || echo "  (none found)"

    echo ""
    echo -e "${BLUE}Kafka Debug Tools (use kafka/<tool>):${NC}"
    ls -1 tools/kafka_debug/*.py 2>/dev/null | grep -v __pycache__ | sed 's/tools\/kafka_debug\//  kafka\//' | sed 's/.py$//' || echo "  (none found)"

    echo ""
    echo -e "${BLUE}Database Tools:${NC}"
    ls -1 tools/database_*.py 2>/dev/null | sed 's/tools\//  /' | sed 's/.py$//' || echo "  (none found)"

    echo ""
    echo -e "${YELLOW}Tip:${NC} Use '$0 <tool-name> --help' to see tool-specific options"
    echo ""
}

# Check if container is running
check_container() {
    local container=$1
    if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        echo -e "${RED}Error: Container ${container} is not running${NC}"
        echo ""
        echo "Available running containers:"
        docker ps --format '{{.Names}}' | grep -E "^(crawler-|telegram-|database-|health-|challenge-|enrichment-|memory-|data-cleanup-|queue-dispatcher)" || echo "  (none running)"
        echo ""
        echo -e "${YELLOW}Tip:${NC} Start services with: ${BLUE}./scripts/up-local.sh${NC}"
        exit 1
    fi
}

# Detect if we have a TTY
if [ -t 0 ]; then
    DOCKER_EXEC="docker exec -it"
else
    DOCKER_EXEC="docker exec"
fi

# Parse arguments
TOOL_NAME=""
TOOL_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            usage
            exit 0
            ;;
        --list|-l)
            list_tools
            exit 0
            ;;
        --container|-c)
            if [ -z "$2" ]; then
                echo -e "${RED}Error: --container requires a container name${NC}"
                exit 1
            fi
            CONTAINER="$2"
            shift 2
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            exit 1
            ;;
        *)
            if [ -z "$TOOL_NAME" ]; then
                TOOL_NAME=$1
            else
                TOOL_ARGS+=("$1")
            fi
            shift
            ;;
    esac
done

# Validate tool name
if [ -z "$TOOL_NAME" ]; then
    echo -e "${RED}Error: Tool name is required${NC}"
    echo ""
    usage
    exit 1
fi

# Check container is running
check_container "$CONTAINER"

# Handle Kafka shortcuts (kafka/analyze -> analyze_kafka)
if [[ "$TOOL_NAME" == kafka/* ]]; then
    kafka_tool="${TOOL_NAME#kafka/}"

    case "$kafka_tool" in
        analyze)
            SCRIPT_PATH="/app/tools/kafka_debug/analyze_kafka.py"
            ;;
        peek)
            SCRIPT_PATH="/app/tools/kafka_debug/deep_peek_kafka.py"
            ;;
        duplicates)
            SCRIPT_PATH="/app/tools/kafka_debug/check_duplicates.py"
            ;;
        reset-offset|reset_offset)
            SCRIPT_PATH="/app/tools/kafka_debug/reset_offset_tool_v2.py"
            echo -e "${RED}WARNING: This will reset consumer group offset!${NC}"
            echo -e "${YELLOW}This action should only be used for debugging/recovery${NC}"
            read -p "Are you sure? (yes/no): " confirm
            if [ "$confirm" != "yes" ]; then
                echo "Cancelled."
                exit 0
            fi
            ;;
        *)
            SCRIPT_PATH="/app/tools/kafka_debug/${kafka_tool}.py"
            ;;
    esac
else
    # Regular tool path
    SCRIPT_PATH="/app/tools/${TOOL_NAME}.py"
fi

# Run the tool
echo "=========================================="
echo "RUNNING TOOL: $TOOL_NAME"
echo "=========================================="
echo ""
echo -e "${BLUE}Container:${NC} $CONTAINER"
echo -e "${BLUE}Script:${NC}    $SCRIPT_PATH"
if [ ${#TOOL_ARGS[@]} -gt 0 ]; then
    echo -e "${BLUE}Args:${NC}      ${TOOL_ARGS[*]}"
fi
echo ""
echo -e "${YELLOW}Output:${NC}"
echo "---"

# Execute the tool in the container
${DOCKER_EXEC} ${CONTAINER} python ${SCRIPT_PATH} "${TOOL_ARGS[@]}"

EXIT_CODE=$?

echo "---"
echo ""

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ Tool completed successfully${NC}"
else
    echo -e "${RED}✗ Tool exited with code: $EXIT_CODE${NC}"
fi

echo ""

exit $EXIT_CODE

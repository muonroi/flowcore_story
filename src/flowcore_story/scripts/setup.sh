#!/bin/bash
# ================================================================
# 🚀 StoryFlow-Core: ONE-CLICK SETUP SCRIPT
# ================================================================
# Chạy script này sau khi pull code để setup toàn bộ hệ thống
# với env values từ production config
# ================================================================

set -e

echo "🚀 StoryFlow-Core: One-Click Setup"
echo "===================================="
echo ""

# Get project root (go up one level from scripts folder)
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$PROJECT_ROOT"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not installed!${NC}"
    echo "   Install: curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose not installed!${NC}"
    echo "   Install: sudo apt-get install docker-compose-plugin"
    exit 1
fi

echo -e "${GREEN}✅ Docker: $(docker --version)${NC}"
echo -e "${GREEN}✅ Docker Compose: $(docker-compose --version)${NC}"
echo ""

# ================================================================
# STEP 1: Create directories
# ================================================================
echo -e "${YELLOW}📁 Step 1: Creating directories...${NC}"

mkdir -p config/env
mkdir -p proxies
mkdir -p state
mkdir -p logs
mkdir -p truyen_data
mkdir -p completed_stories
mkdir -p backup

echo -e "${GREEN}✅ Directories created${NC}"
echo ""

# ================================================================
# STEP 2: Create environment files with production values
# ================================================================
echo -e "${YELLOW}📝 Step 2: Creating environment files...${NC}"

# 2.1 Common env
cat > config/env/common.env << 'EOF'
# Common non-sensitive settings - Auto-generated with production values

DATA_FOLDER=/app/truyen_data
COMPLETED_FOLDER=/app/completed_stories
BACKUP_FOLDER=/app/backup
STATE_FOLDER=/app/state
LOG_FOLDER=/app/logs
SKIPPED_STORIES_FILE=/app/state/skipped_stories.json
FAILED_GENRES_FILE=/app/state/failed_genres.json
ERROR_CHAPTERS_FILE=/app/state/error_chapters.json
MISSING_CHAPTERS_FILE=/app/state/missing_chapters.json
BANNED_PROXIES_LOG=/app/state/banned_proxies.log
PROXIES_FOLDER=/app/proxies
PROXIES_FILE=/app/proxies/proxies.txt
CATEGORY_SNAPSHOT_DB_PATH=/app/state/category_snapshot.db
METADATA_DB_PATH=/app/state/metadata.db
USE_PROXY=false
PATTERN_FILE=/app/config/blacklist_patterns.txt
ANTI_BOT_PATTERN_FILE=/app/config/anti_bot_patterns.txt

# Kafka settings
KAFKA_TOPIC=storyflow.crawl
KAFKA_BROKERS=kafka:29092
KAFKA_GROUP_ID=storyflow
PROGRESS_TOPIC=storyflow.crawl.progress
PROGRESS_GROUP_ID=storyflow-progress
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_USERNAME=admin
KAFKA_PASSWORD=change-me
KAFKA_SSL_VERIFY=0
KAFKA_SSL_CA_FILE=
KAFKA_BOOTSTRAP_MAX_RETRIES=5
KAFKA_BOOTSTRAP_RETRY_DELAY=5

# Crawler tuning (optimized for stability)
REQUEST_DELAY=4
TIMEOUT_REQUEST=30
RETRY_ATTEMPTS=3
DELAY_ON_RETRY=1.5
RETRY_GENRE_ROUND_LIMIT=3
RETRY_SLEEP_SECONDS=5
RETRY_FAILED_CHAPTERS_PASSES=3
NUM_CHAPTER_BATCHES=3
RETRY_STORY_ROUND_LIMIT=3
MAX_CHAPTER_RETRY=3
ASYNC_SEMAPHORE_LIMIT=3
GENRE_ASYNC_LIMIT=2
GENRE_BATCH_SIZE=2
STORY_ASYNC_LIMIT=2
STORY_BATCH_SIZE=2
BATCH_SIZE=2
MAX_CHAPTERS_PER_STORY=0
MAX_GENRES_TO_CRAWL=2
MAX_STORIES_PER_GENRE_PAGE=2
MAX_STORIES_TOTAL_PER_GENRE=0
MAX_CHAPTER_PAGES_TO_CRAWL=0

# Story duplicate detection
STORY_LOOP_MIN_ITEMS=60
STORY_LOOP_DUPLICATE_RATIO=0.6
STORY_LOOP_MAX_PATTERN_LENGTH=12
STORY_LOOP_MIN_PATTERN_REPETITIONS=3

# Category refresh
CATEGORY_BATCH_JOB_SIZE=0
CATEGORY_MAX_JOBS_PER_BATCH=0
CATEGORY_MAX_JOBS_PER_DOMAIN=0
CATEGORY_CHANGE_REFRESH_RATIO=0.35
CATEGORY_CHANGE_REFRESH_ABSOLUTE=50
CATEGORY_REFRESH_BATCH_SIZE=6
CATEGORY_CHANGE_MIN_STORIES=40

# AI configuration
AI_PROFILES_PATH=/app/state/ai/profiles.json
AI_METRICS_PATH=/app/state/ai/metrics.json
AI_MODEL=gpt-3.5-turbo
AI_PROFILE_TTL_HOURS=24
AI_TRIM_MAX_BYTES=4096
OPENAI_BASE=https://api.openai.com/v1
AI_PRINT_METRICS=0

# Base URLs
BASE_XTRUYEN=https://xtruyen.vn
BASE_TANGTHUVIEN=https://tangthuvien.net
BASE_TRUYENCOM=https://truyencom.com
ENABLED_SITE_KEYS=xtruyen,tangthuvien,truyencom

# Dashboard settings
DASHBOARD_PERCENT_MODE=stories
DASHBOARD_HIDE_SITES=
KAFKA_BACKLOG_WARN=100
KAFKA_BACKLOG_ERROR=1000
CRAWL_ERROR_RATE_WARN=0.15
CRAWL_ERROR_RATE_ERROR=0.3
DEAD_STORY_WARN_THRESHOLD=5
DEAD_STORY_ERROR_THRESHOLD=20
DEAD_STORY_REFRESH_SECONDS=300
PROMETHEUS_PUSHGATEWAY_URL=
PROMETHEUS_JOB_NAME=storyflow_crawler
PROMETHEUS_PUSH_INTERVAL=60
STORYFLOW_DASHBOARD_FILE=/app/state/dashboard.json
ENABLE_FILE_LOGS=1

# Crawl mode
MODE=all_sites
CRAWL_MODE=full

# Missing chapter monitoring
MISSING_CRAWL_TIMEOUT_SECONDS=60
MISSING_CRAWL_TIMEOUT_PER_CHAPTER=2.0
MISSING_CRAWL_TIMEOUT_MAX=180
MISSING_WARNING_TOPIC=storyflow.missing
MISSING_WARNING_GROUP=storyflow-missing-group

# Progress optimization
PROGRESS_DISABLE_CATEGORIES=worker,retry_worker,single_story_worker,runner,snapshot
EOF

echo -e "${GREEN}✅ Created config/env/common.env${NC}"

# 2.2 Crawler env
cat > config/env/crawler.env << 'EOF'
# Crawler sensitive settings - Auto-generated

KAFKA_USERNAME=admin
KAFKA_PASSWORD=change-me
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SSL_VERIFY=0
KAFKA_SSL_CA_FILE=

# Proxy settings (ADD YOUR PROXY HERE)
USE_PROXY=true
PROXY_USER=
PROXY_PASS=
PROXY_API_URL=

# AI provider (optional)
OPENAI_API_KEY=
OPENAI_BASE=https://api.openai.com/v1

# Challenge harvester
CHALLENGE_HARVESTER_ENABLED=true
CHALLENGE_HARVESTER_URL=http://challenge-harvester:8099/harvest
CHALLENGE_HARVESTER_TIMEOUT=60

# Playwright fallback
PLAYWRIGHT_MAX_RETRIES=3
PLAYWRIGHT_REQUEST_TIMEOUT=25
PLAYWRIGHT_WAIT_FOR_NETWORKIDLE=false
EOF

echo -e "${GREEN}✅ Created config/env/crawler.env${NC}"

# 2.3 Fingerprint env
cat > config/env/fingerprint.env << 'EOF'
# Fingerprinting configuration - Auto-generated

ENABLE_FINGERPRINT_POOL=true
FINGERPRINT_STORAGE_DIR=./state
FINGERPRINT_POOL_SIZE_PER_PROXY=5
FINGERPRINT_ROTATION_INTERVAL=3600

# TLS/HTTP2
ENABLE_HTTP2=true
ENABLE_HTTP3=false
PREFER_HTTP3=false

# Headers randomization
ENABLE_HEADERS_RANDOMIZATION=true
RANDOMIZE_HEADERS_ORDER=true
RANDOMIZE_HEADERS_CASING=false
RANDOMIZE_HEADERS_SPACING=false

# Cookie integration
COOKIE_NODE_ID=worker-01
COOKIE_CACHE_L1_SIZE=256
COOKIE_COOLDOWN_SECONDS=120
COOKIE_MAX_COOLDOWN_SECONDS=3600
COOKIE_COOLDOWN_THRESHOLD=2

# Monitoring
FINGERPRINT_LOG_LEVEL=INFO
FINGERPRINT_VERBOSE_LOGGING=false

# Advanced
FINGERPRINT_MAX_AGE_HOURS=48
FINGERPRINT_AUTO_CLEANUP_INTERVAL=7200
FINGERPRINT_DIVERSITY_FACTOR=1.0
HTTP2_SETTINGS_VARIATION=0.05
RANDOMIZE_TLS_EXTENSIONS=false

# Performance
FINGERPRINT_CACHE_SCRIPTS=true
FINGERPRINT_PARALLEL_GENERATION=true
CACHE_HTTP2_VARIANTS=true
EOF

echo -e "${GREEN}✅ Created config/env/fingerprint.env${NC}"

# 2.4 Challenge harvester env
cat > config/env/challenge_harvester.env << 'EOF'
# Challenge harvester settings - Auto-generated

CHALLENGE_HARVESTER_HOST=0.0.0.0
CHALLENGE_HARVESTER_PORT=8099
CHALLENGE_HARVESTER_HEADFUL=false
CHALLENGE_HARVESTER_NAVIGATION_TIMEOUT=60
CHALLENGE_HARVESTER_DELAY_MIN=0.3
CHALLENGE_HARVESTER_DELAY_MAX=0.9
CHALLENGE_HARVESTER_WAIT_FOR_NETWORKIDLE=false
CHALLENGE_HARVESTER_MAX_RETRIES=2
CHALLENGE_HARVESTER_RETRY_DELAY=2.0

# Turnstile solver API (optional - ADD KEY HERE)
TURNSTILE_SOLVER_API_KEY=
EOF

echo -e "${GREEN}✅ Created config/env/challenge_harvester.env${NC}"

# 2.5 Dashboard env
cat > config/env/dashboard.env << 'EOF'
# Dashboard credentials - Auto-generated

DASHBOARD_KAFKA_USERNAME=dashboard
DASHBOARD_KAFKA_PASSWORD=dashboard-secret
EOF

echo -e "${GREEN}✅ Created config/env/dashboard.env${NC}"

# 2.6 Telegram env
cat > config/env/telegram.env << 'EOF'
# Telegram bot settings - Auto-generated (ADD YOUR TOKENS)

TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_THREAD_ID=
EOF

echo -e "${GREEN}✅ Created config/env/telegram.env${NC}"

# 2.7 Database env
cat > config/env/database.env << 'EOF'
# Database sync settings - Auto-generated

DATABASE_TYPE=mysql
DATABASE_HOST=storyflow-mysql
DATABASE_PORT=3306
DATABASE_USER=storyflow
DATABASE_PASSWORD=storyflow_password
DATABASE_NAME=storyflow
DATABASE_SYNC_INTERVAL=60
EOF

echo -e "${GREEN}✅ Created config/env/database.env${NC}"

echo ""

# ================================================================
# STEP 3: Create state files
# ================================================================
echo -e "${YELLOW}📄 Step 3: Creating state files...${NC}"

echo "[]" > state/skipped_stories.json
echo "[]" > state/failed_genres.json
echo "[]" > state/error_chapters.json
echo "[]" > state/missing_chapters.json
echo "{}" > state/dashboard.json
touch state/banned_proxies.log

# Create proxies file with example
cat > proxies/proxies.txt << 'EOF'
# Add your proxies here, one per line
# Format: http://user:pass@host:port
# Example: http://user-ZPHsTEjW-region-vn:password@101.46.139.241:12233
EOF

echo -e "${GREEN}✅ State files created${NC}"
echo ""

# ================================================================
# STEP 4: Make scripts executable
# ================================================================
echo -e "${YELLOW}🔧 Step 4: Making scripts executable...${NC}"

chmod +x docker/*.sh
chmod +x scripts/setup.sh

echo -e "${GREEN}✅ Scripts executable${NC}"
echo ""

# ================================================================
# STEP 5: Start Docker containers
# ================================================================
echo -e "${YELLOW}🐳 Step 5: Starting Docker containers...${NC}"

cd docker
docker-compose down 2>/dev/null || true
docker-compose up -d --build

echo -e "${GREEN}✅ Containers started${NC}"
echo ""

# ================================================================
# STEP 6: Wait for containers
# ================================================================
echo -e "${YELLOW}⏳ Step 6: Waiting for containers (60s)...${NC}"
sleep 60
echo -e "${GREEN}✅ Containers should be ready${NC}"
echo ""

# ================================================================
# STEP 7: Setup auto-restart
# ================================================================
echo -e "${YELLOW}⏰ Step 7: Setting up auto-restart...${NC}"

bash setup-auto-restart.sh

echo -e "${GREEN}✅ Auto-restart configured${NC}"
echo ""

# ================================================================
# STEP 8: Health check
# ================================================================
echo -e "${YELLOW}🏥 Step 8: Final health check...${NC}"

bash check-health.sh

echo ""

# ================================================================
# DONE!
# ================================================================
echo "=================================================================="
echo -e "${GREEN}🎉 SETUP COMPLETE!${NC}"
echo "=================================================================="
echo ""
echo "📊 Services Status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "NAMES|crawler|realtime|grafana"
echo ""
echo "🎯 Next Steps:"
echo "  1. ✅ Add your proxies to: proxies/proxies.txt"
echo "  2. ✅ Add Telegram tokens to: config/env/telegram.env (optional)"
echo "  3. ✅ Add Turnstile API key to: config/env/challenge_harvester.env (optional)"
echo "  4. ✅ Monitor dashboard: http://localhost:8080"
echo "  5. ✅ Check Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "🔧 Management Commands:"
echo "  Manual restart:   bash docker/restart-crawlers.sh"
echo "  Health check:     bash docker/check-health.sh"
echo "  View logs:        docker logs -f crawler-producer"
echo "  Auto-restart log: tail -f logs/auto-restart.log"
echo ""
echo "📋 Auto-Restart:"
crontab -l | grep restart-crawlers || echo "  (Not configured - run 'bash docker/setup-auto-restart.sh')"
echo ""
echo "🎉 System is ready for crawling!"


# State Cleanup & Database Strategy

## Executive Summary

**Current Status**:
- ✅ PostgreSQL đã được implement và đang hoạt động
- ⚠️ Không có strategy cleanup state outdated
- ✅ Schema design tốt với 10 tables + views + helper functions
- 📊 Data size hiện tại: 1.5 MB (16 crawl states)

**Recommendations**:
1. **Keep PostgreSQL** - Không cần migrate sang MySQL/MongoDB
2. **Implement State Cleanup** - Automated cleanup cho outdated data
3. **Add Archival Strategy** - Archive old data thay vì delete

---

## Part 1: Database Choice Analysis

### Current: PostgreSQL ✅ RECOMMENDED

**Lý do nên giữ PostgreSQL**:

1. **Schema đã tối ưu**:
   - JSONB support (flexible + indexed)
   - Array types (processed_urls, etc.)
   - Generated columns (failure_rate)
   - Advanced indexing (GIN, partial indexes)

2. **Performance tốt**:
   - Size nhỏ: 1.5 MB (16 states)
   - Fast queries với indexes
   - Transaction support
   - Connection pooling implemented

3. **Features cần thiết**:
   - ACID compliance
   - Complex queries support
   - Full-text search (future)
   - Replication support

4. **Already production-ready**:
   - Đã có migrations
   - Đã có circuit breaker
   - Đã có batch writer
   - Đã có health checks

### Comparison with Alternatives

#### MongoDB ❌ NOT RECOMMENDED

**Cons**:
- Không cần schema flexibility (đã có JSONB)
- Thêm complexity (driver, deployment)
- Loss của ACID transactions
- Không có generated columns
- **Migration cost**: High effort, no benefit

**When to use**:
- Document-heavy workloads
- Unstructured data
- Need horizontal scaling NOW

**Verdict**: Không cần thiết cho use case hiện tại

#### MySQL ❌ NOT RECOMMENDED

**Cons**:
- Yếu hơn PostgreSQL về JSON support
- Không có array types native
- Đã có migrations cho PostgreSQL
- **Migration cost**: Medium-high effort, minimal benefit

**Pros**:
- Slightly simpler (debatable)
- Wider hosting support (not an issue for self-host)

**Verdict**: Downgrade không cần thiết

#### Keep PostgreSQL ✅ RECOMMENDED

**Decision**: **STAY WITH POSTGRESQL**

Lý do:
1. ✅ Already implemented and working
2. ✅ Best-in-class for this use case
3. ✅ No migration cost
4. ✅ Room to grow
5. ✅ Industry standard for complex data

---

## Part 2: State Cleanup Strategy

### Problem Analysis

**Current Situation**:
```sql
-- 16 crawl states, oldest 5 hours ago
SELECT
    site_key,
    worker_id,
    updated_at,
    NOW() - updated_at as age
FROM crawl_states
ORDER BY updated_at;
```

**Issues**:
1. ❌ State grows indefinitely (processed_urls arrays)
2. ❌ No cleanup of completed stories
3. ❌ Old story_events accumulate (if used)
4. ❌ No archival strategy

**Impact**:
- **Current**: Minimal (1.5 MB)
- **6 months**: ~50 MB (estimated)
- **1 year**: ~100 MB
- **Risk**: Array bloat, slow queries

### Cleanup Strategy Design

#### 1. Retention Policy

```yaml
Data Type             Retention    Action
─────────────────────────────────────────────
Active crawl_states   Forever      Keep + Trim
Inactive crawl_states 30 days      Archive
story_events          30 days      Delete
story_progress        Completed    90 days → Archive
genre_progress        Completed    90 days → Archive
site_health           Forever      Keep
```

#### 2. Array Trimming Strategy

**Problem**: `processed_story_urls` can grow to 10,000+ items

**Solution**: Keep recent N items only

```sql
-- Trim array to last 5000 items
UPDATE crawl_states
SET processed_story_urls = processed_story_urls[
    GREATEST(1, array_length(processed_story_urls, 1) - 5000):
]
WHERE array_length(processed_story_urls, 1) > 5000;
```

**Limits**:
- `processed_story_urls`: 5,000 items
- `processed_genre_urls`: 1,000 items
- `globally_completed_story_urls`: 10,000 items

#### 3. Automated Cleanup Schedule

```
Frequency    Task
─────────────────────────────────────────────
Daily        - Trim arrays
             - Delete old story_events (>30d)

Weekly       - Archive completed stories (>90d)
             - Archive completed genres (>90d)
             - Vacuum database

Monthly      - Deep cleanup
             - Generate analytics
             - Backup before cleanup
```

---

## Part 3: Implementation Plan

### Phase 1: Add Cleanup Functions (Week 1)

Create `migrations/004_add_cleanup_functions.sql`:

```sql
-- =============================================================================
-- STATE CLEANUP FUNCTIONS
-- =============================================================================

-- 1. Trim arrays in crawl_states
CREATE OR REPLACE FUNCTION trim_crawl_state_arrays()
RETURNS TABLE(
    site_key VARCHAR,
    trimmed_story_urls INTEGER,
    trimmed_genre_urls INTEGER,
    trimmed_completed_urls INTEGER
) AS $$
BEGIN
    RETURN QUERY
    UPDATE crawl_states
    SET
        processed_story_urls = CASE
            WHEN array_length(processed_story_urls, 1) > 5000
            THEN processed_story_urls[
                array_length(processed_story_urls, 1) - 5000 + 1:
            ]
            ELSE processed_story_urls
        END,
        processed_genre_urls = CASE
            WHEN array_length(processed_genre_urls, 1) > 1000
            THEN processed_genre_urls[
                array_length(processed_genre_urls, 1) - 1000 + 1:
            ]
            ELSE processed_genre_urls
        END,
        globally_completed_story_urls = CASE
            WHEN array_length(globally_completed_story_urls, 1) > 10000
            THEN globally_completed_story_urls[
                array_length(globally_completed_story_urls, 1) - 10000 + 1:
            ]
            ELSE globally_completed_story_urls
        END
    RETURNING
        crawl_states.site_key,
        GREATEST(0, array_length(processed_story_urls, 1) - 5000),
        GREATEST(0, array_length(processed_genre_urls, 1) - 1000),
        GREATEST(0, array_length(globally_completed_story_urls, 1) - 10000);
END;
$$ LANGUAGE plpgsql;

-- 2. Archive old completed stories
CREATE OR REPLACE FUNCTION archive_old_completed_stories(
    retention_days INTEGER DEFAULT 90
)
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    -- Insert into archive table (create if needed)
    CREATE TABLE IF NOT EXISTS story_progress_archive (LIKE story_progress INCLUDING ALL);

    WITH archived AS (
        DELETE FROM story_progress
        WHERE status = 'completed'
          AND completed_at < NOW() - (retention_days || ' days')::INTERVAL
        RETURNING *
    )
    INSERT INTO story_progress_archive SELECT * FROM archived;

    GET DIAGNOSTICS archived_count = ROW_COUNT;
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- 3. Archive old completed genres
CREATE OR REPLACE FUNCTION archive_old_completed_genres(
    retention_days INTEGER DEFAULT 90
)
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    CREATE TABLE IF NOT EXISTS genre_progress_archive (LIKE genre_progress INCLUDING ALL);

    WITH archived AS (
        DELETE FROM genre_progress
        WHERE status = 'completed'
          AND completed_at < NOW() - (retention_days || ' days')::INTERVAL
        RETURNING *
    )
    INSERT INTO genre_progress_archive SELECT * FROM archived;

    GET DIAGNOSTICS archived_count = ROW_COUNT;
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- 4. Cleanup old inactive states
CREATE OR REPLACE FUNCTION cleanup_inactive_states(
    retention_days INTEGER DEFAULT 30
)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Archive first
    CREATE TABLE IF NOT EXISTS crawl_states_archive (LIKE crawl_states INCLUDING ALL);

    WITH archived AS (
        DELETE FROM crawl_states
        WHERE updated_at < NOW() - (retention_days || ' days')::INTERVAL
          AND NOT EXISTS (
              -- Keep if referenced by active stories
              SELECT 1 FROM story_progress sp
              WHERE sp.primary_site = crawl_states.site_key
                AND sp.status IN ('queued', 'running', 'cooldown')
          )
        RETURNING *
    )
    INSERT INTO crawl_states_archive SELECT * FROM archived;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- 5. Comprehensive cleanup - runs all cleanup tasks
CREATE OR REPLACE FUNCTION run_comprehensive_cleanup()
RETURNS TABLE(
    task VARCHAR,
    items_affected INTEGER,
    status VARCHAR
) AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- 1. Trim arrays
    SELECT COUNT(*) INTO v_count FROM trim_crawl_state_arrays();
    RETURN QUERY SELECT 'trim_arrays'::VARCHAR, v_count, 'completed'::VARCHAR;

    -- 2. Delete old story events
    SELECT cleanup_old_story_events(30) INTO v_count;
    RETURN QUERY SELECT 'delete_story_events'::VARCHAR, v_count, 'completed'::VARCHAR;

    -- 3. Archive completed stories
    SELECT archive_old_completed_stories(90) INTO v_count;
    RETURN QUERY SELECT 'archive_stories'::VARCHAR, v_count, 'completed'::VARCHAR;

    -- 4. Archive completed genres
    SELECT archive_old_completed_genres(90) INTO v_count;
    RETURN QUERY SELECT 'archive_genres'::VARCHAR, v_count, 'completed'::VARCHAR;

    -- 5. Cleanup inactive states
    SELECT cleanup_inactive_states(30) INTO v_count;
    RETURN QUERY SELECT 'cleanup_states'::VARCHAR, v_count, 'completed'::VARCHAR;

    -- 6. Vacuum analyze
    VACUUM ANALYZE;
    RETURN QUERY SELECT 'vacuum_analyze'::VARCHAR, 0, 'completed'::VARCHAR;

    RETURN;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- MONITORING FUNCTIONS
-- =============================================================================

-- Get cleanup statistics
CREATE OR REPLACE VIEW v_cleanup_stats AS
SELECT
    'crawl_states' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN updated_at > NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) as active_7d,
    SUM(CASE WHEN updated_at > NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END) as active_30d,
    MAX(array_length(processed_story_urls, 1)) as max_story_urls,
    AVG(array_length(processed_story_urls, 1))::INTEGER as avg_story_urls,
    pg_size_pretty(pg_total_relation_size('crawl_states')) as size
FROM crawl_states
UNION ALL
SELECT
    'story_progress',
    COUNT(*),
    SUM(CASE WHEN updated_at > NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END),
    SUM(CASE WHEN updated_at > NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END),
    NULL,
    NULL,
    pg_size_pretty(pg_total_relation_size('story_progress'))
FROM story_progress
UNION ALL
SELECT
    'story_events',
    COUNT(*),
    SUM(CASE WHEN created_at > NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END),
    SUM(CASE WHEN created_at > NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END),
    NULL,
    NULL,
    pg_size_pretty(pg_total_relation_size('story_events'))
FROM story_events;

COMMENT ON FUNCTION trim_crawl_state_arrays IS 'Trims large arrays in crawl_states to prevent bloat';
COMMENT ON FUNCTION archive_old_completed_stories IS 'Moves completed stories older than N days to archive table';
COMMENT ON FUNCTION cleanup_inactive_states IS 'Removes inactive crawl states older than N days';
COMMENT ON FUNCTION run_comprehensive_cleanup IS 'Runs all cleanup tasks and returns summary';
```

### Phase 2: Create Cleanup Worker (Week 1-2)

Create `src/storyflow_core/workers/state_cleanup_worker.py`:

```python
"""
State Cleanup Worker
Automatically cleans up outdated state data
"""

import asyncio
import os
from datetime import datetime
from typing import Dict, Any

from storyflow_core.storage.db_pool import get_db_pool
from storyflow_core.utils.logger import logger


class StateCleanupWorker:
    """Worker to clean up outdated state data"""

    def __init__(self):
        self.cleanup_interval = int(os.getenv("CLEANUP_INTERVAL", "86400"))  # Daily
        self.retention_days = int(os.getenv("STATE_RETENTION_DAYS", "90"))
        self.enable_cleanup = os.getenv("ENABLE_STATE_CLEANUP", "true").lower() == "true"

    async def run_cleanup(self) -> Dict[str, Any]:
        """Run comprehensive cleanup"""
        pool = await get_db_pool()
        if not pool:
            logger.error("[Cleanup] DB pool not available")
            return {}

        try:
            async with pool.acquire() as conn:
                # Run comprehensive cleanup function
                results = await conn.fetch("SELECT * FROM run_comprehensive_cleanup()")

                summary = {}
                for row in results:
                    task = row['task']
                    count = row['items_affected']
                    summary[task] = count
                    logger.info(f"[Cleanup] {task}: {count} items")

                return summary

        except Exception as e:
            logger.error(f"[Cleanup] Error: {e}")
            return {}

    async def get_cleanup_stats(self) -> Dict[str, Any]:
        """Get cleanup statistics"""
        pool = await get_db_pool()
        if not pool:
            return {}

        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM v_cleanup_stats")
                stats = {}
                for row in rows:
                    stats[row['table_name']] = dict(row)
                return stats
        except Exception as e:
            logger.error(f"[Cleanup] Error getting stats: {e}")
            return {}

    async def run(self):
        """Main loop"""
        logger.info(f"[Cleanup] Worker started")
        logger.info(f"[Cleanup] Interval: {self.cleanup_interval}s")
        logger.info(f"[Cleanup] Retention: {self.retention_days} days")
        logger.info(f"[Cleanup] Enabled: {self.enable_cleanup}")

        while True:
            try:
                if self.enable_cleanup:
                    logger.info("[Cleanup] Starting cleanup cycle...")

                    # Get stats before
                    stats_before = await self.get_cleanup_stats()
                    logger.info(f"[Cleanup] Stats before: {stats_before}")

                    # Run cleanup
                    summary = await self.run_cleanup()
                    logger.info(f"[Cleanup] Summary: {summary}")

                    # Get stats after
                    stats_after = await self.get_cleanup_stats()
                    logger.info(f"[Cleanup] Stats after: {stats_after}")

                    logger.info("[Cleanup] Cleanup cycle completed")
                else:
                    logger.debug("[Cleanup] Cleanup disabled, skipping...")

                # Wait for next cycle
                await asyncio.sleep(self.cleanup_interval)

            except KeyboardInterrupt:
                logger.info("[Cleanup] Worker stopped by user")
                break
            except Exception as e:
                logger.error(f"[Cleanup] Error in main loop: {e}")
                await asyncio.sleep(60)


async def main():
    worker = StateCleanupWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

### Phase 3: Add to Docker Compose (Week 2)

Add to `docker/docker-compose.yml`:

```yaml
  state-cleanup-worker:
    image: ${STORYFLOW_CORE_IMAGE:-muonroii/storyflow-core:0.1.0}
    container_name: state-cleanup-worker
    restart: unless-stopped
    command: sh -c "export PYTHONPATH=/app/src && cd /app/src && python storyflow_core/workers/state_cleanup_worker.py"
    environment:
      PYTHONPATH: /app/src
      ENABLE_FILE_LOGS: ${ENABLE_FILE_LOGS:-0}
      # Cleanup settings
      ENABLE_STATE_CLEANUP: ${ENABLE_STATE_CLEANUP:-true}
      CLEANUP_INTERVAL: ${CLEANUP_INTERVAL:-86400}  # 24h
      STATE_RETENTION_DAYS: ${STATE_RETENTION_DAYS:-90}
    env_file:
      - ../config/env/common.env
      - ../config/env/database.env
    volumes:
      - ../state:/app/state
      - ../logs:/app/logs
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck:
      test: [ "CMD-SHELL", "pgrep -f 'python.*state_cleanup_worker' > /dev/null || exit 1" ]
      interval: 60s
      timeout: 10s
      retries: 3
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 512M
```

### Phase 4: Manual Cleanup Scripts (Week 2)

Create `scripts/cleanup_state.sh`:

```bash
#!/bin/bash
# Manual state cleanup script

set -e

echo "=== State Cleanup Script ==="
echo "Time: $(date)"
echo ""

# Connect to database
CONTAINER="storyflow-postgres"
DB_USER="storyflow"
DB_NAME="storyflow_core"

echo "1. Getting current stats..."
docker exec $CONTAINER psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM v_cleanup_stats;"

echo ""
echo "2. Running cleanup..."
docker exec $CONTAINER psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM run_comprehensive_cleanup();"

echo ""
echo "3. Getting updated stats..."
docker exec $CONTAINER psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM v_cleanup_stats;"

echo ""
echo "4. Database size..."
docker exec $CONTAINER psql -U $DB_USER -d $DB_NAME -c "
    SELECT
        pg_size_pretty(pg_database_size('$DB_NAME')) as database_size,
        pg_size_pretty(pg_total_relation_size('crawl_states')) as crawl_states_size,
        pg_size_pretty(pg_total_relation_size('story_progress')) as story_progress_size;
"

echo ""
echo "Cleanup completed!"
```

---

## Part 4: Monitoring & Alerting

### Metrics to Track

```python
# In state_metrics.py or dashboard

cleanup_metrics = {
    "last_cleanup_at": datetime,
    "items_cleaned": {
        "story_events": int,
        "archived_stories": int,
        "archived_genres": int,
        "trimmed_arrays": int,
    },
    "database_size_mb": float,
    "largest_arrays": {
        "site_key": str,
        "array_size": int,
    },
    "cleanup_duration_seconds": float,
}
```

### Alerts

```yaml
Condition                         Alert Level   Action
──────────────────────────────────────────────────────────
Array size > 10,000              Warning       Log + trim
Array size > 50,000              Critical      Alert + trim
DB size > 1 GB                   Warning       Review
story_events > 100,000           Warning       Delete old
Cleanup failed                   Critical      Alert admin
```

---

## Part 5: Migration Plan (If Needed)

### SQLite → PostgreSQL Migration

**Status**: ✅ Already completed (PostgreSQL is active)

If you have old SQLite data to migrate:

```python
# scripts/migrate_sqlite_to_postgres.py

import sqlite3
import asyncpg
import asyncio

async def migrate_sqlite_to_postgres():
    # Connect to SQLite
    sqlite_conn = sqlite3.connect('state.db')
    sqlite_conn.row_factory = sqlite3.Row

    # Connect to PostgreSQL
    pg_conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='storyflow',
        password='storyflow_secret',
        database='storyflow_core'
    )

    # Migrate crawl_states
    cursor = sqlite_conn.execute("SELECT * FROM crawl_states")
    for row in cursor:
        await pg_conn.execute("""
            INSERT INTO crawl_states (...)
            VALUES (...)
            ON CONFLICT (site_key, worker_id) DO UPDATE ...
        """, ...)

    await pg_conn.close()
    sqlite_conn.close()
```

---

## Part 6: Rollout Schedule

### Week 1: Preparation
- [x] Review current state (this document)
- [ ] Create migration `004_add_cleanup_functions.sql`
- [ ] Test cleanup functions in development
- [ ] Create cleanup worker code

### Week 2: Implementation
- [ ] Deploy migration to production
- [ ] Add cleanup worker to docker-compose
- [ ] Deploy cleanup worker
- [ ] Monitor for 48 hours

### Week 3: Validation
- [ ] Verify cleanup working
- [ ] Check database size reduction
- [ ] Review logs for errors
- [ ] Adjust retention policies if needed

### Week 4: Optimization
- [ ] Fine-tune cleanup intervals
- [ ] Add Grafana dashboards
- [ ] Document procedures
- [ ] Train team on manual cleanup

---

## Conclusion

### Decision Summary

1. **Database**: ✅ Keep PostgreSQL (no migration needed)
2. **Cleanup**: ✅ Implement automated cleanup
3. **Retention**:
   - Active states: Forever (with array trimming)
   - Completed stories/genres: 90 days → archive
   - Events: 30 days → delete
4. **Schedule**: Daily cleanup + weekly deep clean

### Expected Benefits

- **Storage**: 70-80% reduction in growth rate
- **Performance**: Faster queries (smaller arrays)
- **Maintenance**: Automated, no manual intervention
- **Scalability**: Can handle 10x more stories

### Next Steps

1. Review and approve this strategy
2. Create migration `004_add_cleanup_functions.sql`
3. Implement cleanup worker
4. Deploy to production
5. Monitor and adjust

---

## References

- [PostgreSQL Array Documentation](https://www.postgresql.org/docs/current/arrays.html)
- [VACUUM Best Practices](https://www.postgresql.org/docs/current/routine-vacuuming.html)
- [Archival Strategies](https://wiki.postgresql.org/wiki/Archive)

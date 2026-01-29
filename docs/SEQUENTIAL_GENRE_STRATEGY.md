# Sequential Genre Completion Strategy

> **Tối ưu crawl bằng cách xử lý từng genre đến hoàn thành thay vì discover lại toàn bộ**

## Tổng quan

### Vấn đề với strategy cũ:

```
Restart → Discover ALL genres → Queue ALL stories → Process
                ↑
          Lãng phí nếu đã có queue!
```

**Issues**:
- ❌ Re-discover tất cả genres mỗi lần restart
- ❌ Không tận dụng queue đã có
- ❌ Lãng phí time và bandwidth

### Strategy mới:

```
Restart → For each genre:
           ├─ Check status from DB
           ├─ If complete → Skip
           ├─ If has queue → Process from queue
           └─ If needs discovery → Discover & process
```

**Benefits**:
- ✅ Không re-discover genres đã complete
- ✅ Tận dụng queue existing
- ✅ Tiết kiệm time và resources
- ✅ Clear progress tracking

---

## Cách hoạt động

### Flow diagram:

```
┌────────────────────────────────────────────────────────────┐
│  FOR EACH GENRE:                                           │
└────────────────────────────────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │  Get completion status from DB │
        └────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │  completed >= total?           │
        └────────────────────────────────┘
                 │              │
             YES │              │ NO
                 ▼              ▼
        ┌────────────┐   ┌──────────────┐
        │ SKIP       │   │ Has queue?   │
        │ (Complete) │   └──────────────┘
        └────────────┘          │        │
                            YES │        │ NO
                                ▼        ▼
                      ┌──────────────┐  ┌──────────────────┐
                      │ Process from │  │ Quick discovery  │
                      │ queue        │  │ (get total only) │
                      └──────────────┘  └──────────────────┘
                                                 │
                                                 ▼
                                        ┌──────────────────┐
                                        │ Save total to DB │
                                        └──────────────────┘
                                                 │
                                                 ▼
                                        ┌──────────────────┐
                                        │ Full discovery   │
                                        │ & process        │
                                        └──────────────────┘
```

---

## Database Schema

### Tables sử dụng:

#### 1. `story_queue` - Track story progress

```sql
CREATE TABLE story_queue (
    site_key VARCHAR,
    genre_name VARCHAR,
    genre_url VARCHAR,
    story_url VARCHAR,
    status VARCHAR,  -- 'pending', 'processing', 'completed', 'failed'
    ...
);
```

#### 2. `genre_queue_metadata` - Track genre total

```sql
CREATE TABLE genre_queue_metadata (
    site_key VARCHAR,
    genre_name VARCHAR,
    genre_url VARCHAR,
    total_stories INTEGER,  -- Estimated/discovered total
    planning_status VARCHAR,
    updated_at TIMESTAMP,
    ...
);
```

### Query để check completion:

```sql
SELECT
    -- Queue stats
    COUNT(*) FILTER (WHERE status = 'completed') as completed,
    COUNT(*) FILTER (WHERE status = 'pending') as pending,
    COUNT(*) FILTER (WHERE status = 'processing') as processing,

    -- Metadata total
    (SELECT total_stories FROM genre_queue_metadata
     WHERE site_key = $1 AND genre_url = $2) as total

FROM story_queue
WHERE site_key = $1 AND genre_url = $2;
```

**Logic completion**:
```python
if total is not None:
    is_complete = (completed >= total)
else:
    # Unknown total - conservative check
    is_complete = (pending == 0 and processing == 0 and completed > 0)
```

---

## Implementation

### 1. Core function: `get_genre_completion_status()`

```python
status = await get_genre_completion_status(site_key, genre_url)

# Returns:
{
    "total_stories": 5000,      # From metadata
    "completed": 4950,          # From queue
    "pending": 50,              # From queue
    "processing": 0,            # From queue
    "is_complete": False,       # 4950 < 5000
    "has_queue": True,          # Has stories in queue
    "needs_discovery": False,   # Has queue, no need to discover
}
```

### 2. Quick discovery: `discover_genre_total_only()`

**Mục đích**: Lấy total mà không load toàn bộ stories

```python
# Only discover first page
plan = await build_category_plan(
    adapter,
    genre_data,
    site_key,
    max_pages=1,  # ← Key: only first page
)

# Get total from adapter or estimate
if plan.planned_story_total:
    return plan.planned_story_total  # From API

if plan.total_pages:
    # Estimate: stories_per_page × total_pages
    return len(plan.stories) * plan.total_pages
```

**Performance**:
- Old: Discovery 100 pages = 100 requests
- New: Quick check = 1 request

### 3. Main loop: `run_sequential_genre_crawl()`

```python
for genre in genres:
    status = await get_genre_completion_status(...)

    if status['is_complete']:
        logger.info("✅ Complete - skipping")
        continue

    if status['has_queue']:
        logger.info("📋 Processing from queue")
        await process_genre_item(genre_data)  # Uses queue

    elif status['needs_discovery']:
        logger.info("🔍 Discovering...")

        # Quick check total
        if not status['total_stories']:
            total = await discover_genre_total_only(...)
            await save_genre_total(...)

        # Full discovery
        plan = await build_category_plan(...)
        await process_genre_item(plan)
```

---

## Configuration

### Environment variables:

```bash
# Enable sequential strategy (default: true)
ENABLE_SEQUENTIAL_GENRE_STRATEGY=true

# Completion threshold (default: 0.95 = 95%)
GENRE_COMPLETION_THRESHOLD=0.95

# Cache validity for genre total (default: 7 days)
GENRE_TOTAL_CACHE_SECONDS=604800
```

### How to enable:

1. Copy env example:
```bash
cp config/env/sequential_strategy.env.example config/env/sequential_strategy.env
```

2. Edit if needed:
```bash
vim config/env/sequential_strategy.env
```

3. Add to docker-compose:
```yaml
crawler-producer:
  env_file:
    - ../config/env/sequential_strategy.env
```

4. Restart:
```bash
bash scripts/up-local.sh
```

---

## Logs & Monitoring

### Log format:

```
[SEQUENTIAL] ════════════════════════════════════════
[SEQUENTIAL] Starting sequential crawl for xtruyen
[SEQUENTIAL] Total genres: 41
[SEQUENTIAL] ════════════════════════════════════════

[SEQUENTIAL] ┌─ Genre 1/41: Tiên Hiệp
[SEQUENTIAL] │  URL: https://xtruyen.vn/theloai/tien-hiep
[SEQUENTIAL] │  Status: 1250/5000 completed (25.0%), 3750 pending, 0 processing
[SEQUENTIAL] │  Action: 📋 Processing from existing queue (3750 pending)
[SEQUENTIAL] └─ ✓ Processed from queue

[SEQUENTIAL] ┌─ Genre 2/41: Ngôn Tình
[SEQUENTIAL] │  URL: https://xtruyen.vn/theloai/ngon-tinh
[SEQUENTIAL] │  Status: 24815/24815 completed (100.0%), 0 pending, 0 processing
[SEQUENTIAL] └─ ✅ COMPLETE - Skipping to next

[SEQUENTIAL] ┌─ Genre 3/41: Huyền Huyễn
[SEQUENTIAL] │  URL: https://xtruyen.vn/theloai/huyen-huyen
[SEQUENTIAL] │  Status: ?/? completed, 0 pending, 0 processing
[SEQUENTIAL] │  Action: 🔍 Quick discovery for total count...
[SEQUENTIAL] │  Total estimated: 7807 stories
[SEQUENTIAL] │  Action: 🚀 Full discovery and processing...
[SEQUENTIAL] └─ ✓ Discovery and processing completed

[SEQUENTIAL] Progress: 1 done, 2 in progress, 1 discovered
```

### Metrics to track:

```python
metrics = {
    "genres_completed": 15,       # Already done
    "genres_from_queue": 20,      # Processed from queue
    "genres_discovered": 6,       # Needed discovery
    "genres_skipped": 0,          # Errors
    "discovery_saved": 35,        # Genres that didn't need re-discovery
    "time_saved_minutes": 45,     # Estimated time saved
}
```

---

## Performance Comparison

### Scenario: 41 genres, restart after 15 completed

| Metric | Old Strategy | New Strategy | Improvement |
|--------|-------------|--------------|-------------|
| **Genres re-discovered** | 41 (all) | 26 (incomplete only) | 37% less |
| **API requests** | ~4100 | ~1600 | 61% less |
| **Time to start** | 10 min | 4 min | 60% faster |
| **Bandwidth** | High | Low | 60% saved |

### Scenario: 41 genres, 30 completed, restart

| Metric | Old Strategy | New Strategy | Improvement |
|--------|-------------|--------------|-------------|
| **Genres re-discovered** | 41 (all) | 11 (incomplete) | 73% less |
| **API requests** | ~4100 | ~550 | 87% less |
| **Time to start** | 10 min | 2 min | 80% faster |

---

## Testing

### Unit tests:

```python
# Test completion status
async def test_genre_completion_status():
    # Mock database with 100/100 completed
    status = await get_genre_completion_status("xtruyen", "genre_url")
    assert status['is_complete'] == True

# Test quick discovery
async def test_quick_discovery():
    total = await discover_genre_total_only(adapter, genre_data, "xtruyen")
    assert total > 0

# Test sequential flow
async def test_sequential_flow():
    # Mock 2 genres: 1 complete, 1 needs discovery
    await run_sequential_genre_crawl(...)
    # Verify only 1 discovery happened
```

### Integration test:

```bash
# Test with real site
python -m pytest tests/test_sequential_strategy.py -v

# Test in docker
docker exec crawler-producer pytest tests/test_sequential_strategy.py
```

---

## Migration Guide

### Step 1: Backup database

```bash
bash scripts/backup.sh
```

### Step 2: Add environment variable

```bash
echo "ENABLE_SEQUENTIAL_GENRE_STRATEGY=true" >> config/env/crawler.env
```

### Step 3: Deploy

```bash
bash scripts/up-local.sh
```

### Step 4: Monitor logs

```bash
docker logs -f crawler-producer | grep SEQUENTIAL
```

### Step 5: Verify improvement

```bash
# Check database queries
docker exec -it storyflow-postgres psql -U storyflow -d storyflow_core -c "
    SELECT site_key, genre_name,
           COUNT(*) FILTER (WHERE status='completed') as completed,
           (SELECT total_stories FROM genre_queue_metadata gqm
            WHERE gqm.site_key=sq.site_key AND gqm.genre_url=sq.genre_url) as total
    FROM story_queue sq
    WHERE site_key='xtruyen'
    GROUP BY site_key, genre_name, genre_url;
"
```

---

## Troubleshooting

### Issue: Genre stuck at 99% complete

**Cause**: Some stories failed/missing

**Solution**:
```sql
-- Check failed stories
SELECT story_url, retry_count, last_error
FROM story_queue
WHERE site_key='xtruyen' AND genre_name='Tiên Hiệp' AND status='failed';

-- Reset to pending for retry
UPDATE story_queue
SET status='pending', retry_count=0
WHERE site_key='xtruyen' AND genre_name='Tiên Hiệp' AND status='failed';
```

### Issue: Total stories incorrect

**Cause**: Site added/removed stories

**Solution**:
```sql
-- Clear cached total to force re-discovery
UPDATE genre_queue_metadata
SET total_stories = NULL
WHERE site_key='xtruyen' AND genre_name='Tiên Hiệp';
```

### Issue: Strategy not activating

**Check**:
```bash
# Verify env var
docker exec crawler-producer env | grep SEQUENTIAL

# Check logs for strategy selection
docker logs crawler-producer 2>&1 | grep "Using.*strategy"
```

---

## Rollback

To disable and return to old strategy:

```bash
# Method 1: Environment variable
echo "ENABLE_SEQUENTIAL_GENRE_STRATEGY=false" >> config/env/crawler.env
bash scripts/up-local.sh

# Method 2: Code level (emergency)
# Edit main.py line ~2500:
enable_sequential = False  # Force disable
```

---

## Future Improvements

### 1. Parallel genre processing

Process multiple genres in parallel (when safe):

```python
# Process N genres concurrently
semaphore = asyncio.Semaphore(3)  # Max 3 genres at once

async def process_with_limit(genre):
    async with semaphore:
        await process_genre(genre)

await asyncio.gather(*[process_with_limit(g) for g in genres])
```

### 2. Smart genre ordering

Process genres by priority:

```python
# Order by: incomplete > has queue > needs discovery
genres_sorted = sorted(
    genres,
    key=lambda g: (
        not is_complete(g),      # Incomplete first
        has_queue(g),            # Has queue second
        -get_completed_count(g)  # Most progress third
    ),
    reverse=True
)
```

### 3. Adaptive thresholds

Adjust completion threshold based on genre size:

```python
# Smaller threshold for large genres (allow some missing)
if total > 10000:
    threshold = 0.98
elif total > 5000:
    threshold = 0.95
else:
    threshold = 0.90
```

---

## References

- [CRAWLING_WORKFLOW.md](CRAWLING_WORKFLOW.md) - Original workflow
- [DATABASE_TRACKING_IMPLEMENTATION.md](DATABASE_TRACKING_IMPLEMENTATION.md) - Database schema
- [SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md) - System architecture

---

**Last updated**: 2025-12-20
**Author**: StoryFlow Team
**Status**: Ready for production

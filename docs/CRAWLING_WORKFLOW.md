# Crawling Workflow - Quy trình Crawl Chi tiết

> **Hướng dẫn chi tiết về cách hệ thống crawl data, tạo metadata, xử lý content và vượt anti-bot**

## 📋 Mục lục

1. [Tổng quan workflow](#tổng-quan-workflow)
2. [Phase 1: Planning & Discovery](#phase-1-planning--discovery)
3. [Phase 2: Content Crawling](#phase-2-content-crawling)
4. [Phase 3: Metadata Generation](#phase-3-metadata-generation)
5. [Phase 4: Health Check & Fix](#phase-4-health-check--fix)
6. [Anti-bot Techniques](#anti-bot-techniques)
7. [Error Handling & Retry](#error-handling--retry)

---

## Tổng quan workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  CRAWLING WORKFLOW - 4 PHASES                                   │
└─────────────────────────────────────────────────────────────────┘

Phase 1: Planning & Discovery
├─▶ Scan categories (CategorySnapshot)
├─▶ Discover stories
├─▶ Extract chapter list
└─▶ Queue chapters to Kafka

Phase 2: Content Crawling
├─▶ Consume chapter tasks from Kafka
├─▶ Apply anti-bot measures
├─▶ Crawl chapter content
└─▶ Save to filesystem

Phase 3: Metadata Generation
├─▶ Create metadata.json
├─▶ Create chapter_metadata.json
└─▶ Organize by genre folder

Phase 4: Health Check & Fix
├─▶ Verify completeness
├─▶ Auto-fix missing data
├─▶ Refetch if needed
└─▶ Relocate to correct genre
```

---

## Phase 1: Planning & Discovery

### **1.1. Category Scanning (CategorySnapshot)**

**Mục đích**: Track categories over time để phát hiện stories mới

```python
# Producer scans category page
category_url = "https://site.com/the-loai/tien-hiep/"

# Create snapshot
snapshot = CategorySnapshot(
    site_key="site_com",
    version=timestamp,  # e.g., "20250126_120000"
)

# Scan all pages in category
for page in range(1, max_pages):
    stories = crawl_category_page(category_url, page)

    for story in stories:
        # Add to snapshot
        snapshot.add_story(
            url=story["url"],
            title=story["title"],
            position=story["position"]  # Track story ranking
        )
```

**CategorySnapshot giúp gì?**:
- ✅ Phát hiện stories mới xuất hiện trong category
- ✅ Track xem story bị remove khỏi category
- ✅ Track position changes (ranking)
- ✅ So sánh với snapshot trước đó

**Storage**: MongoDB `category_snapshots` collection

---

### **1.2. Story Metadata Discovery**

**Mục đích**: Thu thập metadata ban đầu của story

```python
# Crawl story page
story_url = "https://site.com/truyen/tien-nghich/"
story_page = crawl_page(story_url)

# Extract metadata
metadata = {
    "title": extract_title(story_page),
    "author": extract_author(story_page),
    "cover": extract_cover_image(story_page),
    "description": extract_description(story_page),
    "categories": extract_categories(story_page),  # ["Tiên Hiệp", "Huyền Huyễn"]
    "status": extract_status(story_page),  # "completed" hoặc "ongoing"
    "source": site_key,
    "site_key": site_key,
    "url": story_url,
    "crawled_at": datetime.now()
}

# Save to MetadataStore (MongoDB)
metadata_store.upsert(metadata, fallback_key=slugify(story_url))
```

**Metadata fields required**:
- `title` - Tên truyện
- `author` - Tác giả
- `cover` - URL ảnh bìa
- `description` - Mô tả/tóm tắt
- `categories` - Danh sách thể loại
- `status` - Trạng thái (completed/ongoing)
- `source` - Site nguồn
- `url` - URL story page

---

### **1.3. Chapter List Discovery**

**Mục đích**: Lấy danh sách tất cả chapters của story

```python
# Crawl chapter list page
chapter_list_url = "https://site.com/truyen/tien-nghich/"
chapter_page = crawl_page(chapter_list_url)

# Extract chapter list
chapters = []
for chapter_elem in chapter_page.select(".chapter-item"):
    chapter = {
        "chapter_number": extract_chapter_number(chapter_elem),
        "title": extract_chapter_title(chapter_elem),
        "url": extract_chapter_url(chapter_elem),
    }
    chapters.append(chapter)

# Queue chapters to Kafka
for chapter in chapters:
    kafka_producer.send("chapter.detected", {
        "story_url": story_url,
        "chapter_url": chapter["url"],
        "chapter_number": chapter["chapter_number"],
        "title": chapter["title"],
        "story_metadata": metadata
    })
```

**Kafka message format**:
```json
{
  "story_url": "https://site.com/truyen/tien-nghich/",
  "chapter_url": "https://site.com/truyen/tien-nghich/chuong-1/",
  "chapter_number": 1,
  "title": "Chương 1: Khởi Đầu",
  "story_metadata": {
    "title": "Tiên Nghịch",
    "author": "Nhĩ Căn",
    "categories": ["Tiên Hiệp"]
  }
}
```

---

## Phase 2: Content Crawling

### **2.1. Consume Chapter Tasks**

**Consumer** lắng nghe Kafka topic `chapter.detected`:

```python
# Consumer loop
for message in kafka_consumer:
    chapter_task = message.value

    # Check if already crawled
    if already_crawled(chapter_task["chapter_url"]):
        continue

    # Crawl chapter content
    await crawl_chapter_content(chapter_task)
```

---

### **2.2. Apply Anti-bot Measures**

Trước khi crawl, apply các biện pháp chống anti-bot:

#### **Step 1: Select Browser Profile**

```python
# Rotate profile mỗi request
profile = profile_manager.get_random_profile()

# Profile bao gồm:
# - User-Agent
# - Screen resolution
# - Timezone
# - Language
# - Fonts installed
# - WebGL vendor/renderer
```

#### **Step 2: Apply Fingerprinting**

```python
# Randomize browser fingerprints
fingerprint = {
    "canvas": randomize_canvas_fingerprint(),
    "webgl": randomize_webgl_fingerprint(),
    "audio": randomize_audio_fingerprint(),
    "fonts": profile.fonts,
    "plugins": profile.plugins,
    "screen": profile.screen_resolution,
}
```

#### **Step 3: Apply Stealth Mode**

```python
# Use stealth mode to evade WebDriver detection
await page.add_init_script(stealth_js)

# Stealth techniques:
# - Hide navigator.webdriver
# - Override navigator properties
# - Randomize canvas/WebGL fingerprints
# - Spoof plugins
```

**Xem chi tiết**: [Anti-bot Techniques](#anti-bot-techniques)

---

### **2.3. Crawl Chapter Content**

```python
async def crawl_chapter_content(chapter_task):
    # 1. Apply anti-bot measures
    profile = select_profile()
    fingerprint = apply_fingerprinting()

    # 2. Navigate to chapter URL
    page = await browser.new_page()
    await page.goto(chapter_task["chapter_url"])

    # 3. Handle challenges (if any)
    if await detect_challenge(page):
        await handle_challenge(page)

    # 4. Extract content
    content = await page.evaluate('''
        () => {
            const contentDiv = document.querySelector(".chapter-content");
            return contentDiv ? contentDiv.innerText : "";
        }
    ''')

    # 5. Clean content
    cleaned_content = clean_text(content)

    # 6. Save to filesystem
    await save_chapter_file(
        story_slug=chapter_task["story_metadata"]["slug"],
        chapter_number=chapter_task["chapter_number"],
        content=cleaned_content,
        metadata=chapter_task["story_metadata"]
    )

    # 7. Update crawl state
    await update_crawl_state(chapter_task["chapter_url"], "completed")
```

---

### **2.4. Content Cleaning**

Làm sạch nội dung chapter:

```python
def clean_text(content: str) -> str:
    # Remove ads
    content = remove_ads(content)

    # Remove extra whitespace
    content = re.sub(r'\s+', ' ', content)

    # Remove HTML tags (if any)
    content = re.sub(r'<[^>]+>', '', content)

    # Normalize Vietnamese characters
    content = normalize_vietnamese(content)

    # Remove footer/header
    content = remove_boilerplate(content)

    return content.strip()
```

---

### **2.5. Save to Filesystem**

```python
async def save_chapter_file(story_slug, chapter_number, content, metadata):
    # Determine genre folder
    genre = metadata["categories"][0] if metadata["categories"] else "Unknown"

    # Create folder structure
    story_folder = f"/app/completed_stories/{genre}/{story_slug}"
    os.makedirs(story_folder, exist_ok=True)

    # Save chapter file
    chapter_filename = f"{chapter_number:04d}_{slugify(chapter_title)}.txt"
    chapter_path = os.path.join(story_folder, chapter_filename)

    await safe_write_file(chapter_path, content)

    # Save/update metadata.json
    metadata_path = os.path.join(story_folder, "metadata.json")
    await safe_write_json(metadata_path, metadata)
```

---

## Phase 3: Metadata Generation

### **3.1. Generate metadata.json**

File `metadata.json` chứa thông tin story:

```json
{
  "title": "Tiên Nghịch",
  "url": "https://site.com/truyen/tien-nghich/",
  "slug": "tien-nghich",
  "author": "Nhĩ Căn",
  "cover": "https://site.com/covers/tien-nghich.jpg",
  "description": "Thuận vi phàm, nghịch tắc tiên...",
  "categories": ["Tiên Hiệp", "Huyền Huyễn"],
  "status": "completed",
  "source": "site_com",
  "site_key": "site_com",
  "total_chapters": 2085,
  "crawled_at": "2025-01-26T12:00:00",
  "metadata_updated_at": "2025-01-26T12:00:00"
}
```

**Khi nào tạo?**:
- Lần đầu crawl story
- Cập nhật mỗi khi có changes (health checker refetch)

---

### **3.2. Generate chapter_metadata.json**

File `chapter_metadata.json` chứa danh sách chapters:

```json
{
  "title": "Tiên Nghịch",
  "url": "https://site.com/truyen/tien-nghich/",
  "site_key": "site_com",
  "total_chapters": 2085,
  "chapters": [
    {
      "chapter_number": 1,
      "title": "Chương 1: Khởi Đầu",
      "url": "https://site.com/truyen/tien-nghich/chuong-1/",
      "filename": "0001_chuong-1-khoi-dau.txt"
    },
    {
      "chapter_number": 2,
      "title": "Chương 2: Tu Luyện",
      "url": "https://site.com/truyen/tien-nghich/chuong-2/",
      "filename": "0002_chuong-2-tu-luyen.txt"
    }
  ],
  "generated_at": "2025-01-26T12:00:00",
  "generated_by": "crawler"
}
```

**Khi nào tạo?**:
- Sau khi crawl xong tất cả chapters
- Health checker auto-generate nếu thiếu

---

### **3.3. Organize by Genre Folder**

Stories được tổ chức theo genre:

```
/app/completed_stories/
├── Tiên Hiệp/
│   ├── tien-nghich/
│   │   ├── metadata.json
│   │   ├── chapter_metadata.json
│   │   ├── 0001_chuong-1.txt
│   │   └── ...
│   └── bach-luyen-thanh-than/
│       └── ...
├── Ngôn Tình/
│   └── ...
└── Unknown/  # Stories chưa xác định được genre
    └── ...
```

**Logic chọn genre**:
1. Lấy `categories[0]` từ metadata
2. Normalize genre name (bỏ dấu, lowercase)
3. Nếu không có categories → folder "Unknown"

---

## Phase 4: Health Check & Fix

### **4.1. Periodic Health Check**

Health Checker chạy định kỳ (mặc định 1 giờ/lần):

```python
async def health_check_cycle():
    # Scan all stories in completed_stories/
    for genre_folder in list_genre_folders():
        for story_folder in list_stories(genre_folder):
            await check_and_fix_story(story_folder)
```

---

### **4.2. Check Metadata Completeness**

```python
def check_metadata(metadata):
    required_fields = ["title", "author", "description", "categories", "source"]
    missing = []

    for field in required_fields:
        if not metadata.get(field):
            missing.append(field)

    return missing
```

**Auto-fix cho missing fields**:
- `source` missing → Populate from `site_key` or `sources[0].site_key`

---

### **4.3. Check Unknown Category**

```python
def has_unknown_category(metadata):
    categories = metadata.get("categories", [])

    # Check if empty
    if not categories or len(categories) == 0:
        return True

    # Check if contains "unknown"
    for cat in categories:
        if "unknown" in str(cat).lower():
            return True

    return False
```

**Fix**: Refetch metadata từ site để get categories đúng

---

### **4.4. Check Missing Chapters**

```python
def check_missing_chapters(metadata, story_folder):
    total_chapters = metadata.get("total_chapters", 0)

    # Count actual chapter files
    chapter_files = glob(f"{story_folder}/*.txt")
    actual_count = len(chapter_files)

    if actual_count < total_chapters:
        missing = total_chapters - actual_count
        return True, actual_count, total_chapters

    return False, actual_count, total_chapters
```

**Fix**: Re-queue missing chapters to Kafka

---

### **4.5. Generate Missing chapter_metadata.json**

Nếu thiếu file `chapter_metadata.json`, auto-generate từ chapter files:

```python
def generate_chapter_metadata(story_folder, metadata):
    # Scan all .txt files
    chapter_files = []
    for filename in sorted(os.listdir(story_folder)):
        if not filename.endswith('.txt'):
            continue

        # Extract chapter number from filename (0001_title.txt)
        chapter_num = int(filename.split('_')[0])
        chapter_title = filename.split('_', 1)[1].replace('.txt', '')

        chapter_files.append({
            "chapter_number": chapter_num,
            "title": chapter_title,
            "filename": filename,
            "url": ""  # Cannot infer URL from filename
        })

    # Create metadata
    chapter_metadata = {
        "title": metadata["title"],
        "url": metadata["url"],
        "site_key": metadata.get("site_key") or metadata.get("source"),
        "total_chapters": len(chapter_files),
        "chapters": chapter_files,
        "generated_at": datetime.now().isoformat(),
        "generated_by": "health_checker"
    }

    return chapter_metadata
```

---

### **4.6. Relocate to Correct Genre**

Nếu story đang ở sai genre folder (e.g., "Unknown" nhưng đã có categories):

```python
def relocate_story(story_folder, genre_folder, metadata):
    # Get expected genre from metadata
    expected_genre = metadata["categories"][0] if metadata["categories"] else None

    if not expected_genre:
        return story_folder  # Cannot relocate

    # Normalize genre names
    current = genre_folder.lower().strip()
    expected = expected_genre.lower().strip()

    if current == expected or current not in ["unknown", "unknown-uncategorized"]:
        return story_folder  # Already correct

    # Move to correct genre folder
    new_folder = f"/app/completed_stories/{expected_genre}/{os.path.basename(story_folder)}"

    shutil.move(story_folder, new_folder)
    logger.info(f"Relocated story from {genre_folder} to {expected_genre}")

    return new_folder
```

---

## Anti-bot Techniques

### **Tại sao cần anti-bot?**

Nhiều websites có cơ chế anti-bot để chặn crawlers:
- Rate limiting (block IPs crawl quá nhanh)
- Browser fingerprinting detection
- WebDriver detection
- CAPTCHA challenges
- Cloudflare protection

---

### **Technique 1: Browser Profile Rotation**

**Mục đích**: Mỗi request giống như từ user khác nhau

```python
class ProfileManager:
    def __init__(self):
        self.profiles = load_profiles_from_folder("profiles/")

    def get_random_profile(self):
        profile = random.choice(self.profiles)
        return {
            "user_agent": profile["user_agent"],
            "viewport": profile["viewport"],
            "timezone": profile["timezone"],
            "locale": profile["locale"],
            "fonts": profile["fonts"],
            "webgl_vendor": profile["webgl_vendor"],
            "webgl_renderer": profile["webgl_renderer"],
        }
```

**Profile structure**:
```json
{
  "name": "chrome_windows_1080p",
  "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...",
  "viewport": {"width": 1920, "height": 1080},
  "timezone": "Asia/Ho_Chi_Minh",
  "locale": "vi-VN",
  "fonts": ["Arial", "Times New Roman", ...],
  "webgl_vendor": "Google Inc.",
  "webgl_renderer": "ANGLE (Intel HD Graphics 630)"
}
```

**Xem thêm**: [PROFILE_MANAGEMENT.md](PROFILE_MANAGEMENT.md)

---

### **Technique 2: Protocol Fingerprinting**

**Mục đích**: Randomize TLS/HTTP fingerprints

```python
# Randomize TLS fingerprint
tls_config = {
    "ciphers": random.choice(TLS_CIPHER_SUITES),
    "extensions": random.choice(TLS_EXTENSIONS),
    "curves": random.choice(TLS_CURVES),
}

# Randomize HTTP/2 settings
http2_settings = {
    "HEADER_TABLE_SIZE": random.randint(4096, 65536),
    "ENABLE_PUSH": random.choice([0, 1]),
    "MAX_CONCURRENT_STREAMS": random.randint(100, 1000),
    "INITIAL_WINDOW_SIZE": random.randint(65535, 1048576),
}
```

**Xem thêm**: [FINGERPRINTING.md](FINGERPRINTING.md)

---

### **Technique 3: Advanced Stealth Mode**

**Mục đích**: Bypass WebDriver detection

```javascript
// Hide navigator.webdriver
Object.defineProperty(navigator, 'webdriver', { get: () => false });

// Override navigator properties
Object.defineProperty(navigator, 'plugins', {
  get: () => [/* fake plugins */]
});

// Randomize canvas fingerprint
const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
HTMLCanvasElement.prototype.toDataURL = function() {
  // Add random noise to canvas
  const noise = Math.random() * 0.01;
  // ... add noise to image data
  return originalToDataURL.apply(this, arguments);
};
```

**Xem thêm**: [ADVANCED_STEALTH.md](ADVANCED_STEALTH.md)

---

### **Technique 4: Challenge Harvester**

**Mục đích**: Xử lý CAPTCHA và Cloudflare challenges

```python
class ChallengeHarvester:
    async def detect_challenge(self, page):
        # Check for CAPTCHA
        if await page.locator("#captcha").count() > 0:
            return "captcha"

        # Check for Cloudflare challenge
        if "Checking your browser" in await page.content():
            return "cloudflare"

        return None

    async def handle_challenge(self, page, challenge_type):
        if challenge_type == "cloudflare":
            # Wait for CF challenge to complete
            await page.wait_for_selector(".main-content", timeout=30000)

            # Extract cookies
            cookies = await page.context.cookies()
            self.save_cookies(cookies)

        elif challenge_type == "captcha":
            # Manual intervention required
            logger.warning("CAPTCHA detected - manual solve required")
            await self.notify_admin("captcha_detected")
```

**Xem thêm**: [CHALLENGE_HARVESTER.md](CHALLENGE_HARVESTER.md)

---

### **Technique 5: Smart Delays**

**Mục đích**: Tránh crawl quá nhanh

```python
async def smart_delay():
    # Random delay between 1-5 seconds
    delay = random.uniform(1.0, 5.0)

    # Add extra delay nếu gần đây có lỗi
    if recent_errors > 3:
        delay += random.uniform(5.0, 10.0)

    await asyncio.sleep(delay)
```

---

### **Technique 6: Rotating Proxies (Optional)**

**Mục đích**: Thay đổi IP address

```python
proxy_pool = [
    "http://proxy1:8080",
    "http://proxy2:8080",
    "http://proxy3:8080",
]

proxy = random.choice(proxy_pool)
browser = await playwright.chromium.launch(proxy={"server": proxy})
```

**Note**: Proxies optional, không bắt buộc cho hầu hết sites

---

## Error Handling & Retry

### **Retry Strategy**

```python
async def crawl_with_retry(chapter_task, max_retries=3):
    for attempt in range(max_retries):
        try:
            await crawl_chapter_content(chapter_task)
            return True

        except ChallengeDetectedError:
            logger.warning(f"Challenge detected, retry {attempt+1}/{max_retries}")
            await asyncio.sleep(30)  # Wait longer for challenges

        except NetworkError as e:
            logger.warning(f"Network error: {e}, retry {attempt+1}/{max_retries}")
            await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if attempt == max_retries - 1:
                # Save to failed queue
                await save_to_failed_queue(chapter_task)
                return False

    return False
```

---

### **Error Types**

| Error Type | Handling Strategy |
|------------|------------------|
| **ChallengeDetectedError** | Wait + retry with fresh cookies |
| **RateLimitError** | Exponential backoff (30s, 60s, 120s) |
| **NetworkError** | Retry immediately (3x max) |
| **ContentNotFoundError** | Mark as failed, manual review |
| **TimeoutError** | Retry with longer timeout |

---

### **Failed Chapter Queue**

Chapters failed sau max retries → Lưu vào queue riêng:

```python
# PostgreSQL table
CREATE TABLE failed_chapters (
    id SERIAL PRIMARY KEY,
    story_url TEXT,
    chapter_url TEXT,
    chapter_number INT,
    error_message TEXT,
    retry_count INT,
    failed_at TIMESTAMP,
    last_retry_at TIMESTAMP
);
```

**crawler-consumer-missing** sẽ periodically retry failed chapters.

---

## Summary: Crawling Workflow

### **Complete Flow**

1. **Planning Phase**:
   - ✅ Producer scans categories
   - ✅ Creates CategorySnapshot
   - ✅ Discovers stories & chapters
   - ✅ Queues chapters to Kafka

2. **Crawling Phase**:
   - ✅ Consumer receives chapter tasks
   - ✅ Applies anti-bot measures
   - ✅ Crawls chapter content
   - ✅ Saves to filesystem

3. **Metadata Phase**:
   - ✅ Generates metadata.json
   - ✅ Generates chapter_metadata.json
   - ✅ Organizes by genre folder

4. **Health Check Phase**:
   - ✅ Verifies completeness
   - ✅ Auto-fixes missing data
   - ✅ Refetches if needed
   - ✅ Relocates to correct genre

### **Key Success Factors**

- ✅ **Anti-bot**: Profile rotation + Fingerprinting + Stealth
- ✅ **Reliability**: Kafka queue + PostgreSQL state tracking
- ✅ **Completeness**: Health checker ensures no missing data
- ✅ **Scalability**: Add more consumers to scale

---

## Next Steps

- **[SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md)** - Kiến trúc tổng quan
- **[OPERATIONS_GUIDE.md](OPERATIONS_GUIDE.md)** - Hướng dẫn vận hành
- **[DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)** - Phát triển/mở rộng

---

**Last updated**: 2025-01-26

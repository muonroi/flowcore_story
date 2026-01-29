import asyncio
import json
import logging
import os
import random
import re
import time
import subprocess
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, TimeoutError

from flowcore_story.config import config
from flowcore_story.config.config import DATA_FOLDER
from flowcore_story.config.proxy_provider import load_proxies, get_random_proxy_url
from flowcore.utils.logger import logger
from flowcore.utils.advanced_stealth import (
    STEALTH_JS_SCRIPTS,
    HumanBehaviorSimulator,
    inject_canvas_noise,
    AdvancedStealthContext
)
from flowcore.utils.io_utils import resolve_completed_story_path, safe_write_file
from flowcore.utils.meta_utils import derive_story_slug
from flowcore_story.adapters.factory import get_adapter
from flowcore.storage.db_pool import get_db_pool

# Constants
RETRY_DELAY = 5
MAX_RETRIES = 3
# Prefer explicit sticky site list; fall back to the generic enabled sites.
ENABLED_SITES_ENV = os.environ.get(
    "STICKY_ENABLED_SITE_KEYS",
    os.environ.get("ENABLED_SITE_KEYS", "tangthuvien"),
)
STICKY_SITE_KEYS = set(s.strip() for s in ENABLED_SITES_ENV.split(",") if s.strip())

class StickyCrawlerWorker:
    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.current_proxy: Optional[str] = None
        self.last_activity = 0
        
        # Load proxies on init
        self.proxies_loaded = False
        
        self.browser_start_time = 0
        self.chapters_crawled = 0
        
        # Health limits
        self.MAX_BROWSER_AGE_SECONDS = 21600  # 6 hours
        self.MAX_CHAPTERS_PER_SESSION = 500   # Safety limit

    async def _check_browser_health(self) -> bool:
        if not self.browser:
            return False

        # Age check
        age = time.time() - self.browser_start_time
        if age > self.MAX_BROWSER_AGE_SECONDS:
            logger.warning(f"[StickyWorker] Browser age {age/3600:.1f}h exceeds limit")
            return False

        # Chapter count check
        if self.chapters_crawled > self.MAX_CHAPTERS_PER_SESSION:
            logger.warning(f"[StickyWorker] Crawled {self.chapters_crawled} chapters, exceeds limit")
            return False

        # Memory check
        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            if mem_mb > 2048:  # 2GB
                logger.warning(f"[StickyWorker] Memory usage {mem_mb:.0f}MB exceeds limit")
                return False
        except ImportError:
            pass

        return True

    async def _update_job_status(self, queue_id: int, status: str, error: str = None):
        """Update job status in PostgreSQL to close the loop."""
        if not queue_id:
            return
            
        try:
            pool = await get_db_pool()
            if pool:
                async with pool.acquire() as conn:
                    if status == 'completed':
                        await conn.execute(
                            "UPDATE story_queue SET status = 'completed', updated_at = NOW() WHERE id = $1",
                            queue_id,
                        )
                        logger.info(f"[StickyWorker] ✅ Marked job {queue_id} as COMPLETED in DB")
                    else:
                        await conn.execute(
                            "UPDATE story_queue SET status = 'failed', last_error = $2, updated_at = NOW() WHERE id = $1",
                            queue_id,
                            error or "Sticky worker failed"
                        )
                        logger.info(f"[StickyWorker] ❌ Marked job {queue_id} as FAILED in DB")
        except Exception as e:
            logger.error(f"[StickyWorker] Failed to update DB status: {e}")

    async def _ensure_proxies(self):
        if not self.proxies_loaded:
            await load_proxies(config.PROXIES_FILE)
            self.proxies_loaded = True

    async def start_browser(self, proxy_url: Optional[str] = None):
        """Starts the browser with specific configuration for anti-bot."""
        if self.browser:
            await self.close_browser()

        logger.info(f"[StickyWorker] Starting browser... (Proxy: {proxy_url})")
        
        self.browser_start_time = time.time()
        self.chapters_crawled = 0
        
        # Cleanup Xvfb locks if any
        try:
            display_num = os.environ.get("DISPLAY", ":99").replace(":", "")
            lock_file = f"/tmp/.X{display_num}-lock"
            if os.path.exists(lock_file):
                logger.info(f"[StickyWorker] Removing stale Xvfb lock: {lock_file}")
                os.remove(lock_file)
        except Exception as e:
            logger.debug(f"Failed to cleanup Xvfb lock: {e}")

        self.playwright = await async_playwright().start()
        
        headless_mode = os.getenv("STICKY_HEADLESS", "false").lower() == "true"
        # Use Headful mode for maximum trust unless overridden
        self.browser = await self.playwright.chromium.launch(
            headless=headless_mode,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ]
        )
        
        # Prepare context options
        context_args = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "locale": "vi-VN",
            "timezone_id": "Asia/Ho_Chi_Minh",
        }
        
        if proxy_url:
            from urllib.parse import urlparse
            parsed = urlparse(proxy_url)
            proxy_config = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
            if parsed.username:
                proxy_config["username"] = parsed.username
                proxy_config["password"] = parsed.password
            context_args["proxy"] = proxy_config

        # Create Advanced Stealth Context
        self.context = await self.browser.new_context(**context_args)
        
        # Inject Stealth Scripts
        for script in STEALTH_JS_SCRIPTS:
            await self.context.add_init_script(script)
        await inject_canvas_noise(self.context)
        
        self.page = await self.context.new_page()
        self.current_proxy = proxy_url
        self.last_activity = time.time()
        
        # Warmup
        await self._warmup()

    async def _warmup(self):
        if not self.page: return
        logger.info("[StickyWorker] Warming up browser...")
        try:
            # Visit a neutral high-trust site
            await self.page.goto("https://www.google.com", timeout=30000)
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning(f"[StickyWorker] Warmup failed: {e}")

    async def close_browser(self):
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logger.debug(f"Error during browser shutdown: {e}")
        
        self.context = None
        self.browser = None
        self.playwright = None
        self.page = None

    async def navigate_and_bypass(self, url: str) -> bool:
        """Navigates to URL and handles potential challenges."""
        if not self.page:
            logger.error("[StickyWorker] Page object is None")
            return False
            
        if not url:
            logger.error("[StickyWorker] navigate_and_bypass called with empty URL")
            return False

        logger.info(f"[StickyWorker] Navigating to {url}...")
        try:
            # Use a longer timeout and wait for network idle if possible
            # wait_until="domcontentloaded" is safer than "commit" to ensure we have some body
            await self.page.goto(url, timeout=90000, wait_until="domcontentloaded")
            logger.info(f"[StickyWorker] Page loaded (domcontentloaded). Checking for Cloudflare...")
            
            # 1. Initial wait to see if Cloudflare appears
            await asyncio.sleep(5)
            
            # 2. Check and wait for Cloudflare challenge to resolve
            for attempt in range(10):
                title = await self.page.title()
                content = await self.page.content()
                
                is_cf = "Just a moment" in title or "Attention Required" in title or "cloudflare" in content.lower()
                
                if not is_cf:
                    # Check if we see actual site content
                    if "tangthuvien" in content.lower() or "truyen" in content.lower():
                        logger.info(f"[StickyWorker] Successfully reached site content on attempt {attempt}")
                        return True
                
                logger.info(f"[StickyWorker] Cloudflare challenge active (Attempt {attempt+1}). Waiting...")
                
                # Human behavior simulation
                await HumanBehaviorSimulator.random_delay(3000, 6000)
                await HumanBehaviorSimulator.simulate_mouse_movement(self.page, 500, 300)
                
                # Try to find and click Turnstile checkbox if it exists
                try:
                    # Look for the turnstile iframe
                    frames = self.page.frames
                    clicked = False
                    for frame in frames:
                        if "cloudflare" in frame.url and "turnstile" in frame.url:
                            logger.info(f"[StickyWorker] Found Turnstile iframe: {frame.url}")
                            # Get iframe element handle directly from frame object
                            try:
                                iframe_element = await frame.frame_element()
                                if iframe_element:
                                    box = await iframe_element.bounding_box()
                                    if box:
                                        logger.info(f"[StickyWorker] Iframe Box: {box}")
                                        # Click slightly offset from center to look more human
                                        x = box["x"] + 30
                                        y = box["y"] + box["height"] / 2
                                        await self.page.mouse.move(x, y, steps=10)
                                        await asyncio.sleep(0.5)
                                        await self.page.mouse.click(x, y)
                                        logger.info(f"[StickyWorker] Clicked Turnstile at ({x}, {y}). Waiting 10s for verification...")
                                        clicked = True
                                        await asyncio.sleep(10) # Wait for verification
                                        break
                            except Exception as e:
                                logger.warning(f"[StickyWorker] Failed to get frame element: {e}")
                    
                    if not clicked:
                        logger.info(f"[StickyWorker] No Turnstile iframe found on attempt {attempt+1}")
                        # Fallback: check shadow dom or other selectors
                        pass
                except Exception as ex:
                    logger.warning(f"[StickyWorker] Error interacting with Turnstile: {ex}")

            return False
        except Exception as e:
            logger.error(f"[StickyWorker] Navigation failed: {e}")
            return False

    async def process_job(self, job: dict):
        site_key = job.get("site_key")
        story_url = job.get("story_url")
        story_title = job.get("story_title")
        queue_id = job.get("queue_id")
        
        # Check if this job is explicitly forced to sticky worker (fallback mechanism)
        is_forced = job.get("force_sticky", False)
        
        # If explicit list is set, filter. If generic worker, allow all.
        # Logic: Run if (Site is assigned to me) OR (Job is forced to me)
        is_assigned = not STICKY_SITE_KEYS or site_key in STICKY_SITE_KEYS
        
        if not is_assigned and not is_forced:
            return 
            
        if not story_url:
            logger.error(f"[StickyWorker] Job missing story_url: {job}")
            await self._update_job_status(queue_id, 'failed', "Missing story_url")
            return

        logger.info(f"[StickyWorker] Processing job for {site_key}: {story_title} (Forced: {is_forced})")
        
        await self._ensure_proxies()
        
        # Health check
        if not await self._check_browser_health():
            logger.info("[StickyWorker] Browser health check failed, restarting")
            await self.close_browser()
            proxy = get_random_proxy_url()
            await self.start_browser(proxy)
        
        # 1. Start Browser if needed (Session Management)
        if not self.page or (time.time() - self.last_activity > 3600):
            proxy = get_random_proxy_url()
            # proxy = None # Disable proxy for testing
            await self.start_browser(proxy)
        
        try:
            if not await self.navigate_and_bypass(story_url):
                raise Exception("Failed to access story page")

            # 3. Wait for content if needed (Site specific)
            if site_key == "tangthuvien":
                logger.info("[StickyWorker] Waiting for tangthuvien catalog to load...")
                try:
                    await self.page.wait_for_selector(".book-info", timeout=10000)
                    # Scroll down to trigger lazy loading if any
                    await self.page.evaluate("window.scrollTo(0, 800)")
                    await asyncio.sleep(2)
                except Exception:
                    logger.warning("[StickyWorker] Timeout waiting for .book-info")

            # 2. Get HTML and Parse via Adapter
            html = await self.page.content()
            
            # TASK 2.3: Cookie Sharing - Share successful cookies with stateless workers
            try:
                cookies = await self.context.cookies()
                if cookies:
                    from flowcore.utils.cookie_manager import set_cookies
                    # Use current user agent for fingerprinting
                    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    set_cookies(
                        site_key=site_key,
                        cookies=cookies,
                        url=story_url,
                        proxy_url=self.current_proxy,
                        headers={"User-Agent": ua},
                        ttl_seconds=1200,  # 20min (shorter TTL for sticky cookies)
                    )
                    logger.info(f"[StickyWorker] 🍪 Shared {len(cookies)} cookies to pool for {site_key}")
                    
                    try:
                        from flowcore.utils.metrics_tracker import metrics_tracker
                        metrics_tracker.update_system_metrics(
                            sticky_shared_cookies_total=metrics_tracker._system_custom.get("sticky_shared_cookies_total", 0) + len(cookies)
                        )
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"[StickyWorker] Failed to share cookies: {e}")

            adapter = get_adapter(site_key)
            
            if not hasattr(adapter, 'extract_chapter_list'):
                logger.error(f"[StickyWorker] Adapter {site_key} does not support extract_chapter_list!")
                await self._update_job_status(queue_id, 'failed', "Adapter missing extract_chapter_list")
                return

            chapter_links = adapter.extract_chapter_list(html, story_url)
            logger.info(f"[StickyWorker] Found {len(chapter_links)} chapters via adapter.")
            
            if not chapter_links:
                debug_file = f"debug_sticky_{site_key}.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(f"[StickyWorker] 🛑 No chapters found. Dumped HTML to {debug_file} for inspection.")
                await self._update_job_status(queue_id, 'failed', "No chapters found in page")
                return

            # 3. Iterate and Crawl
            count = 0
            for chap in chapter_links:
                await self.process_chapter(chap['url'], chap['title'], story_title, site_key, adapter)
                count += 1
            
            self.chapters_crawled += count
                
            self.last_activity = time.time()
            
            # 4. Success - Update DB
            await self._update_job_status(queue_id, 'completed')
            
            # 5. Send sync event to DatabaseSyncWorker (Event-Driven Sync)
            try:
                slug = derive_story_slug(story_title)
                story_folder = os.path.join(DATA_FOLDER, slug)
                
                # Import send_job here to avoid circular imports or early init issues
                from flowcore_story.kafka.kafka_producer import send_job
                
                completed_story_path = resolve_completed_story_path(story_folder)
                if completed_story_path:
                    sync_event = {
                        "type": "story_updated",
                        "story_path": completed_story_path,
                        "site_key": site_key,
                        "story_title": story_title,
                        "timestamp": time.time(),
                    }
                    # Run in background to not block
                    asyncio.create_task(send_job(sync_event, topic="storyflow.sync"))
                    logger.info(f"[StickyWorker] 📨 Sent sync event for {story_title}")
                else:
                    logger.debug(
                        "[StickyWorker] Skip sync event for %s; completed path not ready.",
                        story_title,
                    )
            except Exception as e:
                logger.warning(f"[StickyWorker] Failed to send sync event: {e}")
            
        except Exception as e:
            logger.error(f"[StickyWorker] Job failed: {e}")
            await self._update_job_status(queue_id, 'failed', str(e))
            await self.close_browser()

    async def process_chapter(self, url: str, title: str, story_title: str, site_key: str, adapter: Any):
        # Sticky navigation
        if not await self.navigate_and_bypass(url):
             logger.warning(f"[StickyWorker] Failed navigation to {url}")
             return

        # Get HTML
        html = await self.page.content()
        
        # Parse content via Adapter
        try:
            content = adapter.extract_chapter_content(html, url)
        except NotImplementedError:
             logger.error(f"[StickyWorker] Adapter {site_key} missing extract_chapter_content")
             return

        if content and len(content) > 100:
            logger.info(f"[StickyWorker] Crawled {title} ({len(content)} chars)")
            
            # Save to file
            try:
                slug = derive_story_slug(story_title)
                story_folder = os.path.join(DATA_FOLDER, slug)
                if not os.path.exists(story_folder):
                    os.makedirs(story_folder, exist_ok=True)
                
                safe_title = "".join([c for c in title if c.isalnum() or c in " -_."]).strip()
                filename = os.path.join(story_folder, f"{safe_title}.txt")
                
                await safe_write_file(filename, content)
                logger.debug(f"[StickyWorker] Saved to {filename}")

                completed_story_path = resolve_completed_story_path(story_folder)
                if completed_story_path:
                    completed_chapter_path = os.path.join(
                        completed_story_path,
                        os.path.basename(filename),
                    )
                    try:
                        from flowcore_story.kafka.kafka_producer import send_job

                        sync_event = {
                            "type": "chapter_updated",
                            "story_path": completed_story_path,
                            "chapter_path": completed_chapter_path,
                            "chapter_filename": os.path.basename(filename),
                            "chapter_title": title,
                            "chapter_url": url,
                            "chapter_index": None,
                            "site_key": site_key,
                            "story_title": story_title,
                            "genre_name": None,
                            "timestamp": time.time(),
                        }
                        await send_job(sync_event, topic="storyflow.sync")
                    except Exception as e:
                        logger.warning(f"[StickyWorker] Failed to send chapter sync event: {e}")
                else:
                    logger.debug(
                        "[StickyWorker] Skip chapter event for %s; completed path not ready.",
                        title,
                    )
            except Exception as e:
                logger.error(f"[StickyWorker] Failed to save chapter {title}: {e}")

            await asyncio.sleep(random.uniform(2.0, 5.0))
        else:
            logger.warning(f"[StickyWorker] Empty/Failed parse for {title}")

    async def run(self):
        brokers = config.KAFKA_BOOTSTRAP_SERVERS
        
        # Support multiple topics
        env_topics = os.environ.get("KAFKA_TOPICS")
        if env_topics:
            topics = [t.strip() for t in env_topics.split(",") if t.strip()]
        else:
            topics = [config.KAFKA_TOPIC]
            
        logger.info(f"[StickyWorker] Connecting to Kafka: {brokers} | topics={topics}")
        
        consumer = None
        attempt = 0
        session_timeout_ms = int(
            os.getenv(
                "STICKY_KAFKA_SESSION_TIMEOUT_MS",
                os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"),
            )
        )
        max_poll_interval_ms = int(
            os.getenv(
                "STICKY_KAFKA_MAX_POLL_INTERVAL_MS",
                os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "3600000"),
            )
        )
        max_poll_records = int(
            os.getenv(
                "STICKY_KAFKA_MAX_POLL_RECORDS",
                os.getenv("KAFKA_MAX_POLL_RECORDS", "10"),
            )
        )
        request_timeout_ms = int(
            os.getenv(
                "STICKY_KAFKA_REQUEST_TIMEOUT_MS",
                os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "120000"),
            )
        )
        heartbeat_interval_ms = int(
            os.getenv(
                "STICKY_KAFKA_HEARTBEAT_INTERVAL_MS",
                os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"),
            )
        )
        while True:
            attempt += 1
            try:
                consumer = AIOKafkaConsumer(
                    *topics,
                    bootstrap_servers=brokers,
                    group_id="storyflow-sticky-workers",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    session_timeout_ms=session_timeout_ms,
                    max_poll_interval_ms=max_poll_interval_ms,
                    max_poll_records=max_poll_records,
                    request_timeout_ms=request_timeout_ms,
                    heartbeat_interval_ms=heartbeat_interval_ms,
                )
                await consumer.start()
                logger.info(f"[StickyWorker] Kafka connected on attempt {attempt}")
                break
            except KafkaConnectionError as e:
                logger.warning(f"[StickyWorker] Kafka connection failed (attempt {attempt}): {e}")
                if attempt > 20:
                    logger.error("[StickyWorker] Max Kafka retries reached. Exiting.")
                    return
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"[StickyWorker] Unexpected error during Kafka init: {e}")
                await asyncio.sleep(5)
        
        try:
            logger.info(f"[StickyWorker] Waiting for jobs on {topics}...")
            async for msg in consumer:
                job = msg.value
                await self.process_job(job)
        finally:
            if consumer:
                await consumer.stop()
            await self.close_browser()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = StickyCrawlerWorker()
    asyncio.run(worker.run())

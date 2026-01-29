"""Adaptive Worker Scaler - Auto-scales workers based on queue depth."""
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
import importlib.util

# Setup basic logging before anything else
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("storyflow.autoscaler")

# TASK: Avoid shadowing the 'docker' library with the local 'docker/' directory
def _get_docker_lib():
    try:
        # Hide local 'docker' and try to import the library
        original_path = sys.path[:]
        # Remove any path that might contain the local 'docker' directory
        sys.path = [p for p in sys.path if p not in ('', os.getcwd(), os.path.join(os.getcwd(), 'docker'))]
        
        # Clear cached 'docker' module if it's the directory
        if 'docker' in sys.modules and not hasattr(sys.modules['docker'], 'from_env'):
            del sys.modules['docker']
        
        import docker as d
        sys.path = original_path
        return d
    except Exception as e:
        logger.error(f"[Autoscaler] Failed to import docker library: {e}")
        return None

docker_lib = _get_docker_lib()

from flowcore_story.storage.db_pool import get_db_pool
# Re-import logger from our internal utils if possible
try:
    from flowcore_story.utils.logger import logger as storyflow_logger
    logger = storyflow_logger
except ImportError:
    pass

class AdaptiveScaler:
    def __init__(self):
        try:
            self.docker_client = docker_lib.from_env()
        except Exception as e:
            logger.error(f"[Autoscaler] Failed to connect to Docker: {e}")
            self.docker_client = None

        self.site_key = os.environ.get("AUTOSCALE_SITE_KEY", "tangthuvien")
        self.worker_image = os.environ.get("WORKER_IMAGE", "storyflow-core:local")
        self.network_name = os.environ.get("NETWORK_NAME", "storyflow-network")

        # Scaling rules
        self.MIN_WORKERS = int(os.environ.get("MIN_WORKERS", "1"))
        self.MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "3"))
        self.PENDING_PER_WORKER = int(os.environ.get("PENDING_PER_WORKER", "50"))
        self.SCALE_UP_THRESHOLD = int(os.environ.get("SCALE_UP_THRESHOLD", "100"))
        self.SCALE_DOWN_THRESHOLD = int(os.environ.get("SCALE_DOWN_THRESHOLD", "20"))
        
        self.CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "300"))  # 5min
        self.COOLDOWN_PERIOD = int(os.environ.get("COOLDOWN_PERIOD", "600")) # 10min
        
        self.last_scale_time = 0.0
        self._running = True

    async def get_queue_stats(self):
        """Get current queue statistics from PostgreSQL."""
        pool = await get_db_pool()
        if not pool:
            return {"pending": 0, "processing": 0}
            
        async with pool.acquire() as conn:
            row = await conn.fetchrow (
                """
                SELECT
                    COUNT(*) FILTER (WHERE status='pending') as pending,
                    COUNT(*) FILTER (WHERE status='processing') as processing
                FROM story_queue
                WHERE site_key = $1
            """, self.site_key)

        return dict(row) if row else {"pending": 0, "processing": 0}

    def get_current_workers(self):
        """Get number of currently running sticky workers."""
        if not self.docker_client:
            return 0
            
        # We look for containers with name 'crawler-sticky-worker-'
        containers = self.docker_client.containers.list(
            filters={"name": "crawler-sticky-worker-"}
        )
        # Also check the main 'crawler-sticky-worker' (non-suffixed)
        main_container = self.docker_client.containers.list(
            filters={"name": "^/crawler-sticky-worker$"}
        )
        
        return len(containers) + len(main_container)

    def can_scale(self):
        """Check if we're outside cooldown period."""
        elapsed = time.time() - self.last_scale_time
        return elapsed > self.COOLDOWN_PERIOD

    async def scale_up(self, current: int, desired: int):
        """Scale up workers."""
        if not self.docker_client: return
        
        logger.info(f"[Autoscaler] Scaling UP: {current} -> {desired} workers for {self.site_key}")

        for i in range(current + 1, desired + 1):
            container_name = f"crawler-sticky-worker-{i}"
            
            # Check if exists (might be stopped)
            try:
                existing = self.docker_client.containers.get(container_name)
                logger.info(f"[Autoscaler] Starting existing container {container_name}")
                existing.start()
                continue
            except docker_lib.errors.NotFound:
                pass

            try:
                # Get env vars from environment but filter for crawler
                env = {
                    "ENABLED_SITE_KEYS": self.site_key,
                    "KAFKA_GROUP_ID": "storyflow-sticky-workers",
                    "WORKER_ID": f"sticky-{i}",
                    "KAFKA_TOPICS": f"storyflow.crawl.hard",
                    "DISPLAY": ":99", # Might need careful DISPLAY management if on same host
                    "PYTHONPATH": "/app/src",
                }
                
                # Copy some critical env from host if present
                for key in ["KAFKA_BROKERS", "POSTGRES_STATE_HOST", "DB_HOST"]:
                    if os.environ.get(key):
                        env[key] = os.environ[key]

                self.docker_client.containers.run(
                    image=self.worker_image,
                    name=container_name,
                    environment=env,
                    network=self.network_name,
                    detach=True,
                    restart_policy={"Name": "unless-stopped"},
                    volumes= {
                        "/home/storyflow-core/src": {"bind": "/app/src", "mode": "rw"},
                        "/home/storyflow-core/truyen_data": {"bind": "/app/truyen_data", "mode": "rw"},
                        "/home/storyflow-core/logs": {"bind": "/app/logs", "mode": "rw"},
                    },
                    command="sh -c 'xvfb-run --server-args=\"-screen 0 1920x1080x24\" python -m flowcore_story.workers.sticky_crawler_worker'"
                )
                logger.info(f"[Autoscaler] Started new container {container_name}")

            except Exception as e:
                logger.error(f"[Autoscaler] Failed to start {container_name}: {e}")

        self.last_scale_time = time.time()

    async def scale_down(self, current: int, desired: int):
        """Scale down workers gracefully."""
        if not self.docker_client: return
        
        logger.info(f"[Autoscaler] Scaling DOWN: {current} -> {desired} workers for {self.site_key}")

        containers = self.docker_client.containers.list(
            filters={"name": "crawler-sticky-worker-"}
        )

        # Sort by suffix (remove highest numbered first)
        containers.sort(key=lambda c: c.name, reverse=True)

        to_remove = current - desired
        for container in containers[:to_remove]:
            try:
                logger.info(f"[Autoscaler] Stopping {container.name} gracefully...")
                container.stop(timeout=60)
                # We keep the container but stopped, or remove?
                # Removing is cleaner for dynamic scaling
                container.remove()
                logger.info(f"[Autoscaler] Removed {container.name}")
            except Exception as e:
                logger.error(f"[Autoscaler] Failed to stop {container.name}: {e}")

        self.last_scale_time = time.time()

    async def autoscale_loop(self):
        """Main autoscaling loop."""
        logger.info(f"[Autoscaler] Starting adaptive scaler for {self.site_key}")
        logger.info(f"[Autoscaler] Limits: {self.MIN_WORKERS}-{self.MAX_WORKERS} workers")

        while self._running:
            try:
                # Get queue stats
                stats = await self.get_queue_stats()
                pending = stats.get('pending', 0)
                processing = stats.get('processing', 0)

                # Get current workers
                current = self.get_current_workers()

                # Calculate desired workers
                # Rule: 1 worker per PENDING_PER_WORKER pending jobs, within [MIN, MAX]
                desired = max(self.MIN_WORKERS, min((pending // self.PENDING_PER_WORKER) + 1, self.MAX_WORKERS))

                logger.info(
                    f"[Autoscaler] Queue: {pending} pending, {processing} processing | "
                    f"Workers: {current} current, {desired} desired"
                )

                # Scale if needed and outside cooldown
                if desired > current and pending >= self.SCALE_UP_THRESHOLD:
                    if self.can_scale():
                        await self.scale_up(current, desired)
                    else:
                        logger.info("[Autoscaler] Scale up needed but in cooldown")

                elif desired < current and pending <= self.SCALE_DOWN_THRESHOLD:
                    if self.can_scale():
                        await self.scale_down(current, desired)
                    else:
                        logger.info("[Autoscaler] Scale down needed but in cooldown")

                else:
                    logger.info("[Autoscaler] No scaling actions needed")

            except Exception as e:
                logger.error(f"[Autoscaler] Error in autoscale loop: {e}")

            # Wait before next check
            for _ in range(self.CHECK_INTERVAL):
                if not self._running: break
                await asyncio.sleep(1)

    def stop(self):
        self._running = False

async def main():
    scaler = AdaptiveScaler()
    
    def signal_handler():
        logger.info("[Autoscaler] Stopping...")
        scaler.stop()
        
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
        
    try:
        await scaler.autoscale_loop()
    except Exception as e:
        logger.error(f"[Autoscaler] Fatal error: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

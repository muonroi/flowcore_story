"""Worker for proactively renewing cookies before they expire."""
import asyncio
import signal
import os
import sys

# Ensure project root is in path
sys.path.append(os.getcwd())

from flowcore_story.utils.challenge_harvester_client import get_challenge_harvester_client, close_challenge_harvester_client
from flowcore_story.utils.cookie_manager import get_expiring_entries, set_cookies
from flowcore_story.utils.logger import logger

_running = True
_task: asyncio.Task | None = None

async def cookie_refresh_loop() -> None:
    """Proactively refresh cookies before expiration."""
    logger.info("[CookieRenewal] Starting cookie auto-renewal worker")

    harvester = get_challenge_harvester_client()

    while _running:
        try:
            # Find cookies expiring in next 5min (300s)
            expiring_soon = get_expiring_entries(threshold_seconds=300)
            
            if expiring_soon:
                logger.info(f"[CookieRenewal] Found {len(expiring_soon)} expiring cookies")

            for site_key, info, entry in expiring_soon:
                if not _running:
                    break
                    
                url = entry.get("url")
                if not url:
                    continue
                    
                logger.info(f"[{site_key}] Pre-emptive cookie refresh (expires in <5min)")
                
                # Use same proxy/headers
                proxy = entry.get("proxy")
                if proxy == "__no_proxy__":
                    proxy = None
                    
                headers = {}
                user_agent = entry.get("user_agent")
                if user_agent:
                    headers["User-Agent"] = user_agent

                try:
                    clearance = await harvester.request_clearance(
                        url, 
                        site_key,
                        headers=headers,
                        proxy=proxy
                    )

                    if clearance and clearance.cookies:
                        set_cookies(
                            site_key,
                            clearance.cookies,
                            url=url,
                            headers=headers,
                            proxy_url=proxy,
                            ttl_seconds=1800,  # 30min
                        )
                        logger.info(f"[{site_key}] Successfully refreshed cookie")
                    else:
                        logger.warning(f"[{site_key}] Refresh failed: No cookies returned")

                except Exception as e:
                    logger.warning(f"[{site_key}] Proactive refresh failed: {e}")
                
                # Small delay to avoid thundering herd on harvester
                await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"[CookieRenewal] Loop error: {e}")

        # Sleep before next check
        for _ in range(60): # Sleep 60s, checking running flag
             if not _running: break
             await asyncio.sleep(1)

async def start_cookie_auto_renewal() -> None:
    """Starts the cookie auto-renewal worker in the background."""
    global _running, _task
    _running = True
    _task = asyncio.create_task(cookie_refresh_loop())
    logger.info("[CookieRenewal] Background task started")

async def stop_cookie_auto_renewal() -> None:
    """Stops the cookie auto-renewal worker."""
    global _running, _task
    _running = False
    if _task:
        logger.info("[CookieRenewal] Waiting for task to stop...")
        try:
            await asyncio.wait_for(_task, timeout=10)
        except asyncio.TimeoutError:
            _task.cancel()
        _task = None
    logger.info("[CookieRenewal] Background task stopped")

def signal_handler() -> None:
    global _running
    _running = False
    logger.info("[CookieRenewal] Stopping...")

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await cookie_refresh_loop()
    finally:
        await close_challenge_harvester_client()

if __name__ == "__main__":
    asyncio.run(main())
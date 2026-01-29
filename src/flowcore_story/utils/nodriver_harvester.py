"""Nodriver integration for Cloudflare bypass (CDP-less approach)."""
import asyncio
import logging
import os
import random
from typing import Any, Mapping, List

try:
    import nodriver as uc
    NODRIVER_AVAILABLE = True
except ImportError:
    uc = None
    NODRIVER_AVAILABLE = False

from flowcore_story.config.proxy_provider import parse_proxy_url

logger = logging.getLogger("storyflow.nodriver")

async def harvest_with_nodriver(
    url: str,
    proxy: str | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 60.0,
    wait_for_selector: str | None = None,
    sniff_network: bool = False,
) -> dict[str, Any]:
    """
    Harvest content using Nodriver (undetected-chromedriver v2).
    """
    if not NODRIVER_AVAILABLE:
        raise RuntimeError("Nodriver is not installed")

    browser_args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--window-size=1920,1080",
    ]

    # Proxy setup
    if proxy:
        scheme, normalized_proxy = parse_proxy_url(proxy)
        if normalized_proxy:
             browser_args.append(f"--proxy-server={normalized_proxy}")

    # Headless mode and DISPLAY setup
    use_xvfb = os.environ.get("CHALLENGE_HARVESTER_USE_XVFB", "true").lower() == "true"
    if use_xvfb and not os.environ.get("DISPLAY"):
        display = os.environ.get("CHALLENGE_HARVESTER_XVFB_DISPLAY", ":99")
        os.environ["DISPLAY"] = display
        logger.info(f"[Nodriver] XVFB enabled, setting DISPLAY={display}")
    
    browser = None
    sniffed_urls = []
    
    # Callback for network events
    def network_handler(event: uc.cdp.network.RequestWillBeSent):
        if event.request and event.request.url:
            sniffed_urls.append(event.request.url)

    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"[Nodriver] Connection attempt {attempt}/{max_attempts} for {url} (Proxy: {bool(proxy)})")
            
            # Start browser
            browser = await uc.start(
                browser_args=browser_args,
                sandbox=False, # This is the critical flag for root/docker
                headless=False, 
            )
            logger.info("[Nodriver] Browser connection established")
            break
        except Exception as e:
            logger.error(f"[Nodriver] Failed to start browser on attempt {attempt}: {e}")
            if attempt == max_attempts:
                raise
            await asyncio.sleep(2)

    try:
        # Enable network sniffing if requested
        if sniff_network:
            # uc.start returns a Browser object which has a connection to CDP
            # We can use the connection to subscribe to network events
            try:
                page = await browser.get("about:blank") # Start with blank
                # Subscribe to RequestWillBeSent
                browser.connection.add_handler(uc.cdp.network.RequestWillBeSent, network_handler)
                await browser.connection.send(uc.cdp.network.enable())
                logger.info("[Nodriver] Network sniffing enabled")
            except Exception as se:
                logger.warning(f"[Nodriver] Failed to enable sniffing: {se}")

        # Navigate
        page = await browser.get(url)
        
        # Wait for content or challenge
        start_time = asyncio.get_event_loop().time()
        await asyncio.sleep(5) # Give it more time to load
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            content = await page.get_content()
            
            # Simple check for CF challenge
            if "challenges.cloudflare.com" in content or "Just a moment" in content:
                logger.debug("[Nodriver] Cloudflare challenge detected, waiting...")
                await asyncio.sleep(2)
                continue
                
            if wait_for_selector:
                try:
                    elem = await page.select(wait_for_selector, timeout=0.1)
                    if elem:
                        logger.info(f"[Nodriver] Selector {wait_for_selector} found!")
                        break
                except Exception:
                    pass
                
                if asyncio.get_event_loop().time() - start_time > timeout:
                    break
                await asyncio.sleep(1)
                continue
            
            break
            
        content = await page.get_content()
        cookies = await browser.cookies.get_all()
        cookies_list = []
        for c in cookies:
            cookies_list.append({
                "name": c.name,
                "value": c.value,
                "domain": c.domain,
                "path": c.path,
                "secure": c.secure,
                "expires": c.expires
            })
            
        return {
            "url": url,
            "status": 200, 
            "body": content,
            "cookies": cookies_list,
            "sniffed_urls": sniffed_urls,
            "headers": {}, 
            "metadata": {
                "engine": "nodriver",
                "proxy_used": bool(proxy),
                "sniffing": sniff_network
            }
        }
        
    except Exception as e:
        logger.error(f"[Nodriver] Error harvesting {url}: {e}")
        raise
    finally:
        if browser:
            try:
                browser.stop()
            except Exception:
                pass

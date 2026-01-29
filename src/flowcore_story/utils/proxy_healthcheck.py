import asyncio

from flowcore_story.config import config as app_config
from flowcore_story.config.config import LOADED_PROXIES, USE_PROXY
from flowcore_story.config.proxy_provider import mark_bad_proxy, reload_proxies_if_changed
from flowcore_story.utils.httpx_compat import create_async_client
from flowcore_story.utils.logger import logger

TEST_URL = "http://httpbin.org/ip"

async def check_proxy(proxy: str) -> bool:
    proxies = {
        "http://": proxy,
        "https://": proxy,
    }
    try:
        async with create_async_client(proxies=proxies, timeout=10) as client:
            await client.get(TEST_URL)
        return True
    except Exception:
        return False

def _is_proxy_enabled() -> bool:
    """Return the latest USE_PROXY flag from runtime configuration."""

    try:
        # ``app_config`` allows monkeypatching in tests to toggle proxy usage dynamically.
        return bool(app_config.USE_PROXY)
    except AttributeError:
        # Fallback to the module level constant when the config object is unavailable.
        return bool(USE_PROXY)


async def healthcheck_loop(interval: int = 300, iterations: int | None = None):
    loops_remaining = iterations
    while True:
        use_proxy = _is_proxy_enabled()
        if not use_proxy and not LOADED_PROXIES:
            logger.info("[HEALTHCHECK] USE_PROXY=false, bỏ qua vòng health-check.")
            if iterations is not None:
                break
            await asyncio.sleep(interval)
            continue

        await reload_proxies_if_changed("proxies/proxies.txt")
        active_proxies = list(LOADED_PROXIES)
        if not active_proxies and not use_proxy:
            logger.info("[HEALTHCHECK] Không có proxy nào để kiểm tra khi USE_PROXY=false.")
            if iterations is not None:
                break
            await asyncio.sleep(interval)
            continue

        for proxy in active_proxies:
            if "://" not in proxy:
                proxy_url = f"http://{proxy}"
            else:
                proxy_url = proxy
            ok = await check_proxy(proxy_url)
            if not ok:
                logger.warning(f"[HEALTHCHECK] Proxy {proxy_url} failed")
                await mark_bad_proxy(proxy_url)
        if not LOADED_PROXIES:
            logger.warning("[HEALTHCHECK] Không còn proxy khả dụng sau vòng kiểm tra.")
        await asyncio.sleep(interval)
        if loops_remaining is not None:
            loops_remaining -= 1
            if loops_remaining <= 0:
                break

if __name__ == "__main__":
    asyncio.run(healthcheck_loop())

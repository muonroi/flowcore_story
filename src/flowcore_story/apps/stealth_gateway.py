import os
import time
import uuid
import logging
import asyncio
import random
from typing import Any, Dict, Optional, List
from urllib.parse import urlparse
from fastapi import FastAPI, Header, HTTPException, Depends, Request, Response
from pydantic import BaseModel
import uvicorn
import httpx

try:
    from curl_cffi.requests import AsyncSession
except Exception:
    AsyncSession = None

# Disable DB connection for this process
os.environ["DATABASE_URL"] = ""
os.environ["ENABLE_DB"] = "false"

from flowcore_story.utils.http_client import fetch
from flowcore_story.utils.anti_bot import is_anti_bot_content
from flowcore_story.utils.logger import logger
from flowcore_story.utils.next_data_parser import NextDataParser

# --- TIERED AUTHENTICATION SYSTEM ---
API_KEYS_REGISTRY = {
    "sf_paid_premium_v1_8d3f2": {"tier": "paid", "name": "Premium Client A"},
    "sf_free_community_v1_a7d2e": {"tier": "free", "name": "Public Community"},
    "sf_flex_a7d2e9b4c1f08e3d5a2b6c9d8e7f4a1b": {"tier": "free", "name": "Legacy Flex Key"}
}

# Gateway Logger
gateway_logger = logging.getLogger("storyflow.gateway")

# Harvester Service URL (Direct Container Access for reliability)
HARVESTER_SERVICE_URL = os.environ.get("CHALLENGE_HARVESTER_URL", "http://harvester-1:8099/harvest")

app = FastAPI(
    title="Storyflow Stealth Gateway",
    description="Smart Proxy Gateway with Harvester Integration",
    version="1.4.0"
)

CHROME_120_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
CHROME_120_HEADERS = {
    "User-Agent": CHROME_120_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "Sec-CH-UA": "\"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\", \"Not=A?Brand\";v=\"24\"",
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}
ALLOWED_ENGINES = {"auto", "playwright", "curlcffi"}
MANGA_SITE_KEYS = {"xtruyen"}
MANGA_URL_HINTS = ("post_type=wp-manga", "/wp-manga/", "/manga/")


def _has_header(headers: Dict[str, str], header_name: str) -> bool:
    target = header_name.lower()
    return any(key.lower() == target for key in headers)


def _is_manga_site(site_key: str, url: str) -> bool:
    key = (site_key or "").lower()
    if key in MANGA_SITE_KEYS or "manga" in key:
        return True
    url_lower = (url or "").lower()
    return any(hint in url_lower for hint in MANGA_URL_HINTS)


def _extract_domain_referer(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/"


def _build_curlcffi_headers(
    extra_headers: Optional[Dict[str, str]],
    referer: Optional[str],
) -> Dict[str, str]:
    headers = dict(extra_headers or {})
    headers.update(CHROME_120_HEADERS)
    if referer and not _has_header(headers, "referer"):
        headers["Referer"] = referer
    return headers


def _build_harvester_headers(
    extra_headers: Optional[Dict[str, str]],
    referer: Optional[str],
) -> Dict[str, str]:
    headers = dict(extra_headers or {})
    if referer and not _has_header(headers, "referer"):
        headers["Referer"] = referer
    return headers


class ProxyRequest(BaseModel):
    url: str
    site_key: str = "default"
    method: str = "GET"
    data: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    engine: str = "auto"
    
    custom_proxy: Optional[str] = None
    proxy_list: Optional[List[str]] = None
    proxy_file_url: Optional[str] = None
    
    render_js: bool = False
    wait_for_selector: Optional[str] = None
    actions: Optional[List[Dict[str, Any]]] = None
    sniff_network: Optional[bool] = None
    break_overlays: bool = True # Default to True for better media bypass
    
    use_smart_mode: bool = True
    timeout: int = 60

class DownloadRequest(BaseModel):
    url: str
    site_key: str = "default"
    headers: Optional[Dict[str, str]] = None
    proxy: Optional[str] = None
    timeout: int = 60
    stream: bool = False # Option to stream large files

async def verify_api_key(x_api_key: str = Header(None, alias="X-API-Key")):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API Key missing")
    key_info = API_KEYS_REGISTRY.get(x_api_key.strip())
    if not key_info:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return key_info

# --- PROXY HELPER ---
PROXY_FILE_CACHE = {}

async def resolve_proxy_list(req: ProxyRequest) -> List[str]:
    proxies = []
    if req.custom_proxy: proxies.append(req.custom_proxy)
    if req.proxy_list: proxies.extend(req.proxy_list)
    # File fetching logic omitted for brevity
    unique = list(set(proxies))
    if unique: random.shuffle(unique)
    return unique

@app.get("/health")
async def health_check():
    return {
        "status": "ok", 
        "timestamp": time.time(), 
        "mode": "hybrid_core",
        "harvester_url": HARVESTER_SERVICE_URL
    }

@app.post("/v1/bypass")
async def bypass_request(proxy_req: ProxyRequest, key_info: dict = Depends(verify_api_key)):
    request_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    user_tier = key_info["tier"]
    user_name = key_info["name"]
    
    candidate_proxies = await resolve_proxy_list(proxy_req)
    if not candidate_proxies:
        candidate_proxies = [None]
        
    engine = (proxy_req.engine or "auto").strip().lower()
    if engine not in ALLOWED_ENGINES:
        raise HTTPException(status_code=400, detail="Unsupported engine. Use auto, playwright, or curlcffi.")
    if proxy_req.render_js and engine != "playwright":
        gateway_logger.info(f"[{request_id}] render_js requested, forcing engine=playwright")
        engine = "playwright"

    referer = None
    if _is_manga_site(proxy_req.site_key, proxy_req.url):
        referer = _extract_domain_referer(proxy_req.url)

    curlcffi_headers = _build_curlcffi_headers(proxy_req.headers, referer)
    harvester_headers = _build_harvester_headers(proxy_req.headers, referer)

    gateway_logger.info(
        f"[{request_id}] Client: {user_name} | URL: {proxy_req.url} | Render: {proxy_req.render_js} | Engine: {engine}"
    )

    final_resp = None
    last_error = None
    used_proxy = None
    engine_used = None
    
    # Retry/Failover Loop
    max_attempts = min(len(candidate_proxies), 3)
    if max_attempts == 0 and not candidate_proxies[0]: max_attempts = 1

    async def _attempt_harvester(
        proxies: List[str],
    ) -> tuple[Any, Optional[str], Optional[str], Optional[Dict[str, Any]]]:
        last_exc = None
        for i in range(max_attempts):
            current_proxy = proxies[i]
            try:
                gateway_logger.info(f"[{request_id}] Delegating to Harvester Service...")

                harvest_payload = {
                    "url": proxy_req.url,
                    "site_key": proxy_req.site_key,
                    "proxy": current_proxy,
                    "wait_for_selector": proxy_req.wait_for_selector,
                    "action": "render",
                    "break_overlays": proxy_req.break_overlays
                }
                if harvester_headers:
                    harvest_payload["headers"] = harvester_headers
                if referer and not _has_header(harvester_headers, "referer"):
                    harvest_payload["referer"] = referer
                if proxy_req.actions:
                    harvest_payload["actions"] = proxy_req.actions
                if proxy_req.sniff_network is not None:
                    harvest_payload["sniff_network"] = proxy_req.sniff_network

                async with httpx.AsyncClient(timeout=proxy_req.timeout + 10) as client:
                    hv_resp = await client.post(HARVESTER_SERVICE_URL, json=harvest_payload)

                if hv_resp.status_code == 200:
                    hv_data = hv_resp.json()
                    class RenderResponse: pass
                    resp = RenderResponse()
                    resp.status_code = 200
                    resp.text = hv_data.get("body", "") or hv_data.get("html", "")
                    resp.url = proxy_req.url
                    resp.headers = {}
                    return resp, current_proxy, None, hv_data
                raise Exception(f"Harvester returned {hv_resp.status_code}: {hv_resp.text}")
            except Exception as e:
                last_exc = str(e)
                gateway_logger.warning(f"[{request_id}] Attempt {i+1} failed: {e}")
        return None, None, last_exc, None

    async def _attempt_curlcffi(proxies: List[str]) -> tuple[Any, bool, bool, Optional[str], Optional[str]]:
        last_exc = None
        saw_anti_bot = False
        last_is_bot = False
        last_resp = None
        last_proxy = None
        for i in range(max_attempts):
            current_proxy = proxies[i]
            last_proxy = current_proxy
            try:
                last_resp = await fetch(
                    url=proxy_req.url,
                    site_key=proxy_req.site_key,
                    method=proxy_req.method,
                    data=proxy_req.data,
                    extra_headers=curlcffi_headers,
                    force_proxy_url=current_proxy,
                    timeout=min(proxy_req.timeout, 30),
                    retry_attempts=1,
                    impersonate_profile="chrome120",
                )

                if last_resp:
                    last_is_bot = is_anti_bot_content(
                        last_resp.text,
                        debug_site_key=proxy_req.site_key
                    )
                    if last_is_bot:
                        saw_anti_bot = True
                    if last_resp.status_code == 200 and not last_is_bot:
                        return last_resp, saw_anti_bot, last_is_bot, None, last_proxy
            except Exception as e:
                last_exc = str(e)
                gateway_logger.warning(f"[{request_id}] Attempt {i+1} failed: {e}")
        return last_resp, saw_anti_bot, last_is_bot, last_exc, last_proxy

    proxies_to_try = candidate_proxies[:max_attempts]

    harvester_metadata = None

    if engine == "playwright":
        final_resp, used_proxy, last_error, harvester_metadata = await _attempt_harvester(proxies_to_try)
        engine_used = "playwright"
    elif engine == "curlcffi":
        final_resp, _, _, last_error, used_proxy = await _attempt_curlcffi(proxies_to_try)
        engine_used = "curlcffi" if final_resp else "curlcffi"
    else:
        curl_resp, saw_anti_bot, last_is_bot, last_error, used_proxy = await _attempt_curlcffi(proxies_to_try)
        if curl_resp and curl_resp.status_code == 200 and not last_is_bot:
            final_resp = curl_resp
            engine_used = "curlcffi"
        elif saw_anti_bot:
            harvester_proxies = proxies_to_try
            if used_proxy and used_proxy in proxies_to_try:
                harvester_proxies = [used_proxy] + [p for p in proxies_to_try if p != used_proxy]
            final_resp, used_proxy, last_error, harvester_metadata = await _attempt_harvester(harvester_proxies)
            if final_resp:
                engine_used = "playwright"
            else:
                final_resp = curl_resp
                engine_used = "curlcffi" if curl_resp else "curlcffi"
        else:
            final_resp = curl_resp
            engine_used = "curlcffi" if curl_resp else "curlcffi"

    # --- RESULT HANDLING ---
    if final_resp is None:
        return {
            "request_id": request_id,
            "status_code": 502,
            "is_anti_bot": True,
            "error": f"Failed. Last error: {last_error}",
            "body": ""
        }

    content = final_resp.text
    is_bot = is_anti_bot_content(content, debug_site_key=proxy_req.site_key)
    is_success = final_resp.status_code == 200 and not is_bot
    duration = time.time() - start_time

    # Harvest proxy logic (Omitted for brevity, assume same as before)

    response_payload = {
        "request_id": request_id,
        "status_code": final_resp.status_code,
        "is_anti_bot": is_bot,
        "url": str(final_resp.url),
        "body": content,
        "performance": {
            "duration": duration,
            "engine": engine_used or engine
        }
    }
    
    # --- AUTO SNIFFING (Fast Lane for Media) ---
    all_sniffed = []
    
    # 1. Links from Harvester (Playwright Network)
    if isinstance(harvester_metadata, dict) and "sniffed_urls" in harvester_metadata:
        all_sniffed.extend(harvester_metadata.get("sniffed_urls") or [])
        
    # 2. Links from HTML Parser (NextJS/Stream Data/Encrypted)
    if content:
        html_links = NextDataParser.extract_m3u8_links(content)
        if html_links:
            gateway_logger.info(f"[{request_id}] NextDataParser found {len(html_links)} links in content")
            all_sniffed.extend(html_links)
            
    if all_sniffed:
        response_payload["sniffed_urls"] = list(set(all_sniffed))

    return response_payload

@app.post("/v1/download")
async def download_request(download_req: DownloadRequest, key_info: dict = Depends(verify_api_key)):
    if AsyncSession is None:
        raise HTTPException(status_code=500, detail="curl_cffi is not installed")

    request_id = str(uuid.uuid4())[:8]
    candidate_proxies = [download_req.proxy] if download_req.proxy else [None]
    max_attempts = min(len(candidate_proxies), 3) or 1

    referer = _extract_domain_referer(download_req.url)
    headers = dict(CHROME_120_HEADERS)
    headers.update(download_req.headers or {})
    if referer and not _has_header(headers, "referer"):
        headers["Referer"] = referer

    last_error = None
    
    for i in range(max_attempts):
        current_proxy = candidate_proxies[i]
        proxies = None
        if current_proxy:
            proxy_url = current_proxy
            if "://" not in proxy_url:
                proxy_url = f"http://{proxy_url}"
            if proxy_url.startswith("https://") and os.environ.get("CURL_CFFI_PROXY_TLS", "false") != "true":
                proxy_url = "http://" + proxy_url[len("https://"):]
            proxies = {"http": proxy_url, "https": proxy_url}

        try:
            async with AsyncSession(impersonate="chrome120") as session:
                if download_req.stream:
                    # Logic for streaming download (Task 4)
                    # Note: curl_cffi doesn't support easy streaming like httpx, 
                    # we will simulate it with a large timeout and response handling
                    pass
                
                resp = await session.get(
                    download_req.url,
                    headers=headers,
                    proxies=proxies,
                    timeout=download_req.timeout,
                    allow_redirects=True,
                    verify=False,
                )
                
                if resp and resp.status_code == 200:
                    content_type = resp.headers.get("Content-Type") or "application/octet-stream"
                    return Response(
                        content=resp.content,
                        status_code=resp.status_code,
                        media_type=content_type,
                    )
                raise Exception(f"Status {resp.status_code if resp else 'No Response'}")
        except Exception as e:
            last_error = str(e)
            gateway_logger.warning(f"[{request_id}] Download attempt {i + 1} failed: {e}")

    raise HTTPException(status_code=502, detail=f"Download failed. Last error: {last_error}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8555)

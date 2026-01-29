"""Telemetry Logger - Records detailed request metrics for AI training."""
import asyncio
import json
import os
import time
from typing import Any

import aiofiles

from flowcore_story.config.config import LOG_FOLDER

TELEMETRY_FILE = os.path.join(LOG_FOLDER, "crawler_telemetry.jsonl")
_TELEMETRY_LOCK = asyncio.Lock()

async def record_telemetry(
    site_key: str,
    url: str,
    method: str,
    status_code: int | None,
    duration: float,
    proxy_url: str | None = None,
    user_agent: str | None = None,
    success: bool = False,
    engine: str = "unknown",
    error_type: str | None = None,
    response_size: int = 0,
) -> None:
    """Record a telemetry event to JSONL file."""
    
    event = {
        "timestamp": time.time(),
        "site": site_key,
        "url": url,
        "method": method,
        "status": status_code,
        "duration_ms": int(duration * 1000),
        "proxy_provider": _extract_proxy_provider(proxy_url),
        "user_agent": user_agent,
        "success": success,
        "engine": engine,
        "error": error_type,
        "size": response_size,
    }
    
    line = json.dumps(event) + "\n"
    
    try:
        async with _TELEMETRY_LOCK:
            async with aiofiles.open(TELEMETRY_FILE, "a", encoding="utf-8") as f:
                await f.write(line)
    except Exception:
        # Fail silently to not impact crawler
        pass

def _extract_proxy_provider(proxy_url: str | None) -> str:
    if not proxy_url:
        return "direct"
    if "103.117.198.60" in proxy_url: # Example check
        return "mproxy" 
    # Add more heuristics or return simplified domain
    try:
        from urllib.parse import urlparse
        return urlparse(proxy_url).hostname or "unknown"
    except Exception:
        return "invalid"

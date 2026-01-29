"""Crawler Session Management - Explicit session state tracking for story crawling."""
import time
from typing import Any, Mapping
from dataclasses import dataclass, field

@dataclass
class CrawlerSession:
    """Holds session state for a specific story or site crawl session."""
    site_key: str
    proxy_url: str | None = None
    user_agent: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    cookies: list[dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)
    
    # TASK 3.3: Session Kill Switch tracking
    consecutive_failures: int = 0
    
    def update_from_harvester(self, result: dict[str, Any]) -> None:
        """Update session state with results from challenge harvester."""
        self.last_used_at = time.time()
        self.reset_failures() # Reset failures on successful harvest
        
        # Capture cookies if provided
        if result.get("cookies"):
            self.cookies = result["cookies"]
            
        # Capture headers if provided
        if result.get("headers"):
            # Update internal headers but preserve key identity
            self.headers.update(result["headers"])
            
        # Use UA from harvester if it changed
        if result.get("user_agent"):
            self.user_agent = result["user_agent"]
            self.headers["User-Agent"] = self.user_agent

    def mark_failed(self) -> int:
        """Increment failure count and return current count."""
        self.consecutive_failures += 1
        return self.consecutive_failures

    def reset_failures(self) -> None:
        """Reset failure count on success."""
        self.consecutive_failures = 0

    def should_kill(self, threshold: int = 2) -> bool:
        """Check if session should be killed due to excessive failures."""
        return self.consecutive_failures >= threshold

    def get_request_headers(self) -> dict[str, str]:
        """Get headers for a follow-up HTTP request."""
        headers = self.headers.copy()
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        return headers

    def is_expired(self, ttl: float = 3600.0) -> bool:
        """Check if session is older than TTL."""
        return time.time() - self.created_at > ttl

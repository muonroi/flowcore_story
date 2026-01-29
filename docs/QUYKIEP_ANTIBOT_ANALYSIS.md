# QuýKiếp Anti-Bot Analysis Report

**Date:** 2025-12-29
**Analyst:** Claude Code
**Status:** 🔴 Chapter access blocked

## Executive Summary

Site quykiep.com has implemented sophisticated protection specifically targeting chapter pages while allowing access to story/category pages. This differs from xtruyen and truyencom which have simpler protection.

## Findings

### 1. URL Behavior Comparison

| Page Type | URL Pattern | Status | Content |
|-----------|-------------|--------|---------|
| Homepage | `quykiep.com/` | 200 ✅ | Real content |
| Genre list | `quykiep.com/truyen-dich-ds` | 200 ✅ | Real content |
| Story page | `quykiep.com/truyen/{slug}` | 200 ✅ | Real content |
| Chapter list | `quykiep.com/truyen/{slug}/danh-sach-chuong` | 200 ✅ | Real content |
| **Chapter page** | `quykiep.com/truyen/{slug}/{chapter-slug}` | **301 → Story** | ❌ Redirected |

### 2. Chapter Access Attempts

| Method | Result |
|--------|--------|
| Direct URL (curl_cffi) | 301 redirect to story page |
| With session cookies | 301 redirect to story page |
| With Referer header | 301 redirect to story page |
| Next.js data API (`/_next/data/...json`) | Returns `__N_REDIRECT` |
| Playwright browser | Browser crashes / 403 Cloudflare |
| Challenge Harvester | 403 Cloudflare block |

### 3. Next.js Server Response

When accessing chapter via Next.js data API:
```json
{
  "pageProps": {
    "__N_REDIRECT": "/truyen/{story-slug}",
    "__N_REDIRECT_STATUS": 301
  }
}
```

This indicates **server-side redirect logic** in Next.js, not Cloudflare.

### 4. Root Cause Analysis

The chapter redirect is **intentional server-side behavior**:

1. **Not Cloudflare:** Story/genre/chapter-list pages work fine
2. **Not TLS fingerprint:** All impersonation options get redirected
3. **Not cookies/session:** Redirect happens regardless of session state
4. **Next.js middleware:** The `__N_REDIRECT` response indicates Next.js middleware is redirecting

**Likely explanations:**
- Chapters may require **user login** (paywall/registration)
- Site may have changed to **require client-side navigation only**
- Anti-scraping protection via **server-side redirect rules**

## Comparison with Working Sites

| Feature | xtruyen | truyencom | quykiep |
|---------|---------|-----------|---------|
| Framework | Traditional | Traditional | **Next.js** |
| Chapter access | Direct URL works | Direct URL works | **Redirects** |
| Protection | Cloudflare | Minimal | **Server-side** |
| Harvester | Works | Works | **Fails** |

## Proposed Solutions

### Solution 1: Client-Side Navigation (Recommended)

Since the site is Next.js with client-side routing, we need to:

1. Load story page first via Playwright
2. Click on chapter link (trigger client-side navigation)
3. Extract content from rendered page

```python
# Pseudocode
async def fetch_chapter_content(story_url, chapter_index):
    page = await browser.new_page()
    await page.goto(story_url)

    # Find and click chapter link
    chapter_link = await page.query_selector(f'a[href*="chuong-{chapter_index}"]')
    await chapter_link.click()
    await page.wait_for_load_state("networkidle")

    # Now extract content
    content = await page.query_selector('.chapter-content')
    return await content.inner_text()
```

### Solution 2: Discover Hidden API

Check if the site has internal API endpoints:

```python
# Monitor XHR requests when clicking chapter
# May find endpoints like:
# - /api/chapter/{id}
# - /api/book/{id}/chapter/{index}
```

### Solution 3: Use Different IP Pool

The 403 on Playwright suggests IP-based blocking:

1. Rotate to residential proxies
2. Use different datacenter IPs
3. Add longer delays between requests

### Solution 4: Login/Session

If chapters require login:

1. Create test account on quykiep
2. Capture login session cookies
3. Use authenticated session for chapter access

## Recommended Implementation

### Phase 1: Investigate Login Requirement

```python
# Check if login changes behavior
1. Create account on quykiep.com
2. Login via Playwright
3. Try accessing chapter
4. Check if redirect still happens
```

### Phase 2: Upgrade Harvester

Add site-specific handling for Next.js sites:

```python
class NextJsHarvester:
    async def harvest_chapter(self, story_url: str, chapter_slug: str) -> str:
        """Special handling for Next.js sites that require client-side navigation."""

        async with self.browser_context() as page:
            # Load story page first
            await page.goto(story_url, wait_until="networkidle")

            # Wait for hydration
            await page.wait_for_selector('[data-reactroot]')

            # Find chapter link and click
            chapter_link = await page.wait_for_selector(
                f'a[href*="{chapter_slug}"]',
                timeout=10000
            )
            await chapter_link.click()

            # Wait for navigation
            await page.wait_for_url(f"*{chapter_slug}*", timeout=10000)
            await page.wait_for_load_state("networkidle")

            # Extract content
            return await page.content()
```

### Phase 3: Implement in Adapter

```python
class QuykiepAdapter:
    async def get_chapter_content(self, chapter_url, story_url):
        # Check if direct access works
        response = await make_request(chapter_url)

        if response.url != chapter_url:  # Redirected
            # Fallback to client-side navigation
            return await self.harvester.harvest_chapter(story_url, chapter_slug)

        return parse_content(response.text)
```

## Files to Modify

1. **`/src/storyflow_core/services/challenge_harvester.py`**
   - Add `harvest_via_navigation()` method for Next.js sites

2. **`/src/storyflow_core/adapters/quykiep_adapter.py`**
   - Add fallback logic when redirect detected

3. **`/config/sites/quykiep.json`**
   - Add `"requires_navigation": true` flag

## Estimated Effort

| Task | Complexity | Time |
|------|------------|------|
| Test login requirement | Low | 1 hour |
| Implement navigation harvester | Medium | 4-6 hours |
| Update adapter | Low | 2 hours |
| Testing | Medium | 2-3 hours |

## Final Conclusion (Updated)

### Test Results Summary

| Test | Result |
|------|--------|
| Direct chapter URL | 301 redirect to story |
| curl_cffi all impersonations | 301 redirect |
| Playwright direct URL | 301 redirect |
| **Playwright click from story page** | **301 redirect** |
| Client-side navigation | 301 redirect |
| With cookies/session | 301 redirect |

### Root Cause Confirmed

**Server-side redirect is ENFORCED for all chapter URLs regardless of:**
- Browser vs crawler
- TLS fingerprint
- Cookies/session
- Referer header
- Client-side vs server-side navigation

### Possible Reasons

1. **Site requires login** to read chapters (paywall/registration wall)
2. **Site changed business model** - chapters no longer publicly accessible
3. **Technical issue** on site's end

### Recommendation

**Disable quykiep crawling** until one of these is resolved:
1. Site fixes their chapter access
2. We implement login automation
3. Site provides an API for chapter access

### Alternative: Login-Based Access

If login is required, implementation would be:

```python
class QuykiepAuthAdapter:
    async def login(self, username: str, password: str):
        """Login and store session cookies."""
        page = await self.browser.new_page()
        await page.goto("https://quykiep.com")

        # Click login button
        await page.click('button:has-text("Đăng nhập")')

        # Fill credentials
        await page.fill('input[name="email"]', username)
        await page.fill('input[name="password"]', password)
        await page.click('button[type="submit"]')

        # Store cookies
        self.cookies = await page.context.cookies()

    async def get_chapter_with_auth(self, chapter_url: str):
        """Access chapter with authenticated session."""
        page = await self.browser.new_page()
        await page.context.add_cookies(self.cookies)
        await page.goto(chapter_url)
        # ... extract content
```

### Harvester Upgrade Recommendations

Based on this analysis, the Harvester should be upgraded with:

1. **Redirect Detection**
   ```python
   async def harvest(self, url):
       response = await self.fetch(url)
       if response.was_redirected:
           logger.warning(f"Chapter redirected: {url} -> {response.final_url}")
           return HarvestResult(success=False, error="redirect_detected")
   ```

2. **Site-Specific Handlers**
   ```python
   SITE_HANDLERS = {
       "quykiep": QuykiepHandler,  # Requires login
       "xtruyen": XtruyenHandler,  # Works with curl_cffi
       "truyencom": TruyencomHandler,  # Works with curl_cffi
   }
   ```

3. **Login Session Management**
   ```python
   class AuthSessionManager:
       async def get_authenticated_session(self, site_key: str):
           if site_key in self.sessions:
               return self.sessions[site_key]
           # Login and cache session
           session = await self.login(site_key)
           self.sessions[site_key] = session
           return session
   ```

## Action Items

1. **Immediate:** Temporarily disable quykiep in crawler config
2. **Short-term:** Contact site admin or check if login requirement is documented
3. **Long-term:** Implement login-based harvesting if needed

## Files Modified/Created

- `/home/storyflow-core/scripts/explore_quykiep_antibot.py` - Anti-bot exploration
- `/home/storyflow-core/scripts/compare_site_antibot.py` - Site comparison
- `/home/storyflow-core/scripts/analyze_quykiep_redirect.py` - Redirect analysis
- `/home/storyflow-core/scripts/test_nextjs_navigation.py` - Client navigation test
- `/home/storyflow-core/docs/QUYKIEP_ANTIBOT_ANALYSIS.md` - This report

import json
import re
from functools import lru_cache

from bs4 import BeautifulSoup

from flowcore_story.config.config import ANTI_BOT_PATTERN_FILE
from flowcore_story.utils.io_utils import load_patterns


@lru_cache
def get_anti_bot_patterns():
    """Tải các pattern anti-bot từ file."""
    return load_patterns(ANTI_BOT_PATTERN_FILE)


def is_cloudflare_challenge(text: str, soup: BeautifulSoup | None = None) -> bool:
    """
    Detect Cloudflare Turnstile/Challenge specifically.

    Args:
        text: Raw HTML content
        soup: Optional pre-parsed BeautifulSoup object

    Returns:
        True if Cloudflare challenge is detected
    """
    if not text:
        return False

    lower_text = text.lower()

    # Cloudflare Turnstile & Challenge signatures
    # Note: 'ray id:' removed as it appears in all Cloudflare responses (not just challenges)
    # Note: 'challenge-platform' removed as it's a bot management script injected into ALL pages
    turnstile_indicators = [
        'cf-turnstile',
        'turnstile.render',
        'challenges.cloudflare.com/turnstile',
        'cf-challenge-form',  # Actual challenge form
        'cf_chl_opt',  # Challenge options
        'cf-wrapper',  # Challenge wrapper div
        'cf-error-details',
        'cf-alert',
    ]

    if any(indicator in lower_text for indicator in turnstile_indicators):
        return True

    # Check for Cloudflare challenge scripts
    # Note: Removed 'challenges\.cloudflare\.com' as challenge-platform scripts are injected into ALL pages
    # Only check for actual challenge-specific patterns
    challenge_script_patterns = [
        r'window\._cf_chl_opt',  # Challenge options object
        r'turnstile\.render',  # Turnstile CAPTCHA render
        r'cf-challenge-running',  # Active challenge indicator
    ]

    for pattern in challenge_script_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True

    # Parse HTML for challenge elements if not already parsed
    if soup is None:
        try:
            soup = BeautifulSoup(text, 'html.parser')
        except Exception:
            return False

    # Check for challenge containers and forms
    # Note: Made selectors more specific to avoid false positives
    challenge_selectors = [
        'div#challenge-running',  # Active challenge indicator
        'div#challenge-stage',  # Challenge stage container
        'div[class*="cf-challenge"]',
        'div[class*="cf-wrapper"]',
        'form#challenge-form',  # Actual challenge form (exact match)
        'iframe[src*="challenges.cloudflare.com/turnstile"]',  # Turnstile iframe
        'div.cf-error-details',
        'div#cf-error-details',
    ]

    for selector in challenge_selectors:
        if soup.select_one(selector):
            return True

    # Check meta tags for Cloudflare challenge indicators only
    # Note: Many legitimate pages have cloudflare-related meta tags
    # Only check for specific challenge-related meta tags
    meta_tags = soup.find_all('meta', attrs={'name': True})
    for meta in meta_tags:
        meta_name = meta.get('name', '').lower()
        # Only trigger on specific challenge meta tags
        if meta_name in ['cf-challenge', 'cf-turnstile']:
            return True

    return False


def is_anti_bot_content(text: str, debug_site_key: str = None) -> bool:
    if not text:
        return False

    stripped_text = text.strip()

    # JSON (including BOM-stripped) is considered safe, even if short
    cleaned = stripped_text.lstrip("\ufeff")
    try:
        json.loads(cleaned)
        return False
    except Exception:
        pass

    # Fast check for JSON content
    if (stripped_text.startswith('{') and stripped_text.endswith('}')) or \
       (stripped_text.startswith('[') and stripped_text.endswith(']')):
        # Likely JSON content (common for APIs), usually safe
        # Anti-bot pages are almost always HTML
        return False

    lower_text = text.lower()

    # Refined signature phrases to be more specific and avoid false positives
    # Note: Removed "performance & security by cloudflare" as it appears in legitimate pages' footer
    # Note: "access denied" needs to be checked more carefully - only in title/main content
    signature_phrases = [
        "bạn đã bị chặn",
        "bạn thao tác quá nhanh",  # XTruyen rate limit message
        "vui lòng thử lại sau",  # Vietnamese "please try again later"
        "checking your browser",
        "ddos protection by cloudflare",
        "verifying you are human",
        "enable javascript and cookies to continue",
        "cloudflare's security check",
        "please wait while we verify",
        "you have been blocked",
        "checking if the site connection is secure",  # TangThuVien Cloudflare
        "website đang bảo trì",
        "hệ thống đang bận",
        "phát hiện truy cập bất thường",
        "vui lòng nhập mã xác nhận",
        "please verify you are a human",
        "thao tác quá nhanh",
        "bạn đang truy cập quá nhanh",
        "you are being rate limited",
    ]

    for sig in signature_phrases:
        if sig in lower_text:
            if debug_site_key:
                from flowcore_story.utils.logger import logger
                logger.warning(
                    f"[AntiBot][{debug_site_key}] Triggered by signature phrase: '{sig}'"
                )
            return True

    # Check for "Just a moment" with Cloudflare
    if "just a moment" in lower_text and "cloudflare" in lower_text:
        if debug_site_key:
            from flowcore_story.utils.logger import logger
            logger.warning(
                f"[AntiBot][{debug_site_key}] Triggered by 'Just a moment' + Cloudflare"
            )
        return True

    # Check for "access denied" only if page is sparse (likely a block page)
    # Legitimate pages may have "access denied" in various contexts
    if "access denied" in lower_text:
        # Only trigger if content is short (likely a block page)
        if len(text) < 5000:
            # Check if it's in a meaningful context (title or main message)
            if "<title>" in lower_text and "access denied" in lower_text.split("<title>")[1].split("</title>")[0] if "</title>" in lower_text else False:
                if debug_site_key:
                    from flowcore_story.utils.logger import logger
                    logger.warning(
                        f"[AntiBot][{debug_site_key}] Triggered by 'access denied' in title"
                    )
                return True

    # Short content is suspicious - likely a block page or rate limit
    # But only if it doesn't contain expected content markers
    if len(text) < 500:
        # Check for valid HTML fragment markers (AJAX responses often return fragments)
        # These are common elements in chapter lists, story lists, etc.
        html_fragment_markers = [
            '<html', '<body', '<head',  # Full document
            '<ul', '<ol', '<li',  # Lists (chapter lists)
            '<div', '<span',  # Containers
            '<a href',  # Links
            '<table', '<tr', '<td',  # Tables
            '<p>', '<p ',  # Paragraphs
            '<!doctype',  # Document type
        ]

        has_valid_html = any(marker in lower_text for marker in html_fragment_markers)

        if not has_valid_html:
            # Check if it might be a simple WordPress response
            # WordPress often returns "0", "-1", or simple text for AJAX
            stripped = stripped_text.strip()
            if stripped in ('0', '-1', '1', 'success', 'error', 'true', 'false'):
                # Valid WordPress AJAX response
                return False

            # Check if it's a numeric response (common for AJAX)
            if stripped.lstrip('-').isdigit():
                return False

            if debug_site_key:
                from flowcore_story.utils.logger import logger
                logger.warning(
                    f"[AntiBot][{debug_site_key}] Triggered by short content (<500 chars) without valid HTML markers. Length: {len(text)}"
                )
            return True

        # If we have valid HTML markers, allow short content
        # This handles AJAX fragments, short responses, etc.
        # The has_valid_html check above already verified this is likely legitimate HTML

    # Parse HTML for detailed analysis
    try:
        soup = BeautifulSoup(text, 'html.parser')

        # Check if this is a Cloudflare challenge page
        if is_cloudflare_challenge(text, soup):
            if debug_site_key:
                from flowcore_story.utils.logger import logger
                logger.warning(
                    f"[AntiBot][{debug_site_key}] Triggered by is_cloudflare_challenge()"
                )
            return True

        # Check page title for challenge indicators
        title_tag = soup.find('title')
        if title_tag:
            lower_title = title_tag.get_text().lower()
            # Only check for specific challenge phrases in title
            # Removed "cloudflare" and "access denied" as they can appear in legitimate titles
            challenge_phrases = [
                "just a moment",
                "checking your browser",
                "ddos protection",
                "attention required",
                "checking connection",
                "please wait",
                "security check",
            ]
            for phrase in challenge_phrases:
                if phrase in lower_title:
                    if debug_site_key:
                        from flowcore_story.utils.logger import logger
                        logger.warning(
                            f"[AntiBot][{debug_site_key}] Triggered by title phrase: '{phrase}' in title: '{title_tag.get_text()}'"
                        )
                    return True

            # Special check: "Access denied" in title with Cloudflare indicators
            if "access denied" in lower_title and ("cloudflare" in lower_title or "cf-" in lower_text):
                if debug_site_key:
                    from flowcore_story.utils.logger import logger
                    logger.warning(
                        f"[AntiBot][{debug_site_key}] Triggered by 'access denied' + Cloudflare in title: '{title_tag.get_text()}'"
                    )
                return True

        # Check for sparse body content with JavaScript requirement
        body_tag = soup.body
        if body_tag:
            body_text = body_tag.get_text(strip=True)
            word_count = len(body_text.split())

            # Sparse page with JavaScript requirement
            if word_count < 50 and "javascript" in lower_text:
                if "enable javascript" in lower_text or "turn on javascript" in lower_text:
                    if debug_site_key:
                        from flowcore_story.utils.logger import logger
                        logger.warning(
                            f"[AntiBot][{debug_site_key}] Triggered by sparse content ({word_count} words) + JavaScript requirement"
                        )
                    return True

            # Check for CAPTCHA indicators - only flag if page has sparse content
            # Many legitimate pages have reCAPTCHA for comments/forms
            # Only consider it anti-bot if the page is mostly a CAPTCHA challenge
            if word_count < 100:  # Only check CAPTCHA on sparse pages
                captcha_indicators = [
                    'g-recaptcha',
                    'h-captcha',
                    'captcha-box',
                    'recaptcha',
                    'hcaptcha',
                ]

                for indicator in captcha_indicators:
                    if soup.select_one(f'[class*="{indicator}"], [id*="{indicator}"]'):
                        if debug_site_key:
                            from flowcore_story.utils.logger import logger
                            logger.warning(
                                f"[AntiBot][{debug_site_key}] Triggered by CAPTCHA indicator: '{indicator}' on sparse page ({word_count} words)"
                            )
                        return True

    except Exception:
        # If HTML parsing fails, assume it's not anti-bot
        return False

    return False

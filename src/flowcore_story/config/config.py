from __future__ import annotations

import asyncio
import os
import random
import re
from typing import Any
from urllib.parse import urlparse

from flowcore_story.config.env_loader import (
    EnvironmentConfigurationError,
    get_bool,
    get_float,
    get_int,
    get_list,
    get_str,
)
from flowcore_story.config.useragent_list import STATIC_USER_AGENTS
from flowcore_story.utils.async_primitives import LoopBoundLock

BASE_URL_ENV_MAP: dict[str, str] = {
    "xtruyen": "BASE_XTRUYEN",
    "tangthuvien": "BASE_TANGTHUVIEN",
    "truyencom": "BASE_TRUYENCOM",
    "quykiep": "BASE_QUYKIEP",
    "truyenfullmoi": "BASE_TRUYENFULLMOI",
    "metruyenful": "BASE_METRUYENFUL",
}

BASE_URL_DEFAULTS: dict[str, str] = {
    "xtruyen": "https://xtruyen.vn",
    "tangthuvien": "https://tangthuvien.net",
    "truyencom": "https://truyencom.com",
    "quykiep": "https://quykiep.com",
    "truyenfullmoi": "https://truyenfullmoi.com",
    "metruyenful": "https://metruyenful.com",
}

# Site-specific configuration
# Sites with strong Cloudflare protection that need more retries
DIFFICULT_SITES: set = {"tangthuvien", "xtruyen", "quykiep"}

# Site tiers for Kafka routing
SITE_TIERS = {
    "easy": ["truyencom", "metruyenful", "truyenfullmoi"],
    "medium": ["xtruyen"],
    "hard": ["tangthuvien", "quykiep"],
}

def get_topic_for_site(site_key: str, default_topic: str) -> str:
    """Determine the Kafka topic for a given site key based on its tier."""
    if not site_key:
        return default_topic
        
    for tier, sites in SITE_TIERS.items():
        if site_key in sites:
            return f"{default_topic}.{tier}"
            
    return f"{default_topic}.easy" # Default to easy tier

# Sites that require story ID in URL (e.g., truyencom uses slug.id format)
SITES_REQUIRING_ID: set = {"truyencom"}

# Minimum retry attempts for difficult sites
DIFFICULT_SITE_MIN_RETRIES: int = 4


def _sanitize_base_url(raw_value: str | None, env_name: str) -> str:
    """Normalize BASE_URL style inputs to avoid malformed URLs."""

    if raw_value is None:
        raise EnvironmentConfigurationError(f"Missing environment variable {env_name}")

    candidate = raw_value.strip()
    if not candidate:
        raise EnvironmentConfigurationError(f"Environment variable {env_name} must not be empty")

    candidate = candidate.rstrip("!?#'\"")
    candidate = candidate.rstrip("/ \t\n\r")
    if not candidate:
        raise EnvironmentConfigurationError(f"Environment variable {env_name} must not be empty")

    parsed = urlparse(candidate)
    if not parsed.scheme:
        candidate = f"https://{candidate}"
        parsed = urlparse(candidate)
        if not parsed.scheme:
            raise EnvironmentConfigurationError(
                f"Environment variable {env_name} must include scheme (e.g. https://)."
            )
    return candidate


def _load_base_urls() -> dict[str, str]:
    base_urls: dict[str, str] = {}
    for site_key, env_name in BASE_URL_ENV_MAP.items():
        raw_value = get_str(env_name)
        if raw_value is None:
            raw_value = BASE_URL_DEFAULTS.get(site_key)
            if raw_value is None:
                continue

        base_urls[site_key] = _sanitize_base_url(raw_value, env_name)

    enabled_site_keys = get_list("ENABLED_SITE_KEYS") or []
    if enabled_site_keys:
        missing_sites = [key for key in enabled_site_keys if key not in base_urls]
        if missing_sites:
            raise EnvironmentConfigurationError(
                "ENABLED_SITE_KEYS contains unsupported site(s): " + ", ".join(missing_sites)
            )
        base_urls = {key: base_urls[key] for key in enabled_site_keys}

    if not base_urls:
        raise EnvironmentConfigurationError("No BASE_URLS configured. Please check your environment settings.")

    return base_urls


def _normalize_optional_limit(value: int | None) -> int | None:
    if value is None or value <= 0:
        return None
    return value


def _normalize_positive_int(
    value: int | None, *, default: int, minimum: int = 1
) -> int:
    candidate = value if value is not None else default
    try:
        numeric = int(candidate)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        numeric = default
    return max(numeric, minimum)


def _normalize_positive_float(
    value: float | None, *, default: float, minimum: float = 0.0
) -> float:
    candidate = value if value is not None else default
    try:
        numeric = float(candidate)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        numeric = default
    return max(numeric, minimum)


def _normalize_ratio(
    value: float | None, *, default: float, minimum: float = 0.0, maximum: float = 1.0
) -> float:
    candidate = value if value is not None else default
    try:
        numeric = float(candidate)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        numeric = default
    if minimum > maximum:
        minimum, maximum = maximum, minimum
    if minimum == maximum:
        return float(minimum)
    return float(min(max(numeric, minimum), maximum))


def _ensure_directories(*paths: str) -> None:
    for path in paths:
        if path:
            os.makedirs(path, exist_ok=True)


def _load_settings() -> dict[str, Any]:
    base_urls = _load_base_urls()

    request_delay = get_float("REQUEST_DELAY", required=True)
    timeout_request = get_int("TIMEOUT_REQUEST", required=True)
    retry_attempts = get_int("RETRY_ATTEMPTS", required=True)
    delay_on_retry = get_float("DELAY_ON_RETRY", required=True)
    fallback_backoff = max(float(delay_on_retry) * 3.0, 2.0)
    rate_limit_backoff_seconds = _normalize_positive_float(
        get_float("RATE_LIMIT_BACKOFF_SECONDS"),
        default=fallback_backoff,
        minimum=0.0,
    )
    rate_limit_backoff_max_seconds = _normalize_positive_float(
        get_float("RATE_LIMIT_BACKOFF_MAX_SECONDS"),
        default=max(rate_limit_backoff_seconds * 4.0, rate_limit_backoff_seconds),
        minimum=0.0,
    )
    if rate_limit_backoff_max_seconds < rate_limit_backoff_seconds:
        rate_limit_backoff_max_seconds = rate_limit_backoff_seconds

    data_folder = get_str("DATA_FOLDER", required=True)
    completed_folder = get_str("COMPLETED_FOLDER", required=True)
    backup_folder = get_str("BACKUP_FOLDER", required=True)
    state_folder = get_str("STATE_FOLDER", required=True)
    log_folder = get_str("LOG_FOLDER", required=True)
    _ensure_directories(
        data_folder or "",
        completed_folder or "",
        backup_folder or "",
        state_folder or "",
        log_folder or "",
    )

    category_snapshot_db = get_str("CATEGORY_SNAPSHOT_DB_PATH")
    if not category_snapshot_db:
        category_snapshot_db = os.path.join(state_folder, "category_snapshots.sqlite3")
    snapshot_dir = os.path.dirname(category_snapshot_db)
    if snapshot_dir:
        _ensure_directories(snapshot_dir)

    metadata_db_path = get_str("METADATA_DB_PATH")
    if not metadata_db_path:
        metadata_db_path = os.path.join(state_folder, "metadata.sqlite3")
    metadata_dir = os.path.dirname(metadata_db_path)
    if metadata_dir:
        _ensure_directories(metadata_dir)

    retry_genre_round_limit = get_int("RETRY_GENRE_ROUND_LIMIT", required=True)
    retry_sleep_seconds = get_int("RETRY_SLEEP_SECONDS", required=True)
    retry_failed_chapters_passes = get_int("RETRY_FAILED_CHAPTERS_PASSES", required=True)
    num_chapter_batches = get_int("NUM_CHAPTER_BATCHES", required=True)
    max_chapters_per_story = _normalize_optional_limit(get_int("MAX_CHAPTERS_PER_STORY"))
    retry_story_round_limit = get_int("RETRY_STORY_ROUND_LIMIT", required=True)
    skipped_stories_file = get_str("SKIPPED_STORIES_FILE", required=True)
    max_chapter_retry = get_int("MAX_CHAPTER_RETRY", required=True)

    kafka_topic = get_str("KAFKA_TOPIC", required=True)
    kafka_bootstrap_servers = get_str("KAFKA_BROKERS", required=True)
    kafka_group_id = get_str("KAFKA_GROUP_ID", required=True)
    kafka_bootstrap_max_retries = get_int("KAFKA_BOOTSTRAP_MAX_RETRIES", required=True)
    kafka_bootstrap_retry_delay = get_float("KAFKA_BOOTSTRAP_RETRY_DELAY", required=True)
    kafka_max_request_size = get_int("KAFKA_MAX_REQUEST_SIZE") or (2 * 1024 * 1024) # Default to 2MB
    progress_topic = get_str("PROGRESS_TOPIC") or f"{kafka_topic}.progress"
    progress_group_id = get_str("PROGRESS_GROUP_ID") or f"{kafka_group_id}-progress"
    kafka_security_protocol = get_str("KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism = get_str("KAFKA_SASL_MECHANISM")
    kafka_username = get_str("KAFKA_USERNAME")
    kafka_password = get_str("KAFKA_PASSWORD")
    kafka_ssl_ca_file = get_str("KAFKA_SSL_CA_FILE")
    kafka_ssl_verify = get_bool("KAFKA_SSL_VERIFY")
    if kafka_ssl_verify is None:
        kafka_ssl_verify = True
    dashboard_kafka_username = get_str("DASHBOARD_KAFKA_USERNAME")
    dashboard_kafka_password = get_str("DASHBOARD_KAFKA_PASSWORD")

    kafka_backlog_warn_raw = get_int("KAFKA_BACKLOG_WARN")
    kafka_backlog_error_raw = get_int("KAFKA_BACKLOG_ERROR")
    kafka_backlog_warn = max(int(kafka_backlog_warn_raw), 0) if kafka_backlog_warn_raw is not None else 100
    kafka_backlog_error = (
        max(int(kafka_backlog_error_raw), 0)
        if kafka_backlog_error_raw is not None
        else 1000
    )

    error_rate_warn_raw = get_float("CRAWL_ERROR_RATE_WARN")
    error_rate_error_raw = get_float("CRAWL_ERROR_RATE_ERROR")
    crawl_error_rate_warn = (
        max(float(error_rate_warn_raw), 0.0)
        if error_rate_warn_raw is not None
        else 0.15
    )
    crawl_error_rate_error = (
        max(float(error_rate_error_raw), 0.0)
        if error_rate_error_raw is not None
        else 0.3
    )

    dead_story_warn_raw = get_int("DEAD_STORY_WARN_THRESHOLD")
    dead_story_error_raw = get_int("DEAD_STORY_ERROR_THRESHOLD")
    dead_story_warn_threshold = (
        max(int(dead_story_warn_raw), 0)
        if dead_story_warn_raw is not None
        else 5
    )
    dead_story_error_threshold = (
        max(int(dead_story_error_raw), 0)
        if dead_story_error_raw is not None
        else 20
    )

    dead_story_refresh_raw = get_float("DEAD_STORY_REFRESH_SECONDS")
    dead_story_refresh_seconds = (
        max(float(dead_story_refresh_raw), 30.0)
        if dead_story_refresh_raw is not None
        else 300.0
    )

    prometheus_pushgateway_url = get_str("PROMETHEUS_PUSHGATEWAY_URL")
    prometheus_job_name = get_str("PROMETHEUS_JOB_NAME") or "storyflow_crawler"
    prometheus_push_interval_raw = get_float("PROMETHEUS_PUSH_INTERVAL")
    prometheus_push_interval = (
        max(float(prometheus_push_interval_raw), 15.0)
        if prometheus_push_interval_raw is not None
        else 60.0
    )

    cookie_renew_interval_raw = get_float("COOKIE_RENEW_CHECK_INTERVAL")
    cookie_renew_interval = (
        max(float(cookie_renew_interval_raw), 30.0)
        if cookie_renew_interval_raw is not None
        else 300.0
    )
    cookie_renew_window_raw = get_float("COOKIE_RENEW_REFRESH_WINDOW")
    cookie_renew_window = (
        max(float(cookie_renew_window_raw), 60.0)
        if cookie_renew_window_raw is not None
        else 900.0
    )
    cookie_renew_min_interval_raw = get_float("COOKIE_RENEW_MIN_INTERVAL")
    cookie_renew_min_interval = (
        max(float(cookie_renew_min_interval_raw), 60.0)
        if cookie_renew_min_interval_raw is not None
        else 300.0
    )
    cookie_renew_max_age_raw = get_float("COOKIE_RENEW_MAX_AGE")
    cookie_renew_max_age = (
        max(float(cookie_renew_max_age_raw), cookie_renew_interval)
        if cookie_renew_max_age_raw is not None
        else max(3600.0, cookie_renew_interval)
    )

    rate_limit_window_raw = get_float("RATE_LIMIT_WINDOW_SECONDS")
    rate_limit_window_seconds = (
        max(float(rate_limit_window_raw), 60.0)
        if rate_limit_window_raw is not None
        else 600.0
    )
    rate_limit_threshold = _normalize_ratio(
        get_float("RATE_LIMIT_ALERT_THRESHOLD"), default=0.35
    )
    rate_limit_min_requests = _normalize_positive_int(
        get_int("RATE_LIMIT_ALERT_MIN_REQUESTS"), default=20, minimum=1
    )
    rate_limit_cooldown_raw = get_float("RATE_LIMIT_ALERT_COOLDOWN")
    rate_limit_cooldown = (
        max(float(rate_limit_cooldown_raw), 60.0)
        if rate_limit_cooldown_raw is not None
        else 600.0
    )

    challenge_harvester_enabled = bool(get_bool("CHALLENGE_HARVESTER_ENABLED") or False)
    challenge_harvester_url = get_str("CHALLENGE_HARVESTER_URL")
    challenge_harvester_timeout_raw = get_float("CHALLENGE_HARVESTER_TIMEOUT")
    challenge_harvester_timeout = (
        max(float(challenge_harvester_timeout_raw), 5.0)
        if challenge_harvester_timeout_raw is not None
        else 45.0
    )

    # Sites that should use curl_cffi impersonate mode instead of Playwright
    # This bypasses browser-based anti-bot by using TLS fingerprint impersonation
    # Format: comma-separated site keys, e.g., "tangthuvien,truyencom"
    impersonate_sites_raw = get_str("IMPERSONATE_SITES") or "tangthuvien"
    impersonate_sites = set(
        s.strip().lower()
        for s in impersonate_sites_raw.split(",")
        if s.strip()
    )
    # Default impersonate profile (browser to mimic)
    impersonate_profile = get_str("IMPERSONATE_PROFILE") or "chrome120"

    # FIX 2025-12-10: Site-specific impersonate profiles
    # Some sites work better with specific TLS fingerprints
    # tangthuvien: safari15_5 works, chrome profiles get TLS errors with proxy
    # Format: IMPERSONATE_PROFILE_<SITE_KEY>=<profile>
    site_impersonate_profiles: dict[str, str] = {}
    for site_key in impersonate_sites:
        env_key = f"IMPERSONATE_PROFILE_{site_key.upper()}"
        site_profile = get_str(env_key)
        if site_profile:
            site_impersonate_profiles[site_key] = site_profile.strip()

    # Default profiles for known problematic sites
    # tangthuvien: Use Playwright (full browser) instead of curl_cffi
    # Empty string = no impersonate = use Playwright for better Cloudflare bypass
    if "tangthuvien" not in site_impersonate_profiles:
        site_impersonate_profiles["tangthuvien"] = ""  # Empty = use Playwright

    # ZenRows has been removed - no longer needed

    # Profile Management & Fingerprinting
    enable_fingerprint_pool = bool(get_bool("ENABLE_FINGERPRINT_POOL") or False)
    fingerprint_pool_size_per_proxy = _normalize_positive_int(
        get_int("FINGERPRINT_POOL_SIZE_PER_PROXY"),
        default=5,
        minimum=1,
    )
    fingerprint_rotation_interval_raw = get_float("FINGERPRINT_ROTATION_INTERVAL")
    fingerprint_rotation_interval = (
        max(float(fingerprint_rotation_interval_raw), 300.0)
        if fingerprint_rotation_interval_raw is not None
        else 3600.0
    )
    enable_headers_randomization = bool(get_bool("ENABLE_HEADERS_RANDOMIZATION") or False)
    randomize_headers_order = bool(get_bool("RANDOMIZE_HEADERS_ORDER") or False)
    randomize_headers_casing = bool(get_bool("RANDOMIZE_HEADERS_CASING") or False)
    fingerprint_storage_dir = get_str("FINGERPRINT_STORAGE_DIR") or state_folder
    fingerprint_diversity_factor_raw = get_float("FINGERPRINT_DIVERSITY_FACTOR")
    fingerprint_diversity_factor = (
        max(float(fingerprint_diversity_factor_raw), 0.5)
        if fingerprint_diversity_factor_raw is not None
        else 1.0
    )
    fingerprint_max_age_hours_raw = get_int("FINGERPRINT_MAX_AGE_HOURS")
    fingerprint_max_age_hours = (
        max(int(fingerprint_max_age_hours_raw), 1)
        if fingerprint_max_age_hours_raw is not None
        else 48
    )
    fingerprint_auto_cleanup_interval_raw = get_float("FINGERPRINT_AUTO_CLEANUP_INTERVAL")
    fingerprint_auto_cleanup_interval = (
        max(float(fingerprint_auto_cleanup_interval_raw), 600.0)
        if fingerprint_auto_cleanup_interval_raw is not None
        else 7200.0
    )
    enable_http2 = bool(get_bool("ENABLE_HTTP2") or True)
    enable_http3 = bool(get_bool("ENABLE_HTTP3") or False)

    playwright_max_retries = _normalize_positive_int(
        get_int("PLAYWRIGHT_MAX_RETRIES"),
        default=3,
        minimum=1,
    )
    playwright_request_timeout = _normalize_positive_int(
        get_int("PLAYWRIGHT_REQUEST_TIMEOUT"),
        default=25,
        minimum=10,
    )
    playwright_wait_for_networkidle = bool(
        get_bool("PLAYWRIGHT_WAIT_FOR_NETWORKIDLE") or False
    )
    playwright_skip_proxy = bool(get_bool("PLAYWRIGHT_SKIP_PROXY") or False)

    use_proxy = bool(get_bool("USE_PROXY", required=True))
    proxies_folder = get_str("PROXIES_FOLDER", required=True)
    proxies_file = get_str("PROXIES_FILE", required=True)
    global_proxy_username = get_str("PROXY_USER")
    global_proxy_password = get_str("PROXY_PASS")
    proxy_api_url = get_str("PROXY_API_URL")

    # Single proxy URL support - if set, this takes priority over file/API
    # Format: http://user:pass@host:port or http://host:port
    proxy_url = get_str("PROXY_URL")
    proxy_host = get_str("PROXY_HOST")
    proxy_port = get_str("PROXY_PORT")

    # Build PROXY_URL from components if not directly specified
    if not proxy_url and proxy_host and proxy_port:
        if global_proxy_username and global_proxy_password:
            proxy_url = f"http://{global_proxy_username}:{global_proxy_password}@{proxy_host}:{proxy_port}"
        else:
            proxy_url = f"http://{proxy_host}:{proxy_port}"

    telegram_bot_token = get_str("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = get_str("TELEGRAM_CHAT_ID")
    telegram_thread_id = get_str("TELEGRAM_THREAD_ID")
    telegram_parse_mode = get_str("TELEGRAM_PARSE_MODE")
    telegram_disable_preview = get_bool("TELEGRAM_DISABLE_WEB_PAGE_PREVIEW")

    telegram_allowed_chat_ids = get_list("TELEGRAM_ALLOWED_CHAT_IDS") or []
    telegram_allowed_user_ids = get_list("TELEGRAM_ALLOWED_USER_IDS") or []

    # Always include the configured notification chat in the allow-list so the
    # dashboard bot keeps working without extra configuration.
    if telegram_chat_id:
        telegram_allowed_chat_ids = list(
            dict.fromkeys(telegram_allowed_chat_ids + [str(telegram_chat_id)])
        )

    max_genres_to_crawl = _normalize_optional_limit(get_int("MAX_GENRES_TO_CRAWL"))
    max_stories_per_genre_page = _normalize_optional_limit(get_int("MAX_STORIES_PER_GENRE_PAGE"))
    max_stories_total_per_genre = _normalize_optional_limit(get_int("MAX_STORIES_TOTAL_PER_GENRE"))
    max_chapter_pages_to_crawl = _normalize_optional_limit(get_int("MAX_CHAPTER_PAGES_TO_CRAWL"))

    story_loop_min_items = _normalize_positive_int(
        get_int("STORY_LOOP_MIN_ITEMS"), default=60, minimum=1
    )
    story_loop_duplicate_ratio = _normalize_ratio(
        get_float("STORY_LOOP_DUPLICATE_RATIO"), default=0.6
    )
    story_loop_max_pattern_length = _normalize_positive_int(
        get_int("STORY_LOOP_MAX_PATTERN_LENGTH"), default=12, minimum=1
    )
    story_loop_min_pattern_repetitions = _normalize_positive_int(
        get_int("STORY_LOOP_MIN_PATTERN_REPETITIONS"), default=3, minimum=2
    )

    async_semaphore_limit = get_int("ASYNC_SEMAPHORE_LIMIT", required=True)
    genre_async_limit = get_int("GENRE_ASYNC_LIMIT", required=True)
    genre_batch_size = get_int("GENRE_BATCH_SIZE", required=True)
    story_async_limit = get_int("STORY_ASYNC_LIMIT", required=True)
    story_batch_size = get_int("STORY_BATCH_SIZE", required=True)

    category_batch_job_size = _normalize_optional_limit(get_int("CATEGORY_BATCH_JOB_SIZE"))
    category_max_jobs_per_batch = _normalize_optional_limit(get_int("CATEGORY_MAX_JOBS_PER_BATCH"))
    category_max_jobs_per_domain = _normalize_optional_limit(get_int("CATEGORY_MAX_JOBS_PER_DOMAIN"))

    category_change_refresh_ratio = (
        get_float("CATEGORY_CHANGE_REFRESH_RATIO") or 0.35
    )
    category_change_refresh_absolute = (
        get_int("CATEGORY_CHANGE_REFRESH_ABSOLUTE") or 50
    )
    category_refresh_batch_size = (
        get_int("CATEGORY_REFRESH_BATCH_SIZE") or story_batch_size
    )
    category_change_min_stories = (
        get_int("CATEGORY_CHANGE_MIN_STORIES") or 40
    )

    failed_genres_file = get_str("FAILED_GENRES_FILE", required=True)
    error_chapters_file = get_str("ERROR_CHAPTERS_FILE", required=True)
    missing_chapters_file = get_str("MISSING_CHAPTERS_FILE", required=True)
    banned_proxies_log = get_str("BANNED_PROXIES_LOG", required=True)
    pattern_file = get_str("PATTERN_FILE", required=True)
    anti_bot_pattern_file = get_str("ANTI_BOT_PATTERN_FILE", required=True)

    batch_size_override = get_int("BATCH_SIZE")

    ai_profiles_path = get_str("AI_PROFILES_PATH", required=True)
    ai_metrics_path = get_str("AI_METRICS_PATH", required=True)
    ai_model = get_str("AI_MODEL", required=True)
    ai_profile_ttl_hours = get_int("AI_PROFILE_TTL_HOURS", required=True)
    openai_base = get_str("OPENAI_BASE", required=True)
    openai_api_key = get_str("OPENAI_API_KEY")
    ai_trim_max_bytes = get_int("AI_TRIM_MAX_BYTES", required=True)
    ai_print_metrics = bool(get_bool("AI_PRINT_METRICS") or False)

    default_mode = get_str("MODE")
    default_crawl_mode = get_str("CRAWL_MODE")


    missing_timeout = get_int("MISSING_CRAWL_TIMEOUT_SECONDS", required=True)
    missing_timeout_per_chapter = get_float("MISSING_CRAWL_TIMEOUT_PER_CHAPTER", required=True)
    missing_timeout_max = get_int("MISSING_CRAWL_TIMEOUT_MAX", required=True)

    # Missing chapter worker batch size optimization
    missing_chapter_batch_size = get_int("MISSING_CHAPTER_BATCH_SIZE", required=False) or 250

    missing_warning_topic = get_str("MISSING_WARNING_TOPIC", required=True)
    missing_warning_group = get_str("MISSING_WARNING_GROUP", required=True)

    # Persistent queue configuration
    enable_persistent_queue = bool(get_bool("ENABLE_PERSISTENT_QUEUE") or True)
    queue_batch_size = get_int("QUEUE_BATCH_SIZE") or 10
    queue_stale_minutes = get_int("QUEUE_STALE_MINUTES") or 30
    queue_retention_days = get_int("QUEUE_RETENTION_DAYS") or 7

    return {
        "BASE_URLS": base_urls,
        "ENABLED_SITE_KEYS": list(base_urls.keys()),
        "REQUEST_DELAY": request_delay,
        "TIMEOUT_REQUEST": timeout_request,
        "RETRY_ATTEMPTS": retry_attempts,
        "DELAY_ON_RETRY": delay_on_retry,
        "RATE_LIMIT_BACKOFF_SECONDS": rate_limit_backoff_seconds,
        "RATE_LIMIT_BACKOFF_MAX_SECONDS": rate_limit_backoff_max_seconds,
        "DATA_FOLDER": data_folder,
        "COMPLETED_FOLDER": completed_folder,
        "BACKUP_FOLDER": backup_folder,
        "STATE_FOLDER": state_folder,
        "LOG_FOLDER": log_folder,
        "CATEGORY_SNAPSHOT_DB_PATH": category_snapshot_db,
        "METADATA_DB_PATH": metadata_db_path,
        "RETRY_GENRE_ROUND_LIMIT": retry_genre_round_limit,
        "RETRY_SLEEP_SECONDS": retry_sleep_seconds,
        "RETRY_FAILED_CHAPTERS_PASSES": retry_failed_chapters_passes,
        "NUM_CHAPTER_BATCHES": num_chapter_batches,
        "MAX_CHAPTERS_PER_STORY": max_chapters_per_story,
        "RETRY_STORY_ROUND_LIMIT": retry_story_round_limit,
        "SKIPPED_STORIES_FILE": skipped_stories_file,
        "MAX_CHAPTER_RETRY": max_chapter_retry,
        "KAFKA_TOPIC": kafka_topic,
        "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
        "KAFKA_GROUP_ID": kafka_group_id,
        "KAFKA_BOOTSTRAP_MAX_RETRIES": kafka_bootstrap_max_retries,
        "KAFKA_BOOTSTRAP_RETRY_DELAY": kafka_bootstrap_retry_delay,
        "KAFKA_MAX_REQUEST_SIZE": kafka_max_request_size,
        "PROGRESS_TOPIC": progress_topic,
        "PROGRESS_GROUP_ID": progress_group_id,
        "KAFKA_SECURITY_PROTOCOL": kafka_security_protocol,
        "KAFKA_SASL_MECHANISM": kafka_sasl_mechanism,
        "KAFKA_USERNAME": kafka_username,
        "KAFKA_PASSWORD": kafka_password,
        "KAFKA_SSL_CA_FILE": kafka_ssl_ca_file,
        "KAFKA_SSL_VERIFY": kafka_ssl_verify,
        "DASHBOARD_KAFKA_USERNAME": dashboard_kafka_username,
        "DASHBOARD_KAFKA_PASSWORD": dashboard_kafka_password,
        "KAFKA_BACKLOG_WARN": kafka_backlog_warn,
        "KAFKA_BACKLOG_ERROR": kafka_backlog_error,
        "CRAWL_ERROR_RATE_WARN": crawl_error_rate_warn,
        "CRAWL_ERROR_RATE_ERROR": crawl_error_rate_error,
        "DEAD_STORY_WARN_THRESHOLD": dead_story_warn_threshold,
        "DEAD_STORY_ERROR_THRESHOLD": dead_story_error_threshold,
        "DEAD_STORY_REFRESH_SECONDS": dead_story_refresh_seconds,
        "PROMETHEUS_PUSHGATEWAY_URL": prometheus_pushgateway_url,
        "PROMETHEUS_JOB_NAME": prometheus_job_name,
        "PROMETHEUS_PUSH_INTERVAL": prometheus_push_interval,
        "COOKIE_RENEW_CHECK_INTERVAL": cookie_renew_interval,
        "COOKIE_RENEW_REFRESH_WINDOW": cookie_renew_window,
        "COOKIE_RENEW_MIN_INTERVAL": cookie_renew_min_interval,
        "COOKIE_RENEW_MAX_AGE": cookie_renew_max_age,
        "RATE_LIMIT_WINDOW_SECONDS": rate_limit_window_seconds,
        "RATE_LIMIT_ALERT_THRESHOLD": rate_limit_threshold,
        "RATE_LIMIT_ALERT_MIN_REQUESTS": rate_limit_min_requests,
        "RATE_LIMIT_ALERT_COOLDOWN": rate_limit_cooldown,
        "CHALLENGE_HARVESTER_ENABLED": challenge_harvester_enabled,
        "CHALLENGE_HARVESTER_URL": challenge_harvester_url,
        "CHALLENGE_HARVESTER_TIMEOUT": challenge_harvester_timeout,
        "IMPERSONATE_SITES": impersonate_sites,
        "IMPERSONATE_PROFILE": impersonate_profile,
        "SITE_IMPERSONATE_PROFILES": site_impersonate_profiles,
        "ENABLE_FINGERPRINT_POOL": enable_fingerprint_pool,
        "FINGERPRINT_POOL_SIZE_PER_PROXY": fingerprint_pool_size_per_proxy,
        "FINGERPRINT_ROTATION_INTERVAL": fingerprint_rotation_interval,
        "ENABLE_HEADERS_RANDOMIZATION": enable_headers_randomization,
        "RANDOMIZE_HEADERS_ORDER": randomize_headers_order,
        "RANDOMIZE_HEADERS_CASING": randomize_headers_casing,
        "FINGERPRINT_STORAGE_DIR": fingerprint_storage_dir,
        "FINGERPRINT_DIVERSITY_FACTOR": fingerprint_diversity_factor,
        "FINGERPRINT_MAX_AGE_HOURS": fingerprint_max_age_hours,
        "FINGERPRINT_AUTO_CLEANUP_INTERVAL": fingerprint_auto_cleanup_interval,
        "ENABLE_HTTP2": enable_http2,
        "ENABLE_HTTP3": enable_http3,
        "PLAYWRIGHT_MAX_RETRIES": playwright_max_retries,
        "PLAYWRIGHT_REQUEST_TIMEOUT": playwright_request_timeout,
        "PLAYWRIGHT_WAIT_FOR_NETWORKIDLE": playwright_wait_for_networkidle,
        "PLAYWRIGHT_SKIP_PROXY": playwright_skip_proxy,
        "USE_PROXY": use_proxy,
        "PROXIES_FOLDER": proxies_folder,
        "PROXIES_FILE": proxies_file,
        "GLOBAL_PROXY_USERNAME": global_proxy_username,
        "GLOBAL_PROXY_PASSWORD": global_proxy_password,
        "PROXY_API_URL": proxy_api_url,
        "PROXY_URL": proxy_url,
        "TELEGRAM_BOT_TOKEN": telegram_bot_token,
        "TELEGRAM_CHAT_ID": telegram_chat_id,
        "TELEGRAM_THREAD_ID": telegram_thread_id,
        "TELEGRAM_PARSE_MODE": telegram_parse_mode,
        "TELEGRAM_DISABLE_WEB_PAGE_PREVIEW": telegram_disable_preview,
        "TELEGRAM_ALLOWED_CHAT_IDS": telegram_allowed_chat_ids,
        "TELEGRAM_ALLOWED_USER_IDS": telegram_allowed_user_ids,
        "MAX_GENRES_TO_CRAWL": max_genres_to_crawl,
        "MAX_STORIES_PER_GENRE_PAGE": max_stories_per_genre_page,
        "MAX_STORIES_TOTAL_PER_GENRE": max_stories_total_per_genre,
        "MAX_CHAPTER_PAGES_TO_CRAWL": max_chapter_pages_to_crawl,
        "STORY_LOOP_MIN_ITEMS": story_loop_min_items,
        "STORY_LOOP_DUPLICATE_RATIO": story_loop_duplicate_ratio,
        "STORY_LOOP_MAX_PATTERN_LENGTH": story_loop_max_pattern_length,
        "STORY_LOOP_MIN_PATTERN_REPETITIONS": story_loop_min_pattern_repetitions,
        "ASYNC_SEMAPHORE_LIMIT": async_semaphore_limit,
        "GENRE_ASYNC_LIMIT": genre_async_limit,
        "GENRE_BATCH_SIZE": genre_batch_size,
        "STORY_ASYNC_LIMIT": story_async_limit,
        "STORY_BATCH_SIZE": story_batch_size,
        "CATEGORY_CHANGE_REFRESH_RATIO": category_change_refresh_ratio,
        "CATEGORY_CHANGE_REFRESH_ABSOLUTE": category_change_refresh_absolute,
        "CATEGORY_REFRESH_BATCH_SIZE": category_refresh_batch_size,
        "CATEGORY_CHANGE_MIN_STORIES": category_change_min_stories,
        "CATEGORY_BATCH_JOB_SIZE": category_batch_job_size,
        "CATEGORY_MAX_JOBS_PER_BATCH": category_max_jobs_per_batch,
        "CATEGORY_MAX_JOBS_PER_DOMAIN": category_max_jobs_per_domain,
        "FAILED_GENRES_FILE": failed_genres_file,
        "ERROR_CHAPTERS_FILE": error_chapters_file,
        "MISSING_CHAPTERS_FILE": missing_chapters_file,
        "BANNED_PROXIES_LOG": banned_proxies_log,
        "PATTERN_FILE": pattern_file,
        "ANTI_BOT_PATTERN_FILE": anti_bot_pattern_file,
        "BATCH_SIZE_OVERRIDE": batch_size_override,
        "AI_PROFILES_PATH": ai_profiles_path,
        "AI_METRICS_PATH": ai_metrics_path,
        "AI_MODEL": ai_model,
        "AI_PROFILE_TTL_HOURS": ai_profile_ttl_hours,
        "OPENAI_BASE": openai_base,
        "OPENAI_API_KEY": openai_api_key,
        "AI_TRIM_MAX_BYTES": ai_trim_max_bytes,
        "AI_PRINT_METRICS": ai_print_metrics,
        "DEFAULT_MODE": default_mode,
        "DEFAULT_CRAWL_MODE": default_crawl_mode,
        "MISSING_CRAWL_TIMEOUT_SECONDS": missing_timeout,
        "MISSING_CRAWL_TIMEOUT_PER_CHAPTER": missing_timeout_per_chapter,
        "MISSING_CRAWL_TIMEOUT_MAX": missing_timeout_max,
        "MISSING_CHAPTER_BATCH_SIZE": missing_chapter_batch_size,
        "MISSING_WARNING_TOPIC": missing_warning_topic,
        "MISSING_WARNING_GROUP": missing_warning_group,
        "ENABLE_PERSISTENT_QUEUE": enable_persistent_queue,
        "QUEUE_BATCH_SIZE": queue_batch_size,
        "QUEUE_STALE_MINUTES": queue_stale_minutes,
        "QUEUE_RETENTION_DAYS": queue_retention_days,
    }


def _apply_settings(settings: dict[str, Any]) -> None:
    globals().update(settings)


def reload_from_env() -> None:
    """Reload configuration values from the current environment."""

    _apply_settings(_load_settings())


# Default placeholders; populated by `_apply_settings` at import time.
BASE_URLS: dict[str, str] = {}
STATE_FOLDER: str = ""

# Load initial configuration on module import.
_apply_settings(_load_settings())


ENABLED_SITE_KEYS: list[str] = list(BASE_URLS.keys())

HEADER_PATTERNS = [r"^nguồn:", r"^truyện:", r"^thể loại:", r"^chương:"]
HEADER_RE = re.compile("|".join(HEADER_PATTERNS), re.IGNORECASE)

SITE_SELECTORS: dict[str, Any] = {}


LOADED_PROXIES: list[str] = []
LOCK = LoopBoundLock()


_MOBILE_USER_AGENT_KEYWORDS = (
    "android",
    "iphone",
    "ipad",
    "ipod",
    "iemobile",
    "mobile",
    "opera mini",
    "blackberry",
    "windows phone",
)


def _is_desktop_user_agent(user_agent: str) -> bool:
    lowered = user_agent.lower()
    return not any(keyword in lowered for keyword in _MOBILE_USER_AGENT_KEYWORDS)


async def _get_fake_user_agent() -> str | None:
    global _UA_OBJ, _DISABLE_FAKE_UA
    if _DISABLE_FAKE_UA:
        return None

    loop = asyncio.get_event_loop()
    if _UA_OBJ is None:
        _UA_OBJ = await loop.run_in_executor(None, _init_user_agent)
        if _UA_OBJ is None:
            _DISABLE_FAKE_UA = True
            return None

    try:
        return await loop.run_in_executor(None, lambda: _UA_OBJ.random)  # type: ignore
    except Exception:
        _DISABLE_FAKE_UA = True
        return None


async def get_random_user_agent(desktop_only: bool = False) -> str:
    global _UA_OBJ, _DISABLE_FAKE_UA

    attempts = 5 if desktop_only else 1
    for _ in range(attempts):
        candidate = await _get_fake_user_agent()
        if candidate is None:
            candidate = random.choice(STATIC_USER_AGENTS)

        if not desktop_only or _is_desktop_user_agent(candidate):
            return candidate

    for ua in STATIC_USER_AGENTS:
        if not desktop_only or _is_desktop_user_agent(ua):
            return ua

    return random.choice(STATIC_USER_AGENTS)


_UA_OBJ = None
_DISABLE_FAKE_UA = False


def _init_user_agent():
    try:
        from fake_useragent import UserAgent

        return UserAgent(fallback=STATIC_USER_AGENTS[0])
    except Exception as e:
        print(f"Lỗi khi khởi tạo UserAgent: {e}")
        return None


async def get_random_headers(site_key, desktop_only: bool = False):
    """
    Generate randomized HTTP headers with modern browser fingerprinting.

    Includes Sec-CH-UA headers (Chrome 90+) to avoid detection by advanced
    anti-bot systems like Cloudflare Turnstile.
    """
    ua_string = await get_random_user_agent(desktop_only=desktop_only)

    # Parse Chrome version from User-Agent for Sec-CH-UA headers
    chrome_version = "120"  # Default
    chrome_match = re.search(r'Chrome/(\d+)', ua_string)
    if chrome_match:
        chrome_version = chrome_match.group(1)

    # Detect if this is a Chromium-based browser
    is_chromium = 'chrome' in ua_string.lower() or 'edge' in ua_string.lower()

    # Base headers for all browsers
    headers = {
        "User-Agent": ua_string,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
    }

    # Add Chromium-specific headers (Sec-CH-UA)
    if is_chromium:
        # Sec-CH-UA: Client Hints for User-Agent
        headers["Sec-CH-UA"] = f'"Chromium";v="{chrome_version}", "Google Chrome";v="{chrome_version}", "Not=A?Brand";v="24"'
        headers["Sec-CH-UA-Mobile"] = "?0"
        headers["Sec-CH-UA-Platform"] = '"Windows"'

        # Sec-Fetch headers (mandatory for modern Chrome)
        headers["Sec-Fetch-Dest"] = "document"
        headers["Sec-Fetch-Mode"] = "navigate"
        headers["Sec-Fetch-Site"] = "none"
        headers["Sec-Fetch-User"] = "?1"

    # Add referer for site-specific requests
    base_url = BASE_URLS.get(site_key)
    if base_url:
        headers["Referer"] = base_url.rstrip('/') + '/'

    return headers


def get_state_file(site_key: str) -> str:
    return os.path.join(STATE_FOLDER, f"crawl_state_{site_key}.json")


def load_blacklist_patterns(file_path):
    patterns, contains_list = [], []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith('^') or line.endswith('$') or re.search(r'[.*?|\[\]()\\]', line):
                patterns.append(re.compile(line, re.IGNORECASE))
            else:
                contains_list.append(line.lower())
    return patterns, contains_list

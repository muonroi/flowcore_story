import os
from collections.abc import Iterable

from flowcore.utils.logger import logger

_ENABLE_ENV_VAR = "ALLOW_RUNTIME_ENV_OVERRIDES"
_ALLOWLIST_ENV_VAR = "RUNTIME_ENV_OVERRIDE_ALLOWLIST"
_TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
_has_warned_disabled = False
_has_warned_allowlist = False


def _is_override_enabled() -> bool:
    raw_value = os.getenv(_ENABLE_ENV_VAR, "").strip().lower()
    return raw_value in _TRUE_VALUES


def _parse_allowlist(raw_allowlist: str) -> set[str]:
    if not raw_allowlist:
        return set()
    parts: Iterable[str] = (item.strip() for item in raw_allowlist.split(","))
    return {item for item in parts if item}


def _get_allowed_keys() -> set[str]:
    return _parse_allowlist(os.getenv(_ALLOWLIST_ENV_VAR, ""))


def apply_env_overrides(config: dict, prefix: str = "[ENV]") -> None:
    """Apply safe, allowlisted runtime environment overrides from ``config``."""

    overrides = config.get("env_override") or {}

    if not isinstance(overrides, dict):
        logger.warning(f"{prefix} Không có env_override hợp lệ trong config: {overrides}")
        return

    if not overrides:
        return

    global _has_warned_disabled, _has_warned_allowlist

    if not _is_override_enabled():
        if not _has_warned_disabled:
            logger.warning(

                    f"{prefix} Bỏ qua env_override vì {_ENABLE_ENV_VAR} không được bật. "
                    "Thiết lập biến môi trường này thành true để cho phép override."

            )
            _has_warned_disabled = True
        return

    allowed_keys = _get_allowed_keys()
    if not allowed_keys:
        if not _has_warned_allowlist:
            logger.warning(

                    f"{prefix} env_override bị từ chối: không có allowlist trong {_ALLOWLIST_ENV_VAR}. "
                    "Cấu hình danh sách biến được phép, ví dụ: KEY1,KEY2."

            )
            _has_warned_allowlist = True
        return

    applied: dict[str, str] = {}
    for key, val in overrides.items():
        if not isinstance(key, str):
            continue

        normalized_key = key.strip()
        if not normalized_key:
            continue

        if normalized_key not in allowed_keys:
            logger.warning(
                f"{prefix} Bỏ qua env_override cho {normalized_key}: không có trong allowlist"
            )
            continue

        string_value = "" if val is None else str(val)
        os.environ[normalized_key] = string_value
        applied[normalized_key] = string_value

    if not applied:
        return

    for key, value in applied.items():
        logger.info(f"{prefix} Gán ENV {key} = {value}")
        from flowcore_story.config import config as app_config
    import main

    app_config.reload_from_env()
    main.refresh_runtime_settings()

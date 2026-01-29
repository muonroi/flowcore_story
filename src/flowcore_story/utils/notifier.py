import importlib
import importlib.util
from dataclasses import dataclass
from typing import Any

from flowcore_story.config.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_DISABLE_WEB_PAGE_PREVIEW,
    TELEGRAM_PARSE_MODE,
    TELEGRAM_THREAD_ID,
)

try:
    _AIOHTTP_SPEC = importlib.util.find_spec("aiohttp")
except (ModuleNotFoundError, ValueError):  # pragma: no cover - defensive guard for stubbed modules
    _AIOHTTP_SPEC = None
if _AIOHTTP_SPEC:
    aiohttp = importlib.import_module("aiohttp")  # type: ignore
else:
    aiohttp = None

_STATUS_ICONS = {
    "success": "✅",
    "error": "❌",
    "warning": "⚠️",
    "info": "ℹ️",
}

@dataclass
class NotificationProgress:
    current: int
    total: int | None = None
    label: str | None = None

@dataclass
class NotificationOptions:
    status: str = "info"
    silent: bool = False
    parse_mode: str | None = None
    disable_web_page_preview: bool | None = None
    reply_to_message_id: int | None = None
    extra: dict[str, Any] | None = None
    progress: NotificationProgress | None = None

    def resolve_parse_mode(self) -> str | None:
        if self.parse_mode is None:
            return TELEGRAM_PARSE_MODE
        return self.parse_mode

    def resolve_disable_preview(self) -> bool | None:
        if self.disable_web_page_preview is None:
            return TELEGRAM_DISABLE_WEB_PAGE_PREVIEW
        return self.disable_web_page_preview


def _format_extra(extra: dict[str, Any]) -> str:
    lines = []
    for key, value in extra.items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def _format_progress(progress: NotificationProgress) -> str:
    total = progress.total
    current = progress.current
    label = progress.label or "Progress"
    if total and total > 0:
        percent = (current / total) * 100
        return f"{label}: {current}/{total} ({percent:.2f}%)"
    return f"{label}: {current}"


def _build_message(message: str, options: NotificationOptions) -> str:
    icon = _STATUS_ICONS.get(options.status.lower(), "") if options.status else ""
    parts = []
    if icon:
        parts.append(f"{icon} {message}")
    else:
        parts.append(message)
    if options.progress:
        parts.append(_format_progress(options.progress))
    if options.extra:
        parts.append(_format_extra(options.extra))
    return "\n\n".join(parts)


async def send_telegram_notify(message: str, **kwargs: Any):
    """Send a notification message via Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    options = NotificationOptions(**kwargs)
    payload_text = _build_message(message, options)
    if aiohttp is None:
        print(f"[Telegram Notify] aiohttp not available. Message: {payload_text}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload: dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": payload_text,
    }
    parse_mode = options.resolve_parse_mode()
    if parse_mode:
        payload["parse_mode"] = parse_mode
    disable_preview = options.resolve_disable_preview()
    if disable_preview is not None:
        payload["disable_web_page_preview"] = disable_preview
    if options.silent:
        payload["disable_notification"] = True
    if TELEGRAM_THREAD_ID:
        payload["message_thread_id"] = TELEGRAM_THREAD_ID
    if options.reply_to_message_id:
        payload["reply_to_message_id"] = options.reply_to_message_id
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    print(f"[Telegram Notify] Failed: {resp.status} {text}")
                else:
                    return await resp.json()
    except Exception as ex:
        print(f"[Telegram Notify] Send failed: {ex}")


async def notify_progress(message: str, current: int, total: int | None = None, **kwargs: Any):
    options_kwargs = {**kwargs, "progress": NotificationProgress(current=current, total=total)}
    await send_telegram_notify(message, **options_kwargs)


__all__ = [
    "NotificationOptions",
    "NotificationProgress",
    "notify_progress",
    "send_telegram_notify",
]

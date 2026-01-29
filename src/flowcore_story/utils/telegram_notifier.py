import asyncio

from flowcore_story.config.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_DISABLE_WEB_PAGE_PREVIEW,
    TELEGRAM_PARSE_MODE,
    TELEGRAM_THREAD_ID,
)
from flowcore_story.utils.httpx_compat import create_async_client, httpx
from flowcore_story.utils.logger import logger


async def send_telegram_message(message: str):
    """
    Sends a message to a specified Telegram chat using the bot API.

    This function constructs and sends a POST request to the Telegram `sendMessage`
    API endpoint. It includes the chat ID, message text, and other optional
    parameters like parse mode, message thread ID, and web page preview settings,
    which are configured via environment variables.

    The function requires `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to be set
    in the environment configuration to work. If they are missing, it will
    log a warning and exit gracefully.

    Args:
        message (str): The text message to be sent.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning(
            "[Telegram] TELEGRAM_BOT_TOKEN và TELEGRAM_CHAT_ID chưa được cấu hình. Bỏ qua gửi tin nhắn."
        )
        return

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    }

    if TELEGRAM_PARSE_MODE:
        payload["parse_mode"] = TELEGRAM_PARSE_MODE
    if TELEGRAM_DISABLE_WEB_PAGE_PREVIEW is not None:
        payload["disable_web_page_preview"] = TELEGRAM_DISABLE_WEB_PAGE_PREVIEW

    if TELEGRAM_THREAD_ID:
        payload["message_thread_id"] = TELEGRAM_THREAD_ID

    async with create_async_client() as client:
        try:
            response = await client.post(api_url, json=payload, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            logger.debug("[Telegram] Đã gửi tin nhắn thành công.")
        except httpx.HTTPStatusError as e:
            logger.error(
                f"[Telegram] Lỗi khi gửi tin nhắn. Status code: {e.response.status_code}, Response: {e.response.text}"
            )
        except httpx.RequestError as e:
            logger.error(f"[Telegram] Lỗi mạng khi gửi tin nhắn: {e}")
        except Exception as e:
            logger.exception(f"[Telegram] Lỗi không xác định khi gửi tin nhắn: {e}")

if __name__ == "__main__":
    # Example usage for testing
    async def main():
        test_message = "Đây là tin nhắn test từ `telegram_notifier.py`."
        await send_telegram_message(test_message)

    asyncio.run(main())

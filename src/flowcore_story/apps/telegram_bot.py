
import asyncio
import glob
import json
import os
import signal
import time
import subprocess
import warnings
from collections import Counter
from collections.abc import Iterable
from functools import wraps
from html import escape
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
if GEMINI_API_KEY:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=FutureWarning,
            module=r"google\\.generativeai",
        )
        import google.generativeai as genai

    genai.configure(api_key=GEMINI_API_KEY)

from flowcore_story.config.config import (
    BASE_URLS,
    COMPLETED_FOLDER,
    DATA_FOLDER,
    LOG_FOLDER,
    TELEGRAM_ALLOWED_CHAT_IDS,
    TELEGRAM_ALLOWED_USER_IDS,
    TELEGRAM_BOT_TOKEN,
)
from flowcore_story.scripts.show_crawl_dashboard import (
    build_updated_timestamp_line,
    format_genre_summary,
    load_dashboard,
    summarize_alerts,
    summarize_key_metrics,
)
from flowcore.utils.kafka_producer import send_kafka_job, stop_kafka_producer
from flowcore.utils.logger import logger
from flowcore.utils.metrics_tracker import metrics_tracker
from flowcore.utils.story_analyzer import get_all_stories, get_disk_usage, get_health_stats

DASHBOARD_FILE = os.environ.get(
    "STORYFLOW_DASHBOARD_FILE",
    os.path.join("state", "dashboard.json"),
)

SYSTEM_FIELD_LABELS = {
    "proxies_in_use": "Proxy đang dùng",
    "kafka_queue_backlog": "Kafka backlog",
    "kafka_queue_status": "Kafka trạng thái",
    "kafka_queue_error": "Kafka cảnh báo",
    "kafka_queue_partitions": "Kafka partitions",
    "retry_queue_total_enqueued": "Retry tổng đã enqueue",
    "retry_queue_last_alert": "Retry cảnh báo gần nhất",
    "skipped_queue_size": "Skip queue",
    "registry_backlog": "Backlog registry",
    "stories_in_progress": "Truyện đang crawl",
    "stories_completed": "Truyện hoàn thành",
    "stories_skipped": "Truyện bị skip",
}


def _parse_allowed_identifiers(values: Iterable[Any] | None) -> tuple[set[int], set[str]]:
    """Split allow-list entries into numeric IDs and usernames."""

    numeric_ids: set[int] = set()
    usernames: set[str] = set()

    if not values:
        return numeric_ids, usernames

    for raw in values:
        if raw is None:
            continue
        candidate = str(raw).strip()
        if not candidate:
            continue
        if candidate.startswith("@"):
            candidate = candidate[1:]
        if not candidate:
            continue
        try:
            numeric_ids.add(int(candidate))
        except ValueError:
            usernames.add(candidate.lower())

    return numeric_ids, usernames


ALLOWED_CHAT_IDS, ALLOWED_CHAT_USERNAMES = _parse_allowed_identifiers(
    TELEGRAM_ALLOWED_CHAT_IDS
)
ALLOWED_USER_IDS, ALLOWED_USERNAMES = _parse_allowed_identifiers(
    TELEGRAM_ALLOWED_USER_IDS
)

UNAUTHORIZED_MESSAGE = "🚫 Bạn không có quyền sử dụng bot này."

if not (ALLOWED_CHAT_IDS or ALLOWED_CHAT_USERNAMES or ALLOWED_USER_IDS or ALLOWED_USERNAMES):
    logger.error(
        "[Bot] Không có danh sách user hoặc chat được phép sử dụng bot. Mọi lệnh sẽ bị từ chối."
    )


def _resolve_effective_message(update: Update):
    """Return the most relevant message object for the given update."""
    message = update.effective_message
    if message is not None:
        return message
    query = update.callback_query
    if query and query.message is not None:
        return query.message
    return None


def _resolve_effective_chat(update: Update):
    """Return the most relevant chat object for the given update."""
    chat = update.effective_chat
    if chat is not None:
        return chat
    message = _resolve_effective_message(update)
    if message is not None:
        return message.chat
    query = update.callback_query
    if query and query.message is not None:
        return query.message.chat
    return None


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _matches_username(username: str | None, allowed_usernames: set[str]) -> bool:
    return bool(username and username.lower() in allowed_usernames)


def _is_authorized(update: Update) -> bool:
    """Check whether the incoming update is allowed to interact with the bot."""

    user = update.effective_user
    chat = _resolve_effective_chat(update)

    if user:
        if user.id in ALLOWED_USER_IDS or _matches_username(user.username, ALLOWED_USERNAMES):
            return True

    if chat:
        if chat.id in ALLOWED_CHAT_IDS or _matches_username(getattr(chat, "username", None), ALLOWED_CHAT_USERNAMES):
            return True

        # For private chats the chat id equals the user id, so reuse the user rules
        if chat.type == "private" and chat.id in ALLOWED_USER_IDS:
            return True

    return False


async def _handle_unauthorized(update: Update) -> None:
    """Inform the user about missing permissions and log the attempt."""

    user = update.effective_user
    chat = _resolve_effective_chat(update)

    user_repr = "unknown"
    if user:
        user_repr = f"id={user.id}"
        if user.username:
            user_repr += f" username={user.username}"

    chat_repr = "unknown"
    if chat:
        chat_repr = f"id={chat.id} type={getattr(chat, 'type', 'unknown')}"
        username = getattr(chat, "username", None)
        if username:
            chat_repr += f" username={username}"

    logger.warning(
        "[Bot] Unauthorized access attempt rejected (user: %s, chat: %s)",
        user_repr,
        chat_repr,
    )

    query = update.callback_query
    if query:
        try:
            await query.answer(UNAUTHORIZED_MESSAGE, show_alert=True)
        except BadRequest:
            pass
        return

    message = _resolve_effective_message(update)
    if message:
        await message.reply_text(UNAUTHORIZED_MESSAGE)
    elif chat:
        await chat.send_message(UNAUTHORIZED_MESSAGE)


def require_authorization(handler):
    """Decorator that blocks access to handlers for unauthorized users."""

    @wraps(handler)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not _is_authorized(update):
            await _handle_unauthorized(update)
            return
        return await handler(update, context, *args, **kwargs)

    return wrapper


def _format_system_metrics_block(system: dict) -> str:
    if not isinstance(system, dict) or not system:
        return "  - (Chưa có dữ liệu)\n"

    lines = []
    for key, value in sorted(system.items()):
        label = SYSTEM_FIELD_LABELS.get(key, key.replace("_", " ").capitalize())
        if isinstance(value, float) and value.is_integer():
            value_display = str(int(value))
        else:
            value_display = str(value)
        lines.append(f"  - {label}: {value_display}")
    return "\n".join(lines) + "\n"

# --- Helper for sending long messages ---
async def send_in_chunks(update: Update, text: str, max_chars: int = 4000):
    """Sends a long message in chunks to avoid hitting Telegram's limit."""
    message = _resolve_effective_message(update)
    chat = _resolve_effective_chat(update)
    if not text:
        if message:
            await message.reply_text("Không có dữ liệu để hiển thị.")
        elif chat:
            await chat.send_message("Không có dữ liệu để hiển thị.")
        return

    if not message and not chat:
        return

    for i in range(0, len(text), max_chars):
        chunk = text[i:i + max_chars]
        safe_chunk = escape(chunk)
        html_text = f"<pre>{safe_chunk}</pre>"
        if message:
            await message.reply_html(html_text)
        elif chat:
            await chat.send_message(html_text, parse_mode=ParseMode.HTML)


# --- Menu Helpers ---

def build_main_menu() -> InlineKeyboardMarkup:
    """Creates the main inline keyboard menu."""
    keyboard = [
        [InlineKeyboardButton("📊 Crawl Dashboard", callback_data="action_dashboard")],
        [InlineKeyboardButton("🤖 AI Agent", callback_data="input_agent_mode")],
        [InlineKeyboardButton("🕵️ Chi tiết Crawl", callback_data="action_crawl_status")],
        [InlineKeyboardButton("📈 Thống kê Site", callback_data="input_site_summary")],
        [InlineKeyboardButton("ℹ️ Trạng thái hệ thống", callback_data="action_status")],
        [InlineKeyboardButton("🚀 Build & Push Image", callback_data="action_build")],
        [InlineKeyboardButton("🕷️ Crawl dữ liệu", callback_data="submenu_crawl")],
        [InlineKeyboardButton("📚 Danh sách truyện", callback_data="submenu_list")],
        [InlineKeyboardButton("📊 Thống kê", callback_data="submenu_stats")],
        [InlineKeyboardButton("🧾 Kiểm tra chương thiếu", callback_data="action_check_missing")],
        [InlineKeyboardButton("🔄 Retry site lỗi", callback_data="input_retry_failed")],
        [InlineKeyboardButton("🪵 Xem logs", callback_data="input_get_logs")],
    ]
    return InlineKeyboardMarkup(keyboard)


async def show_main_menu(update: Update, text: str | None = None, edit: bool = False) -> None:
    """Displays the main action menu either by editing or replying."""
    menu_text = text or "👇 Chọn một chức năng để tiếp tục:"
    markup = build_main_menu()
    callback_query = update.callback_query
    message = _resolve_effective_message(update)
    chat = _resolve_effective_chat(update)

    if edit and callback_query:
        try:
            await callback_query.edit_message_text(menu_text, reply_markup=markup)
            return
        except BadRequest as exc:
            error_text = str(exc).lower()
            if "message is not modified" in error_text:
                return
            logger.warning(
                "[Bot] Không thể chỉnh sửa menu hiện tại (%s). Sẽ gửi menu mới thay thế.",
                exc,
            )

    if message:
        await message.reply_text(menu_text, reply_markup=markup)
        return

    if chat:
        await chat.send_message(menu_text, reply_markup=markup)


def build_crawl_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🌐 Crawl toàn bộ site", callback_data="action_crawl_all")],
        [InlineKeyboardButton("🧩 Crawl missing only", callback_data="action_crawl_missing")],
        [InlineKeyboardButton("🔗 Crawl truyện theo URL", callback_data="input_crawl_story")],
        [InlineKeyboardButton("🏠 Crawl theo site", callback_data="input_crawl_site")],
        [InlineKeyboardButton("⬅️ Quay lại", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_list_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📦 Tổng quan thư viện", callback_data="action_list_summary")],
        [InlineKeyboardButton("✅ Truyện đã hoàn thành", callback_data="action_list_completed")],
        [InlineKeyboardButton("📚 Danh sách thể loại", callback_data="action_list_genres")],
        [InlineKeyboardButton("⬅️ Quay lại", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_stats_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("❤️ Sức khỏe hệ thống", callback_data="action_stats_health")],
        [InlineKeyboardButton("💾 Dung lượng lưu trữ", callback_data="action_stats_disk")],
        [InlineKeyboardButton("🏆 Top thể loại", callback_data="action_stats_top_genres")],
        [InlineKeyboardButton("📖 Truyện dài nhất", callback_data="action_stats_longest")],
        [InlineKeyboardButton("⬅️ Quay lại", callback_data="menu_main")],
    ]
    return InlineKeyboardMarkup(keyboard)


PENDING_ACTION_KEY = "pending_action"


async def prompt_for_input(update: Update, context: ContextTypes.DEFAULT_TYPE, action_key: str, prompt_text: str) -> None:
    """Stores the pending action and prompts the user for additional input."""
    logger.info(f"[Bot] Setting pending action: {action_key} for user {update.effective_user.id}")
    context.user_data[PENDING_ACTION_KEY] = action_key

    # For callback queries, we need to send a new message instead of replying
    query = update.callback_query
    if query and query.message:
        try:
            # Edit the current message to show the prompt
            await query.edit_message_text(f"{prompt_text}\n\nGõ /cancel để hủy thao tác.")
        except BadRequest as e:
            logger.warning(f"[Bot] Cannot edit message for prompt: {e}")
            # Fallback: send a new message
            await query.message.reply_text(f"{prompt_text}\nGõ /cancel để hủy thao tác.")
    else:
        message = _resolve_effective_message(update)
        if message:
            await message.reply_text(f"{prompt_text}\nGõ /cancel để hủy thao tác.")


@require_authorization
async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button presses from the inline menu."""
    query = update.callback_query
    if not query:
        logger.warning("[Bot] Received callback without query object")
        return

    try:
        await query.answer()
    except Exception as e:
        logger.error(f"[Bot] Error answering callback query: {e}")
        return

    data = query.data
    logger.info(f"[Bot] Processing callback: {data}")

    try:
        if data == "submenu_crawl":
            await query.edit_message_text("🕷️ Chọn chế độ crawl:", reply_markup=build_crawl_menu())
            return
        if data == "submenu_list":
            await query.edit_message_text("📚 Chọn loại danh sách cần xem:", reply_markup=build_list_menu())
            return
        if data == "submenu_stats":
            await query.edit_message_text("📊 Chọn loại thống kê:", reply_markup=build_stats_menu())
            return
        if data == "menu_main":
            await show_main_menu(update, edit=True)
            return

        if data == "action_crawl_status":
            await crawl_status_command(update, context)
            await show_main_menu(update)
            return
        if data == "action_dashboard":
            await dashboard_command(update, context)
            await show_main_menu(update)
            return
        if data == "action_status":
            await status_command(update, context)
            await show_main_menu(update)
            return
        if data == "action_build":
            await build_command(update, context)
            await show_main_menu(update)
            return
        if data == "action_check_missing":
            await check_missing_command(update, context)
            await show_main_menu(update)
            return

        if data == "action_crawl_all":
            await crawl_command(update, context, crawl_mode_override="all_sites")
            await show_main_menu(update)
            return
        if data == "action_crawl_missing":
            await crawl_command(update, context, crawl_mode_override="missing_only")
            await show_main_menu(update)
            return
        if data == "action_list_summary":
            await list_command(update, context, scope_override="summary")
            await show_main_menu(update)
            return
        if data == "action_list_completed":
            await list_command(update, context, scope_override="completed")
            await show_main_menu(update)
            return
        if data == "action_list_genres":
            await list_command(update, context, scope_override="genres")
            await show_main_menu(update)
            return
        if data == "action_stats_health":
            await stats_command(update, context, scope_override="health")
            await show_main_menu(update)
            return
        if data == "action_stats_disk":
            await stats_command(update, context, scope_override="disk_usage")
            await show_main_menu(update)
            return
        if data == "action_stats_top_genres":
            await stats_command(update, context, scope_override="top_genres")
            await show_main_menu(update)
            return
        if data == "action_stats_longest":
            await stats_command(update, context, scope_override="longest_stories")
            await show_main_menu(update)
            return

        if data == "input_site_summary":
            await prompt_for_input(update, context, "site_summary", "📈 Nhập site key bạn muốn xem thống kê.")
            return
        if data == "input_crawl_story":
            await prompt_for_input(update, context, "crawl_story", "🔗 Gửi URL của truyện bạn muốn crawl.")
            return
        if data == "input_crawl_site":
            supported_sites = ", ".join(BASE_URLS.keys())
            await prompt_for_input(
                update,
                context,
                "crawl_site",
                f"🏠 Nhập site key cần crawl (ví dụ: {supported_sites}).",
            )
            return
        if data == "input_retry_failed":
            await prompt_for_input(update, context, "retry_failed", "🔄 Nhập site key cần retry (ví dụ: xtruyen).")
            return
        if data == "input_agent_mode":
            await prompt_for_input(
                update,
                context,
                "agent_mode",
                "🤖 **Chế độ AI Agent**\n\nHãy nhập yêu cầu của bạn (ví dụ: 'Kiểm tra sức khỏe hệ thống', 'Đọc log database').\nAgent sẽ tự động chạy lệnh và trả lời bạn.",
            )
            return
        if data == "input_get_logs":
            await prompt_for_input(
                update,
                context,
                "get_logs",
                "🪵 Nhập số dòng log muốn xem (để trống sử dụng mặc định 50).",
            )
            return

        # If we reach here, unknown callback data
        logger.warning(f"[Bot] Unknown callback data: {data}")
        await query.answer("⚠️ Chức năng này chưa được hỗ trợ.", show_alert=True)

    except Exception as e:
        logger.error(f"[Bot] Error processing callback {data}: {e}", exc_info=True)
        try:
            await query.answer("❌ Đã xảy ra lỗi khi xử lý yêu cầu.", show_alert=True)
        except Exception:
            pass


@require_authorization
async def handle_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processes free-text replies when the bot is waiting for more details."""
    message = update.effective_message
    if not message or not message.text:
        return

    logger.info(f"[Bot] Received text message: '{message.text}' from user {update.effective_user.id}")
    pending_action = context.user_data.get(PENDING_ACTION_KEY)
    logger.info(f"[Bot] Current pending action: {pending_action}")

    if not pending_action:
        return

    text = message.text.strip()
    if text.lower() in {"/cancel", "cancel", "hủy", "huy"}:
        context.user_data.pop(PENDING_ACTION_KEY, None)
        await message.reply_text("Đã hủy thao tác. Bạn có thể chọn lại trong menu.")
        await show_main_menu(update)
        return

    if not text:
        await message.reply_text("⚠️ Vui lòng nhập dữ liệu hợp lệ hoặc gõ /cancel để hủy.")
        return

    context.user_data.pop(PENDING_ACTION_KEY, None)

    if pending_action == "site_summary":
        site_key = text.split()[0].lower()
        await site_summary_command(update, context, site_key_override=site_key)
    elif pending_action == "crawl_story":
        await crawl_story_command(update, context, story_url_override=text)
    elif pending_action == "crawl_site":
        site_key = text.split()[0].lower()
        await crawl_site_command(update, context, site_key_override=site_key)
    elif pending_action == "retry_failed":
        site_key = text.split()[0].lower()
        await retry_failed_command(update, context, site_key_override=site_key)
    elif pending_action == "get_logs":
        num_lines = int(text) if text.isdigit() else None
        await get_logs_command(update, context, num_lines_override=num_lines)
    elif pending_action == "agent_mode":
        await message.reply_text("🤖 Agent đang xử lý trên Host...")
        
        try:
            # 1. Load context
            agent_context = ""
            if os.path.exists("AGENT.md"):
                with open("AGENT.md", "r", encoding="utf-8") as f:
                    agent_context = f.read()
            
            # 2. Construct Prompt
            full_prompt = (
                f"{agent_context}\n\n"
                f"USER REQUEST: {text}\n\n"
                "INSTRUCTIONS:\n"
                "1. Act as a DevOps Assistant in STRICT READ-ONLY MODE.\n"
                "2. You MUST NOT change any data, file, or system state.\n"
                "3. If you need to run a command, output ONLY the command prefixed with 'COMMAND:'. Example: COMMAND: docker logs ...\n"
                "4. ONLY suggest read commands (ls, cat, grep, tail, docker logs, docker ps, find, df, free, etc.).\n"
                "5. NEVER suggest commands like rm, mv, cp, docker restart, docker stop, or file redirection (>).\n"
                "6. DO NOT USE ANY TOOLS (function calling). DO NOT SHOW THINKING PROCESS. JUST OUTPUT THE RESPONSE.\n"
                "7. Keep response concise."
            )

            # 3. Write prompt to a shared file
            prompt_file = "agent_prompt.txt"
            with open(prompt_file, "w", encoding="utf-8") as f:
                f.write(full_prompt)

            # 3b. Create a runner script to handle environment and quoting safely
            runner_script = "run_agent.sh"
            with open(runner_script, "w", encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write("source /root/.bashrc\n") # Load nvm/gemini
                f.write("PROMPT=$(cat /home/storyflow-core/agent_prompt.txt)\n")
                f.write('gemini "$PROMPT"\n')
            
            os.chmod(runner_script, 0o755)

            # 4. Execute the runner script on HOST via nsenter
            # Adjust the path if necessary
            cmd = f"nsenter -t 1 -m -u -n -i bash -l /home/storyflow-core/{runner_script}"
            
            logger.info(f"[Agent] Executing on host: {cmd}")

            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            
            # Add timeout to prevent hanging
            try:
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=120.0)
                response_text = stdout.decode('utf-8', errors='ignore').strip()
            except asyncio.TimeoutError:
                proc.kill()
                await message.reply_text("❌ Lỗi: Agent phản hồi quá lâu (Timeout).")
                return

            if not response_text:
                await message.reply_text("❌ Lỗi: Agent không trả về kết quả nào.")
                return

            # 5. Process Response (Check for COMMAND:)
            if "COMMAND:" in response_text:
                # Extract command (take the first line containing COMMAND:)
                lines = response_text.split('\n')
                cmd_to_run = ""
                for line in lines:
                    if "COMMAND:" in line:
                        cmd_to_run = line.split("COMMAND:", 1)[1].strip()
                        break
                
                if cmd_to_run:
                    await message.reply_text(f"💻 Agent đề xuất lệnh:\n`{cmd_to_run}`", parse_mode=ParseMode.MARKDOWN)
                    
                    # Security validation (STRICT READ-ONLY)
                    forbidden_patterns = [
                        "rm ", "mv ", "cp ", "mkdir ", "touch ", "chmod ", "chown ", "wget ", "curl -O", # Filesystem
                        ">", ">>", # Redirection (Write)
                        "docker stop", "docker start", "docker restart", "docker kill", "docker rm", "docker rmi", "docker run", "docker build", "docker compose", # Docker Write
                        "systemctl stop", "systemctl start", "systemctl restart", # Systemd
                        "shutdown", "reboot", "init 0", ":(){:|:&};:" # System
                    ]
                    
                    if any(bad in cmd_to_run for bad in forbidden_patterns):
                        await message.reply_text(f"❌ Lệnh bị TỪ CHỐI vì vi phạm chính sách READ-ONLY (Chứa từ khóa: ...).")
                        return

                    # Run the suggested command ON HOST as well (since it might use host tools like docker)
                    # Always cd to project dir first
                    host_cmd = f"nsenter -t 1 -m -u -n -i bash -l -c 'cd /home/storyflow-core && {cmd_to_run}'"
                    
                    proc_cmd = await asyncio.create_subprocess_shell(
                        host_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT
                    )
                    out_cmd, _ = await proc_cmd.communicate()
                    final_output = out_cmd.decode('utf-8', errors='ignore').strip()
                    
                    if len(final_output) > 3000:
                        final_output = final_output[:3000] + "\n...(truncated)"
                    if not final_output:
                        final_output = "(Lệnh đã chạy thành công, không có output)"

                    await send_in_chunks(update, f"📄 **Kết quả:**\n\n{final_output}")
                else:
                    await send_in_chunks(update, response_text)
            else:
                await send_in_chunks(update, response_text)
                
        except Exception as e:
            logger.error(f"Agent error: {e}")
            await message.reply_text(f"❌ Lỗi Agent: {e}")
            
    else:
        await message.reply_text("❔ Không nhận diện được thao tác. Hãy thử lại từ menu.")


@require_authorization
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows users to cancel any pending interactive action."""
    message = _resolve_effective_message(update)
    had_action = context.user_data.pop(PENDING_ACTION_KEY, None)
    if message:
        if had_action:
            await message.reply_text("Đã hủy thao tác hiện tại.")
        else:
            await message.reply_text("Không có thao tác nào cần hủy.")
    await show_main_menu(update)

# --- Command Handlers ---

@require_authorization
async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the crawl dashboard."""
    message = _resolve_effective_message(update)
    if not message:
        return

    try:
        dashboard_data = load_dashboard(DASHBOARD_FILE)
        dashboard_text = format_dashboard_data(dashboard_data)
        await send_in_chunks(update, dashboard_text)
    except FileNotFoundError:
        await message.reply_text("Không tìm thấy file dashboard.json. Crawler có thể chưa chạy.")
    except json.JSONDecodeError:
        await message.reply_text("File dashboard.json bị hỏng.")

def format_dashboard_data(data: dict) -> str:
    """Formats the dashboard data into a string."""
    aggregates = data.get("aggregates", {})
    sites = data.get("sites", [])
    registry_summary = data.get("registry", {})
    output = "=== StoryFlow Crawl Dashboard ===\n"
    timestamp_line, level = build_updated_timestamp_line(data)
    if level == "danger":
        timestamp_line = "[⚠️] " + timestamp_line
    elif level == "warning":
        timestamp_line = "[⏱️] " + timestamp_line
    output += f"{timestamp_line}\n\n"

    key_metrics = summarize_key_metrics(data, colorize=False)
    if key_metrics:
        output += "Chỉ số chính:\n"
        output += "\n".join(key_metrics) + "\n\n"

    alerts = summarize_alerts(data, colorize=False)
    if alerts:
        output += "⚠️  Cảnh báo phát hiện:\n"
        for alert in alerts:
            output += f"  - {alert}\n"
        output += "\n"

    output += "Tổng quan:\n"
    output += f"  - Truyện đang crawl : {aggregates.get('stories_in_progress', 0)}\n"
    output += f"  - Truyện hoàn thành: {aggregates.get('stories_completed', 0)}\n"
    output += f"  - Truyện bị skip   : {aggregates.get('stories_skipped', 0)}\n"
    output += f"  - Tổng chương thiếu: {aggregates.get('total_missing_chapters', 0)}\n"
    output += f"  - Hàng đợi skip    : {aggregates.get('skipped_queue_size', 0)}\n"
    backlog_total = _coerce_int(registry_summary.get("total_backlog")) or 0
    output += f"  - Backlog toàn hệ thống: {backlog_total}\n"
    planned_total = _coerce_int(registry_summary.get("planned_total"))
    if planned_total is not None:
        output += f"  - Tổng truyện đã lên kế hoạch: {planned_total}\n"
    stories_active = _coerce_int(registry_summary.get("stories_active"))
    if stories_active is not None:
        output += f"  - Truyện đang active (IN_PROGRESS): {stories_active}\n"
    total_genres = _coerce_int(registry_summary.get("total_genres"))
    genres_done_registry = _coerce_int(registry_summary.get("genres_done")) or 0
    if total_genres is not None:
        output += f"  - Tổng thể loại: {total_genres} (đã xong {genres_done_registry})\n"
    output += "\n"
    genres_total = aggregates.get("genres_total_configured")
    genres_done = aggregates.get("genres_total_completed", 0)
    if genres_total:
        output += f"  - Thể loại hoàn thành: {genres_done}/{genres_total}\n\n"
    else:
        output += f"  - Thể loại hoàn thành: {genres_done}\n\n"

    system_metrics = data.get("system", {})
    output += "Tình trạng hệ thống:\n"
    output += _format_system_metrics_block(system_metrics)
    output += "\n"

    active = data.get("stories", {}).get("in_progress", [])
    current_genre = None
    active_genres = data.get("genres", {}).get("in_progress", [])
    if active_genres:
        current_genre = max(active_genres, key=lambda item: item.get("updated_at", ""))
    if current_genre:
        genre_name_display = current_genre.get("name") or current_genre.get("url") or "?"
        genre_position = _coerce_int(current_genre.get("position"))
        genre_total = _coerce_int(current_genre.get("total_genres"))
        if genre_total is None:
            genre_total = total_genres
        if genre_position and genre_total:
            output += f"Đang crawl: {genre_name_display} ({genre_position}/{genre_total} thể loại)\n"
        elif genre_total:
            output += f"Đang crawl: {genre_name_display} (tổng {genre_total} thể loại)\n"
        current_story_title = current_genre.get("current_story_title")
        story_position = _coerce_int(current_genre.get("current_story_position"))
        story_total = _coerce_int(current_genre.get("total_stories"))
        processed_stories = _coerce_int(current_genre.get("processed_stories")) or 0
        if story_total is None and processed_stories:
            story_total = max(processed_stories, story_position or 0)
        if story_position and story_total:
            story_total = max(story_total, story_position)
        if current_story_title and story_position:
            total_display = story_total or story_position
            output += (
                f"Truyện đang xử lý: {current_story_title} thuộc {genre_name_display}, "
                f"{story_position}/{total_display}\n"
            )
        output += "\n"
    if active:
        output += "Đang crawl:\n"
        for story in active[:10]:  # show top 10
            output += (
                f"  * {story.get('title')} — {story.get('crawled_chapters', 0)}/"
                f"{story.get('total_chapters', 0)} chương, còn thiếu {story.get('missing_chapters', 0)}\n"
            )
        if len(active) > 10:
            output += f"  ... và {len(active) - 10} truyện khác\n"
        output += "\n"
    if active_genres:
        output += "Thể loại đang xử lý:\n"
        for genre in active_genres[:10]:
            for line in format_genre_summary(genre):
                output += f"{line}\n"
        if len(active_genres) > 10:
            output += f"  ... và {len(active_genres) - 10} thể loại khác\n"
        output += "\n"

    if sites:
        output += "Sức khỏe site:\n"
        for site in sorted(sites, key=lambda item: item.get("failure_rate", 0), reverse=True):
            failure_rate = site.get("failure_rate", 0.0)
            output += (
                f"  - {site.get('site_key')}: {site.get('success', 0)} OK / {site.get('failure', 0)} lỗi"
                f" (tỷ lệ lỗi {failure_rate * 100:.2f}%)\n"
            )
        output += "\n"

    site_genres = data.get("site_genres", [])
    if site_genres:
        output += "Tổng kết thể loại theo site:\n"
        for site in site_genres:
            total = site.get("total_genres") or 0
            completed = site.get("completed_genres") or 0
            output += (
                f"  - {site.get('site_key')}: {completed}/{total} thể loại, cập nhật {site.get('updated_at', '-')}\n"
            )
            genres = site.get("genres", [])
            for genre in genres[:10]:
                status = genre.get("status", "completed")
                extra = f" ({status})" if status != "completed" else ""
                output += (
                    f"      * {genre.get('name')} — {genre.get('stories', 0)} truyện{extra}\n"
                )
            if len(genres) > 10:
                output += f"      ... và {len(genres) - 10} thể loại khác\n"
        output += "\n"
    return output

@require_authorization
async def crawl_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the detailed crawling status."""
    message = _resolve_effective_message(update)
    if not message:
        return

    snapshot = metrics_tracker.get_snapshot()
    status_text = format_crawl_status(snapshot)
    await send_in_chunks(update, status_text)

def format_crawl_status(snapshot: dict) -> str:
    """Formats the crawl status snapshot into a string."""
    output = "- 🕵️ **Trạng thái Crawl chi tiết** -\n\n"

    genres_in_progress = snapshot.get("genres", {}).get("in_progress", [])
    if not genres_in_progress:
        output += "Không có thể loại nào đang được crawl.\n"
    else:
        output += f"**Đang crawl {len(genres_in_progress)} thể loại:**\n"
        for genre in genres_in_progress:
            output += f"- **{genre.get('name')}** ({genre.get('site_key')})\n"
            output += f"  - Trang: {genre.get('crawled_pages', 0)}/{genre.get('total_pages', '?')}\n"
            output += f"  - Truyện: {genre.get('processed_stories', 0)}/{genre.get('total_stories', '?')}\n"
            active_stories = genre.get('active_stories', [])
            if active_stories:
                output += "  - Đang xử lý: " + ", ".join(active_stories) + "\n"

    stories_in_progress = snapshot.get("stories", {}).get("in_progress", [])
    if stories_in_progress:
        output += "\n**Các truyện đang trong hàng đợi:**\n"
        for story in stories_in_progress:
            output += f"- **{story.get('title')}**\n"
            output += f"  - Tiến trình: {story.get('crawled_chapters', 0)}/{story.get('total_chapters', '?')} chương\n"
            if story.get('last_source'):
                output += f"  - Nguồn: {story.get('last_source')}\n"

    return output

@require_authorization
async def site_summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE, site_key_override: str | None = None) -> None:
    """Displays the genre and story summary for a given site."""
    message = _resolve_effective_message(update)
    if not message:
        return

    site_key = site_key_override
    if site_key is None:
        if not context.args:
            await message.reply_text("Vui lòng cung cấp site key. Ví dụ: `/site_summary xtruyen`")
            return
        site_key = context.args[0]

    snapshot = metrics_tracker.get_snapshot()
    summary_text = format_site_summary(snapshot, site_key)
    await send_in_chunks(update, summary_text)

def format_site_summary(snapshot: dict, site_key: str) -> str:
    """Formats the site summary into a string."""
    output = f"- 📈 **Thống kê cho site: {site_key}** -\n\n"

    site_genres = snapshot.get("site_genres", [])
    site_summary = None
    for summary in site_genres:
        if summary.get("site_key") == site_key:
            site_summary = summary
            break

    if not site_summary:
        return f"Không tìm thấy dữ liệu cho site: {site_key}"

    output += "**Tổng quan:**\n"
    output += f"- Tổng số thể loại: {site_summary.get('total_genres', '?')}\n"
    output += f"- Đã hoàn thành: {site_summary.get('completed_genres', '?')}\n\n"

    genres = site_summary.get("genres", [])
    if not genres:
        output += "Chưa có thể loại nào được crawl.\n"
    else:
        output += "**Chi tiết thể loại:**\n"
        for genre in genres:
            output += f"- **{genre.get('name')}**: {genre.get('stories', 0)} truyện\n"

    return output

@require_authorization
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message and displays the quick action menu."""
    user = update.effective_user
    message = _resolve_effective_message(update)
    if message:
        help_text = (
            f"👋 Chào {user.first_name}, mình là Bot Crawler đây!\n\n"
            "Bạn có thể chọn nhanh chức năng trong menu bên dưới hoặc tiếp tục sử dụng các lệnh quen thuộc:\n"
            "`/build`, `/crawl`, `/status`, `/crawl_story`, `/crawl_site`, `/check_missing`, `/retry_failed`, `/get_logs`, `/list`, `/stats`.\n\n"
            "<b>Mẹo:</b> Menu sẽ hướng dẫn bạn nhập các thông tin cần thiết chỉ với vài bước chạm."
        )
        await message.reply_html(help_text)

    await show_main_menu(update)


@require_authorization
async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the quick action menu on demand."""
    await show_main_menu(update)

@require_authorization
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Checks the status of the crawler system."""
    message = _resolve_effective_message(update)
    if message:
        await message.reply_text("✅ Bot is running and listening for commands.")

@require_authorization
async def build_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Builds and pushes the Docker image to Docker Hub."""
    message = _resolve_effective_message(update)
    if message:
        await message.reply_text("⏳ Bắt đầu quá trình build và push image... Logs sẽ được gửi ngay sau đây.")

    command = "docker compose build && docker compose push"

    try:
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT  # Redirect stderr to stdout
        )

        output_chunk = ""
        last_sent_time = time.time()

        # Stream the output
        if process.stdout:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break

                decoded_line = line.decode('utf-8', errors='ignore')
                output_chunk += decoded_line

                # Send in chunks of text or every 2 seconds
                if message and (len(output_chunk) > 3500 or (time.time() - last_sent_time > 2 and output_chunk)):
                    await message.reply_html(f"<pre>{output_chunk}</pre>")
                    output_chunk = ""
                    last_sent_time = time.time()

        # Send any remaining output
        if message and output_chunk:
            await message.reply_html(f"<pre>{output_chunk}</pre>")

        await process.wait()

        if message:
            if process.returncode == 0:
                await message.reply_text("✅ Build và push image thành công!")
            else:
                await message.reply_text(f"❌ Build và push image thất bại! (Exit code: {process.returncode})")

    except Exception as e:
        logger.error(f"[Bot] Lỗi khi thực thi lệnh build: {e}")
        if message:
            await message.reply_text(f"❌ Đã xảy ra lỗi nghiêm trọng khi chạy lệnh build: {e}")

@require_authorization
async def crawl_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    crawl_mode_override: str | None = None,
) -> None:
    """Triggers a global crawl job."""
    message = _resolve_effective_message(update)
    args = context.args
    if crawl_mode_override is not None:
        args = [crawl_mode_override]
    if not args:
        if message:
            await message.reply_text(
                "⚠️ Vui lòng cung cấp chế độ crawl.\nVí dụ: `/crawl all_sites` hoặc `/crawl missing_only`"
            )
        return

    crawl_mode = args[0]
    job_type = ""

    # Determine job type based on crawl mode
    if crawl_mode in ["all_sites", "full", "genres_only"]:
        job_type = "all_sites"
    elif crawl_mode in ["missing_only", "missing"]:
        job_type = "missing_check"
    else:
        if message:
            await message.reply_text(f"❌ Chế độ crawl '{crawl_mode}' không hợp lệ.")
        return

    job = {"type": job_type, "crawl_mode": crawl_mode}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `{job_type}` với mode `{crawl_mode}` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

@require_authorization
async def list_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    scope_override: str | None = None,
    filters_override: dict | None = None,
) -> None:
    """Lists and filters stories based on various criteria."""
    message = _resolve_effective_message(update)
    args = context.args
    if scope_override is not None:
        args = [scope_override]
    if not args:
        if message:
            await message.reply_text(
                "Vui lòng cung cấp scope. Ví dụ: `/list completed`, `/list all`, `/list summary`"
            )
        return

    if message:
        await message.reply_text("Đang quét và phân tích thư mục... việc này có thể mất vài giây.")

    scope = args[0].lower()
    filters = filters_override or {
        'genre': None,
        'min_chapters': None,
        'max_chapters': None
    }

    if filters_override is None:
        try:
            for i in range(1, len(args), 2):
                if args[i].startswith('--'):
                    key = args[i][2:]
                    if key in filters and i + 1 < len(args):
                        if key.endswith('_chapters'):
                            filters[key] = int(args[i+1])
                        else:
                            filters[key] = args[i+1]
        except (ValueError, IndexError):
            if message:
                await message.reply_text("❌ Lỗi cú pháp filter. Ví dụ: `--min-chapters 100`")
            return

    stories = get_all_stories()

    if scope == 'summary':
        total_stories = len(stories)
        completed_stories = sum(1 for s in stories if s['status'] == 'completed')
        ongoing_stories = total_stories - completed_stories
        genres = {s['genre'] for s in stories if s['genre'] != 'Unknown'}
        summary_text = (
            f"<b>📊 Thống kê tổng quan:</b>\n"
            f"- Tổng số truyện: {total_stories}\n"
            f"- Đã hoàn thành: {completed_stories}\n"
            f"- Đang theo dõi: {ongoing_stories}\n"
            f"- Số lượng thể loại: {len(genres)}"
        )
        if message:
            await message.reply_html(summary_text)
        return

    if scope == 'genres':
        genres = sorted({s['genre'] for s in stories if s['genre'] != 'Unknown'})
        genre_text = "<b>📚 Danh sách các thể loại:</b>\n\n" + "\n".join(f"- {g}" for g in genres)
        await send_in_chunks(update, genre_text)
        return

    if scope == 'completed':
        filtered_stories = [s for s in stories if s['status'] == 'completed']
    elif scope == 'all':
        filtered_stories = stories
    else:
        if message:
            await message.reply_text(
                f"Scope không hợp lệ: `{scope}`. Dùng `completed`, `all`, `summary`, hoặc `genres`."
            )
        return

    if filters['genre']:
        filtered_stories = [s for s in filtered_stories if s['genre'] and filters['genre'].lower() in s['genre'].lower()]
    if filters['min_chapters'] is not None:
        filtered_stories = [s for s in filtered_stories if s['total_chapters'] >= filters['min_chapters']]
    if filters['max_chapters'] is not None:
        filtered_stories = [s for s in filtered_stories if s['total_chapters'] <= filters['max_chapters']]

    if not filtered_stories:
        if message:
            await message.reply_text("Không tìm thấy truyện nào khớp với tiêu chí của bạn.")
        return

    output_lines = [f"🔎 Tìm thấy {len(filtered_stories)} truyện:"]
    for story in filtered_stories:
        progress = f"{story['crawled_chapters']}/{story['total_chapters']}"
        line = f"- <b>{story['title']}</b> ({story['status']}) [{story['genre']}] - {progress}"
        output_lines.append(line)

    await send_in_chunks(update, "\n".join(output_lines))

@require_authorization
async def stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    scope_override: str | None = None,
) -> None:
    """Provides detailed statistics about the crawler system."""
    message = _resolve_effective_message(update)
    args = context.args
    if scope_override is not None:
        args = [scope_override]
    if not args:
        if message:
            await message.reply_text("Vui lòng cung cấp scope. Ví dụ: `/stats health`, `/stats disk_usage`")
        return

    scope = args[0].lower()
    if message:
        await message.reply_text(f"Đang tính toán thống kê cho `{scope}`...")

    stories = get_all_stories()

    if scope == 'health':
        health_stats = get_health_stats(stories)
        skipped_stories = health_stats['skipped_stories']

        text = "<b>❤️ Thống kê sức khỏe hệ thống:</b>\n"
        text += f"- Số truyện bị skip: {len(skipped_stories)}\n"
        text += f"- Tổng số chương lỗi: {health_stats['total_dead_chapters']}\n"

        if skipped_stories:
            text += "\n<b>Truyện bị skip:</b>\n"
            for s in skipped_stories:
                text += f"- {s['title']} (Lý do: {s['skip_reason']})\n"

        if health_stats['stories_with_dead_chapters']:
            text += "\n<b>Truyện có chương lỗi:</b>\n"
            for s in health_stats['stories_with_dead_chapters']:
                text += f"- {s['title']} ({s['dead_count']} chương lỗi)\n"
        await send_in_chunks(update, text)

    elif scope == 'disk_usage':
        total_size_bytes = get_disk_usage(DATA_FOLDER) + get_disk_usage(COMPLETED_FOLDER)
        total_size_gb = total_size_bytes / (1024**3)
        text = "<b>💾 Thống kê dung lượng:</b>\n"
        text += f"- Tổng dung lượng: {total_size_gb:.2f} GB"
        if message:
            await message.reply_html(text)

    elif scope == 'top_genres':
        count = 5
        if len(args) > 1 and args[1].isdigit():
            count = int(args[1])
        genre_counts = Counter(s['genre'] for s in stories if s['genre'] != 'Unknown')
        top_genres = genre_counts.most_common(count)
        text = f"<b>🏆 Top {len(top_genres)} thể loại có nhiều truyện nhất:</b>\n"
        for i, (genre, num) in enumerate(top_genres):
            text += f"{i+1}. {genre}: {num} truyện\n"
        if message:
            await message.reply_html(text)

    elif scope == 'longest_stories':
        count = 10
        if len(args) > 1 and args[1].isdigit():
            count = int(args[1])

        # Sort by total_chapters, descending
        longest = sorted(stories, key=lambda s: s.get('total_chapters', 0), reverse=True)
        top_longest = longest[:count]

        text = f"<b>📖 Top {len(top_longest)} truyện dài nhất:</b>\n"
        for i, s in enumerate(top_longest):
            text += f"{i+1}. {s['title']} - {s['total_chapters']} chương\n"
        await send_in_chunks(update, text)

    else:
        if message:
            await message.reply_text(
                f"Scope không hợp lệ: `{scope}`. Dùng `health`, `disk_usage`, `top_genres`, `longest_stories`."
            )


@require_authorization
async def crawl_story_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    story_url_override: str | None = None,
) -> None:
    """Triggers a crawl job for a single story URL."""
    message = _resolve_effective_message(update)
    story_url = story_url_override
    if story_url is None:
        if not context.args:
            if message:
                await message.reply_text(
                    "⚠️ Vui lòng cung cấp URL của truyện.\nVí dụ: `/crawl_story https://xtruyen.vn/truyen/...`"
                )
            return
        story_url = context.args[0]

    if not story_url:
        if message:
            await message.reply_text("⚠️ URL không hợp lệ.")
        return
    job = {"type": "single_story", "url": story_url}

    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `crawl_story` cho URL: {story_url} vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

@require_authorization
async def crawl_site_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    site_key_override: str | None = None,
) -> None:
    """Triggers a crawl job for a full site."""
    message = _resolve_effective_message(update)
    site_key = site_key_override
    if site_key is None:
        if not context.args:
            if message:
                await message.reply_text(
                    f"⚠️ Vui lòng cung cấp site key.\nVí dụ: `/crawl_site xtruyen`\nCác site được hỗ trợ: {', '.join(BASE_URLS.keys())}"
                )
            return
        site_key = context.args[0]

    if site_key not in BASE_URLS:
        if message:
            await message.reply_text(
                f"❌ Site key '{site_key}' không hợp lệ.\nCác site được hỗ trợ: {', '.join(BASE_URLS.keys())}"
            )
        return

    job = {"type": "full_site", "site_key": site_key}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `crawl_site` cho site: `{site_key}` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

@require_authorization
async def check_missing_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Triggers a job to check for missing chapters."""
    message = _resolve_effective_message(update)
    job = {"type": "missing_check"}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text("✅ Đã đưa job `check_missing` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

@require_authorization
async def retry_failed_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    site_key_override: str | None = None,
) -> None:
    """Triggers a job to retry failed genres/stories."""
    message = _resolve_effective_message(update)
    site_key = site_key_override
    if site_key is None:
        if not context.args:
            if message:
                await message.reply_text("⚠️ Vui lòng cung cấp site key để retry.\nVí dụ: `/retry_failed xtruyen`")
            return
        site_key = context.args[0]

    if not site_key:
        return
    job = {"type": "retry_failed_genres", "site_key": site_key}
    success = await send_kafka_job(job)
    if message:
        if success:
            await message.reply_text(f"✅ Đã đưa job `retry_failed` cho site `{site_key}` vào hàng đợi.")
        else:
            await message.reply_text("❌ Gửi job vào Kafka thất bại. Vui lòng kiểm tra logs.")

@require_authorization
async def get_logs_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    num_lines_override: int | None = None,
) -> None:
    """Retrieves the last N lines of the latest log file."""
    message = _resolve_effective_message(update)
    try:
        num_lines = num_lines_override if num_lines_override is not None else 50
        if num_lines_override is None and context.args and context.args[0].isdigit():
            num_lines = int(context.args[0])

        log_files = glob.glob(os.path.join(LOG_FOLDER, '*.log'))
        if not log_files:
            if message:
                await message.reply_text("Không tìm thấy file log nào.")
            return

        latest_log_file = max(log_files, key=os.path.getctime)

        with open(latest_log_file, encoding='utf-8') as f:
            lines = f.readlines()
            last_n_lines = lines[-num_lines:]

        log_content = "".join(last_n_lines)

        if not log_content:
            if message:
                await message.reply_text(f"File log `{os.path.basename(latest_log_file)}` trống.")
            return

        await send_in_chunks(update, log_content)

    except Exception as e:
        logger.error(f"[Bot] Lỗi khi đọc logs: {e}")
        if message:
            await message.reply_text(f"❌ Đã xảy ra lỗi khi cố gắng đọc file log: {e}")

# --- Bot Setup ---

async def log_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log all incoming updates for debugging."""
    if update.message:
        logger.info(f"[Bot] DEBUG: Received message: '{update.message.text}' from {update.effective_user.id}")
    elif update.callback_query:
        logger.info(f"[Bot] DEBUG: Received callback: {update.callback_query.data}")

async def main_bot():
    """Starts the Telegram bot and registers command handlers."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("[Bot] TELEGRAM_BOT_TOKEN chưa được cấu hình. Bot không thể khởi động.")
        return

    logger.info("[Bot] Đang khởi tạo bot...")
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Register command handlers
    # Add logging handler first
    application.add_handler(MessageHandler(filters.ALL, log_update), group=-1)

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", start_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("site_summary", site_summary_command))
    application.add_handler(CommandHandler("crawl_status", crawl_status_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("build", build_command))
    application.add_handler(CommandHandler("crawl", crawl_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("crawl_story", crawl_story_command))
    application.add_handler(CommandHandler("crawl_site", crawl_site_command))
    application.add_handler(CommandHandler("check_missing", check_missing_command))
    application.add_handler(CommandHandler("retry_failed", retry_failed_command))
    application.add_handler(CommandHandler("get_logs", get_logs_command))
    application.add_handler(CallbackQueryHandler(menu_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_input))

    logger.info("[Bot] Bot đang chạy và lắng nghe lệnh...")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_stop_signal() -> None:
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_stop_signal)
        except NotImplementedError:
            # Signal handlers may not be available on all platforms (e.g., Windows)
            pass

    try:
        async with application:
            await application.start()

            if application.updater:
                # Include all update types: messages, callback queries, etc.
                await application.updater.start_polling(allowed_updates=["message", "callback_query", "inline_query"])
                logger.info("[Bot] Polling started with allowed_updates: message, callback_query, inline_query")
            else:
                logger.warning("[Bot] Không thể khởi động polling vì không có updater. Vui lòng kiểm tra cấu hình bot.")

            logger.info("[Bot] Bot đã khởi động thành công và đang chạy.")

            await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("[Bot] Vòng lặp bot đã bị hủy. Đang tiến hành tắt bot...")
    except (KeyboardInterrupt, SystemExit):
        logger.info("[Bot] Nhận được tín hiệu dừng, đang tắt bot...")
    finally:
        if application.updater:
            await application.updater.stop()
        await stop_kafka_producer()
        logger.info("[Bot] Bot đã dừng hoàn toàn.")


def run_bot() -> None:
    """Entry point for starting the Telegram bot."""
    try:
        asyncio.run(main_bot())
    except RuntimeError as exc:
        # Handle the "event loop is already running" scenario gracefully.
        if "event loop is already running" in str(exc):
            logger.error("[Bot] Không thể khởi động bot vì vòng lặp asyncio đã chạy sẵn."
                         " Hãy đảm bảo bot được khởi chạy như một tiến trình riêng biệt.")
        else:
            raise


if __name__ == "__main__":
    run_bot()

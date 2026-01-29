#!/usr/bin/env python3
"""
Script verify nhanh setup của Telegram Bot
Kiểm tra config và handlers
"""

import sys


def verify_bot_config():
    """Kiểm tra config cơ bản của bot"""
    print("=" * 60)
    print("🔍 Verify Telegram Bot Configuration")
    print("=" * 60)
    print()

    # Check bot token
    try:
        from flowcore_story.config.config import TELEGRAM_BOT_TOKEN
        if TELEGRAM_BOT_TOKEN:
            print(f"✅ Bot token: {TELEGRAM_BOT_TOKEN[:10]}... (OK)")
        else:
            print("❌ Bot token: CHƯA CẤU HÌNH")
            print("   → Vui lòng set TELEGRAM_BOT_TOKEN trong .env")
            return False
    except Exception as e:
        print(f"❌ Không thể load bot token: {e}")
        return False

    # Check allowed users/chats
    try:
        from flowcore_story.config.config import (
            TELEGRAM_ALLOWED_CHAT_IDS,
            TELEGRAM_ALLOWED_USER_IDS,
            TELEGRAM_ALLOWED_USERNAMES,
        )

        print("\n📋 Authorization Config:")
        print(f"   - Allowed User IDs: {TELEGRAM_ALLOWED_USER_IDS or '(không có)'}")
        print(f"   - Allowed Chat IDs: {TELEGRAM_ALLOWED_CHAT_IDS or '(không có)'}")
        print(f"   - Allowed Usernames: {TELEGRAM_ALLOWED_USERNAMES or '(không có)'}")

        if not any([TELEGRAM_ALLOWED_USER_IDS, TELEGRAM_ALLOWED_CHAT_IDS, TELEGRAM_ALLOWED_USERNAMES]):
            print("\n⚠️  CẢNH BÁO: Không có user/chat nào được phép sử dụng bot!")
            print("   → Thêm TELEGRAM_ALLOWED_USER_IDS hoặc TELEGRAM_ALLOWED_USERNAMES vào .env")
            return False
    except Exception as e:
        print(f"❌ Không thể load authorization config: {e}")
        return False

    # Check handlers
    print("\n🔧 Checking Handlers...")
    try:
        from telegram_bot import menu_callback
        print("   ✅ menu_callback function: OK")

        # Check if function has the right signature
        import inspect
        sig = inspect.signature(menu_callback)
        if len(sig.parameters) >= 2:
            print("   ✅ menu_callback signature: OK")
        else:
            print("   ❌ menu_callback signature: SAI")
            return False

    except Exception as e:
        print(f"   ❌ Không thể import menu_callback: {e}")
        return False

    print("\n✅ Tất cả checks đã pass!")
    print("\n📝 Hướng dẫn tiếp theo:")
    print("   1. Chạy bot: python telegram_bot.py")
    print("   2. Gửi /start cho bot")
    print("   3. Nhấn vào button và kiểm tra logs")
    print("   4. Nếu thấy 'Unauthorized', thêm User ID vào TELEGRAM_ALLOWED_USER_IDS")

    return True

if __name__ == "__main__":
    success = verify_bot_config()
    sys.exit(0 if success else 1)


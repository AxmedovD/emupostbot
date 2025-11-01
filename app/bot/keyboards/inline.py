from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

from app.core.config import settings


def get_main_keyboard() -> InlineKeyboardMarkup:
    """Get main menu keyboard with Mini App button"""

    buttons = [
        [
            InlineKeyboardButton(
                text="📱 Mini App",
                web_app=WebAppInfo(url=settings.WEBAPP_URL)
            )
        ] if settings.WEBAPP_URL else [],
        [
            InlineKeyboardButton(text="ℹ️ Yordam", callback_data="help"),
            InlineKeyboardButton(text="⚙️ Sozlamalar", callback_data="settings")
        ]
    ]

    # Filter out empty lists
    buttons = [row for row in buttons if row]

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_notification_keyboard() -> InlineKeyboardMarkup:
    """Get notification control keyboard"""

    buttons = [
        [
            InlineKeyboardButton(text="✅ Yoqish", callback_data="notif_on"),
            InlineKeyboardButton(text="❌ O'chirish", callback_data="notif_off")
        ]
    ]

    return InlineKeyboardMarkup(inline_keyboard=buttons)

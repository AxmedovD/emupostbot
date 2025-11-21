from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.utils.i18n import gettext as _

from app.core.config import settings


def get_main_keyboard(is_contract: bool = False) -> InlineKeyboardMarkup:
    """Asosiy klaviatura"""
    keyboard = []
    
    if not is_contract:
        keyboard.append([InlineKeyboardButton(text=_("📄 Shartnoma"), callback_data="contract")])
    
    keyboard.extend([
        [InlineKeyboardButton(
            text="🚀 Mini App ochish",
            web_app=WebAppInfo(url=settings.WEBAPP_URL)
        )],
        [
            InlineKeyboardButton(text=_("⚙️ Sozlamalar"), callback_data="settings"),
            InlineKeyboardButton(text=_("ℹ️ Ma'lumot"), callback_data="info")
        ]
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_notification_keyboard() -> InlineKeyboardMarkup:
    """Get notification control keyboard"""

    buttons = [
        [
            InlineKeyboardButton(text="✅ Yoqish", callback_data="notif_on"),
            InlineKeyboardButton(text="❌ O'chirish", callback_data="notif_off")
        ]
    ]

    return InlineKeyboardMarkup(inline_keyboard=buttons)

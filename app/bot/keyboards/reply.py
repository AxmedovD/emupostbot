from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from aiogram.utils.i18n import gettext as _

from app.core.config import settings


def get_language_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🇺🇿 O'zbek"),
                KeyboardButton(text="🇷🇺 Русский")
            ],
            # [
            #     KeyboardButton(text="🇬🇧 English")
            # ]
        ],
        resize_keyboard=True
    )


def share_phone_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text=_("📲 Telefon raqamni ulashish"),
                    request_contact=True
                )
            ]
        ],
        resize_keyboard=True
    )

 
def get_main_keyboard(is_contract: bool = False) -> ReplyKeyboardMarkup:
    """Asosiy klaviatura"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [] if is_contract else [KeyboardButton(text=_("📄 Shartnoma"))],
            [
                KeyboardButton(
                    text="🚀 Mini App ochish",
                    web_app=WebAppInfo(url=settings.WEBAPP_URL)
                )
            ],
            [
                KeyboardButton(text=_("⚙️ Sozlamalar")),
                KeyboardButton(text=_("ℹ️ Ma'lumot"))
            ]
        ],
        resize_keyboard=True
    )


def stop_keyboard() -> ReplyKeyboardMarkup:
    """Asosiy klaviatura"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=_("❎ Bekor qilish"))
            ]
        ],
        resize_keyboard=True
    )

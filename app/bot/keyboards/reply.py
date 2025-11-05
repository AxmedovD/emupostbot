from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.i18n import gettext as _


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


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Asosiy klaviatura"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=_("📄 Shartnoma")),
            ],
            [
                KeyboardButton(text=_("⚙️ Sozlamalar")),
                KeyboardButton(text=_("ℹ️ Ma'lumot"))
            ]
        ],
        resize_keyboard=True
    )

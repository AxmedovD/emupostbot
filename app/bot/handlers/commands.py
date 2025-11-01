from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

from app.db.pool import Database
from app.core.logger import bot_logger

router = Router(name="commands")


@router.message(CommandStart())
async def cmd_start(message: Message, db: Database):
    telegram_id = message.from_user.id
    first_name = message.from_user.first_name

    try:
        welcome_text = (
            f"👋 Salom, {first_name}!\n\n"
            "EmuPostBot ga xush kelibsiz!\n"
            "Men sizga muhim bildirishnomalarni yuboraman."
        )

        await message.answer(
            welcome_text
        )

    except Exception as e:
        bot_logger.error(f"Error in /start handler for {telegram_id}: {e}")
        await message.answer(
            "Xatolik yuz berdi. Iltimos qaytadan urinib ko'ring."
        )

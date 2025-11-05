from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

from app.db.pool import Database
from app.core.logger import bot_logger

router = Router(name="commands")


@router.message(CommandStart())
async def cmd_start(message: Message, db: Database):
    username: str = message.from_user.username if message.from_user.username else str(message.from_user.id)
    user: dict = await db.read(
        table="users",
        conditions={
            "telegram_id": message.from_user.id,
            "OR username": username,
            "OR phone": str(message.from_user.id)
        },
        limit=1,
        result_type="row"
    )
    print(user)
    pk: int = await db.create(
        table="users",
        data={
            "name": str(message.from_user.id),
            "telegram_id": message.from_user.id,
            "phone": str(message.from_user.id),
            "username": username.lower(),
            "password": str(message.from_user.id)
        }
    )
    print(pk)
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

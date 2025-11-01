"""Mini App (WebApp) handlers"""

from aiogram import Router, F
from aiogram.types import Message, WebAppInfo

from app.core.config import settings

router = Router(name="webapp")


@router.message(F.web_app_data)
async def handle_web_app_data(message: Message):
    """Handle data from Mini App"""

    # Get data from Mini App
    web_app_data = message.web_app_data.data

    await message.answer(
        f"Mini App'dan ma'lumot qabul qilindi:\n\n"
        f"<code>{web_app_data}</code>"
    )

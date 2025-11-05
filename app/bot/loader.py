from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.base import DefaultKeyBuilder
# from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.utils.i18n import I18n

from app.core.config import settings
from app.core.settings import BASE_DIR

bot = Bot(
    token=settings.BOT_TOKEN,
    default=DefaultBotProperties(
        parse_mode=ParseMode.HTML,
        link_preview_is_disabled=True
    )
)

# storage = MemoryStorage()
storage = RedisStorage.from_url(
    url="redis://127.0.0.1:6379/0",
    key_builder=DefaultKeyBuilder(prefix="emupostbot")
)
dp = Dispatcher(storage=storage)

# I18n sozlash
i18n = I18n(path=BASE_DIR / 'bot' / "locales", default_locale="uz", domain="emupostbot")


async def setup_bot():
    from app.bot.middlewares.database import DatabaseMiddleware
    from app.bot.middlewares.i18n import CustomI18nMiddleware
    from app.bot.handlers import commands, webapp

    dp.update.middleware(DatabaseMiddleware())
    dp.update.middleware(CustomI18nMiddleware(i18n))

    dp.include_router(commands.router)
    dp.include_router(webapp.router)

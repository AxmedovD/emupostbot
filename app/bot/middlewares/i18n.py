from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, User
from aiogram.utils.i18n import I18n

from app.db.pool import Database


class CustomI18nMiddleware(BaseMiddleware):
    DEFAULT_LOCALE: str = "uz"

    def __init__(self, i18n: I18n):
        self.i18n = i18n
        super().__init__()

    async def __call__(
            self,
            handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
            event: TelegramObject,
            data: Dict[str, Any]
    ) -> Any:

        locale = await self._get_user_locale(data)

        data["i18n"] = self.i18n
        data["locale"] = locale

        with self.i18n.context(), self.i18n.use_locale(locale):
            return await handler(event, data)

    async def _get_user_locale(self, data: Dict[str, Any]) -> str:
        user: User | None = data.get("event_from_user")

        if not user:
            return self.DEFAULT_LOCALE

        locale = await self._get_locale_from_db(user.id, data.get("db"))
        if locale:
            return locale

        if user.language_code:
            return user.language_code

        return self.DEFAULT_LOCALE

    async def _get_locale_from_db(
            self,
            telegram_id: int,
            db: Database | None
    ) -> str | None:
        if not db:
            return None

        try:
            user_data: dict = await db.read(
                table='users',
                conditions={'telegram_id': telegram_id},
                limit=1,
                result_type='row'
            )
            if user_data:
                locale = user_data.get('lang')
                if locale:
                    return locale
        except Exception as e:
            pass

        return None

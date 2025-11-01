from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

from app.db.pool import db


class DatabaseMiddleware(BaseMiddleware):
    """Middleware to inject database instance into handler data"""

    async def __call__(
            self,
            handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
            event: TelegramObject,
            data: Dict[str, Any]
    ) -> Any:
        # Inject database instance
        data["db"] = db

        # Call next handler
        return await handler(event, data)

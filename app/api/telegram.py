from fastapi import APIRouter, Header, HTTPException, Request
from aiogram.types import Update

from app.bot.loader import dp, bot
from app.core.config import settings
from app.core.logger import telegram_logger

router = APIRouter(prefix="/telegram")


@router.post(path="/")
async def telegram_webhook(
        request: Request,
        x_telegram_bot_api_secret_token: str = Header(None)
):
    """
    Telegram Bot API webhook endpoint

    Security: Validates secret token from Telegram
    """

    if x_telegram_bot_api_secret_token != settings.WEBHOOK_SECRET:
        telegram_logger.warning(f"Invalid webhook secret token attempt from {request.client.host}")
        raise HTTPException(status_code=403, detail="Invalid secret token")

    try:

        update_data = await request.json()
        update = Update(**update_data)

        telegram_logger.debug(f"Telegram update received: {update.update_id}")

        await dp.feed_update(bot, update)

        return {"ok": True}

    except Exception as e:
        telegram_logger.error(f"Error processing Telegram update: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(path="/info/")
async def webhook_info():
    """Get current webhook info"""

    try:
        webhook_info = await bot.get_webhook_info()
        return {
            "url": webhook_info.url,
            "has_custom_certificate": webhook_info.has_custom_certificate,
            "pending_update_count": webhook_info.pending_update_count,
            "last_error_date": webhook_info.last_error_date,
            "last_error_message": webhook_info.last_error_message,
            "max_connections": webhook_info.max_connections,
        }
    except Exception as e:
        telegram_logger.error(f"Error getting webhook info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

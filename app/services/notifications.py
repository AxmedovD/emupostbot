from typing import Dict, Any
from aiogram import Bot

from app.db.pool import Database
from app.core.logger import service_logger


async def send_notification_to_user(
        bot: Bot,
        db: Database,
        telegram_id: int,
        message: str
) -> bool:
    """
    Send notification to specific user

    Args:
        bot: Aiogram Bot instance
        db: Database instance
        telegram_id: Telegram user ID
        message: Message to send

    Returns:
        True if sent successfully, False otherwise
    """

    try:
        await bot.send_message(chat_id=telegram_id, text=message)
        service_logger.info(f"Notification sent to {telegram_id}")
        return True

    except Exception as e:
        service_logger.error(f"Failed to send notification to {telegram_id}: {e}")
        return False


async def process_external_webhook(payload: Dict[str, Any], db: Database):
    """
    Process external webhook and trigger notifications

    Args:
        payload: Webhook payload data
        db: Database instance
    """

    event_type = payload.get("event_type")
    data = payload.get("data", {})

    service_logger.info(f"Processing webhook event: {event_type}")

    # Example: Handle notification event
    if event_type == "notification":
        message = data.get("message", "No message provided")
        user_id = payload.get("user_id")

        if user_id:
            # Send to specific user
            service_logger.info(f"Queueing notification for user {user_id}")
            # In production, you'd use the bot instance here
            # For now, just log the webhook
        else:
            # Broadcast to all users
            service_logger.info("Queueing broadcast notification")

    # Add more event handlers as needed
    elif event_type == "update":
        service_logger.info(f"Processing update event: {data}")

    else:
        service_logger.warning(f"Unknown event type: {event_type}")

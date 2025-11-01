from typing import Annotated
from fastapi import HTTPException, Header

from app.core.config import settings
from app.db.pool import db


async def get_db():
    """Get database instance"""
    return db


async def verify_external_webhook_secret(
        x_webhook_signature: Annotated[str | None, Header()] = None
) -> bool:
    """Verify external webhook secret from header"""

    if not settings.EXTERNAL_WEBHOOK_SECRET:
        return True

    if not x_webhook_signature:
        raise HTTPException(status_code=401, detail="Missing webhook signature")

    # Simple bearer token check (you can enhance this with HMAC)
    if x_webhook_signature != f"Bearer {settings.EXTERNAL_WEBHOOK_SECRET}":
        raise HTTPException(status_code=403, detail="Invalid webhook signature")

    return True

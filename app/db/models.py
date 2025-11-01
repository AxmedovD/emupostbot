from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class UserModel(BaseModel):
    """User database model"""

    id: int
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_active: bool = True
    notifications_enabled: bool = True
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_activity: Optional[datetime] = None

    class Config:
        from_attributes = True


class NotificationModel(BaseModel):
    """Notification database model"""

    id: int
    user_id: int
    message: str
    sent: bool = False
    created_at: datetime
    sent_at: Optional[datetime] = None

    class Config:
        from_attributes = True

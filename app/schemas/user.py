from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class UserCreate(BaseModel):
    """Schema for creating a user"""

    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class UserResponse(BaseModel):
    """Schema for user response"""

    id: int
    telegram_id: int
    username: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    is_active: bool
    notifications_enabled: bool
    created_at: datetime

    class Config:
        from_attributes = True

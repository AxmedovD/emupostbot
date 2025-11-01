from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class ExternalWebhookPayload(BaseModel):
    """Schema for external webhook payload"""

    event_type: str = Field(..., description="Type of event")
    data: Dict[str, Any] = Field(..., description="Event data")
    timestamp: Optional[str] = None
    user_id: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "notification",
                "data": {
                    "message": "Test notification",
                    "priority": "high"
                },
                "timestamp": "2025-10-31T12:00:00Z",
                "user_id": 123456789
            }
        }

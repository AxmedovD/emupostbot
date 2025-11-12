from pydantic import BaseModel


class ExternalWebhookPayload(BaseModel):
    order_no: str
    webhook_id: int
    parcel_id: int

    class Config:
        json_schema_extra = {
            "example": {
                "order_no": "EMU23423432",
                "webhook_id": 12345,
                "parcel_id": 123
            }
        }

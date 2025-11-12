from fastapi import APIRouter, Depends, BackgroundTasks

from app.core.dependencies import verify_external_webhook_secret, get_db
from app.core.logger import webhook_logger
from app.db.pool import Database
from app.schemas.webhook import ExternalWebhookPayload
from app.services.notifications import process_external_webhook

router = APIRouter()


@router.post(path="/", dependencies=[Depends(verify_external_webhook_secret)])
async def external_webhook(
        data: ExternalWebhookPayload,
        background_tasks: BackgroundTasks,
        db: Database = Depends(get_db)
):
    try:
        return {
            "status": "success"
        }

    except Exception as e:
        webhook_logger.error(f"Error processing external webhook: {e}")
        return {
            "status": "error",
            "message": str(e)
        }

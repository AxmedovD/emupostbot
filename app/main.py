from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request

from app.api import webhooks, telegram
from app.core.config import settings
from app.core.logger import logger
from app.core.responses import standard_response
from app.db.pool import db
from app.bot.loader import bot, setup_bot


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("=" * 50)
    logger.info("Starting EmuPostBot application...")
    logger.info("=" * 50)

    try:
        await db.create_session_pool()
        logger.info("✓ Database connected")

        await setup_bot()
        logger.info("✓ Bot handlers configured")

        webhook_url = f"{settings.WEBHOOK_URL}/webhook/telegram/"
        await bot.set_webhook(
            url=webhook_url,
            secret_token=settings.WEBHOOK_SECRET,
            drop_pending_updates=True
        )
        logger.info(f"✓ Webhook set: {webhook_url}")
        logger.info("=" * 50)
        logger.info("Application started successfully!")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"✗ Startup failed: {e}")
        raise

    yield

    logger.info("=" * 50)
    logger.info("Shutting down application...")
    logger.info("=" * 50)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✓ Webhook deleted")

        await db.close()
        logger.info("✓ Database disconnected")

        await bot.session.close()
        logger.info("✓ Bot session closed")

        logger.info("=" * 50)
        logger.info("Application shut down successfully!")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"✗ Shutdown error: {e}")


app = FastAPI(
    title="EmuPostBot",
    description="Telegram Bot",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None
)

# CORS middleware for Mini App
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://telegram.org",
        "https://web.telegram.org",
        "https://t.me"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    webhooks.router,
    prefix="/webhooks",
    tags=["External Webhooks"]
)

app.include_router(
    telegram.router,
    prefix="/webhook",
    tags=["Telegram"]
)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    return standard_response(
        success=False,
        message=str(exc.detail),
        errors=[str(exc.detail)],
        status_code=exc.status_code
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app="app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="info"
    )

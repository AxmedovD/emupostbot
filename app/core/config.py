from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Bot Configuration
    BOT_TOKEN: str
    WEBHOOK_SECRET: str
    WEBHOOK_URL: str

    # Database Configuration
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    # Redis Configuration (optional)
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_ENABLED: bool = False

    # External Webhook Security (optional)
    EXTERNAL_WEBHOOK_SECRET: str = ""

    # FastAPI Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = False

    # Mini App Configuration
    WEBAPP_URL: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="UTF-8",
        extra="ignore"
    )

    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from loguru import logger

class Settings(BaseSettings):
    """
    Pydantic based settings class for managing application settings
    """
    model_config : SettingsConfigDict = SettingsConfigDict(
        env_file = ".env",
        env_file_encoding = "utf-8"
    )

    NOTION_API_KEY : str | None = Field(
        default = None, description = "Secret key for Notion API"
    )

try:
    settings = Settings()
except Exception as e:
    logger.error(f"Error loading settings: {e}")
    raise SystemExit(e)
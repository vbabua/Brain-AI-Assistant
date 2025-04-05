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

    MONGODB_DATABASE_NAME : str = Field(
        default = "brain_ai_assistant",
        description = "Name of the MongoDB database"
    )

    MONGODB_DATABASE_URI : str = Field(
    default="mongodb://decodingml:decodingml@localhost:27017/?directConnection=true",
    description = "MongoDB connection URI"
    )

    OPENAI_API_KEY: str = Field(
        description="API key for OpenAI",
    )


try:
    settings = Settings()
except Exception as e:
    logger.error(f"Error loading settings: {e}")
    raise SystemExit(e)
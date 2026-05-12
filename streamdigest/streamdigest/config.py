from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    github_token: str = Field(default="", description="GitHub PAT for notifications API")

    ollama_host: str = "http://localhost:11434"
    ollama_model: str = "llama3.2:3b"

    duckdb_path: Path = Path("./data/streamdigest.duckdb")

    gmail_host: str = ""
    gmail_email: str = ""
    gmail_app_password: str = ""
    slack_bot_token: str = ""
    linear_api_key: str = ""


settings = Settings()

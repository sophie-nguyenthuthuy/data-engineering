from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "sqlite:///./data/rates.db"
    request_timeout_seconds: int = 30
    request_retry_attempts: int = 3
    use_mock_data: bool = False
    scrape_interval_hours: int = 6
    user_agent: str = "SavingsRateEngine/0.1"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = False
    log_level: str = "INFO"

    @field_validator("log_level")
    @classmethod
    def upper_log_level(cls, v: str) -> str:
        return v.upper()


settings = Settings()

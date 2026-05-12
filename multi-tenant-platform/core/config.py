from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/platform"
    database_pool_size: int = 20
    database_max_overflow: int = 10

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Object storage (MinIO / S3)
    storage_endpoint: str = "http://localhost:9000"
    storage_access_key: str = "minioadmin"
    storage_secret_key: SecretStr = SecretStr("minioadmin")
    storage_bucket_prefix: str = "tenant"
    storage_region: str = "us-east-1"

    # Auth
    jwt_secret: SecretStr = SecretStr("change-me-in-production")
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60
    admin_token: SecretStr = SecretStr("admin-secret")

    # Platform
    environment: str = "development"
    log_level: str = "INFO"


settings = Settings()

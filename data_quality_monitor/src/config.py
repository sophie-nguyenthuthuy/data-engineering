from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Literal


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_input_topic: str = "raw_data_batches"
    kafka_results_topic: str = "quality_results"
    kafka_consumer_group: str = "dq_monitor"
    kafka_batch_timeout_ms: int = 5000
    kafka_max_poll_records: int = 500

    # PostgreSQL
    database_url: str = "postgresql+asyncpg://dqm:dqm_pass@localhost:5432/dqmonitor"
    database_pool_size: int = 10
    database_max_overflow: int = 20

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_results_ttl_seconds: int = 86400  # 24 h
    redis_block_key_prefix: str = "dq:block:"
    redis_metrics_channel: str = "dq:metrics"

    # Validation
    ge_data_context_root: str = "config/expectations"
    soda_config_path: str = "config/soda/soda_config.yml"
    validator_backend: Literal["great_expectations", "soda", "both"] = "both"
    failure_threshold: float = Field(default=0.95, ge=0.0, le=1.0)  # min pass rate

    # Dashboard
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8000
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8080"]

    # Job blocking
    block_ttl_seconds: int = 3600
    downstream_jobs: list[str] = ["etl_transform", "ml_feature_pipeline", "reporting_export"]

    # Logging
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"


settings = Settings()

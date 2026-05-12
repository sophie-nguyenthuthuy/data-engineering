"""Central configuration for the lambda-kappa-migration project."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
HISTORICAL_DIR = DATA_DIR / "historical"
LOCAL_KAFKA_FILE = DATA_DIR / "local_kafka.jsonl"  # used in LOCAL_MODE


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class KafkaConfig(BaseModel):
    """Kafka broker and topic configuration."""

    brokers: str = "localhost:9092"
    topic_live: str = "events-live"
    topic_replay: str = "events-replay"
    consumer_group_speed: str = "lambda-speed-layer"
    consumer_group_kappa: str = "kappa-processor"
    auto_offset_reset: str = "earliest"


class S3Config(BaseModel):
    """S3 / LocalStack configuration."""

    endpoint_url: str = "http://localhost:4566"
    bucket_name: str = "lambda-kappa-historical"
    region: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"


class ToleranceConfig(BaseModel):
    """Tolerance rules for the correctness validator."""

    count_exact: bool = True  # counts must match exactly
    amount_rel_tolerance: float = 0.0001  # 0.01% for amounts/averages
    missing_key_is_mismatch: bool = True


class AppConfig(BaseSettings):
    """Top-level application settings, overridable via environment variables."""

    local_mode: bool = Field(default=False, alias="LOCAL_MODE")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    backfill_rate: int = Field(default=1000, alias="BACKFILL_RATE")  # events/sec

    kafka: KafkaConfig = KafkaConfig()
    s3: S3Config = S3Config()
    tolerance: ToleranceConfig = ToleranceConfig()

    model_config = {"populate_by_name": True, "env_nested_delimiter": "__"}

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load config from environment, falling back to defaults."""
        return cls()


# Singleton instance used across the project
config = AppConfig.from_env()

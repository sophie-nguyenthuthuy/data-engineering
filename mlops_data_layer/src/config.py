from __future__ import annotations
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Literal


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # ── Kafka ────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_serving_topic: str = "serving_requests"       # live prediction requests
    kafka_drift_topic: str = "drift_alerts"             # outbound drift events
    kafka_retrain_topic: str = "retrain_triggers"       # outbound retrain commands
    kafka_consumer_group: str = "mlops_data_layer"

    # ── PostgreSQL ───────────────────────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://mlops:mlops_pass@localhost:5432/mlops"
    database_pool_size: int = 10
    database_max_overflow: int = 20

    # ── Redis (feature cache) ────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"
    feature_cache_ttl_seconds: int = 3600
    drift_channel: str = "mlops:drift"
    retrain_channel: str = "mlops:retrain"

    # ── Feature store ────────────────────────────────────────────────────────
    feature_definitions_path: str = "config/features/feature_definitions.yml"
    feature_store_window_days: int = 30     # how far back to look for training stats

    # ── Drift detection ──────────────────────────────────────────────────────
    drift_thresholds_path: str = "config/drift/thresholds.yml"
    drift_eval_window_size: int = 500       # number of serving samples per eval
    drift_eval_interval_seconds: int = 300  # run drift check every 5 min

    # KS test p-value threshold (below this → drift)
    ks_pvalue_threshold: float = Field(default=0.05, ge=0.0, le=1.0)
    # Population Stability Index threshold (above this → drift)
    psi_threshold: float = Field(default=0.2, ge=0.0)
    # Jensen-Shannon divergence threshold (above this → drift)
    js_threshold: float = Field(default=0.1, ge=0.0, le=1.0)

    # ── Skew detection ───────────────────────────────────────────────────────
    skew_eval_window_size: int = 1000
    skew_psi_threshold: float = Field(default=0.1, ge=0.0)

    # ── Retraining ───────────────────────────────────────────────────────────
    retrain_cooldown_seconds: int = 3600    # min time between consecutive triggers
    retrain_min_drift_features: int = 2     # how many drifted features trigger retrain
    retrain_webhook_url: str | None = None  # optional webhook for ML platform

    # ── API ──────────────────────────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: list[str] = ["http://localhost:3000"]

    # ── Logging ──────────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"


settings = Settings()

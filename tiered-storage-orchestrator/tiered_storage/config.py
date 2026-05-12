"""Central configuration — reads from environment variables with sane defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from tiered_storage.schemas import LifecyclePolicy, RehydrationPriority


@dataclass
class StorageConfig:
    # ---- Hot tier -------------------------------------------------------
    redis_url: str = field(
        default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379")
    )
    redis_ttl_seconds: int = field(
        default_factory=lambda: int(os.getenv("REDIS_TTL_SECONDS", "3600"))
    )
    postgres_dsn: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_DSN",
            "postgresql://postgres:postgres@localhost:5432/tiered_storage",
        )
    )

    # ---- Warm tier -------------------------------------------------------
    s3_bucket: str = field(
        default_factory=lambda: os.getenv("S3_BUCKET", "tiered-storage-warm")
    )
    s3_warm_prefix: str = field(
        default_factory=lambda: os.getenv("S3_WARM_PREFIX", "warm")
    )
    s3_region: str = field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1")
    )
    s3_endpoint_url: Optional[str] = field(
        default_factory=lambda: os.getenv("S3_ENDPOINT_URL")  # LocalStack / MinIO
    )
    aws_access_key_id: Optional[str] = field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID")
    )
    aws_secret_access_key: Optional[str] = field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    # ---- Cold tier -------------------------------------------------------
    cold_local_path: Optional[str] = field(
        default_factory=lambda: os.getenv("COLD_LOCAL_PATH")  # use S3 if unset
    )
    s3_cold_prefix: str = field(
        default_factory=lambda: os.getenv("S3_COLD_PREFIX", "cold")
    )
    use_glacier: bool = field(
        default_factory=lambda: os.getenv("USE_GLACIER", "false").lower() == "true"
    )

    # ---- Lifecycle -------------------------------------------------------
    lifecycle_interval_seconds: float = field(
        default_factory=lambda: float(os.getenv("LIFECYCLE_INTERVAL_SECONDS", "3600"))
    )
    hot_to_warm_idle_days: float = field(
        default_factory=lambda: float(os.getenv("HOT_TO_WARM_IDLE_DAYS", "7"))
    )
    warm_to_cold_idle_days: float = field(
        default_factory=lambda: float(os.getenv("WARM_TO_COLD_IDLE_DAYS", "30"))
    )
    hot_max_size_gb: float = field(
        default_factory=lambda: float(os.getenv("HOT_MAX_SIZE_GB", "10"))
    )
    warm_max_size_gb: float = field(
        default_factory=lambda: float(os.getenv("WARM_MAX_SIZE_GB", "500"))
    )
    hot_min_access_freq: float = field(
        default_factory=lambda: float(os.getenv("HOT_MIN_ACCESS_FREQ", "1.0"))
    )

    # ---- Rehydration -----------------------------------------------------
    rehydration_default_priority: RehydrationPriority = field(
        default_factory=lambda: RehydrationPriority(
            os.getenv("REHYDRATION_PRIORITY", "standard")
        )
    )
    promote_freq_threshold: float = field(
        default_factory=lambda: float(os.getenv("PROMOTE_FREQ_THRESHOLD", "5.0"))
    )
    block_on_cold: bool = field(
        default_factory=lambda: os.getenv("BLOCK_ON_COLD", "false").lower() == "true"
    )

    # ---- Tracking --------------------------------------------------------
    tracker_persist_path: Optional[str] = field(
        default_factory=lambda: os.getenv("TRACKER_PERSIST_PATH")
    )

    def to_lifecycle_policy(self) -> LifecyclePolicy:
        return LifecyclePolicy(
            hot_to_warm_idle_days=self.hot_to_warm_idle_days,
            warm_to_cold_idle_days=self.warm_to_cold_idle_days,
            hot_max_size_gb=self.hot_max_size_gb,
            warm_max_size_gb=self.warm_max_size_gb,
            hot_min_access_freq=self.hot_min_access_freq,
            rehydration_default_priority=self.rehydration_default_priority,
        )

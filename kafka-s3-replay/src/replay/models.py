"""Core domain models."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class ArchiveFormat(str, Enum):
    JSONL = "jsonl"
    AVRO = "avro"
    PARQUET = "parquet"


class TargetType(str, Enum):
    KAFKA = "kafka"
    HTTP = "http"
    STDOUT = "stdout"
    FILE = "file"


class ReplayStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class Event(BaseModel):
    """A single archived event."""

    topic: str
    partition: int = 0
    offset: int = 0
    key: bytes | None = None
    value: bytes
    headers: dict[str, bytes] = Field(default_factory=dict)
    timestamp: datetime
    source_path: str = ""

    @property
    def event_id(self) -> str:
        return hashlib.sha1(
            f"{self.topic}:{self.partition}:{self.offset}".encode()
        ).hexdigest()[:12]

    def model_post_init(self, __context: Any) -> None:
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)


class TimeWindow(BaseModel):
    """Inclusive time window for event selection."""

    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_window(self) -> "TimeWindow":
        if self.start.tzinfo is None:
            self.start = self.start.replace(tzinfo=timezone.utc)
        if self.end.tzinfo is None:
            self.end = self.end.replace(tzinfo=timezone.utc)
        if self.end <= self.start:
            raise ValueError("end must be after start")
        delta = self.end - self.start
        if delta.days > 30:
            raise ValueError("window cannot exceed 30 days")
        return self

    def contains(self, ts: datetime) -> bool:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return self.start <= ts <= self.end

    @property
    def duration_hours(self) -> float:
        return (self.end - self.start).total_seconds() / 3600


class S3ArchiveConfig(BaseModel):
    bucket: str
    prefix: str = ""
    region: str = "us-east-1"
    format: ArchiveFormat = ArchiveFormat.JSONL
    path_template: str = "{topic}/{partition:04d}/{topic}+{partition:04d}+{offset:020d}.{ext}"
    endpoint_url: str | None = None  # for MinIO / localstack


class KafkaTargetConfig(BaseModel):
    bootstrap_servers: str
    topic_mapping: dict[str, str] = Field(default_factory=dict)
    producer_config: dict[str, Any] = Field(default_factory=dict)


class HttpTargetConfig(BaseModel):
    url: str
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float = 30.0
    max_retries: int = 3


class FileTargetConfig(BaseModel):
    path: str
    format: ArchiveFormat = ArchiveFormat.JSONL
    append: bool = False


class ReplayConfig(BaseModel):
    """Top-level replay job configuration."""

    job_id: str
    topics: list[str]
    window: TimeWindow
    archive: S3ArchiveConfig
    target_type: TargetType
    kafka_target: KafkaTargetConfig | None = None
    http_target: HttpTargetConfig | None = None
    file_target: FileTargetConfig | None = None
    rate_limit_per_second: float | None = None
    dry_run: bool = False
    checkpoint_dir: str = "/tmp/replay-checkpoints"
    max_parallel_partitions: int = 4


class ReplayProgress(BaseModel):
    job_id: str
    status: ReplayStatus = ReplayStatus.PENDING
    total_events: int = 0
    replayed_events: int = 0
    failed_events: int = 0
    skipped_events: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_checkpoint: datetime | None = None
    errors: list[str] = Field(default_factory=list)

    @property
    def pct_complete(self) -> float:
        if self.total_events == 0:
            return 0.0
        return 100 * self.replayed_events / self.total_events

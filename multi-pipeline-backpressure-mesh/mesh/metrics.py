"""Core metric and signal types shared across the mesh."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class BackpressureLevel(float, Enum):
    NONE = 0.0
    LOW = 0.25
    MEDIUM = 0.50
    HIGH = 0.75
    CRITICAL = 1.0


@dataclass
class JobMetrics:
    job_id: str
    timestamp: float = field(default_factory=time.monotonic)

    # throughput
    records_in_per_sec: float = 0.0
    records_out_per_sec: float = 0.0

    # queue / buffer health
    input_queue_depth: int = 0
    input_queue_capacity: int = 1000
    output_queue_depth: int = 0
    output_queue_capacity: int = 1000

    # processing
    processing_lag_ms: float = 0.0
    avg_record_latency_ms: float = 0.0

    @property
    def input_utilization(self) -> float:
        if self.input_queue_capacity == 0:
            return 0.0
        return min(self.input_queue_depth / self.input_queue_capacity, 1.0)

    @property
    def output_utilization(self) -> float:
        if self.output_queue_capacity == 0:
            return 0.0
        return min(self.output_queue_depth / self.output_queue_capacity, 1.0)

    @property
    def throughput_ratio(self) -> float:
        """out/in ratio; <1 means job is falling behind."""
        if self.records_in_per_sec == 0:
            return 1.0
        return self.records_out_per_sec / self.records_in_per_sec

    def backpressure_score(self) -> float:
        """0–1 composite score derived from queue fill and throughput lag."""
        queue_pressure = max(self.input_utilization, self.output_utilization)
        throughput_pressure = max(0.0, 1.0 - self.throughput_ratio)
        lag_pressure = min(self.processing_lag_ms / 5000.0, 1.0)
        return max(queue_pressure, throughput_pressure, lag_pressure)


@dataclass
class BackpressureSignal:
    """Emitted by a sidecar when its job detects backpressure."""
    source_job_id: str
    level: BackpressureLevel
    score: float
    timestamp: float = field(default_factory=time.monotonic)
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "source_job_id": self.source_job_id,
            "level": self.level.name,
            "score": self.score,
            "timestamp": self.timestamp,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, d: dict) -> BackpressureSignal:
        return cls(
            source_job_id=d["source_job_id"],
            level=BackpressureLevel[d["level"]],
            score=d["score"],
            timestamp=d["timestamp"],
            message=d.get("message", ""),
        )


@dataclass
class ThrottleCommand:
    """Issued by the coordinator to a job's sidecar."""
    target_job_id: str
    throttle_factor: float       # 0.0 = full stop, 1.0 = no throttle
    reason: str = ""
    timestamp: float = field(default_factory=time.monotonic)
    originating_signal: Optional[BackpressureSignal] = None

    def to_dict(self) -> dict:
        return {
            "target_job_id": self.target_job_id,
            "throttle_factor": self.throttle_factor,
            "reason": self.reason,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> ThrottleCommand:
        return cls(
            target_job_id=d["target_job_id"],
            throttle_factor=d["throttle_factor"],
            reason=d.get("reason", ""),
            timestamp=d["timestamp"],
        )

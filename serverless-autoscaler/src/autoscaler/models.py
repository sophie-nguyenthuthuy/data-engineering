from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class JobType(str, Enum):
    SPARK = "spark"
    FLINK = "flink"


class JobStatus(str, Enum):
    SCHEDULED = "scheduled"
    PREWARMING = "prewarming"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobDefinition:
    job_id: str
    name: str
    job_type: JobType
    hpa_target: str          # name of the HPA object in k8s
    cron_expression: str
    namespace: str = "default"
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class JobRun:
    run_id: str
    job_id: str
    scheduled_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    status: JobStatus
    peak_cpu_millicores: Optional[float] = None
    peak_memory_mib: Optional[float] = None
    avg_workers: Optional[float] = None
    peak_workers: Optional[int] = None
    duration_seconds: Optional[float] = None
    cold_start_avoided: bool = False

    @property
    def wall_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


@dataclass
class ResourceForecast:
    job_id: str
    forecast_at: datetime
    target_start: datetime
    predicted_peak_workers: int
    predicted_peak_cpu_millicores: float
    predicted_peak_memory_mib: float
    confidence_lower: int
    confidence_upper: int
    history_points_used: int
    model_aic: Optional[float] = None


@dataclass
class ScalingAction:
    job_id: str
    hpa_target: str
    namespace: str
    action_at: datetime
    min_replicas_before: int
    min_replicas_after: int
    max_replicas_before: int
    max_replicas_after: int
    reason: str


@dataclass
class ColdStartSavingRecord:
    job_id: str
    run_id: str
    recorded_at: datetime
    workers_prewarmed: int
    cold_start_seconds_saved: float
    prewarm_idle_cost_usd: float
    cold_start_avoided_cost_usd: float

    @property
    def net_saving_usd(self) -> float:
        return self.cold_start_avoided_cost_usd - self.prewarm_idle_cost_usd

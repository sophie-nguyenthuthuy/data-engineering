from __future__ import annotations
import uuid
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class TriggerReason(str, Enum):
    DATA_DRIFT = "data_drift"
    TRAINING_SERVING_SKEW = "training_serving_skew"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    PERFORMANCE_DEGRADATION = "performance_degradation"


class RetrainingTrigger(BaseModel):
    trigger_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_name: str
    model_version: str
    reason: TriggerReason
    triggered_at: datetime = Field(default_factory=datetime.utcnow)
    drift_report_id: str | None = None
    skew_report_id: str | None = None
    drifted_features: list[str] = Field(default_factory=list)
    drift_score: float = 0.0
    metadata: dict = Field(default_factory=dict)


class RetrainingJobStatus(str, Enum):
    PENDING = "pending"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"   # cooldown active


class RetrainingJob(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trigger: RetrainingTrigger
    status: RetrainingJobStatus = RetrainingJobStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    dispatched_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    new_model_version: str | None = None

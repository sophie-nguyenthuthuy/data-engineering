from __future__ import annotations
import uuid
from datetime import datetime
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class PipelineStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class StepResult(BaseModel):
    step_name: str
    status: PipelineStatus
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    error_message: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)


class PipelineRun(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str
    model_name: str
    triggered_by: str = "scheduler"    # scheduler | api | drift_trigger
    status: PipelineStatus = PipelineStatus.PENDING
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None
    duration_ms: float = 0.0
    input_rows: int = 0
    output_rows: int = 0
    step_results: list[StepResult] = Field(default_factory=list)
    artifacts: dict[str, str] = Field(default_factory=dict)   # name → path/uri
    error_message: str | None = None

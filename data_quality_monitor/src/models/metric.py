from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, Field


class QualityMetric(BaseModel):
    table_name: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    pass_rate: float
    total_batches: int
    failed_batches: int
    avg_row_count: float
    avg_duration_ms: float
    active_blocks: int
    checks_passed: int
    checks_failed: int


class MetricSnapshot(BaseModel):
    """Aggregated view pushed to the dashboard WebSocket."""
    snapshot_at: datetime = Field(default_factory=datetime.utcnow)
    overall_pass_rate: float
    total_batches_last_hour: int
    failed_batches_last_hour: int
    active_blocks: list[str] = Field(default_factory=list)
    per_table: list[QualityMetric] = Field(default_factory=list)
    recent_failures: list[dict] = Field(default_factory=list)

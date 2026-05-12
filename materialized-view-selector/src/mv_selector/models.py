"""Core data models for the materialized view selector."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class Warehouse(str, Enum):
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"


@dataclass
class QueryRecord:
    sql: str
    warehouse: Warehouse
    executed_at: datetime
    duration_ms: int
    bytes_processed: int
    cost_usd: float
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user: Optional[str] = None
    project_or_account: Optional[str] = None
    dataset_or_schema: Optional[str] = None
    frequency: int = 1  # bumped when deduped


@dataclass
class CandidateView:
    """A view we *might* materialise."""

    sql: str
    name: str
    referenced_tables: list[str]
    benefiting_query_ids: list[str]
    estimated_storage_bytes: int
    estimated_maintenance_cost_usd: float  # per month
    estimated_benefit_usd: float           # per month
    view_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def net_benefit_usd(self) -> float:
        return self.estimated_benefit_usd - self.estimated_maintenance_cost_usd

    @property
    def benefit_per_storage_byte(self) -> float:
        if self.estimated_storage_bytes == 0:
            return 0.0
        return self.net_benefit_usd / self.estimated_storage_bytes

    def __hash__(self) -> int:
        return hash(self.view_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CandidateView):
            return NotImplemented
        return self.view_id == other.view_id


@dataclass
class MaterializedView:
    """A view that has been (or is being) created."""

    candidate: CandidateView
    warehouse: Warehouse
    created_at: datetime
    fqn: str  # fully-qualified name, e.g. project.dataset.mv_name
    last_refreshed_at: Optional[datetime] = None
    actual_savings_usd: float = 0.0
    refresh_count: int = 0
    is_active: bool = True

    @property
    def calibration_ratio(self) -> float:
        """actual / predicted — used to update cost model."""
        predicted = self.candidate.estimated_benefit_usd
        if predicted <= 0:
            return 1.0
        return self.actual_savings_usd / predicted


@dataclass
class OptimizationResult:
    selected: list[CandidateView]
    total_estimated_benefit_usd: float
    total_storage_bytes: int
    total_maintenance_cost_usd: float
    algorithm: str
    iterations: int
    elapsed_seconds: float
    incumbent_history: list[float] = field(default_factory=list)

    @property
    def net_benefit_usd(self) -> float:
        return self.total_estimated_benefit_usd - self.total_maintenance_cost_usd

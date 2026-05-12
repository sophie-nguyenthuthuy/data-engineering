"""Shared data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class DegradationKind(str, Enum):
    DROP = "drop"
    SPIKE = "spike"
    NULL_INCREASE = "null_increase"
    SCHEMA_BREAK = "schema_break"


class ChangeCategoryKind(str, Enum):
    COLUMN_ADDED = "column_added"
    COLUMN_DROPPED = "column_dropped"
    COLUMN_RENAMED = "column_renamed"
    TYPE_CHANGED = "type_changed"
    PIPELINE_FAILURE = "pipeline_failure"
    LATE_DATA = "late_data"
    VOLUME_ANOMALY = "volume_anomaly"


@dataclass
class MetricPoint:
    timestamp: datetime
    value: float


@dataclass
class MetricDegradation:
    metric_name: str
    detected_at: datetime
    kind: DegradationKind
    observed_value: float
    baseline_value: float
    relative_change: float           # negative = drop, positive = spike
    series: list[MetricPoint] = field(default_factory=list)


@dataclass
class SchemaChange:
    table: str
    column: str | None
    kind: ChangeCategoryKind
    occurred_at: datetime
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class CausalEstimate:
    """Result of an interrupted time series analysis for one candidate cause."""

    candidate: str                   # "table.column" or "pipeline_run" etc.
    change: SchemaChange | None
    effect_size: float               # relative effect on metric (0-1)
    absolute_effect: float
    p_value: float
    confidence_interval: tuple[float, float]
    is_significant: bool
    counterfactual: list[MetricPoint] = field(default_factory=list)


@dataclass
class RootCauseReport:
    incident_id: str
    degradation: MetricDegradation
    top_causes: list[CausalEstimate]
    all_candidates: list[CausalEstimate]
    generated_at: datetime = field(default_factory=datetime.utcnow)
    narrative: str = ""

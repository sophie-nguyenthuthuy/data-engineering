from __future__ import annotations
import uuid
from datetime import datetime
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class FeatureType(str, Enum):
    NUMERICAL = "numerical"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"
    TEXT = "text"
    EMBEDDING = "embedding"


class FeatureDefinition(BaseModel):
    """Schema contract for a single feature column."""
    name: str
    feature_type: FeatureType
    description: str = ""
    version: int = 1
    nullable: bool = True
    tags: dict[str, str] = Field(default_factory=dict)
    # Numerical constraints
    min_value: float | None = None
    max_value: float | None = None
    # Categorical constraints
    allowed_values: list[str] | None = None
    # Embedding
    embedding_dim: int | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class FeatureValue(BaseModel):
    feature_name: str
    value: Any
    entity_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class FeatureVector(BaseModel):
    """A fully materialised feature vector for one entity (one row)."""
    vector_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    entity_id: str
    model_name: str
    features: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = "serving"   # "training" | "serving"


class FeatureStats(BaseModel):
    """Univariate statistics captured from a dataset (training or serving window)."""
    feature_name: str
    feature_type: FeatureType
    count: int
    null_count: int
    null_fraction: float
    # Numerical
    mean: float | None = None
    std: float | None = None
    min: float | None = None
    max: float | None = None
    p25: float | None = None
    p50: float | None = None
    p75: float | None = None
    p95: float | None = None
    p99: float | None = None
    histogram_edges: list[float] | None = None
    histogram_counts: list[int] | None = None
    # Categorical
    value_counts: dict[str, int] | None = None
    cardinality: int | None = None
    top_value: str | None = None


class TrainingSnapshot(BaseModel):
    """
    Reference distribution captured at training time.
    Persisted and compared against live serving data for drift / skew.
    """
    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_name: str
    model_version: str
    captured_at: datetime = Field(default_factory=datetime.utcnow)
    row_count: int
    feature_stats: list[FeatureStats] = Field(default_factory=list)

    def stats_by_name(self) -> dict[str, FeatureStats]:
        return {s.feature_name: s for s in self.feature_stats}

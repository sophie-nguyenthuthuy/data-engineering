from __future__ import annotations
import uuid
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class DriftStatus(str, Enum):
    NO_DRIFT = "no_drift"
    WARNING = "warning"
    DRIFT_DETECTED = "drift_detected"
    INSUFFICIENT_DATA = "insufficient_data"
    ERROR = "error"


class FeatureDriftResult(BaseModel):
    feature_name: str
    status: DriftStatus
    # KS test
    ks_statistic: float | None = None
    ks_pvalue: float | None = None
    # Population Stability Index
    psi_score: float | None = None
    # Jensen-Shannon divergence
    js_divergence: float | None = None
    # Chi-square (categorical)
    chi2_statistic: float | None = None
    chi2_pvalue: float | None = None
    # Human-readable explanation
    explanation: str = ""
    drift_magnitude: float = 0.0   # 0.0–1.0 normalised severity


class DriftReport(BaseModel):
    """Full drift assessment for one model at a point in time."""
    report_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_name: str
    model_version: str
    reference_snapshot_id: str
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)
    window_size: int
    overall_status: DriftStatus
    drifted_feature_count: int
    total_feature_count: int
    drift_score: float = 0.0        # average across all features
    feature_results: list[FeatureDriftResult] = Field(default_factory=list)
    triggers_retraining: bool = False

    def drifted_features(self) -> list[FeatureDriftResult]:
        return [f for f in self.feature_results if f.status == DriftStatus.DRIFT_DETECTED]


class SkewFeatureResult(BaseModel):
    feature_name: str
    status: DriftStatus
    psi_score: float | None = None
    training_mean: float | None = None
    serving_mean: float | None = None
    training_std: float | None = None
    serving_std: float | None = None
    relative_mean_shift: float | None = None
    explanation: str = ""


class SkewReport(BaseModel):
    """Training/serving skew report — compares the live feature distribution
    to what was seen during model training."""
    report_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_name: str
    model_version: str
    snapshot_id: str
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)
    serving_window_size: int
    overall_status: DriftStatus
    skewed_feature_count: int
    total_feature_count: int
    feature_results: list[SkewFeatureResult] = Field(default_factory=list)

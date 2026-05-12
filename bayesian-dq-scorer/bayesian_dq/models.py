from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class DQDimension(str, Enum):
    COMPLETENESS = "completeness"
    FRESHNESS = "freshness"
    UNIQUENESS = "uniqueness"


@dataclass
class PosteriorState:
    """Beta(alpha, beta) posterior for a single quality dimension."""
    dimension: DQDimension
    alpha: float  # successes + prior_alpha
    beta: float   # failures + prior_beta
    batch_count: int = 0
    last_updated: Optional[datetime] = None

    @property
    def mean(self) -> float:
        return self.alpha / (self.alpha + self.beta)

    @property
    def variance(self) -> float:
        a, b = self.alpha, self.beta
        return (a * b) / ((a + b) ** 2 * (a + b + 1))

    @property
    def std(self) -> float:
        return self.variance ** 0.5

    @property
    def concentration(self) -> float:
        """alpha + beta — higher means sharper / more certain posterior."""
        return self.alpha + self.beta

    def to_dict(self) -> dict:
        return {
            "dimension": self.dimension.value,
            "alpha": self.alpha,
            "beta": self.beta,
            "mean": self.mean,
            "std": self.std,
            "batch_count": self.batch_count,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
        }


@dataclass
class BatchObservation:
    """Raw observation fed to a dimension scorer."""
    dimension: DQDimension
    successes: int      # e.g. non-null rows, on-time arrivals, unique rows
    total: int          # total rows in batch
    batch_id: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)

    @property
    def rate(self) -> float:
        return self.successes / self.total if self.total > 0 else 0.0


@dataclass
class BatchResult:
    """Full result for one batch across all dimensions."""
    batch_id: str
    timestamp: datetime
    observations: list[BatchObservation]
    posteriors: dict[DQDimension, PosteriorState]
    p_healthy: dict[DQDimension, float]
    alerts_fired: list["AlertEvent"] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "timestamp": self.timestamp.isoformat(),
            "dimensions": {
                dim.value: {
                    "p_healthy": self.p_healthy[dim],
                    "posterior_mean": self.posteriors[dim].mean,
                    "posterior_std": self.posteriors[dim].std,
                }
                for dim in self.p_healthy
            },
            "alerts_fired": len(self.alerts_fired),
        }


@dataclass
class AlertEvent:
    dimension: DQDimension
    batch_id: str
    timestamp: datetime
    p_healthy: float
    threshold: float
    posterior_mean: float
    posterior_std: float
    message: str

    def to_dict(self) -> dict:
        return {
            "dimension": self.dimension.value,
            "batch_id": self.batch_id,
            "timestamp": self.timestamp.isoformat(),
            "p_healthy": round(self.p_healthy, 4),
            "threshold": self.threshold,
            "posterior_mean": round(self.posterior_mean, 4),
            "posterior_std": round(self.posterior_std, 4),
            "message": self.message,
        }

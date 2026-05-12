from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Tier(str, Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    UNKNOWN = "unknown"


class RehydrationPriority(str, Enum):
    STANDARD = "standard"   # 3–5 hours (Glacier standard)
    EXPEDITED = "expedited" # 1–5 minutes (Glacier expedited)
    BULK = "bulk"           # 5–12 hours (Glacier bulk)


@dataclass
class DataRecord:
    key: str
    value: Any
    size_bytes: int = 0
    tier: Tier = Tier.HOT
    created_at: float = field(default_factory=time.time)
    last_accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class AccessEvent:
    key: str
    timestamp: float
    tier: Tier
    latency_ms: float = 0.0


@dataclass
class TierMetrics:
    tier: Tier
    record_count: int
    total_size_bytes: int
    avg_access_frequency: float  # accesses per day
    oldest_record_age_days: float
    newest_record_age_days: float


@dataclass
class CostBreakdown:
    """Monthly cost estimate for each tier."""
    hot_redis_usd: float
    hot_postgres_usd: float
    warm_s3_usd: float
    cold_archive_usd: float
    rehydration_usd: float
    total_usd: float
    details: dict = field(default_factory=dict)

    def summary(self) -> str:
        lines = [
            f"  Hot  (Redis):     ${self.hot_redis_usd:>8.2f}",
            f"  Hot  (Postgres):  ${self.hot_postgres_usd:>8.2f}",
            f"  Warm (S3 Parquet):${self.warm_s3_usd:>8.2f}",
            f"  Cold (Archive):   ${self.cold_archive_usd:>8.2f}",
            f"  Rehydration:      ${self.rehydration_usd:>8.2f}",
            f"  {'─'*26}",
            f"  TOTAL/month:      ${self.total_usd:>8.2f}",
        ]
        return "\n".join(lines)


@dataclass
class RehydrationJob:
    job_id: str
    key: str
    priority: RehydrationPriority
    requested_at: float
    sla_deadline: float          # epoch seconds by which data must be ready
    completed_at: Optional[float] = None
    target_tier: Tier = Tier.WARM

    @property
    def sla_met(self) -> bool:
        if self.completed_at is None:
            return False
        return self.completed_at <= self.sla_deadline

    @property
    def eta_seconds(self) -> float:
        return max(0.0, self.sla_deadline - time.time())


@dataclass
class LifecyclePolicy:
    """Rules governing when data moves between tiers."""
    hot_to_warm_idle_days: float = 7.0
    warm_to_cold_idle_days: float = 30.0
    hot_max_size_gb: float = 10.0
    warm_max_size_gb: float = 500.0
    # Minimum daily access frequency to stay on hot tier
    hot_min_access_freq: float = 1.0
    # Minimum daily access frequency to stay on warm tier
    warm_min_access_freq: float = 0.01
    rehydration_default_priority: RehydrationPriority = RehydrationPriority.STANDARD


# SLA windows in seconds per priority level
REHYDRATION_SLA_SECONDS: dict[RehydrationPriority, float] = {
    RehydrationPriority.EXPEDITED: 5 * 60,        # 5 minutes
    RehydrationPriority.STANDARD: 5 * 3600,       # 5 hours
    RehydrationPriority.BULK: 12 * 3600,          # 12 hours
}

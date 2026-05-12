"""
Data quality metrics for a single pipeline worker, backed by CRDTs.

Each worker tracks:
  - null_count        : GCounter   — total nulls seen
  - valid_count       : GCounter   — total valid (non-null) values
  - anomaly_count     : PNCounter  — anomalies detected/resolved
  - anomaly_types     : ORSet      — set of distinct anomaly type strings seen
  - distinct_values   : HyperLogLogCRDT — approx count of distinct values
  - value_histogram   : Dict[str, GCounter] — per-bucket value distribution
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .crdts import GCounter, PNCounter, ORSet, HyperLogLogCRDT


HISTOGRAM_BUCKETS = ["[0,10)", "[10,100)", "[100,1k)", "[1k,10k)", "[10k,+)"]


def _bucket(value: float) -> str:
    if value < 10:
        return "[0,10)"
    if value < 100:
        return "[10,100)"
    if value < 1_000:
        return "[100,1k)"
    if value < 10_000:
        return "[1k,10k)"
    return "[10k,+)"


@dataclass
class WorkerMetrics:
    node_id: str
    column: str

    null_count: GCounter = field(init=False)
    valid_count: GCounter = field(init=False)
    anomaly_count: PNCounter = field(init=False)
    anomaly_types: ORSet = field(init=False)
    distinct_values: HyperLogLogCRDT = field(init=False)
    value_histogram: Dict[str, GCounter] = field(init=False)

    def __post_init__(self):
        self.null_count = GCounter(node_id=self.node_id)
        self.valid_count = GCounter(node_id=self.node_id)
        self.anomaly_count = PNCounter(node_id=self.node_id)
        self.anomaly_types = ORSet(node_id=self.node_id)
        self.distinct_values = HyperLogLogCRDT(node_id=self.node_id, precision=10)
        self.value_histogram = {
            bucket: GCounter(node_id=self.node_id) for bucket in HISTOGRAM_BUCKETS
        }

    def observe(self, value: Optional[Any]) -> None:
        if value is None:
            self.null_count.increment()
        else:
            self.valid_count.increment()
            self.distinct_values.add(value)
            try:
                numeric = float(value)
                self.value_histogram[_bucket(numeric)].increment()
            except (TypeError, ValueError):
                pass

    def flag_anomaly(self, anomaly_type: str) -> None:
        self.anomaly_count.increment()
        self.anomaly_types.add(anomaly_type)

    def resolve_anomaly(self, anomaly_type: str) -> None:
        self.anomaly_count.decrement()
        self.anomaly_types.remove(anomaly_type)

    def merge(self, other: "WorkerMetrics") -> "WorkerMetrics":
        result = WorkerMetrics(node_id=self.node_id, column=self.column)
        result.null_count = self.null_count.merge(other.null_count)
        result.valid_count = self.valid_count.merge(other.valid_count)
        result.anomaly_count = self.anomaly_count.merge(other.anomaly_count)
        result.anomaly_types = self.anomaly_types.merge(other.anomaly_types)
        result.distinct_values = self.distinct_values.merge(other.distinct_values)
        result.value_histogram = {
            b: self.value_histogram[b].merge(other.value_histogram[b])
            for b in HISTOGRAM_BUCKETS
        }
        return result

    def merge_into(self, other: "WorkerMetrics") -> None:
        self.null_count.merge_into(other.null_count)
        self.valid_count.merge_into(other.valid_count)
        self.anomaly_count.merge_into(other.anomaly_count)
        self.anomaly_types.merge_into(other.anomaly_types)
        self.distinct_values.merge_into(other.distinct_values)
        for b in HISTOGRAM_BUCKETS:
            self.value_histogram[b].merge_into(other.value_histogram[b])

    def summary(self) -> dict:
        total = self.null_count.value() + self.valid_count.value()
        null_rate = self.null_count.value() / total if total else 0.0
        return {
            "column": self.column,
            "node_id": self.node_id,
            "total_observed": total,
            "null_count": self.null_count.value(),
            "valid_count": self.valid_count.value(),
            "null_rate": round(null_rate, 4),
            "anomaly_count": self.anomaly_count.value(),
            "anomaly_types": sorted(self.anomaly_types.elements()),
            "distinct_values_approx": self.distinct_values.count(),
            "distinct_values_error": f"±{self.distinct_values.error_rate():.2%}",
            "value_histogram": {b: self.value_histogram[b].value() for b in HISTOGRAM_BUCKETS},
        }

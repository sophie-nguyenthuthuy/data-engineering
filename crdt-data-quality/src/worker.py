"""
Pipeline worker: processes a data partition and accumulates quality metrics.
Workers are completely independent — no shared state, no coordinator.
"""
from __future__ import annotations
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .metrics import WorkerMetrics


ANOMALY_TYPES = [
    "out_of_range",
    "type_mismatch",
    "duplicate_key",
    "referential_integrity",
    "schema_drift",
]


@dataclass
class PipelineWorker:
    worker_id: str
    column: str
    seed: Optional[int] = None

    metrics: WorkerMetrics = field(init=False)
    rows_processed: int = field(init=False, default=0)
    processing_time_ms: float = field(init=False, default=0.0)

    def __post_init__(self):
        self.metrics = WorkerMetrics(node_id=self.worker_id, column=self.column)

    def process_partition(self, rows: List[Optional[float]]) -> None:
        rng = random.Random(self.seed)
        t0 = time.perf_counter()

        for value in rows:
            self.metrics.observe(value)
            self.rows_processed += 1

            # ~3% chance of anomaly
            if rng.random() < 0.03:
                atype = rng.choice(ANOMALY_TYPES)
                self.metrics.flag_anomaly(atype)

            # ~1% chance of resolving an existing anomaly
            if rng.random() < 0.01:
                live = list(self.metrics.anomaly_types.elements())
                if live:
                    self.metrics.resolve_anomaly(rng.choice(live))

        self.processing_time_ms = (time.perf_counter() - t0) * 1000

    def get_state(self) -> WorkerMetrics:
        return self.metrics

    def __repr__(self) -> str:
        return f"Worker({self.worker_id}, rows={self.rows_processed}, {self.metrics.summary()})"

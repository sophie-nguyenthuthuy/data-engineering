from __future__ import annotations
from collections import defaultdict, deque
from datetime import datetime, timedelta
from threading import Lock
from typing import Deque

from prometheus_client import Counter, Gauge, Histogram

from ..models import ValidationResult, ValidationStatus

# ---------------------------------------------------------------------------
# Prometheus metrics (process-wide singletons)
# ---------------------------------------------------------------------------

BATCHES_TOTAL = Counter(
    "dq_batches_total",
    "Total number of micro-batches processed",
    ["table", "status"],
)
PASS_RATE_GAUGE = Gauge(
    "dq_pass_rate",
    "Latest pass rate for a table",
    ["table"],
)
CHECKS_TOTAL = Counter(
    "dq_checks_total",
    "Total individual checks run",
    ["table", "status"],
)
VALIDATION_DURATION = Histogram(
    "dq_validation_duration_ms",
    "Validation latency in milliseconds",
    ["table", "backend"],
    buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000],
)
ACTIVE_BLOCKS = Gauge(
    "dq_active_blocks",
    "Number of downstream jobs currently blocked",
)
ROWS_PROCESSED = Counter(
    "dq_rows_processed_total",
    "Total rows validated",
    ["table"],
)


# ---------------------------------------------------------------------------
# In-process rolling window
# ---------------------------------------------------------------------------

_WINDOW = timedelta(hours=1)


class MetricsCollector:
    """
    Tracks validation results in a rolling 1-hour window for dashboard
    snapshots and also updates Prometheus gauges / counters.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        # table → deque of (timestamp, ValidationResult)
        self._window: dict[str, Deque[tuple[datetime, ValidationResult]]] = defaultdict(
            deque
        )

    def record(self, result: ValidationResult) -> None:
        now = datetime.utcnow()
        with self._lock:
            dq = self._window[result.table_name]
            dq.append((now, result))
            self._evict(dq)

        # Prometheus
        BATCHES_TOTAL.labels(
            table=result.table_name, status=result.status.value
        ).inc()
        PASS_RATE_GAUGE.labels(table=result.table_name).set(result.pass_rate)
        CHECKS_TOTAL.labels(table=result.table_name, status="passed").inc(
            result.passed_checks
        )
        CHECKS_TOTAL.labels(table=result.table_name, status="failed").inc(
            result.failed_checks
        )
        VALIDATION_DURATION.labels(
            table=result.table_name, backend=result.backend.value
        ).observe(result.duration_ms)
        ROWS_PROCESSED.labels(table=result.table_name).inc(result.row_count)

    def update_active_blocks(self, count: int) -> None:
        ACTIVE_BLOCKS.set(count)

    def summary(self) -> dict:
        """Return aggregated stats for the last hour across all tables."""
        with self._lock:
            all_results: list[ValidationResult] = []
            per_table: dict[str, list[ValidationResult]] = {}

            for table, dq in self._window.items():
                self._evict(dq)
                results = [r for _, r in dq]
                per_table[table] = results
                all_results.extend(results)

        total = len(all_results)
        if total == 0:
            return {"total": 0, "failed": 0, "overall_pass_rate": 1.0, "per_table": {}}

        failed = sum(
            1 for r in all_results if r.status == ValidationStatus.FAILED
        )
        overall_pass_rate = 1.0 - (failed / total)

        table_stats = {}
        for table, results in per_table.items():
            t_total = len(results)
            t_failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
            table_stats[table] = {
                "total": t_total,
                "failed": t_failed,
                "avg_pass_rate": sum(r.pass_rate for r in results) / t_total,
                "avg_duration_ms": sum(r.duration_ms for r in results) / t_total,
                "avg_row_count": sum(r.row_count for r in results) / t_total,
            }

        return {
            "total": total,
            "failed": failed,
            "overall_pass_rate": overall_pass_rate,
            "per_table": table_stats,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _evict(dq: Deque[tuple[datetime, ValidationResult]]) -> None:
        cutoff = datetime.utcnow() - _WINDOW
        while dq and dq[0][0] < cutoff:
            dq.popleft()

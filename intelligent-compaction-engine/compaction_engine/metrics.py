"""
Performance metrics: before/after query timing, file stats, and Prometheus export.

PerformanceMetrics runs a configurable set of benchmark queries against a table
before and after compaction and records the improvement.  Results are persisted
in SQLite and optionally exported to Prometheus.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Callable

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Gauge, Counter, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


@dataclass
class BenchmarkQuery:
    name: str
    sql: str
    description: str = ""


@dataclass
class QueryTiming:
    query_name: str
    elapsed_seconds: float
    rows_returned: int
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


@dataclass
class BenchmarkRun:
    table_name: str
    phase: str  # "before" | "after"
    timings: list[QueryTiming]
    file_count: int
    avg_file_size_mb: float
    total_size_gb: float
    run_id: str = ""
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        if not self.run_id:
            self.run_id = f"{self.table_name}_{self.phase}_{int(time.time())}"

    @property
    def avg_query_time(self) -> float:
        if not self.timings:
            return 0.0
        return sum(t.elapsed_seconds for t in self.timings) / len(self.timings)


@dataclass
class CompactionImpact:
    table_name: str
    before: BenchmarkRun
    after: BenchmarkRun

    @property
    def query_speedup_pct(self) -> float:
        if self.before.avg_query_time == 0:
            return 0.0
        return (
            (self.before.avg_query_time - self.after.avg_query_time)
            / self.before.avg_query_time
            * 100
        )

    @property
    def file_reduction_pct(self) -> float:
        if self.before.file_count == 0:
            return 0.0
        return (self.before.file_count - self.after.file_count) / self.before.file_count * 100

    def per_query_speedup(self) -> dict[str, float]:
        before_map = {t.query_name: t.elapsed_seconds for t in self.before.timings}
        after_map = {t.query_name: t.elapsed_seconds for t in self.after.timings}
        result = {}
        for name in before_map:
            b = before_map[name]
            a = after_map.get(name, b)
            result[name] = round(((b - a) / b * 100) if b > 0 else 0.0, 2)
        return result

    def report(self) -> str:
        lines = [
            f"\n{'='*60}",
            f"  COMPACTION IMPACT REPORT: {self.table_name}",
            f"{'='*60}",
            f"  Query latency:  {self.before.avg_query_time:.3f}s → {self.after.avg_query_time:.3f}s  ({self.query_speedup_pct:+.1f}%)",
            f"  File count:     {self.before.file_count} → {self.after.file_count}  ({self.file_reduction_pct:+.1f}%)",
            f"  Avg file size:  {self.before.avg_file_size_mb:.1f} MB → {self.after.avg_file_size_mb:.1f} MB",
            f"  Data size:      {self.before.total_size_gb:.2f} GB → {self.after.total_size_gb:.2f} GB",
            f"\n  Per-query speedup:",
        ]
        for name, pct in self.per_query_speedup().items():
            lines.append(f"    {name:<40} {pct:+.1f}%")
        lines.append(f"{'='*60}\n")
        return "\n".join(lines)


class PerformanceMetrics:
    """
    Runs benchmark queries before and after compaction to measure impact.

    Usage
    -----
    metrics = PerformanceMetrics(spark, db_path="metrics.db")
    before = metrics.run_benchmark(table_name, queries, phase="before", health=health)
    # ... run compaction ...
    after  = metrics.run_benchmark(table_name, queries, phase="after", health=health)
    impact = metrics.compare(before, after)
    print(impact.report())
    """

    def __init__(
        self,
        spark,
        db_path: str = "compaction_metrics.db",
        benchmark_runs: int = 3,
        prometheus_port: Optional[int] = None,
    ):
        self.spark = spark
        self.db_path = db_path
        self.benchmark_runs = benchmark_runs
        self._init_db()
        if prometheus_port and PROMETHEUS_AVAILABLE:
            self._init_prometheus(prometheus_port)

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS benchmark_runs (
                    run_id TEXT PRIMARY KEY,
                    table_name TEXT,
                    phase TEXT,
                    avg_query_time REAL,
                    file_count INTEGER,
                    avg_file_size_mb REAL,
                    total_size_gb REAL,
                    timings_json TEXT,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS compaction_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT,
                    before_run_id TEXT,
                    after_run_id TEXT,
                    query_speedup_pct REAL,
                    file_reduction_pct REAL,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def _init_prometheus(self, port: int) -> None:
        self._prom_query_time = Gauge(
            "compaction_query_latency_seconds",
            "Average benchmark query latency",
            ["table", "phase"],
        )
        self._prom_speedup = Gauge(
            "compaction_query_speedup_pct",
            "Query speedup percentage after compaction",
            ["table"],
        )
        self._prom_file_reduction = Gauge(
            "compaction_file_reduction_pct",
            "File count reduction percentage after compaction",
            ["table"],
        )
        try:
            start_http_server(port)
            logger.info("Prometheus metrics served on port %d", port)
        except Exception as e:
            logger.warning("Could not start Prometheus server: %s", e)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_benchmark(
        self,
        table_name: str,
        queries: list[BenchmarkQuery],
        phase: str,
        file_count: int = 0,
        avg_file_size_mb: float = 0.0,
        total_size_gb: float = 0.0,
    ) -> BenchmarkRun:
        """Run each query *benchmark_runs* times and record median latency."""
        timings: list[QueryTiming] = []

        for bq in queries:
            run_times = []
            last_rows = 0
            for _ in range(self.benchmark_runs):
                # Clear Spark cache between runs for fair comparison
                self.spark.catalog.clearCache()
                t0 = time.perf_counter()
                try:
                    df = self.spark.sql(bq.sql)
                    last_rows = df.count()
                except Exception as e:
                    logger.error("Benchmark query '%s' failed: %s", bq.name, e)
                    last_rows = -1
                elapsed = time.perf_counter() - t0
                run_times.append(elapsed)

            median_time = sorted(run_times)[len(run_times) // 2]
            timings.append(QueryTiming(
                query_name=bq.name,
                elapsed_seconds=round(median_time, 4),
                rows_returned=last_rows,
            ))
            logger.info("[%s][%s] %s → %.3fs", table_name, phase, bq.name, median_time)

        run = BenchmarkRun(
            table_name=table_name,
            phase=phase,
            timings=timings,
            file_count=file_count,
            avg_file_size_mb=avg_file_size_mb,
            total_size_gb=total_size_gb,
        )
        self._persist_run(run)
        return run

    def compare(self, before: BenchmarkRun, after: BenchmarkRun) -> CompactionImpact:
        impact = CompactionImpact(
            table_name=before.table_name,
            before=before,
            after=after,
        )
        self._persist_impact(impact)

        if PROMETHEUS_AVAILABLE and hasattr(self, "_prom_speedup"):
            self._prom_query_time.labels(before.table_name, "before").set(before.avg_query_time)
            self._prom_query_time.labels(after.table_name, "after").set(after.avg_query_time)
            self._prom_speedup.labels(before.table_name).set(impact.query_speedup_pct)
            self._prom_file_reduction.labels(before.table_name).set(impact.file_reduction_pct)

        return impact

    def history(self, table_name: str, limit: int = 20) -> list[dict]:
        """Return recent compaction events for a table."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT table_name, query_speedup_pct, file_reduction_pct, recorded_at
                FROM compaction_events
                WHERE table_name = ?
                ORDER BY recorded_at DESC
                LIMIT ?
                """,
                (table_name, limit),
            ).fetchall()
        return [
            {"table": r[0], "query_speedup_pct": r[1], "file_reduction_pct": r[2], "at": r[3]}
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist_run(self, run: BenchmarkRun) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO benchmark_runs
                (run_id, table_name, phase, avg_query_time, file_count, avg_file_size_mb, total_size_gb, timings_json)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    run.run_id, run.table_name, run.phase, run.avg_query_time,
                    run.file_count, run.avg_file_size_mb, run.total_size_gb,
                    json.dumps([asdict(t) for t in run.timings]),
                ),
            )
            conn.commit()

    def _persist_impact(self, impact: CompactionImpact) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO compaction_events
                (table_name, before_run_id, after_run_id, query_speedup_pct, file_reduction_pct)
                VALUES (?,?,?,?,?)
                """,
                (
                    impact.table_name,
                    impact.before.run_id,
                    impact.after.run_id,
                    round(impact.query_speedup_pct, 2),
                    round(impact.file_reduction_pct, 2),
                ),
            )
            conn.commit()

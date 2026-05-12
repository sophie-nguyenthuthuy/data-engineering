"""JOB (Join Order Benchmark) runner.

The JOB benchmark uses the IMDB dataset with 113 multi-join queries designed
to stress the cardinality estimator (Leis et al., VLDB 2015).

This module:
  - Loads JOB query files from experiments/job_queries/
  - Runs them under the Bao selector and baseline (PostgreSQL default)
  - Reports per-query and aggregate statistics
"""
from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from typing import Optional

from ..db.connector import ConnectionPool, DBConfig
from ..db.interceptor import QueryInterceptor
from .metrics import workload_q_error_stats, latency_speedup_stats, print_metric_table

logger = logging.getLogger(__name__)

JOB_QUERIES_DIR = Path(__file__).parent.parent.parent.parent / "experiments" / "job_queries"


def load_job_queries(directory: Path = JOB_QUERIES_DIR) -> list[tuple[str, str]]:
    """Return list of (query_name, sql) sorted by name."""
    queries = []
    for p in sorted(directory.glob("*.sql")):
        sql = p.read_text().strip()
        if sql:
            queries.append((p.stem, sql))
    if not queries:
        logger.warning("No .sql files found in %s", directory)
    return queries


class BaselineBenchmark:
    """Run queries with default PostgreSQL planner — no hints, no model."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool
        self.interceptor = QueryInterceptor(pool)

    def run(
        self,
        queries: list[tuple[str, str]],
        timeout_ms: int = 120_000,
    ) -> list[dict]:
        results = []
        for name, sql in queries:
            logger.info("Baseline: %s", name)
            try:
                plan, latency = self.interceptor.explain_analyze(sql, timeout_ms)
                results.append({
                    "name": name,
                    "latency_ms": latency,
                    "plan": plan,
                    "error": None,
                })
            except Exception as e:
                logger.warning("Baseline failed %s: %s", name, e)
                results.append({"name": name, "latency_ms": None, "plan": None, "error": str(e)})
        return results


def run_comparison_benchmark(
    pool: ConnectionPool,
    bao_selector,
    queries: Optional[list[tuple[str, str]]] = None,
    timeout_ms: int = 120_000,
    results_path: Optional[Path] = None,
) -> dict:
    """Compare Bao vs baseline on JOB workload."""
    if queries is None:
        queries = load_job_queries()
    if not queries:
        raise ValueError("No queries to benchmark")

    logger.info("Running comparison benchmark on %d queries", len(queries))

    baseline = BaselineBenchmark(pool)
    baseline_results = baseline.run(queries, timeout_ms)

    bao_results = []
    for name, sql in queries:
        logger.info("Bao: %s", name)
        try:
            r = bao_selector.run_query(sql)
            bao_results.append({
                "name": name,
                "latency_ms": r.latency_ms,
                "arm": r.chosen_arm,
                "adaptive_speedup": r.adaptive_speedup,
                "error": None,
            })
        except Exception as e:
            logger.warning("Bao failed %s: %s", name, e)
            bao_results.append({"name": name, "latency_ms": None, "error": str(e)})

    # Compute statistics for matching queries
    baseline_lat = []
    bao_lat = []
    base_plans = []
    for b, o in zip(baseline_results, bao_results):
        if b["latency_ms"] is not None and o["latency_ms"] is not None:
            baseline_lat.append(b["latency_ms"])
            bao_lat.append(o["latency_ms"])
        if b.get("plan") is not None:
            base_plans.append(b["plan"])

    speedup_stats = latency_speedup_stats(baseline_lat, bao_lat) if baseline_lat else {}
    q_error_stats = workload_q_error_stats(base_plans) if base_plans else {}

    report = {
        "n_queries": len(queries),
        "n_completed": len(baseline_lat),
        "speedup_stats": speedup_stats,
        "q_error_stats": q_error_stats,
        "per_query": [
            {
                "name": b["name"],
                "baseline_ms": b["latency_ms"],
                "bao_ms": o["latency_ms"],
                "speedup": (b["latency_ms"] / max(o["latency_ms"], 0.001))
                           if b["latency_ms"] and o["latency_ms"] else None,
                "arm": o.get("arm"),
                "adaptive_speedup": o.get("adaptive_speedup", 1.0),
            }
            for b, o in zip(baseline_results, bao_results)
        ],
    }

    print_metric_table(speedup_stats, "Latency Speedup (Bao vs. Default)")
    print_metric_table(q_error_stats, "Baseline Cardinality Q-Error")

    if results_path:
        results_path.parent.mkdir(parents=True, exist_ok=True)
        with open(results_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("Results saved to %s", results_path)

    return report

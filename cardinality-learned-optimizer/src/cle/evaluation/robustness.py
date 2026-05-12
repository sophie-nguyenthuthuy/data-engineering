"""Plan robustness evaluation — reproduces the key experiment from the Bao paper.

The Bao paper (Marcus et al., VLDB 2022) evaluates "plan robustness" as:
  - For each query, compare the distribution of execution times across all
    15 hint sets (plans).
  - The "robust" optimizer should rarely pick a bad plan, whereas the
    default PostgreSQL planner sometimes picks catastrophically slow plans.

We reproduce Figure 9 / Table 2 from the paper:
  - Tail latency at p50/p90/p99
  - Fraction of queries where Bao's chosen plan is within 2× of optimal
  - "Regret" = chosen_latency / optimal_latency (1.0 = optimal)
"""
from __future__ import annotations
import json
import logging
import math
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ..db.connector import ConnectionPool
from ..db.hint_injector import BAO_HINT_SETS, hint_set_to_pg_hints
from ..db.interceptor import QueryInterceptor
from .metrics import print_metric_table

logger = logging.getLogger(__name__)


@dataclass
class PlanProfile:
    """All 15 plans profiled for a single query."""
    query_name: str
    sql: str
    latencies: dict[int, float]   # arm → latency_ms (None if timed out)
    optimal_arm: Optional[int] = None
    optimal_latency: Optional[float] = None

    def __post_init__(self) -> None:
        valid = {arm: ms for arm, ms in self.latencies.items() if ms is not None}
        if valid:
            self.optimal_arm = min(valid, key=valid.get)
            self.optimal_latency = valid[self.optimal_arm]

    def regret(self, chosen_arm: int) -> Optional[float]:
        if self.optimal_latency is None:
            return None
        chosen = self.latencies.get(chosen_arm)
        if chosen is None:
            return None
        return chosen / max(self.optimal_latency, 0.001)


class PlanProfiler:
    """Profile all 15 hint sets for each query — expensive but one-time cost."""

    def __init__(self, pool: ConnectionPool, timeout_ms: int = 60_000) -> None:
        self.pool = pool
        self.interceptor = QueryInterceptor(pool)
        self.timeout_ms = timeout_ms

    def profile_query(self, name: str, sql: str) -> PlanProfile:
        latencies: dict[int, float] = {}
        for arm, hint_set in enumerate(BAO_HINT_SETS):
            hints = hint_set_to_pg_hints(hint_set)
            full_sql = f"/*+ {hints} */\n{sql}" if hints else sql
            try:
                _, lat = self.interceptor.explain_analyze(full_sql, self.timeout_ms)
                latencies[arm] = lat
                logger.debug("  arm=%d %.1fms", arm, lat)
            except Exception as e:
                logger.warning("  arm=%d failed: %s", arm, e)
                latencies[arm] = None
        return PlanProfile(query_name=name, sql=sql, latencies=latencies)

    def profile_workload(
        self,
        queries: list[tuple[str, str]],
        cache_path: Optional[Path] = None,
    ) -> list[PlanProfile]:
        # Load from cache if available
        if cache_path and cache_path.exists():
            logger.info("Loading profile cache from %s", cache_path)
            return _load_profiles(cache_path)

        profiles = []
        for i, (name, sql) in enumerate(queries):
            logger.info("Profiling %d/%d: %s", i + 1, len(queries), name)
            p = self.profile_query(name, sql)
            profiles.append(p)

        if cache_path:
            _save_profiles(profiles, cache_path)

        return profiles


def robustness_report(
    profiles: list[PlanProfile],
    chosen_arms: dict[str, int],    # query_name → chosen arm
    baseline_arm: int = 0,
) -> dict:
    """Compute robustness statistics comparing Bao choices vs baseline.

    chosen_arms: map from query name to the arm Bao selected.
    baseline_arm: the arm PostgreSQL default corresponds to (typically 0).
    """
    bao_regrets = []
    baseline_regrets = []
    bao_latencies = []
    baseline_latencies = []
    opt_latencies = []

    for p in profiles:
        if p.optimal_latency is None:
            continue
        arm = chosen_arms.get(p.query_name, baseline_arm)
        bao_lat = p.latencies.get(arm)
        base_lat = p.latencies.get(baseline_arm)

        if bao_lat is not None:
            bao_regrets.append(bao_lat / p.optimal_latency)
            bao_latencies.append(bao_lat)
            opt_latencies.append(p.optimal_latency)
        if base_lat is not None:
            baseline_regrets.append(base_lat / p.optimal_latency)
            baseline_latencies.append(base_lat)

    def summary(regrets: list[float], label: str) -> dict:
        if not regrets:
            return {}
        regrets = sorted(regrets)
        n = len(regrets)
        return {
            f"{label}_mean_regret": statistics.mean(regrets),
            f"{label}_median_regret": regrets[n // 2],
            f"{label}_p90_regret": regrets[int(0.9 * n)],
            f"{label}_p99_regret": regrets[int(0.99 * n)],
            f"{label}_max_regret": regrets[-1],
            f"{label}_fraction_near_optimal_2x": sum(1 for r in regrets if r <= 2.0) / n,
            f"{label}_fraction_near_optimal_1_1x": sum(1 for r in regrets if r <= 1.1) / n,
        }

    report = {}
    report.update(summary(bao_regrets, "bao"))
    report.update(summary(baseline_regrets, "baseline"))
    if bao_latencies and baseline_latencies:
        report["total_bao_s"] = sum(bao_latencies) / 1000
        report["total_baseline_s"] = sum(baseline_latencies) / 1000
        report["total_optimal_s"] = sum(opt_latencies) / 1000
        report["overall_speedup_vs_baseline"] = (
            sum(baseline_latencies) / max(sum(bao_latencies), 0.001)
        )

    print_metric_table(report, "Plan Robustness: Bao vs. PostgreSQL Default")
    return report


# ── serialization helpers ─────────────────────────────────────────────────────

def _save_profiles(profiles: list[PlanProfile], path: Path) -> None:
    data = [
        {
            "query_name": p.query_name,
            "sql": p.sql,
            "latencies": {str(k): v for k, v in p.latencies.items()},
        }
        for p in profiles
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    logger.info("Saved profiles → %s", path)


def _load_profiles(path: Path) -> list[PlanProfile]:
    data = json.loads(path.read_text())
    return [
        PlanProfile(
            query_name=d["query_name"],
            sql=d["sql"],
            latencies={int(k): v for k, v in d["latencies"].items()},
        )
        for d in data
    ]

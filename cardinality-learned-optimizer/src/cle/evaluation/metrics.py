"""Cardinality estimation quality metrics.

Key metrics:
  q-error     = max(est/act, act/est)   — multiplicative error; 1.0 = perfect
  p-error     = abs(log2(est/act))      — log-scale absolute error
  plan quality= relative latency vs optimal (PostgreSQL default)
"""
from __future__ import annotations
import math
import statistics
from typing import Optional

from ..plan.node import PlanNode


def q_error(estimated: float, actual: float) -> float:
    est = max(estimated, 1.0)
    act = max(actual, 1.0)
    return max(est / act, act / est)


def p_error(estimated: float, actual: float) -> float:
    return abs(math.log2(max(estimated, 1.0)) - math.log2(max(actual, 1.0)))


def plan_node_q_errors(root: PlanNode) -> list[float]:
    return [
        node.q_error
        for node in root.all_nodes()
        if node.q_error is not None
    ]


def tree_q_error_stats(root: PlanNode) -> dict[str, float]:
    errs = plan_node_q_errors(root)
    if not errs:
        return {}
    errs.sort()
    n = len(errs)
    return {
        "mean": statistics.mean(errs),
        "median": errs[n // 2],
        "p90": errs[int(0.9 * n)],
        "p95": errs[int(0.95 * n)],
        "max": errs[-1],
        "n": n,
        "fraction_gt_2x": sum(1 for e in errs if e > 2) / n,
        "fraction_gt_10x": sum(1 for e in errs if e > 10) / n,
        "fraction_gt_100x": sum(1 for e in errs if e > 100) / n,
    }


def workload_q_error_stats(roots: list[PlanNode]) -> dict[str, float]:
    all_errs: list[float] = []
    for root in roots:
        all_errs.extend(plan_node_q_errors(root))
    if not all_errs:
        return {}
    all_errs.sort()
    n = len(all_errs)
    return {
        "mean": statistics.mean(all_errs),
        "median": all_errs[n // 2],
        "p90": all_errs[int(0.9 * n)],
        "p95": all_errs[int(0.95 * n)],
        "p99": all_errs[int(0.99 * n)],
        "max": all_errs[-1],
        "n_nodes": n,
        "n_queries": len(roots),
        "fraction_gt_2x": sum(1 for e in all_errs if e > 2) / n,
        "fraction_gt_10x": sum(1 for e in all_errs if e > 10) / n,
        "fraction_gt_100x": sum(1 for e in all_errs if e > 100) / n,
    }


def latency_speedup_stats(
    baseline_latencies: list[float],
    optimized_latencies: list[float],
) -> dict[str, float]:
    """Compute speedup statistics between two latency lists (ms)."""
    assert len(baseline_latencies) == len(optimized_latencies)
    speedups = [b / max(o, 0.001) for b, o in zip(baseline_latencies, optimized_latencies)]
    speedups.sort()
    n = len(speedups)
    return {
        "geometric_mean_speedup": math.exp(statistics.mean(math.log(max(s, 0.001)) for s in speedups)),
        "median_speedup": speedups[n // 2],
        "p10_speedup": speedups[int(0.1 * n)],   # worst 10%
        "p90_speedup": speedups[int(0.9 * n)],
        "fraction_faster": sum(1 for s in speedups if s > 1.0) / n,
        "fraction_2x_faster": sum(1 for s in speedups if s > 2.0) / n,
        "total_baseline_s": sum(baseline_latencies) / 1000,
        "total_optimized_s": sum(optimized_latencies) / 1000,
    }


def print_metric_table(stats: dict[str, float], title: str = "") -> None:
    if title:
        print(f"\n{'─' * 50}")
        print(f"  {title}")
        print(f"{'─' * 50}")
    width = max(len(k) for k in stats)
    for k, v in stats.items():
        if isinstance(v, float) and v == int(v):
            print(f"  {k:<{width}} : {int(v)}")
        elif isinstance(v, float):
            print(f"  {k:<{width}} : {v:.4f}")
        else:
            print(f"  {k:<{width}} : {v}")

"""
Benchmark runner: compares RMI, B-tree, and Bloom filter on multiple workloads.

Run with::

    python -m benchmarks.runner

Results are printed to stdout as a formatted table and optionally saved to
``benchmark_results/`` as JSON.
"""

from __future__ import annotations

import bisect
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

import numpy as np
from tabulate import tabulate

# Allow running as script from repo root
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from lsm_learned.drift.detector import ADWINDetector, KSWindowDetector
from lsm_learned.indexes.bloom import BloomFilter
from lsm_learned.indexes.btree import BTreeIndex
from lsm_learned.indexes.rmi import RMI

from .workload import ALL_WORKLOADS, Workload, mixed_drift, uniform, zipfian, time_series


@dataclass
class BenchResult:
    workload: str
    index: str
    build_ms: float
    mean_lookup_ns: float
    p99_lookup_ns: float
    memory_kb: float
    fpr: float              # false-positive rate (bloom only, else 0)
    rmi_mean_range: float   # mean search range (rmi only, else 0)
    num_queries: int


def _time_ns() -> int:
    return time.perf_counter_ns()


def bench_rmi(workload: Workload, num_stage2: int = 200) -> BenchResult:
    t0 = time.perf_counter()
    rmi = RMI(num_stage2=num_stage2)
    rmi.train(workload.keys.astype(np.float64))
    build_ms = (time.perf_counter() - t0) * 1000

    latencies = []
    for key in workload.queries:
        t = _time_ns()
        rmi.lookup(float(key))
        latencies.append(_time_ns() - t)

    stats = rmi.stats()
    mem = workload.keys.nbytes + num_stage2 * 40  # rough model footprint

    return BenchResult(
        workload=workload.name,
        index="RMI",
        build_ms=round(build_ms, 2),
        mean_lookup_ns=round(float(np.mean(latencies)), 1),
        p99_lookup_ns=round(float(np.percentile(latencies, 99)), 1),
        memory_kb=round(mem / 1024, 1),
        fpr=0.0,
        rmi_mean_range=round(stats.mean_search_range, 2),
        num_queries=len(workload.queries),
    )


def bench_btree(workload: Workload) -> BenchResult:
    t0 = time.perf_counter()
    bt = BTreeIndex()
    bt.build(workload.keys.tolist())
    build_ms = (time.perf_counter() - t0) * 1000

    latencies = []
    for key in workload.queries:
        t = _time_ns()
        bt.lookup(int(key))
        latencies.append(_time_ns() - t)

    return BenchResult(
        workload=workload.name,
        index="BTree",
        build_ms=round(build_ms, 2),
        mean_lookup_ns=round(float(np.mean(latencies)), 1),
        p99_lookup_ns=round(float(np.percentile(latencies, 99)), 1),
        memory_kb=round(bt.memory_estimate_bytes() / 1024, 1),
        fpr=0.0,
        rmi_mean_range=0.0,
        num_queries=len(workload.queries),
    )


def bench_bloom(workload: Workload, fpr: float = 0.01) -> BenchResult:
    """Bloom filter used as a membership test (no position lookup)."""
    t0 = time.perf_counter()
    bf = BloomFilter(len(workload.keys), fpr=fpr)
    for k in workload.keys:
        bf.add(int(k))
    build_ms = (time.perf_counter() - t0) * 1000

    latencies = []
    false_pos = 0
    key_set = set(workload.keys.tolist())
    for key in workload.queries:
        t = _time_ns()
        result = (key in bf)
        latencies.append(_time_ns() - t)
        if result and int(key) not in key_set:
            false_pos += 1

    measured_fpr = false_pos / len(workload.queries)

    return BenchResult(
        workload=workload.name,
        index="Bloom",
        build_ms=round(build_ms, 2),
        mean_lookup_ns=round(float(np.mean(latencies)), 1),
        p99_lookup_ns=round(float(np.percentile(latencies, 99)), 1),
        memory_kb=round(bf.memory_bytes() / 1024, 1),
        fpr=round(measured_fpr, 5),
        rmi_mean_range=0.0,
        num_queries=len(workload.queries),
    )


def bench_binary_search(workload: Workload) -> BenchResult:
    """Baseline: plain binary search on a sorted numpy array."""
    t0 = time.perf_counter()
    sorted_keys = workload.keys.copy()
    build_ms = (time.perf_counter() - t0) * 1000

    latencies = []
    for key in workload.queries:
        t = _time_ns()
        bisect.bisect_left(sorted_keys, key)
        latencies.append(_time_ns() - t)

    return BenchResult(
        workload=workload.name,
        index="BinarySearch",
        build_ms=round(build_ms, 2),
        mean_lookup_ns=round(float(np.mean(latencies)), 1),
        p99_lookup_ns=round(float(np.percentile(latencies, 99)), 1),
        memory_kb=round(sorted_keys.nbytes / 1024, 1),
        fpr=0.0,
        rmi_mean_range=0.0,
        num_queries=len(workload.queries),
    )


def bench_drift_detection() -> dict:
    """Simulate a distribution shift and measure ADWIN + KS detection latency."""
    rng = np.random.default_rng(0)
    w_before, w_after = mixed_drift(n=200_000, q_per_phase=10_000, rng=rng)

    rmi = RMI(num_stage2=200)
    rmi.train(w_before.keys.astype(np.float64))

    adwin = ADWINDetector(delta=0.002)
    ks = KSWindowDetector(ref_size=300, recent_size=150, alpha=0.01)

    adwin_detected = None
    ks_detected = None

    def measure_error(key: float) -> float:
        lo, hi = rmi.search_range(key)
        return float(hi - lo)

    # Phase 1: uniform queries → feed detectors (reference)
    for i, key in enumerate(w_before.queries):
        err = measure_error(float(key))
        adwin.add(err)
        ks.add(err)

    phase1_queries = len(w_before.queries)

    # Phase 2: post-drift queries
    for i, key in enumerate(w_after.queries):
        err = measure_error(float(key))
        signal_a = adwin.add(err)
        signal_k = ks.add(err)
        if adwin_detected is None and signal_a:
            adwin_detected = phase1_queries + i
        if ks_detected is None and signal_k:
            ks_detected = phase1_queries + i
        if adwin_detected and ks_detected:
            break

    return {
        "phase1_queries": phase1_queries,
        "phase2_queries": len(w_after.queries),
        "adwin_detected_at": adwin_detected,
        "ks_detected_at": ks_detected,
        "adwin_queries_after_drift": (adwin_detected - phase1_queries) if adwin_detected else None,
        "ks_queries_after_drift": (ks_detected - phase1_queries) if ks_detected else None,
    }


def run_all(n: int = 500_000, q: int = 50_000, save: bool = True) -> list[BenchResult]:
    rng = np.random.default_rng(42)
    workloads = [
        uniform(n, q, rng=rng),
        zipfian(n, q, alpha=1.2, rng=rng),
        zipfian(n, q, alpha=1.5, rng=rng),
        time_series(n, q, rng=rng),
    ]

    results: list[BenchResult] = []
    for wl in workloads:
        print(f"  Benchmarking {wl.name} (n={len(wl.keys):,}, q={len(wl.queries):,})...")
        results.append(bench_rmi(wl))
        results.append(bench_btree(wl))
        results.append(bench_bloom(wl))
        results.append(bench_binary_search(wl))

    return results


def print_table(results: list[BenchResult]) -> None:
    headers = [
        "Workload", "Index", "Build(ms)", "Mean(ns)", "P99(ns)",
        "Mem(KB)", "FPR", "RMI Range",
    ]
    rows = [
        [
            r.workload, r.index, r.build_ms, r.mean_lookup_ns, r.p99_lookup_ns,
            r.memory_kb, r.fpr or "-", r.rmi_mean_range or "-",
        ]
        for r in results
    ]
    print(tabulate(rows, headers=headers, tablefmt="github", floatfmt=".1f"))


def main() -> None:
    print("=" * 70)
    print("Learned Index Structures — Benchmark Suite")
    print("=" * 70)

    print("\n[1/2] Index lookup benchmarks\n")
    results = run_all()
    print_table(results)

    out_dir = Path("benchmark_results")
    out_dir.mkdir(exist_ok=True)
    with open(out_dir / "results.json", "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2)
    print(f"\nResults saved → {out_dir / 'results.json'}")

    print("\n[2/2] Drift detection benchmark\n")
    drift = bench_drift_detection()
    for k, v in drift.items():
        print(f"  {k}: {v}")

    with open(out_dir / "drift.json", "w") as f:
        json.dump(drift, f, indent=2)
    print(f"\nDrift results saved → {out_dir / 'drift.json'}")


if __name__ == "__main__":
    main()

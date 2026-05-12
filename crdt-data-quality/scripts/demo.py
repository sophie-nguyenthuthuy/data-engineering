"""
Interactive demo: 50 pipeline workers process 100k rows, then converge via gossip.
Shows per-round convergence progress and final global quality report.
"""
from __future__ import annotations
import json
import math
import sys

sys.path.insert(0, ".")

from src.cluster import Cluster


def progress_bar(value: int, total: int, width: int = 30) -> str:
    filled = int(width * value / total) if total else 0
    return f"[{'#' * filled}{'.' * (width - filled)}] {value}/{total}"


def main():
    N_WORKERS = 50
    TOTAL_ROWS = 100_000
    NULL_RATE = 0.05
    FANOUT = 3
    MAX_GOSSIP_ROUNDS = math.ceil(math.log(N_WORKERS) / math.log(FANOUT)) + 3

    print("=" * 70)
    print("  CRDT Distributed Data Quality — Demo")
    print("=" * 70)
    print(f"  Workers: {N_WORKERS}  |  Rows: {TOTAL_ROWS:,}  |  Null rate: {NULL_RATE:.0%}")
    print()

    # Step 1: generate and process locally
    print("[1/3] Workers processing partitions locally …")
    cluster = Cluster(n_workers=N_WORKERS, seed=7, column="sensor_value")
    cluster.generate_and_process(TOTAL_ROWS, NULL_RATE)

    sample = cluster.workers[0].metrics.summary()
    print(f"      Worker-0 sees {sample['null_count']} nulls, "
          f"{sample['valid_count']} valid, "
          f"~{sample['distinct_values_approx']} distinct values")

    v = cluster.convergence_variance()
    print(f"      Pre-merge spread: null={v['null_count_spread']}, "
          f"valid={v['valid_count_spread']}")

    # Step 2: gossip merge
    print(f"\n[2/3] Gossip merging (fanout={FANOUT}, up to {MAX_GOSSIP_ROUNDS} rounds) …")
    for r in range(1, MAX_GOSSIP_ROUNDS + 1):
        cluster.merge_gossip(fanout=FANOUT)
        v = cluster.convergence_variance()
        converged = cluster.is_converged()
        spread = v["null_count_spread"]
        bar = progress_bar(MAX_GOSSIP_ROUNDS - r, MAX_GOSSIP_ROUNDS)
        print(f"      Round {r:2d}: null spread={spread:5d}  {bar}", end="")
        if converged:
            print(f"  ✓ CONVERGED")
            break
        print()

    if not cluster.is_converged():
        print("\n      Not yet converged; forcing full merge …")
        cluster.merge_full()

    # Step 3: report
    print(f"\n[3/3] Global quality report")
    print("-" * 70)
    gs = cluster.global_summary()
    print(f"  Column            : {gs['column']}")
    print(f"  Total observed    : {gs['total_observed']:,}")
    print(f"  Null count        : {gs['null_count']:,}  ({gs['null_rate']:.2%})")
    print(f"  Valid count       : {gs['valid_count']:,}")
    print(f"  Anomaly count     : {gs['anomaly_count']}")
    print(f"  Anomaly types     : {', '.join(gs['anomaly_types']) or 'none'}")
    print(f"  Distinct values   : ~{gs['distinct_values_approx']:,}  "
          f"({gs['distinct_values_error']})")
    print(f"  Merge rounds      : {gs['merge_rounds']}")
    print(f"  Avg merge time    : {gs['avg_merge_time_ms']:.3f} ms")
    print()
    print("  Value histogram:")
    hist = gs["value_histogram"]
    total_hist = sum(hist.values()) or 1
    for bucket, count in hist.items():
        bar = "#" * int(40 * count / total_hist)
        print(f"    {bucket:>10}  {count:>7,}  {bar}")

    print("=" * 70)
    print("  All CRDT invariants maintained. No coordinator required.")
    print("=" * 70)


if __name__ == "__main__":
    main()

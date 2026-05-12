"""
Benchmark merge overhead for all three topologies across cluster sizes.

Measures:
  1. Full merge       — O(n²) messages, single round
  2. Gossip merge     — O(n·fanout) messages, ceil(log_fanout(n)) rounds
  3. Ring merge       — O(n) messages, O(n) rounds

Output: ASCII table + JSON results written to benchmarks/results.json
"""
from __future__ import annotations
import json
import math
import statistics
import time
from pathlib import Path

from src.cluster import Cluster


CLUSTER_SIZES = [10, 25, 50, 100]
ROWS_PER_RUN = 50_000
REPEATS = 3


def bench(fn, repeats=REPEATS):
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return {
        "mean_ms": round(statistics.mean(times), 3),
        "stdev_ms": round(statistics.stdev(times) if len(times) > 1 else 0, 3),
        "min_ms": round(min(times), 3),
        "max_ms": round(max(times), 3),
    }


def run_benchmarks():
    results = []

    print(f"\n{'':=<78}")
    print(f"  CRDT Merge Benchmark  —  {ROWS_PER_RUN:,} rows, {REPEATS} repeats each")
    print(f"{'':=<78}")
    print(f"{'Size':>6}  {'Topology':>10}  {'Mean ms':>9}  {'Stdev ms':>9}  "
          f"{'Min ms':>8}  {'Max ms':>8}  {'Converged':>10}")
    print(f"{'-'*6}  {'-'*10}  {'-'*9}  {'-'*9}  {'-'*8}  {'-'*8}  {'-'*10}")

    for n in CLUSTER_SIZES:
        gossip_rounds = math.ceil(math.log(n) / math.log(3)) + 2

        # ---- full ----
        def make_and_bench_full():
            c = Cluster(n_workers=n, seed=42)
            c.generate_and_process(ROWS_PER_RUN)
            c.merge_full()
            return c.is_converged()

        c_full = Cluster(n_workers=n, seed=42)
        c_full.generate_and_process(ROWS_PER_RUN)
        t_full = bench(lambda: Cluster(n_workers=n, seed=42).generate_and_process(ROWS_PER_RUN) or
                       c_full.merge_full())
        converged_full = c_full.is_converged()

        # ---- gossip ----
        c_gossip = Cluster(n_workers=n, seed=42)
        c_gossip.generate_and_process(ROWS_PER_RUN)

        def gossip_fn():
            for _ in range(gossip_rounds):
                c_gossip.merge_gossip(fanout=3)

        t_gossip = bench(gossip_fn, repeats=1)  # mutates state, run once
        converged_gossip = c_gossip.is_converged()

        # ---- ring ----
        c_ring = Cluster(n_workers=n, seed=42)
        c_ring.generate_and_process(ROWS_PER_RUN)

        def ring_fn():
            for _ in range(n):
                c_ring.merge_ring()

        t_ring = bench(ring_fn, repeats=1)
        converged_ring = c_ring.is_converged()

        for topo, t, conv in [
            ("full", t_full, converged_full),
            (f"gossip×{gossip_rounds}", t_gossip, converged_gossip),
            (f"ring×{n}", t_ring, converged_ring),
        ]:
            row = {
                "n_workers": n,
                "topology": topo,
                "converged": conv,
                **t,
            }
            results.append(row)
            print(
                f"{n:>6}  {topo:>10}  {t['mean_ms']:>9.3f}  {t['stdev_ms']:>9.3f}  "
                f"{t['min_ms']:>8.3f}  {t['max_ms']:>8.3f}  {'yes' if conv else 'NO':>10}"
            )

        print()

    # HyperLogLog accuracy benchmark
    print(f"\n{'':=<78}")
    print("  HyperLogLog Accuracy vs Precision")
    print(f"{'':=<78}")
    print(f"{'Precision':>10}  {'Registers':>10}  {'True N':>8}  "
          f"{'Estimate':>10}  {'Error %':>8}  {'Theoretical':>12}")
    print(f"{'-'*10}  {'-'*10}  {'-'*8}  {'-'*10}  {'-'*8}  {'-'*12}")

    from src.crdts import HyperLogLogCRDT

    for precision in [8, 10, 12, 14]:
        true_n = 50_000
        h = HyperLogLogCRDT(node_id="bench", precision=precision)
        for i in range(true_n):
            h.add(f"item_{i}")
        est = h.count()
        err_pct = abs(est - true_n) / true_n * 100
        theo_pct = h.error_rate() * 100
        print(
            f"{precision:>10}  {2**precision:>10,}  {true_n:>8,}  "
            f"{est:>10,}  {err_pct:>7.2f}%  {theo_pct:>11.2f}%"
        )

    out = Path(__file__).parent / "results.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nResults written to {out}")


if __name__ == "__main__":
    run_benchmarks()

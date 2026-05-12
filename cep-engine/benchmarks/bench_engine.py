"""
Latency & throughput benchmarks for the CEP engine.

Measures:
  - push() latency (p50 / p95 / p99 / p99.9) with Numba JIT
  - push() latency with Python fallback
  - Batch push throughput
  - Pattern compilation time

Run:
    python benchmarks/bench_engine.py
    python benchmarks/bench_engine.py --n 500000
"""

import argparse
import time

import numpy as np

from cep import CEPEngine, Pattern, make_event
from cep.compiler import PatternCompiler, _make_python_fallback, MAX_ENTITIES

ENTITY = 42


class E:
    A, B, C = 1, 2, 3


def build_pattern() -> Pattern:
    return (
        Pattern("bench_3step")
        .begin(E.A)
        .then(E.B, max_gap_ns=5_000_000_000)
        .then(E.C, max_gap_ns=5_000_000_000)
        .total_window(10_000_000_000)
    )


# -----------------------------------------------------------------------

def bench_latency(n: int, use_numba: bool):
    label = "Numba JIT" if use_numba else "Python fallback"
    pat = build_pattern()
    engine = CEPEngine()
    engine._use_numba = use_numba
    engine.register(pat)

    ev = make_event(E.A, ENTITY, timestamp=1_000_000_000_000)
    samples = np.empty(n, dtype=np.float64)

    # Warmup
    for _ in range(1000):
        engine.push(ev)

    for i in range(n):
        t0 = time.perf_counter_ns()
        engine.push(ev)
        samples[i] = time.perf_counter_ns() - t0

    print(f"\n{'='*55}")
    print(f"  push() latency — {label} ({n:,} samples)")
    print(f"{'='*55}")
    for pct, label_p in [(50, "p50"), (95, "p95"), (99, "p99"), (99.9, "p99.9")]:
        v = np.percentile(samples, pct)
        print(f"  {label_p:6s}  {v:8.1f} ns   ({v/1_000:.2f} µs)")

    tp = n / (samples.sum() / 1e9)
    print(f"  throughput  {tp:,.0f} events/s")
    return samples


def bench_batch(n: int):
    pat = build_pattern()
    events = np.array(
        [make_event(E.A, ENTITY, timestamp=i * 100_000) for i in range(n)],
        dtype=make_event(E.A, ENTITY).dtype,
    )
    engine = CEPEngine()
    engine.register(pat)

    t0 = time.perf_counter_ns()
    engine.buffer.push_batch(events)
    elapsed_ns = time.perf_counter_ns() - t0
    print(f"\n{'='*55}")
    print(f"  Batch ring-buffer push — {n:,} events")
    print(f"{'='*55}")
    print(f"  total   {elapsed_ns/1e6:.2f} ms")
    print(f"  per-ev  {elapsed_ns/n:.1f} ns  ({n*1e9/elapsed_ns:,.0f} ev/s)")


def bench_compile():
    compiler = PatternCompiler()
    pat = build_pattern()
    t0 = time.perf_counter_ns()
    cp = compiler.compile(pat)
    elapsed = (time.perf_counter_ns() - t0) / 1e6
    print(f"\n{'='*55}")
    print(f"  Pattern compilation (including Numba warm-up)")
    print(f"{'='*55}")
    print(f"  {elapsed:.1f} ms  (one-time cost at startup)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200_000, help="sample count")
    args = ap.parse_args()

    bench_compile()
    bench_latency(args.n, use_numba=True)
    bench_latency(args.n, use_numba=False)
    bench_batch(args.n)


if __name__ == "__main__":
    main()

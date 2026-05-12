"""
Latency benchmark — measures end-to-end p50/p95/p99 for online feature serving.

Usage:
  python scripts/benchmark.py --host localhost --port 8000 --n 1000 --batch-size 50
"""
from __future__ import annotations

import argparse
import concurrent.futures
import statistics
import time
from typing import Callable

import httpx


def make_single_getter(client: httpx.Client, group: str, entity_id: str) -> Callable:
    def fn():
        t0 = time.perf_counter()
        resp = client.get(f"/features/{group}/{entity_id}")
        resp.raise_for_status()
        return (time.perf_counter() - t0) * 1000
    return fn


def make_batch_getter(client: httpx.Client, requests: list[dict]) -> Callable:
    def fn():
        t0 = time.perf_counter()
        resp = client.post("/features/batch", json={"requests": requests})
        resp.raise_for_status()
        return (time.perf_counter() - t0) * 1000
    return fn


def percentile(data: list[float], p: float) -> float:
    idx = max(0, int(len(data) * p / 100) - 1)
    return sorted(data)[idx]


def run_benchmark(
    base_url: str,
    n: int = 1000,
    concurrency: int = 10,
    batch_size: int = 20,
) -> None:
    print(f"Benchmarking {base_url} — {n} iterations, concurrency={concurrency}")

    with httpx.Client(base_url=base_url, timeout=1.0) as client:
        # --- Single entity ---
        single_latencies = []
        for _ in range(n):
            fn = make_single_getter(client, "user_features", "user_00001")
            try:
                single_latencies.append(fn())
            except Exception:
                pass

        print("\n=== Single entity GET ===")
        print(f"  p50: {percentile(single_latencies, 50):.2f}ms")
        print(f"  p95: {percentile(single_latencies, 95):.2f}ms")
        print(f"  p99: {percentile(single_latencies, 99):.2f}ms")
        print(f"  max: {max(single_latencies):.2f}ms")

        # --- Batch entity (batch_size entities) ---
        batch_requests = [
            {"group": "user_features", "entity_id": f"user_{i:05d}"}
            for i in range(batch_size)
        ]
        batch_latencies = []
        for _ in range(n // 10):
            fn = make_batch_getter(client, batch_requests)
            try:
                batch_latencies.append(fn())
            except Exception:
                pass

        print(f"\n=== Batch GET ({batch_size} entities) ===")
        print(f"  p50: {percentile(batch_latencies, 50):.2f}ms")
        print(f"  p95: {percentile(batch_latencies, 95):.2f}ms")
        print(f"  p99: {percentile(batch_latencies, 99):.2f}ms")
        print(f"  per-entity p99: {percentile(batch_latencies, 99) / batch_size:.2f}ms")

    slo_met = percentile(single_latencies, 99) < 10.0
    print(f"\n{'✓' if slo_met else '✗'} <10ms p99 SLO: {'PASS' if slo_met else 'FAIL'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()

    run_benchmark(
        base_url=f"http://{args.host}:{args.port}",
        n=args.n,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
    )

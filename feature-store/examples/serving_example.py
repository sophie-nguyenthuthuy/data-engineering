"""
Example: fetch features for ML model inference using the serving client.

Run with:
  python examples/serving_example.py

Assumes the feature server is running on localhost:8000.
"""
from __future__ import annotations

import statistics
import time

from feature_store.serving.client import FeatureStoreClient


def measure_latency(client: FeatureStoreClient, group: str, entity_id: str, n: int = 100) -> dict:
    latencies = []
    for _ in range(n):
        t0 = time.perf_counter()
        client.get(group, entity_id)
        latencies.append((time.perf_counter() - t0) * 1000)
    return {
        "p50_ms": round(statistics.median(latencies), 2),
        "p95_ms": round(sorted(latencies)[int(n * 0.95)], 2),
        "p99_ms": round(sorted(latencies)[int(n * 0.99)], 2),
        "mean_ms": round(statistics.mean(latencies), 2),
    }


def main() -> None:
    with FeatureStoreClient("http://localhost:8000", timeout_ms=50) as client:
        # --- Health check ---
        health = client.health()
        print("Health:", health)

        # --- Single entity fetch ---
        print("\n--- Single feature fetch ---")
        feats = client.get("user_features", "user_00001")
        print(f"user_00001 features: {feats}")

        # --- Batch fetch: multi-group, multi-entity ---
        print("\n--- Batch feature fetch ---")
        requests = [
            ("user_features", "user_00001"),
            ("user_features", "user_00002"),
            ("realtime_features", "user_00001"),
        ]
        t0 = time.perf_counter()
        results = client.get_batch(requests)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        for (group, entity_id), feats in zip(requests, results):
            print(f"  {group}/{entity_id}: {feats}")
        print(f"  Batch latency: {elapsed_ms:.1f}ms")

        # --- Latency benchmark ---
        print("\n--- Latency benchmark (100 calls) ---")
        stats = measure_latency(client, "user_features", "user_00001")
        print(f"  p50: {stats['p50_ms']}ms")
        print(f"  p95: {stats['p95_ms']}ms")
        print(f"  p99: {stats['p99_ms']}ms")
        print(f"  mean: {stats['mean_ms']}ms")
        if stats["p99_ms"] < 10:
            print("  ✓ p99 < 10ms SLO satisfied")
        else:
            print("  ✗ p99 SLO BREACH")


if __name__ == "__main__":
    main()

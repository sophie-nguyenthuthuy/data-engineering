"""Demonstrates schema evolution detection and codec re-evaluation."""
import numpy as np

import cctest
from cctest.schema import SchemaEvolutionTracker


def main() -> None:
    print("=" * 60)
    print("Schema Evolution Demo")
    print("=" * 60)

    rng = np.random.default_rng(1)
    selector = cctest.EncodingSelector(config=cctest.SelectorConfig(sample_size=1024))
    tracker = SchemaEvolutionTracker()

    # --- Batch 1: initial schema ---
    batch1 = {
        "id": np.arange(0, 1000, dtype=np.int64),
        "value": np.round(rng.uniform(0, 100, 1000) * 100) / 100,
    }
    diff1 = tracker.observe(batch1, selector=selector)
    print("\nBatch 1 (initial schema):")
    print(diff1)
    for name, arr in batch1.items():
        codec = selector.select(name, arr)
        print(f"  {name:<8} → {codec.name}")

    # --- Batch 2: add a new column ---
    batch2 = {
        "id": np.arange(1000, 2000, dtype=np.int64),
        "value": np.round(rng.uniform(0, 100, 1000) * 100) / 100,
        "label": np.array(["cat", "dog", "fish"][i] for i in rng.integers(0, 3, 1000)),
    }
    diff2 = tracker.observe(batch2, selector=selector)
    print("\nBatch 2 (added 'label' column):")
    print(diff2)
    for name, arr in batch2.items():
        codec = selector.select(name, arr)
        print(f"  {name:<8} → {codec.name}")

    # --- Batch 3: type change on 'value' (float64 → float32) ---
    batch3 = {
        "id": np.arange(2000, 3000, dtype=np.int64),
        "value": rng.uniform(0, 100, 1000).astype(np.float32),
        "label": np.array(["cat", "dog", "fish"][i] for i in rng.integers(0, 3, 1000)),
    }
    diff3 = tracker.observe(batch3, selector=selector)
    print("\nBatch 3 ('value' dtype changed float64→float32):")
    print(diff3)
    for name, arr in batch3.items():
        codec = selector.select(name, arr)
        print(f"  {name:<8} → {codec.name}")

    # --- Show evolution history ---
    print("\nFull schema history:")
    for i, (old_schema, diff) in enumerate(tracker.history(), 1):
        print(f"  Version {i}: {old_schema}")
        print(f"    changes: {diff}")


if __name__ == "__main__":
    main()

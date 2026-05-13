"""Synthetic DAG generator for benchmarks and tests.

Generates layered DAGs: ``n_layers`` of ~``avg_layer_width`` tasks
each, with each task connected to a small random sample of its
immediate predecessors. Durations are drawn from a lognormal so the
resulting workload looks like a realistic ETL pipeline (a few very long
tasks, many short ones).
"""

from __future__ import annotations

import math
import random

from fps.dag import DAG, Task


def random_layered_dag(
    n_layers: int = 5,
    avg_layer_width: int = 4,
    mu: float = 0.5,
    sigma: float = 0.7,
    max_parents: int = 2,
    seed: int = 0,
) -> DAG:
    """Build a synthetic layered DAG with lognormal durations."""
    if n_layers < 1:
        raise ValueError("n_layers must be ≥ 1")
    if avg_layer_width < 1:
        raise ValueError("avg_layer_width must be ≥ 1")
    if max_parents < 0:
        raise ValueError("max_parents must be ≥ 0")
    rng = random.Random(seed)
    dag = DAG()
    prev_layer: list[str] = []
    for layer in range(n_layers):
        width = max(1, int(rng.gauss(avg_layer_width, 1)))
        current_layer: list[str] = []
        for i in range(width):
            tid = f"L{layer}-T{i}"
            parents: tuple[str, ...] = ()
            if prev_layer and max_parents > 0:
                k = min(len(prev_layer), max(1, rng.randint(1, max_parents)))
                parents = tuple(rng.sample(prev_layer, k))
            duration = math.exp(mu + sigma * rng.gauss(0.0, 1.0))
            dag.add(Task(id=tid, duration=duration, deps=parents))
            current_layer.append(tid)
        prev_layer = current_layer
    return dag


__all__ = ["random_layered_dag"]

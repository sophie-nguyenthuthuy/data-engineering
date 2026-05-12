"""
Simulates a cluster of 50 pipeline workers with gossip-style CRDT merging.

Topology options:
  - full:    every node merges with every other (O(n²) but fastest convergence)
  - ring:    each node merges with its two neighbours
  - random:  each round each node picks k random peers (epidemic / gossip)

No central coordinator. Workers communicate state vectors directly.
"""
from __future__ import annotations
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .metrics import WorkerMetrics
from .worker import PipelineWorker


@dataclass
class Cluster:
    n_workers: int = 50
    column: str = "sensor_value"
    seed: int = 42

    workers: List[PipelineWorker] = field(init=False, default_factory=list)
    merge_rounds: int = field(init=False, default=0)
    total_merge_time_ms: float = field(init=False, default=0.0)

    def __post_init__(self):
        rng = random.Random(self.seed)
        for i in range(self.n_workers):
            w = PipelineWorker(
                worker_id=f"worker-{i:03d}",
                column=self.column,
                seed=rng.randint(0, 2**32),
            )
            self.workers.append(w)

    # ------------------------------------------------------------------ #
    #  Data generation                                                     #
    # ------------------------------------------------------------------ #

    def generate_and_process(
        self, total_rows: int = 100_000, null_rate: float = 0.05
    ) -> None:
        """Distribute rows evenly across workers and process locally."""
        rng = random.Random(self.seed + 1)
        rows_per_worker = total_rows // self.n_workers
        extra = total_rows % self.n_workers

        for i, worker in enumerate(self.workers):
            n = rows_per_worker + (1 if i < extra else 0)
            partition: List[Optional[float]] = []
            for _ in range(n):
                if rng.random() < null_rate:
                    partition.append(None)
                else:
                    # mix of small, medium, large values to exercise histogram
                    scale = rng.choice([1, 10, 100, 1_000, 10_000])
                    partition.append(rng.gauss(scale, scale * 0.1))
            worker.process_partition(partition)

    # ------------------------------------------------------------------ #
    #  Merge strategies                                                    #
    # ------------------------------------------------------------------ #

    def merge_full(self) -> None:
        """All-pairs merge. O(n²) messages, instant convergence."""
        t0 = time.perf_counter()
        states = [w.get_state() for w in self.workers]
        merged = states[0]
        for s in states[1:]:
            merged = merged.merge(s)
        # broadcast merged back
        for w in self.workers:
            w.metrics = merged.merge(w.metrics)
        self.merge_rounds += 1
        self.total_merge_time_ms += (time.perf_counter() - t0) * 1000

    def merge_ring(self) -> None:
        """Ring topology: each node merges with left and right neighbours."""
        t0 = time.perf_counter()
        n = len(self.workers)
        snapshots = [w.get_state() for w in self.workers]
        for i, worker in enumerate(self.workers):
            worker.metrics.merge_into(snapshots[(i - 1) % n])
            worker.metrics.merge_into(snapshots[(i + 1) % n])
        self.merge_rounds += 1
        self.total_merge_time_ms += (time.perf_counter() - t0) * 1000

    def merge_gossip(self, fanout: int = 3, rounds: int = 1) -> None:
        """
        Epidemic gossip: each node pushes its state to `fanout` random peers.
        After log(n)/log(fanout) rounds every node converges with high probability.
        """
        rng = random.Random(self.seed + self.merge_rounds)
        t0 = time.perf_counter()
        for _ in range(rounds):
            snapshots = [w.get_state() for w in self.workers]
            for i, worker in enumerate(self.workers):
                peers = rng.sample(
                    [j for j in range(len(self.workers)) if j != i],
                    min(fanout, len(self.workers) - 1),
                )
                for p in peers:
                    worker.metrics.merge_into(snapshots[p])
        self.merge_rounds += 1
        self.total_merge_time_ms += (time.perf_counter() - t0) * 1000

    # ------------------------------------------------------------------ #
    #  Convergence check                                                   #
    # ------------------------------------------------------------------ #

    def is_converged(self) -> bool:
        """All workers have identical null_count and valid_count values."""
        if not self.workers:
            return True
        ref_null = self.workers[0].metrics.null_count.value()
        ref_valid = self.workers[0].metrics.valid_count.value()
        ref_anomaly = self.workers[0].metrics.anomaly_count.value()
        ref_distinct = self.workers[0].metrics.distinct_values.count()
        return all(
            w.metrics.null_count.value() == ref_null
            and w.metrics.valid_count.value() == ref_valid
            and w.metrics.anomaly_count.value() == ref_anomaly
            and w.metrics.distinct_values.count() == ref_distinct
            for w in self.workers[1:]
        )

    def convergence_variance(self) -> Dict[str, float]:
        """Max spread across workers for each metric (0 = fully converged)."""
        nulls = [w.metrics.null_count.value() for w in self.workers]
        valids = [w.metrics.valid_count.value() for w in self.workers]
        anomalies = [w.metrics.anomaly_count.value() for w in self.workers]
        distincts = [w.metrics.distinct_values.count() for w in self.workers]
        return {
            "null_count_spread": max(nulls) - min(nulls),
            "valid_count_spread": max(valids) - min(valids),
            "anomaly_count_spread": max(anomalies) - min(anomalies),
            "distinct_spread": max(distincts) - min(distincts),
        }

    def global_summary(self) -> dict:
        """Merge all worker states and return the global aggregate."""
        if not self.workers:
            return {}
        merged = self.workers[0].metrics
        for w in self.workers[1:]:
            merged = merged.merge(w.metrics)
        s = merged.summary()
        s["cluster_size"] = self.n_workers
        s["merge_rounds"] = self.merge_rounds
        s["avg_merge_time_ms"] = (
            self.total_merge_time_ms / self.merge_rounds if self.merge_rounds else 0
        )
        return s

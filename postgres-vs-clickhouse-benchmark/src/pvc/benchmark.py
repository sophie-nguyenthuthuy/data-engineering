"""Benchmark runner.

For each ``(engine, query)`` pair the runner:

  1. Discards ``warmup`` runs to give caches / JIT a fair chance.
  2. Times ``repeat`` runs with a monotonic clock.
  3. Optionally drops the slowest / fastest ``trim`` samples per side
     before summarising — a conservative way to suppress GC spikes
     without throwing away signal.
  4. Returns a :class:`QueryResult` with raw timings plus the
     :class:`LatencyStats` summary.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pvc.stats import LatencyStats, summarise

if TYPE_CHECKING:
    from collections.abc import Callable

    from pvc.engines.base import Engine
    from pvc.workloads.base import Query, Workload


@dataclass(frozen=True, slots=True)
class IterationResult:
    """One ``(engine, query)`` × ``repeat`` run."""

    engine: str
    query_id: str
    samples: tuple[float, ...]
    stats: LatencyStats


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Per-query summary across every engine that ran it."""

    query_id: str
    by_engine: dict[str, IterationResult]

    def winner(self) -> str | None:
        """Engine with the lowest p50; ``None`` when no data."""
        if not self.by_engine:
            return None
        return min(self.by_engine.items(), key=lambda kv: kv[1].stats.p50)[0]


@dataclass
class BenchmarkRunner:
    """Drives engines through a workload and collects timings."""

    engines: list[Engine]
    workload: Workload
    warmup: int = 1
    repeat: int = 5
    trim: int = 0
    clock: Callable[[], float] = field(default=time.perf_counter)

    def __post_init__(self) -> None:
        if not self.engines:
            raise ValueError("at least one engine required")
        if self.warmup < 0:
            raise ValueError("warmup must be ≥ 0")
        if self.repeat < 1:
            raise ValueError("repeat must be ≥ 1")
        if self.trim < 0:
            raise ValueError("trim must be ≥ 0")
        if 2 * self.trim >= self.repeat:
            raise ValueError("trim too large for repeat count")

    def run(self) -> list[QueryResult]:
        """Time every query on every engine."""
        results: list[QueryResult] = []
        for q in self.workload.queries:
            per_engine: dict[str, IterationResult] = {}
            for engine in self.engines:
                samples = self._time(engine, q)
                per_engine[engine.name] = IterationResult(
                    engine=engine.name,
                    query_id=q.id,
                    samples=tuple(samples),
                    stats=summarise(samples),
                )
            results.append(QueryResult(query_id=q.id, by_engine=per_engine))
        return results

    # ----------------------------------------------------------- private

    def _time(self, engine: Engine, query: Query) -> list[float]:
        # Warm-up runs we don't measure.
        for _ in range(self.warmup):
            engine.execute(query.sql)
        samples: list[float] = []
        for _ in range(self.repeat):
            t0 = self.clock()
            engine.execute(query.sql)
            samples.append(self.clock() - t0)
        if self.trim:
            samples = sorted(samples)[self.trim : len(samples) - self.trim]
        return samples


__all__ = ["BenchmarkRunner", "IterationResult", "QueryResult"]

"""Runtime cardinality profiler.

Wraps volcano iterators with lightweight instrumentation that counts
actual rows and time.  After a configurable check interval the profiler
compares actual vs. estimated cardinality and raises a HotPathSignal
if the ratio exceeds a threshold.
"""
from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Iterator

from .expressions import Row
from .plan import PlanNode


# ------------------------------------------------------------------
# Data structures
# ------------------------------------------------------------------

@dataclass
class OperatorStats:
    node_id: str
    estimated_rows: int
    actual_rows: int = 0
    elapsed_ns: int = 0
    hot: bool = False

    @property
    def cardinality_ratio(self) -> float:
        if self.estimated_rows <= 0:
            return float("inf") if self.actual_rows > 0 else 1.0
        return self.actual_rows / self.estimated_rows

    @property
    def elapsed_ms(self) -> float:
        return self.elapsed_ns / 1_000_000

    def __repr__(self) -> str:
        return (
            f"[{self.node_id}] actual={self.actual_rows} "
            f"est={self.estimated_rows} "
            f"ratio={self.cardinality_ratio:.2f}x "
            f"time={self.elapsed_ms:.1f}ms "
            f"{'HOT' if self.hot else ''}"
        )


class HotPathSignal(Exception):
    """Raised mid-iteration when a hot path is detected."""

    def __init__(self, node_id: str, stats: OperatorStats) -> None:
        super().__init__(f"Hot path on {node_id}: ratio={stats.cardinality_ratio:.1f}x")
        self.node_id = node_id
        self.stats = stats


# ------------------------------------------------------------------
# Profiling iterator wrapper
# ------------------------------------------------------------------

class ProfilingIterator:
    """Wraps a volcano iterator, counts rows, signals hot paths.

    Args:
        inner:          The underlying row iterator.
        stats:          Shared OperatorStats object to update.
        check_interval: How many rows to emit before checking ratio.
        hot_threshold:  Ratio (actual/estimated) that triggers a HotPathSignal.
        raise_on_hot:   If True, raise HotPathSignal; if False, just mark hot.
    """

    def __init__(
        self,
        inner: Iterator[Row],
        stats: OperatorStats,
        check_interval: int = 100,
        hot_threshold: float = 10.0,
        raise_on_hot: bool = True,
    ) -> None:
        self._inner = inner
        self._stats = stats
        self._interval = check_interval
        self._threshold = hot_threshold
        self._raise = raise_on_hot
        self._start_ns = time.perf_counter_ns()

    def __iter__(self) -> "ProfilingIterator":
        return self

    def __next__(self) -> Row:
        row = next(self._inner)  # propagates StopIteration naturally
        self._stats.actual_rows += 1
        self._stats.elapsed_ns = time.perf_counter_ns() - self._start_ns

        if (
            self._stats.actual_rows % self._interval == 0
            and not self._stats.hot
        ):
            self._check()

        return row

    def _check(self) -> None:
        if self._stats.cardinality_ratio >= self._threshold:
            self._stats.hot = True
            if self._raise:
                raise HotPathSignal(self._stats.node_id, self._stats)


# ------------------------------------------------------------------
# Session-level profiler
# ------------------------------------------------------------------

class QueryProfiler:
    """Maintains stats for every operator in a query execution."""

    def __init__(
        self,
        hot_threshold: float = 10.0,
        check_interval: int = 100,
    ) -> None:
        self._stats: dict[str, OperatorStats] = {}
        self.hot_threshold = hot_threshold
        self.check_interval = check_interval

    def register(self, node: PlanNode) -> OperatorStats:
        stats = OperatorStats(
            node_id=node.node_id or type(node).__name__,
            estimated_rows=node.estimated_rows,
        )
        self._stats[stats.node_id] = stats
        return stats

    def wrap(
        self,
        iterator: Iterator[Row],
        node: PlanNode,
        raise_on_hot: bool = True,
    ) -> ProfilingIterator:
        stats = self.register(node)
        return ProfilingIterator(
            iterator,
            stats,
            check_interval=self.check_interval,
            hot_threshold=self.hot_threshold,
            raise_on_hot=raise_on_hot,
        )

    def finalize(self, iterator: Iterator[Row], node: PlanNode) -> list[Row]:
        """Drain iterator, updating stats, without raising HotPathSignal."""
        stats = self.register(node)
        pi = ProfilingIterator(
            iterator, stats,
            check_interval=self.check_interval,
            hot_threshold=self.hot_threshold,
            raise_on_hot=False,
        )
        rows = list(pi)
        return rows

    def get(self, node_id: str) -> OperatorStats | None:
        return self._stats.get(node_id)

    def all_stats(self) -> list[OperatorStats]:
        return list(self._stats.values())

    def hot_nodes(self) -> list[OperatorStats]:
        return [s for s in self._stats.values() if s.hot]

    def summary(self) -> str:
        lines = ["=== Query Profiler Summary ==="]
        for stats in self._stats.values():
            lines.append(f"  {stats}")
        return "\n".join(lines)

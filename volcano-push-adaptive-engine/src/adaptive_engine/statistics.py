"""Equi-depth histogram statistics for accurate selectivity estimation.

An equi-depth histogram divides sorted column values into N buckets of
approximately equal row count.  This handles skewed distributions far
better than uniform-distribution assumptions, because each bucket
represents the same number of rows regardless of value density.

Usage:
    hist = EquiDepthHistogram(values, n_buckets=20)
    hist.selectivity_eq(42)           # P(col = 42)
    hist.selectivity_range(10, 50)    # P(10 <= col <= 50)
    hist.selectivity_lt(50)           # P(col < 50)
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class Bucket:
    lo: Any
    hi: Any
    row_count: int
    distinct_count: int

    @property
    def density(self) -> float:
        """Rows per distinct value in this bucket."""
        return self.row_count / max(1, self.distinct_count)


class EquiDepthHistogram:
    """Equi-depth (equal-frequency) histogram over a single column."""

    def __init__(self, values: list[Any], n_buckets: int = 20) -> None:
        non_null = [v for v in values if v is not None]
        self.total_rows: int = len(non_null)
        self.null_count: int = len(values) - self.total_rows
        self.buckets: list[Bucket] = []

        if not non_null:
            return

        try:
            non_null.sort()
        except TypeError:
            # Unsortable type (e.g. mixed) — fall back to no histogram
            return

        bucket_size = max(1, self.total_rows // n_buckets)
        for i in range(0, self.total_rows, bucket_size):
            chunk = non_null[i : i + bucket_size]
            self.buckets.append(
                Bucket(
                    lo=chunk[0],
                    hi=chunk[-1],
                    row_count=len(chunk),
                    distinct_count=len(set(chunk)),
                )
            )

    # ------------------------------------------------------------------
    # Selectivity estimates
    # ------------------------------------------------------------------

    def selectivity_eq(self, value: Any) -> float:
        """P(col = value) — summed across all buckets that contain the value.

        Equi-depth buckets can span a single repeated value across many buckets
        (e.g. 90 copies of '1' fills 18 of 20 buckets), so we must accumulate
        contributions from every matching bucket rather than returning early.
        """
        total = 0.0
        for b in self.buckets:
            try:
                if b.lo <= value <= b.hi:
                    total += (b.row_count / self.total_rows) / max(1, b.distinct_count)
            except TypeError:
                continue
        return total

    def selectivity_ne(self, value: Any) -> float:
        """P(col != value)."""
        return 1.0 - self.selectivity_eq(value)

    def selectivity_range(self, lo: Any, hi: Any) -> float:
        """P(lo <= col <= hi)."""
        if self.total_rows == 0:
            return 0.0
        covered = 0.0
        for b in self.buckets:
            try:
                if b.hi < lo or b.lo > hi:
                    continue  # no overlap
                if b.lo >= lo and b.hi <= hi:
                    covered += b.row_count  # fully inside range
                else:
                    # Partial overlap — linear interpolation
                    span = _numeric_span(b.lo, b.hi)
                    if span == 0:
                        covered += b.row_count
                    else:
                        overlap = _numeric_span(
                            max_comparable(lo, b.lo),
                            min_comparable(hi, b.hi),
                        )
                        covered += b.row_count * max(0.0, overlap / span)
            except TypeError:
                continue
        return min(1.0, covered / self.total_rows)

    def selectivity_lt(self, value: Any) -> float:
        """P(col < value)."""
        # Range from -inf to value-ε; use (min, value) as approximation
        if not self.buckets:
            return 0.5
        return self.selectivity_range(self.buckets[0].lo, value)

    def selectivity_lte(self, value: Any) -> float:
        """P(col <= value)."""
        return self.selectivity_lt(value)  # continuous approx

    def selectivity_gt(self, value: Any) -> float:
        """P(col > value)."""
        if not self.buckets:
            return 0.5
        return self.selectivity_range(value, self.buckets[-1].hi)

    def selectivity_gte(self, value: Any) -> float:
        """P(col >= value)."""
        return self.selectivity_gt(value)  # continuous approx

    def for_op(self, op: str, value: Any) -> float:
        """Dispatch selectivity by comparison operator string."""
        match op:
            case "=":
                return self.selectivity_eq(value)
            case "!=":
                return self.selectivity_ne(value)
            case "<":
                return self.selectivity_lt(value)
            case "<=":
                return self.selectivity_lte(value)
            case ">":
                return self.selectivity_gt(value)
            case ">=":
                return self.selectivity_gte(value)
            case _:
                return 0.5

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def min_val(self) -> Any:
        return self.buckets[0].lo if self.buckets else None

    @property
    def max_val(self) -> Any:
        return self.buckets[-1].hi if self.buckets else None

    @property
    def distinct_values(self) -> int:
        return sum(b.distinct_count for b in self.buckets)

    def most_frequent_value(self) -> Any | None:
        """Value from the bucket with the highest row density (most skewed)."""
        if not self.buckets:
            return None
        b = max(self.buckets, key=lambda x: x.density)
        return b.lo

    def __repr__(self) -> str:
        return (
            f"EquiDepthHistogram("
            f"buckets={len(self.buckets)}, "
            f"rows={self.total_rows}, "
            f"range=[{self.min_val}, {self.max_val}])"
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _numeric_span(lo: Any, hi: Any) -> float:
    try:
        return float(hi - lo)
    except TypeError:
        return 1.0


def max_comparable(a: Any, b: Any) -> Any:
    try:
        return a if a >= b else b
    except TypeError:
        return b


def min_comparable(a: Any, b: Any) -> Any:
    try:
        return a if a <= b else b
    except TypeError:
        return a

"""Cost statistics: histograms, column stats, table stats, and registry."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Histogram:
    """Equi-depth histogram with *n+1* boundary values and *n* frequency buckets.

    *boundaries* has length len(frequencies) + 1.
    Each frequency value represents the fraction of rows in that bucket.
    Frequencies should sum to approximately 1.0.
    """

    boundaries: List[float]
    frequencies: List[float]

    def __post_init__(self) -> None:
        if len(self.boundaries) != len(self.frequencies) + 1:
            raise ValueError(
                "boundaries must have exactly len(frequencies)+1 elements; "
                f"got {len(self.boundaries)} boundaries and {len(self.frequencies)} frequencies"
            )

    # ------------------------------------------------------------------
    # Estimation helpers
    # ------------------------------------------------------------------

    def estimate_fraction_lt(self, value: float) -> float:
        """Fraction of rows with column value strictly less than *value*."""
        if value <= self.boundaries[0]:
            return 0.0
        if value >= self.boundaries[-1]:
            return 1.0

        fraction = 0.0
        for i, freq in enumerate(self.frequencies):
            lo = self.boundaries[i]
            hi = self.boundaries[i + 1]
            if value >= hi:
                fraction += freq
            elif value > lo:
                # Partial bucket: interpolate linearly
                bucket_width = hi - lo
                fraction += freq * (value - lo) / bucket_width if bucket_width > 0 else 0.0
                break
        return min(max(fraction, 0.0), 1.0)

    def estimate_fraction_between(self, lo: float, hi: float) -> float:
        """Fraction of rows with column value in [lo, hi]."""
        if lo > hi:
            return 0.0
        return max(self.estimate_fraction_lt(hi + 1e-10) - self.estimate_fraction_lt(lo), 0.0)


@dataclass
class ColumnStats:
    """Statistics for a single column."""

    column: str
    null_fraction: float
    distinct_count: int
    min_value: Optional[float]
    max_value: Optional[float]
    histogram: Optional[Histogram] = None

    def value_fraction_lt(self, value: float) -> float:
        """Estimate fraction of non-null rows with value < *value*."""
        if self.histogram is not None:
            return self.histogram.estimate_fraction_lt(value)
        # Linear interpolation fallback
        if self.min_value is None or self.max_value is None:
            return 0.5
        span = self.max_value - self.min_value
        if span <= 0:
            return 0.0 if value <= self.min_value else 1.0
        return min(max((value - self.min_value) / span, 0.0), 1.0)

    def value_fraction_between(self, lo: float, hi: float) -> float:
        """Estimate fraction of non-null rows with lo <= value <= hi."""
        if self.histogram is not None:
            return self.histogram.estimate_fraction_between(lo, hi)
        if self.min_value is None or self.max_value is None:
            return 0.5
        span = self.max_value - self.min_value
        if span <= 0:
            return 1.0 if lo <= self.min_value <= hi else 0.0
        lo_frac = min(max((lo - self.min_value) / span, 0.0), 1.0)
        hi_frac = min(max((hi - self.min_value) / span, 0.0), 1.0)
        return max(hi_frac - lo_frac, 0.0)


@dataclass
class TableStats:
    """Aggregate statistics for a table."""

    table_name: str
    row_count: int
    column_stats: Dict[str, ColumnStats] = field(default_factory=dict)

    def get_column(self, name: str) -> Optional[ColumnStats]:
        """Return stats for *name*, or None if not available."""
        return self.column_stats.get(name)


class StatsRegistry:
    """Thread-safe store of TableStats keyed by table name."""

    def __init__(self) -> None:
        self._stats: Dict[str, TableStats] = {}
        self._lock = threading.Lock()

    def set_table_stats(self, stats: TableStats) -> None:
        with self._lock:
            self._stats[stats.table_name] = stats

    def get_table_stats(self, table_name: str) -> Optional[TableStats]:
        with self._lock:
            return self._stats.get(table_name)

    def list_tables(self) -> List[str]:
        with self._lock:
            return sorted(self._stats.keys())

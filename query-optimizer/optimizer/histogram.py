"""
Column-level statistics and histogram-based cardinality estimation.

Each column stores:
  - num_distinct: number of distinct values (NDV)
  - min_val / max_val: range
  - null_frac: fraction of NULLs
  - buckets: equi-depth histogram (list of (boundary, freq) pairs)
  - mcv: most-common-values [(value, freq)]

Selectivity estimation follows classic formulas:
  - Equi-join sel  = 1 / max(NDV_left, NDV_right)
  - Range pred sel = (high - low) / (max - min)
  - Default sel    = 0.33
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class ColumnStats:
    name: str
    num_distinct: int
    min_val: float = 0.0
    max_val: float = 1.0
    null_frac: float = 0.0
    # equi-depth histogram: (upper_boundary, row_count_in_bucket)
    buckets: List[Tuple[float, int]] = field(default_factory=list)

    def selectivity_eq(self, other: "ColumnStats") -> float:
        """Selectivity of an equi-join predicate between two columns."""
        return 1.0 / max(self.num_distinct, other.num_distinct, 1)

    def selectivity_range(self, lo: float, hi: float) -> float:
        """Selectivity of a range predicate lo <= col <= hi."""
        span = self.max_val - self.min_val
        if span <= 0:
            return 1.0 if lo <= self.min_val <= hi else 0.0
        overlap = min(hi, self.max_val) - max(lo, self.min_val)
        return max(0.0, min(1.0, overlap / span))


@dataclass
class TableStats:
    name: str
    row_count: int
    columns: Dict[str, ColumnStats] = field(default_factory=dict)
    # average row size in bytes
    avg_row_bytes: int = 100

    def column(self, col_name: str) -> Optional[ColumnStats]:
        return self.columns.get(col_name)

    def add_column(self, stats: ColumnStats) -> None:
        self.columns[stats.name] = stats


class StatsCatalog:
    """Registry of per-table statistics used by the cardinality estimator."""

    def __init__(self) -> None:
        self._tables: Dict[str, TableStats] = {}

    def register(self, stats: TableStats) -> None:
        self._tables[stats.name] = stats

    def get(self, table: str) -> Optional[TableStats]:
        return self._tables.get(table)

    # ------------------------------------------------------------------
    # Cardinality estimation
    # ------------------------------------------------------------------

    def base_rows(self, table: str) -> float:
        ts = self.get(table)
        return float(ts.row_count) if ts else 1_000.0

    def join_output_rows(
        self,
        left_tables: frozenset,
        right_tables: frozenset,
        left_rows: float,
        right_rows: float,
        predicates,
    ) -> float:
        """
        Estimate output cardinality of a join using attribute-level selectivity.
        Falls back to 10% cross-product if no predicate stats are available.
        """
        sel = 1.0
        for pred in predicates:
            lt = self._tables.get(pred.left_table)
            rt = self._tables.get(pred.right_table)
            if lt and rt:
                lc = lt.column(pred.left_col)
                rc = rt.column(pred.right_col)
                if lc and rc:
                    sel *= lc.selectivity_eq(rc)
                    continue
            # fallback: assume moderate selectivity
            sel *= 0.1

        if not predicates:
            # cross join – only happens for Cartesian products
            sel = 1.0

        return max(1.0, left_rows * right_rows * sel)

"""In-memory table catalog with statistics for cost estimation."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .statistics import EquiDepthHistogram

Row = dict[str, Any]


@dataclass
class ColumnStats:
    name: str
    dtype: str  # "int", "float", "str"
    null_frac: float = 0.0
    distinct_values: int = 0
    min_val: Any = None
    max_val: Any = None
    histogram: "EquiDepthHistogram | None" = field(default=None, repr=False)

    # ------------------------------------------------------------------
    # Selectivity (falls back to uniform estimates when no histogram)
    # ------------------------------------------------------------------

    def selectivity_eq(self, value: Any = None) -> float:
        if self.histogram is not None and value is not None:
            return self.histogram.selectivity_eq(value)
        if self.distinct_values <= 0:
            return 1.0
        return 1.0 / self.distinct_values

    def selectivity_range(self, lo: Any, hi: Any) -> float:
        if self.histogram is not None:
            return self.histogram.selectivity_range(lo, hi)
        try:
            span = self.max_val - self.min_val
            if span == 0:
                return 1.0
            return max(0.0, min(1.0, (hi - lo) / span))
        except (TypeError, ZeroDivisionError):
            return 0.5

    def selectivity_for_op(self, op: str, value: Any) -> float:
        """Return selectivity for a comparison predicate (col op value)."""
        if self.histogram is not None:
            return self.histogram.for_op(op, value)
        match op:
            case "=":
                return self.selectivity_eq(value)
            case "!=" | "<>":
                return 1.0 - self.selectivity_eq(value)
            case "<" | "<=":
                return self.selectivity_range(self.min_val, value) if self.min_val is not None else 0.3
            case ">" | ">=":
                return self.selectivity_range(value, self.max_val) if self.max_val is not None else 0.3
            case _:
                return 0.5


@dataclass
class TableStats:
    name: str
    row_count: int
    columns: list[ColumnStats] = field(default_factory=list)
    data: list[Row] = field(default_factory=list)

    def column(self, name: str) -> ColumnStats | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None


class Catalog:
    def __init__(self) -> None:
        self._tables: dict[str, TableStats] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, stats: TableStats) -> None:
        self._tables[stats.name] = stats

    def create_table(
        self,
        name: str,
        data: list[Row],
        *,
        estimated_rows: int | None = None,
        column_stats: list[ColumnStats] | None = None,
        build_histograms: bool = True,
        n_buckets: int = 20,
    ) -> TableStats:
        """Create and register a table.

        Histograms are built automatically from data unless
        build_histograms=False or column_stats are supplied explicitly.
        """
        n = estimated_rows if estimated_rows is not None else len(data)
        cols = column_stats or _derive_stats(data, build_histograms=build_histograms, n_buckets=n_buckets)
        stats = TableStats(name=name, row_count=n, columns=cols, data=data)
        self.register(stats)
        return stats

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def stats(self, name: str) -> TableStats:
        try:
            return self._tables[name]
        except KeyError:
            raise KeyError(f"Table {name!r} not registered in catalog") from None

    def data(self, name: str) -> list[Row]:
        return self.stats(name).data

    def update_row_count(self, name: str, actual: int) -> None:
        """Patch the estimated row count after observing actual cardinality."""
        self._tables[name].row_count = actual

    def tables(self) -> list[str]:
        return list(self._tables.keys())


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _derive_stats(
    data: list[Row],
    build_histograms: bool = True,
    n_buckets: int = 20,
) -> list[ColumnStats]:
    if not data:
        return []

    from .statistics import EquiDepthHistogram

    stats = []
    for col in data[0]:
        all_values = [r[col] for r in data]
        non_null = [v for v in all_values if v is not None]
        if not non_null:
            stats.append(ColumnStats(name=col, dtype="str"))
            continue

        sample = non_null[0]
        dtype = "int" if isinstance(sample, int) else "float" if isinstance(sample, float) else "str"

        try:
            min_v, max_v = min(non_null), max(non_null)
        except TypeError:
            min_v = max_v = None

        histogram = (
            EquiDepthHistogram(non_null, n_buckets=n_buckets)
            if build_histograms and len(non_null) >= 2
            else None
        )

        stats.append(
            ColumnStats(
                name=col,
                dtype=dtype,
                distinct_values=len(set(map(str, non_null))),
                min_val=min_v,
                max_val=max_v,
                null_frac=(len(data) - len(non_null)) / len(data),
                histogram=histogram,
            )
        )
    return stats

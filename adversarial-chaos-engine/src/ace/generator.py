"""Adversarial input generator.

For every column referenced by at least one invariant we generate
values drawn primarily from the relevant edge-case library; unreferenced
columns are filled with cheap fuzz. The bias toward
invariant-referenced columns is what makes this *targeted* instead of
random fuzzing.

A row is a ``dict[str, Any]``; a frame is a ``list[Row]``. The generator
returns a list of frames so the runner can amortise warmup across
trials.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ace.edges.numeric import numeric_edges
from ace.edges.strings import string_edges
from ace.edges.timestamps import timestamp_edges

if TYPE_CHECKING:
    from ace.invariants.catalog import Frame, Row


# Default columns we always populate (regardless of invariant references).
_DEFAULT_COLUMNS: tuple[str, ...] = ("id", "amount", "name", "ts")


@dataclass
class AdversarialGenerator:
    """Edge-case-biased generator for invariant-targeted columns."""

    edge_fraction: float = 0.75
    max_rows: int = 16
    rng: random.Random | None = None

    def __post_init__(self) -> None:
        if not 0.0 <= self.edge_fraction <= 1.0:
            raise ValueError("edge_fraction must be in [0, 1]")
        if self.max_rows < 0:
            raise ValueError("max_rows must be ≥ 0")
        if self.rng is None:
            self.rng = random.Random(0)

    # ------------------------------------------------------------------ API

    def generate(self, targeted_columns: set[str] | None = None) -> Frame:
        """Return one adversarial frame.

        ``targeted_columns`` are the columns at least one invariant
        references; we crank up their edge-case probability and pin the
        column names so they survive even when the invariant uses a
        non-default column name.
        """
        rng = self._rng
        targeted = set(targeted_columns or ()) | set(_DEFAULT_COLUMNS)
        n_rows = rng.randint(0, self.max_rows)
        rows: Frame = []
        for _ in range(n_rows):
            rows.append(self._row(targeted))
        return rows

    def generate_random(self) -> Frame:
        """Pure-random comparison baseline (no edge-case bias)."""
        rng = self._rng
        n_rows = rng.randint(0, self.max_rows)
        rows: Frame = []
        for _ in range(n_rows):
            rows.append(
                {
                    "id": rng.randint(-1000, 1000),
                    "amount": rng.uniform(-1000.0, 1000.0),
                    "name": f"u{rng.randint(0, 1_000)}",
                    "ts": rng.randint(0, 2_000_000_000),
                }
            )
        return rows

    # --------------------------------------------------------------- guts

    @property
    def _rng(self) -> random.Random:
        assert self.rng is not None  # narrowed for mypy via __post_init__
        return self.rng

    def _row(self, targeted: set[str]) -> Row:
        row: Row = {}
        for col in targeted:
            row[col] = self._value(col, edge_bias=self.edge_fraction)
        return row

    def _value(self, col: str, *, edge_bias: float) -> object:
        """Pick an edge case with prob ``edge_bias``, else random fuzz."""
        rng = self._rng
        use_edge = rng.random() < edge_bias
        kind = _column_kind(col)
        if kind == "string":
            pool = string_edges() if use_edge else [f"u{rng.randint(0, 1_000)}"]
            return rng.choice(pool)
        if kind == "timestamp":
            pool_ts = timestamp_edges() if use_edge else [rng.randint(0, 2_000_000_000)]
            return rng.choice(pool_ts)
        if kind == "id":
            return rng.randint(-1000, 1000)
        # default = numeric column
        pool_n = numeric_edges() if use_edge else [rng.uniform(-1000.0, 1000.0)]
        return rng.choice(pool_n)


def _column_kind(col: str) -> str:
    lc = col.lower()
    if lc == "id" or lc.endswith("_id"):
        return "id"
    if "ts" in lc or "time" in lc or "date" in lc:
        return "timestamp"
    if "name" in lc or "text" in lc or "label" in lc:
        return "string"
    return "numeric"


__all__ = ["AdversarialGenerator"]

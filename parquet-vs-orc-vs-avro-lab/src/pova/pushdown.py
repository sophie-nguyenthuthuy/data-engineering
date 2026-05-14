"""Predicate-pushdown reasoning over column statistics."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pova.stats.column import ColumnStats


class Op(str, Enum):
    """Comparison operators a pushdown predicate supports."""

    EQ = "eq"
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"
    NE = "ne"
    IS_NULL = "is_null"
    NOT_NULL = "not_null"


@dataclass(frozen=True, slots=True)
class Predicate:
    """A simple predicate ``column <op> value`` for pushdown reasoning."""

    column: str
    op: Op
    value: Any | None = None

    def __post_init__(self) -> None:
        if not self.column:
            raise ValueError("column must be non-empty")
        if self.op in (Op.IS_NULL, Op.NOT_NULL):
            if self.value is not None:
                raise ValueError(f"{self.op.value} predicate must not carry a value")
        elif self.value is None:
            raise ValueError(f"{self.op.value} predicate requires a non-None value")


def can_skip_row_group(predicate: Predicate, stats: ColumnStats) -> bool:
    """Return ``True`` when the predicate cannot possibly match anything in
    a row group whose column statistics are ``stats``."""
    # If the row group is empty, every selective predicate is unsatisfiable.
    if stats.n_rows == 0:
        return predicate.op not in (Op.IS_NULL, Op.NOT_NULL)
    if predicate.op == Op.IS_NULL:
        return stats.null_count == 0
    if predicate.op == Op.NOT_NULL:
        return stats.null_count == stats.n_rows
    # The remaining predicates compare against `stats.min` / `stats.max`.
    if stats.min is None or stats.max is None:
        # All values are NULL → no non-NULL match possible.
        return True
    v = predicate.value
    if predicate.op == Op.EQ:
        return bool(v < stats.min) or bool(v > stats.max)
    if predicate.op == Op.LT:
        return bool(v <= stats.min)
    if predicate.op == Op.LE:
        return bool(v < stats.min)
    if predicate.op == Op.GT:
        return bool(v >= stats.max)
    if predicate.op == Op.GE:
        return bool(v > stats.max)
    if predicate.op == Op.NE:
        # We can only skip when every value equals v, i.e. min == max == v.
        return bool(stats.min == stats.max == v)
    return False


__all__ = ["Op", "Predicate", "can_skip_row_group"]

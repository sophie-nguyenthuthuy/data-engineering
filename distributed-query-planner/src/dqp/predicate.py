"""Core predicate intermediate representation for federated query planning."""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator, List, Optional, Set


# ---------------------------------------------------------------------------
# Column reference and literal value
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnRef:
    """Reference to a column, optionally qualified by table name."""

    column: str
    table: Optional[str] = None

    def __str__(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column


@dataclass(frozen=True)
class Literal:
    """A typed literal value in a predicate."""

    value: Any
    dtype: str  # int, float, str, bool, date, datetime, null

    def __post_init__(self) -> None:
        valid_dtypes = {"int", "float", "str", "bool", "date", "datetime", "null"}
        if self.dtype not in valid_dtypes:
            raise ValueError(f"Invalid dtype {self.dtype!r}; must be one of {valid_dtypes}")

    def __str__(self) -> str:
        return repr(self.value)


# ---------------------------------------------------------------------------
# Comparison operators
# ---------------------------------------------------------------------------


class ComparisonOp(Enum):
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="


# ---------------------------------------------------------------------------
# Predicate base and compound constructors
# ---------------------------------------------------------------------------


class Predicate(ABC):
    """Abstract base for all predicate types."""

    def __and__(self, other: Predicate) -> AndPredicate:
        # Flatten nested ANDs eagerly
        lefts = list(conjuncts(self))
        rights = list(conjuncts(other))
        return AndPredicate(lefts + rights)

    def __or__(self, other: Predicate) -> OrPredicate:
        lefts = _or_operands(self)
        rights = _or_operands(other)
        return OrPredicate(lefts + rights)

    def __invert__(self) -> NotPredicate:
        return NotPredicate(self)


def _or_operands(pred: Predicate) -> List[Predicate]:
    if isinstance(pred, OrPredicate):
        return list(pred.predicates)
    return [pred]


# ---------------------------------------------------------------------------
# Concrete predicate types
# ---------------------------------------------------------------------------


@dataclass
class ComparisonPredicate(Predicate):
    """column op value, e.g. age > 30"""

    column: ColumnRef
    op: ComparisonOp
    value: Literal

    def __repr__(self) -> str:
        return f"({self.column} {self.op.value} {self.value})"


@dataclass
class InPredicate(Predicate):
    """column [NOT] IN (v1, v2, ...)"""

    column: ColumnRef
    values: List[Literal]
    negated: bool = False

    def __repr__(self) -> str:
        kw = "NOT IN" if self.negated else "IN"
        vals = ", ".join(str(v) for v in self.values)
        return f"({self.column} {kw} ({vals}))"


@dataclass
class BetweenPredicate(Predicate):
    """column [NOT] BETWEEN low AND high"""

    column: ColumnRef
    low: Literal
    high: Literal
    negated: bool = False

    def __repr__(self) -> str:
        kw = "NOT BETWEEN" if self.negated else "BETWEEN"
        return f"({self.column} {kw} {self.low} AND {self.high})"


@dataclass
class LikePredicate(Predicate):
    """column [NOT] LIKE 'pattern'"""

    column: ColumnRef
    pattern: str
    negated: bool = False

    def __repr__(self) -> str:
        kw = "NOT LIKE" if self.negated else "LIKE"
        return f"({self.column} {kw} '{self.pattern}')"


@dataclass
class IsNullPredicate(Predicate):
    """column IS [NOT] NULL"""

    column: ColumnRef
    negated: bool = False  # True → IS NOT NULL

    def __repr__(self) -> str:
        kw = "IS NOT NULL" if self.negated else "IS NULL"
        return f"({self.column} {kw})"


@dataclass
class AndPredicate(Predicate):
    """Conjunction of predicates."""

    predicates: List[Predicate]

    def __repr__(self) -> str:
        inner = " AND ".join(repr(p) for p in self.predicates)
        return f"({inner})"


@dataclass
class OrPredicate(Predicate):
    """Disjunction of predicates."""

    predicates: List[Predicate]

    def __repr__(self) -> str:
        inner = " OR ".join(repr(p) for p in self.predicates)
        return f"({inner})"


@dataclass
class NotPredicate(Predicate):
    """Logical negation of a predicate."""

    predicate: Predicate

    def __repr__(self) -> str:
        return f"(NOT {self.predicate!r})"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def conjuncts(pred: Predicate) -> List[Predicate]:
    """Flatten an AND tree into its leaf conjuncts.

    >>> a & b & c  →  [a, b, c]
    """
    if isinstance(pred, AndPredicate):
        result: List[Predicate] = []
        for child in pred.predicates:
            result.extend(conjuncts(child))
        return result
    return [pred]


def columns_referenced(pred: Predicate) -> Set[ColumnRef]:
    """Return the set of all ColumnRefs mentioned in a predicate tree."""
    if isinstance(pred, (ComparisonPredicate, InPredicate, BetweenPredicate,
                         LikePredicate, IsNullPredicate)):
        return {pred.column}
    elif isinstance(pred, (AndPredicate, OrPredicate)):
        result: Set[ColumnRef] = set()
        for child in pred.predicates:
            result |= columns_referenced(child)
        return result
    elif isinstance(pred, NotPredicate):
        return columns_referenced(pred.predicate)
    return set()


def negate(pred: Predicate) -> Predicate:
    """Push negation inward using De Morgan's laws.

    NOT (A AND B)  →  (NOT A) OR (NOT B)
    NOT (A OR B)   →  (NOT A) AND (NOT B)
    NOT (NOT A)    →  A
    NOT (col = v)  →  col != v
    etc.
    """
    if isinstance(pred, NotPredicate):
        # Double negation elimination
        return negate(negate(pred.predicate)) if isinstance(pred.predicate, NotPredicate) else pred.predicate

    if isinstance(pred, AndPredicate):
        # De Morgan: NOT (A AND B) = (NOT A) OR (NOT B)
        return OrPredicate([negate(child) for child in pred.predicates])

    if isinstance(pred, OrPredicate):
        # De Morgan: NOT (A OR B) = (NOT A) AND (NOT B)
        return AndPredicate([negate(child) for child in pred.predicates])

    if isinstance(pred, ComparisonPredicate):
        opposite = {
            ComparisonOp.EQ: ComparisonOp.NEQ,
            ComparisonOp.NEQ: ComparisonOp.EQ,
            ComparisonOp.LT: ComparisonOp.GTE,
            ComparisonOp.LTE: ComparisonOp.GT,
            ComparisonOp.GT: ComparisonOp.LTE,
            ComparisonOp.GTE: ComparisonOp.LT,
        }
        return ComparisonPredicate(pred.column, opposite[pred.op], pred.value)

    if isinstance(pred, InPredicate):
        return InPredicate(pred.column, pred.values, negated=not pred.negated)

    if isinstance(pred, BetweenPredicate):
        return BetweenPredicate(pred.column, pred.low, pred.high, negated=not pred.negated)

    if isinstance(pred, LikePredicate):
        return LikePredicate(pred.column, pred.pattern, negated=not pred.negated)

    if isinstance(pred, IsNullPredicate):
        return IsNullPredicate(pred.column, negated=not pred.negated)

    # Fallback: wrap in NotPredicate
    return NotPredicate(pred)

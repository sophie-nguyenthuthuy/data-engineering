"""Expressions: scalar AST used by Filter / Project / Join conditions.

Expressions form a closed algebra with a deterministic structural hash, used
both for memoization in the Cascades search and for ColumnRef pushdown
analysis (to compute which columns a predicate references).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Final

from ppc.ir.types import BOOLEAN, DOUBLE, INT64, STRING, DataType, promote

if TYPE_CHECKING:
    from ppc.ir.schema import Schema


class Expr:
    """Base class. Subclasses are frozen dataclasses for hashability."""

    @property
    def dtype(self) -> DataType:  # pragma: no cover - abstract
        raise NotImplementedError

    def referenced_columns(self) -> frozenset[str]:  # pragma: no cover
        raise NotImplementedError

    def evaluate_const(self) -> Any | None:
        """Constant-folding shortcut: return the folded value, or None.

        Only Literal returns a value; everything else returns None unless
        every child is a Literal.
        """
        return None

    def __repr__(self) -> str:  # pragma: no cover - subclasses override
        return self.__class__.__name__


# ---------------------------------------------------------------------------
# Leaves
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, eq=True)
class Literal(Expr):
    value: int | float | str | bool | None
    _dtype: DataType

    @property
    def dtype(self) -> DataType:
        return self._dtype

    def referenced_columns(self) -> frozenset[str]:
        return frozenset()

    def evaluate_const(self) -> Any:
        return self.value

    def __repr__(self) -> str:
        if isinstance(self.value, str):
            return repr(self.value)
        return str(self.value)


def lit(value: int | float | str | bool | None) -> Literal:
    """Construct a Literal with inferred type."""
    if isinstance(value, bool):
        dtype: DataType = BOOLEAN
    elif isinstance(value, int):
        dtype = INT64
    elif isinstance(value, float):
        dtype = DOUBLE
    elif isinstance(value, str):
        dtype = STRING
    elif value is None:
        # NULL: use BOOLEAN with nullable=True as the most permissive choice
        dtype = BOOLEAN.with_nullable(True)
    else:
        raise TypeError(f"unsupported literal: {type(value).__name__}")
    return Literal(value=value, _dtype=dtype)


@dataclass(frozen=True, slots=True, eq=True)
class ColumnRef(Expr):
    name: str
    _dtype: DataType

    @property
    def dtype(self) -> DataType:
        return self._dtype

    def referenced_columns(self) -> frozenset[str]:
        return frozenset({self.name})

    def __repr__(self) -> str:
        return self.name


def column(name: str, dtype: DataType) -> ColumnRef:
    return ColumnRef(name=name, _dtype=dtype)


def column_from_schema(name: str, schema: Schema) -> ColumnRef:
    """Resolve a column against a schema; raises if missing."""
    col = schema[name]
    return ColumnRef(name=name, _dtype=col.dtype)


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------

ARITH_OPS: Final[frozenset[str]] = frozenset({"+", "-", "*", "/", "%"})
COMPARE_OPS: Final[frozenset[str]] = frozenset({"=", "!=", "<", "<=", ">", ">="})
LOGIC_OPS: Final[frozenset[str]] = frozenset({"AND", "OR"})


@dataclass(frozen=True, slots=True, eq=True)
class BinaryOp(Expr):
    op: str
    left: Expr
    right: Expr

    @cached_property
    def dtype(self) -> DataType:  # type: ignore[override]
        if self.op in COMPARE_OPS or self.op in LOGIC_OPS:
            return BOOLEAN
        return promote(self.left.dtype, self.right.dtype)

    def referenced_columns(self) -> frozenset[str]:
        return self.left.referenced_columns() | self.right.referenced_columns()

    def evaluate_const(self) -> Any | None:
        a = self.left.evaluate_const()
        b = self.right.evaluate_const()
        if a is None or b is None:
            # ColumnRef returns None; we can't fold
            if not isinstance(self.left, Literal) or not isinstance(self.right, Literal):
                return None
        return _apply(self.op, a, b)

    def __repr__(self) -> str:
        return f"({self.left} {self.op} {self.right})"


@dataclass(frozen=True, slots=True, eq=True)
class UnaryOp(Expr):
    op: str
    operand: Expr

    @cached_property
    def dtype(self) -> DataType:  # type: ignore[override]
        if self.op == "NOT":
            return BOOLEAN
        return self.operand.dtype

    def referenced_columns(self) -> frozenset[str]:
        return self.operand.referenced_columns()

    def __repr__(self) -> str:
        return f"({self.op} {self.operand})"


def _apply(op: str, a: Any, b: Any) -> Any:
    if op == "+":
        return a + b
    if op == "-":
        return a - b
    if op == "*":
        return a * b
    if op == "/":
        return a / b
    if op == "%":
        return a % b
    if op == "=":
        return a == b
    if op == "!=":
        return a != b
    if op == "<":
        return a < b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    if op == ">=":
        return a >= b
    if op == "AND":
        return bool(a) and bool(b)
    if op == "OR":
        return bool(a) or bool(b)
    raise ValueError(f"unknown op: {op}")


# Pythonic operator overloads for ergonomic test construction:
#   c1 = column("a", INT32); c1 + lit(1)


def _binop(op: str) -> Any:
    def _impl(self: Expr, other: object) -> BinaryOp:
        if not isinstance(other, Expr):
            other = lit(other)  # type: ignore[arg-type]
        return BinaryOp(op=op, left=self, right=other)

    return _impl


# Install operator overloads on Expr. We can't override __eq__/__hash__ here
# because they conflict with @dataclass(frozen=True, eq=True) on subclasses
# and would break memoization-by-equality. Use `.eq(other)` / `.ne(other)`
# instead. Comparisons & arithmetic produce BinaryOp without touching __eq__.
for _op, _name in (
    ("+", "__add__"),
    ("-", "__sub__"),
    ("*", "__mul__"),
    ("/", "__truediv__"),
    ("%", "__mod__"),
    ("<", "__lt__"),
    ("<=", "__le__"),
    (">", "__gt__"),
    (">=", "__ge__"),
):
    setattr(Expr, _name, _binop(_op))


# Explicit non-operator forms for == / != to avoid colliding with dataclass __eq__
def expr_eq(self: Expr, other: object) -> BinaryOp:
    if not isinstance(other, Expr):
        other = lit(other)  # type: ignore[arg-type]
    return BinaryOp(op="=", left=self, right=other)


def expr_ne(self: Expr, other: object) -> BinaryOp:
    if not isinstance(other, Expr):
        other = lit(other)  # type: ignore[arg-type]
    return BinaryOp(op="!=", left=self, right=other)


Expr.eq = expr_eq
Expr.ne = expr_ne


def AND(left: Expr, right: Expr) -> BinaryOp:
    return BinaryOp(op="AND", left=left, right=right)


def OR(left: Expr, right: Expr) -> BinaryOp:
    return BinaryOp(op="OR", left=left, right=right)


def NOT(operand: Expr) -> UnaryOp:
    return UnaryOp(op="NOT", operand=operand)

"""Expression tree: nodes that evaluate to an Arrow array over a RecordBatch."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.compute as pc


class Expr(ABC):
    @abstractmethod
    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        ...

    @abstractmethod
    def columns_used(self) -> set[str]:
        ...

    def __repr__(self) -> str:
        return self.__class__.__name__


# ---------------------------------------------------------------------------
# Leaf nodes
# ---------------------------------------------------------------------------

@dataclass
class ColumnRef(Expr):
    name: str

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        return batch.column(self.name)

    def columns_used(self) -> set[str]:
        return {self.name}

    def __repr__(self) -> str:
        return self.name


@dataclass
class Literal(Expr):
    value: Any
    dtype: Optional[pa.DataType] = None

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        if self.dtype:
            return pa.array([self.value] * len(batch), type=self.dtype)
        return pa.array([self.value] * len(batch))

    def columns_used(self) -> set[str]:
        return set()

    def __repr__(self) -> str:
        return repr(self.value)


# ---------------------------------------------------------------------------
# Binary operators
# ---------------------------------------------------------------------------

_ARITH_OPS = {
    "+": pc.add,
    "-": pc.subtract,
    "*": pc.multiply,
    "/": pc.divide,
}

_CMP_OPS = {
    "=": pc.equal,
    "!=": pc.not_equal,
    "<>": pc.not_equal,
    "<": pc.less,
    "<=": pc.less_equal,
    ">": pc.greater,
    ">=": pc.greater_equal,
}

_BOOL_OPS = {
    "AND": pc.and_,
    "OR": pc.or_,
}


@dataclass
class BinaryExpr(Expr):
    op: str
    left: Expr
    right: Expr

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        lv = self.left.eval(batch)
        rv = self.right.eval(batch)
        op = self.op.upper()
        if op in _ARITH_OPS:
            return _ARITH_OPS[op](lv, rv)
        if op in _CMP_OPS:
            return _CMP_OPS[op](lv, rv)
        if op in _BOOL_OPS:
            return _BOOL_OPS[op](lv, rv)
        if op == "LIKE":
            pattern = rv[0].as_py() if isinstance(rv, pa.Array) else rv.as_py()
            return pc.match_like(lv, pattern)
        raise ValueError(f"Unknown binary op: {self.op!r}")

    def columns_used(self) -> set[str]:
        return self.left.columns_used() | self.right.columns_used()

    def __repr__(self) -> str:
        return f"({self.left!r} {self.op} {self.right!r})"


@dataclass
class UnaryExpr(Expr):
    op: str   # "NOT" | "-"
    expr: Expr

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        v = self.expr.eval(batch)
        if self.op == "NOT":
            return pc.invert(v)
        if self.op == "-":
            return pc.negate(v)
        raise ValueError(f"Unknown unary op: {self.op!r}")

    def columns_used(self) -> set[str]:
        return self.expr.columns_used()


@dataclass
class IsNullExpr(Expr):
    expr: Expr
    negated: bool = False

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        v = self.expr.eval(batch)
        result = pc.is_null(v)
        return pc.invert(result) if self.negated else result

    def columns_used(self) -> set[str]:
        return self.expr.columns_used()


@dataclass
class InExpr(Expr):
    expr: Expr
    values: List[Any]
    negated: bool = False

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        v = self.expr.eval(batch)
        result = pc.is_in(v, value_set=pa.array(self.values))
        return pc.invert(result) if self.negated else result

    def columns_used(self) -> set[str]:
        return self.expr.columns_used()


@dataclass
class BetweenExpr(Expr):
    expr: Expr
    low: Expr
    high: Expr
    negated: bool = False

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        v = self.expr.eval(batch)
        lo = self.low.eval(batch)
        hi = self.high.eval(batch)
        result = pc.and_(pc.greater_equal(v, lo), pc.less_equal(v, hi))
        return pc.invert(result) if self.negated else result

    def columns_used(self) -> set[str]:
        return self.expr.columns_used() | self.low.columns_used() | self.high.columns_used()


@dataclass
class CastExpr(Expr):
    expr: Expr
    to_type: pa.DataType

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        return pc.cast(self.expr.eval(batch), self.to_type)

    def columns_used(self) -> set[str]:
        return self.expr.columns_used()


# ---------------------------------------------------------------------------
# Aggregate expressions (evaluated by the aggregate operator, not per-row)
# ---------------------------------------------------------------------------

@dataclass
class AggExpr(Expr):
    func: str          # count_star | count | sum | avg | min | max
    expr: Optional[Expr] = None
    alias: Optional[str] = None
    distinct: bool = False

    def eval(self, batch: pa.RecordBatch) -> pa.Array:
        raise NotImplementedError("AggExpr is evaluated by the aggregate operator")

    def partial(self, batch: pa.RecordBatch) -> Any:
        """Return a Python scalar for one batch."""
        if self.func == "count_star":
            return pc.count(batch.column(batch.schema.names[0])).as_py()
        v = self.expr.eval(batch)
        if self.func == "count":
            return pc.count(v).as_py()
        if self.func == "sum":
            return pc.sum(v).as_py()
        if self.func == "avg":
            return (pc.sum(v).as_py(), pc.count(v).as_py())
        if self.func == "min":
            return pc.min(v).as_py()
        if self.func == "max":
            return pc.max(v).as_py()
        raise ValueError(f"Unknown aggregate: {self.func}")

    def merge(self, a: Any, b: Any) -> Any:
        if self.func in ("count_star", "count", "sum"):
            av = 0 if a is None else a
            bv = 0 if b is None else b
            return av + bv
        if self.func == "avg":
            return (
                (a[0] or 0) + (b[0] or 0),
                (a[1] or 0) + (b[1] or 0),
            )
        if self.func == "min":
            if a is None:
                return b
            if b is None:
                return a
            return min(a, b)
        if self.func == "max":
            if a is None:
                return b
            if b is None:
                return a
            return max(a, b)
        raise ValueError(f"Unknown aggregate: {self.func}")

    def finalize(self, state: Any) -> Any:
        if self.func == "avg":
            s, c = state
            return (s / c) if c else None
        return state

    def columns_used(self) -> set[str]:
        return self.expr.columns_used() if self.expr else set()

    @property
    def output_name(self) -> str:
        if self.alias:
            return self.alias
        if self.func == "count_star":
            return "count(*)"
        return f"{self.func}({self.expr!r})"

    def __repr__(self) -> str:
        return self.output_name


def split_conjuncts(expr: Expr) -> List[Expr]:
    """Flatten an AND tree into a list of conjuncts."""
    if isinstance(expr, BinaryExpr) and expr.op.upper() == "AND":
        return split_conjuncts(expr.left) + split_conjuncts(expr.right)
    return [expr]


def conjuncts_to_expr(conjuncts: List[Expr]) -> Optional[Expr]:
    if not conjuncts:
        return None
    result = conjuncts[0]
    for c in conjuncts[1:]:
        result = BinaryExpr("AND", result, c)
    return result

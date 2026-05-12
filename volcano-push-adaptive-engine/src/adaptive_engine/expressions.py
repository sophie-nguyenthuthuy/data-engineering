"""Expression and predicate DSL for query plan nodes."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any

Row = dict[str, Any]


class Expr(ABC):
    @abstractmethod
    def eval(self, row: Row) -> Any: ...

    def __and__(self, other: "Expr") -> "AndExpr":
        return AndExpr(self, other)

    def __or__(self, other: "Expr") -> "OrExpr":
        return OrExpr(self, other)

    def __invert__(self) -> "NotExpr":
        return NotExpr(self)


class ColRef(Expr):
    def __init__(self, name: str) -> None:
        self.name = name

    def eval(self, row: Row) -> Any:
        return row[self.name]

    def __repr__(self) -> str:
        return self.name


class Literal(Expr):
    def __init__(self, value: Any) -> None:
        self.value = value

    def eval(self, row: Row) -> Any:
        return self.value

    def __repr__(self) -> str:
        return repr(self.value)


class BinOp(Expr):
    _OPS = {
        "=": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "<": lambda a, b: a < b,
        ">": lambda a, b: a > b,
        "<=": lambda a, b: a <= b,
        ">=": lambda a, b: a >= b,
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a / b,
    }

    def __init__(self, left: Expr, op: str, right: Expr) -> None:
        if op not in self._OPS:
            raise ValueError(f"Unknown operator: {op!r}")
        self.left = left
        self.op = op
        self.right = right

    def eval(self, row: Row) -> Any:
        return self._OPS[self.op](self.left.eval(row), self.right.eval(row))

    def __repr__(self) -> str:
        return f"({self.left} {self.op} {self.right})"


class AndExpr(Expr):
    def __init__(self, *preds: Expr) -> None:
        self.preds = preds

    def eval(self, row: Row) -> bool:
        return all(p.eval(row) for p in self.preds)

    def __repr__(self) -> str:
        return "(" + " AND ".join(map(repr, self.preds)) + ")"


class OrExpr(Expr):
    def __init__(self, *preds: Expr) -> None:
        self.preds = preds

    def eval(self, row: Row) -> bool:
        return any(p.eval(row) for p in self.preds)

    def __repr__(self) -> str:
        return "(" + " OR ".join(map(repr, self.preds)) + ")"


class NotExpr(Expr):
    def __init__(self, pred: Expr) -> None:
        self.pred = pred

    def eval(self, row: Row) -> bool:
        return not self.pred.eval(row)

    def __repr__(self) -> str:
        return f"NOT({self.pred})"


# Convenience constructors
def col(name: str) -> ColRef:
    return ColRef(name)


def lit(value: Any) -> Literal:
    return Literal(value)


def eq(col_name: str, value: Any) -> BinOp:
    return BinOp(ColRef(col_name), "=", Literal(value))


def gt(col_name: str, value: Any) -> BinOp:
    return BinOp(ColRef(col_name), ">", Literal(value))


def lt(col_name: str, value: Any) -> BinOp:
    return BinOp(ColRef(col_name), "<", Literal(value))


def gte(col_name: str, value: Any) -> BinOp:
    return BinOp(ColRef(col_name), ">=", Literal(value))


def lte(col_name: str, value: Any) -> BinOp:
    return BinOp(ColRef(col_name), "<=", Literal(value))

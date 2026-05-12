"""Logical plan nodes produced by the parser and transformed by the optimizer."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .expressions import Expr, AggExpr


class LogicalPlan:
    def schema_names(self) -> list[str]:
        raise NotImplementedError

    def children(self) -> list[LogicalPlan]:
        return []

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        name = self.__class__.__name__
        lines = [f"{prefix}{name}"]
        for child in self.children():
            lines.append(child.pretty(indent + 1))
        return "\n".join(lines)


@dataclass
class Scan(LogicalPlan):
    table: str
    columns: Optional[List[str]] = None          # None = all columns
    pushed_predicates: List[Expr] = field(default_factory=list)

    def schema_names(self) -> list[str]:
        return self.columns or []

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        cols = self.columns or ["*"]
        pred = f" WHERE {self.pushed_predicates}" if self.pushed_predicates else ""
        return f"{prefix}Scan({self.table}, cols={cols}{pred})"


@dataclass
class Filter(LogicalPlan):
    child: LogicalPlan
    predicate: Expr

    def schema_names(self) -> list[str]:
        return self.child.schema_names()

    def children(self) -> list[LogicalPlan]:
        return [self.child]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        lines = [f"{prefix}Filter({self.predicate!r})"]
        lines.append(self.child.pretty(indent + 1))
        return "\n".join(lines)


@dataclass
class Project(LogicalPlan):
    child: LogicalPlan
    exprs: List[Expr]
    aliases: List[Optional[str]] = field(default_factory=list)

    def output_names(self) -> list[str]:
        out = []
        for i, e in enumerate(self.exprs):
            alias = self.aliases[i] if i < len(self.aliases) else None
            if alias:
                out.append(alias)
            else:
                out.append(repr(e))
        return out

    def schema_names(self) -> list[str]:
        return self.output_names()

    def children(self) -> list[LogicalPlan]:
        return [self.child]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        names = self.output_names()
        lines = [f"{prefix}Project({names})"]
        lines.append(self.child.pretty(indent + 1))
        return "\n".join(lines)


@dataclass
class Aggregate(LogicalPlan):
    child: LogicalPlan
    group_by: List[Expr]
    aggregates: List[AggExpr]
    having: Optional[Expr] = None

    def schema_names(self) -> list[str]:
        keys = [repr(e) for e in self.group_by]
        aggs = [a.output_name for a in self.aggregates]
        return keys + aggs

    def children(self) -> list[LogicalPlan]:
        return [self.child]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        keys = [repr(e) for e in self.group_by]
        aggs = [a.output_name for a in self.aggregates]
        lines = [f"{prefix}Aggregate(keys={keys}, aggs={aggs})"]
        lines.append(self.child.pretty(indent + 1))
        return "\n".join(lines)


@dataclass
class Sort(LogicalPlan):
    child: LogicalPlan
    keys: List[Expr]
    ascending: List[bool] = field(default_factory=list)

    def schema_names(self) -> list[str]:
        return self.child.schema_names()

    def children(self) -> list[LogicalPlan]:
        return [self.child]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        orders = [("ASC" if a else "DESC") for a in self.ascending]
        keys = [f"{repr(k)} {o}" for k, o in zip(self.keys, orders)]
        lines = [f"{prefix}Sort({keys})"]
        lines.append(self.child.pretty(indent + 1))
        return "\n".join(lines)


@dataclass
class Limit(LogicalPlan):
    child: LogicalPlan
    n: int
    offset: int = 0

    def schema_names(self) -> list[str]:
        return self.child.schema_names()

    def children(self) -> list[LogicalPlan]:
        return [self.child]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        lines = [f"{prefix}Limit({self.n}, offset={self.offset})"]
        lines.append(self.child.pretty(indent + 1))
        return "\n".join(lines)


@dataclass
class Join(LogicalPlan):
    left: LogicalPlan
    right: LogicalPlan
    condition: Expr
    join_type: str = "INNER"   # INNER | LEFT | RIGHT

    def schema_names(self) -> list[str]:
        return self.left.schema_names() + self.right.schema_names()

    def children(self) -> list[LogicalPlan]:
        return [self.left, self.right]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        lines = [f"{prefix}Join({self.join_type} ON {self.condition!r})"]
        lines.append(self.left.pretty(indent + 1))
        lines.append(self.right.pretty(indent + 1))
        return "\n".join(lines)

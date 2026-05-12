"""Logical plan operators."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LogicalOp:
    """Base. Subclasses define op-specific fields."""
    kind: str
    children: list = field(default_factory=list)

    def __repr__(self):
        c = ",".join(repr(ch) for ch in self.children)
        return f"{self.kind}({c})"


@dataclass
class Source(LogicalOp):
    table: str = ""
    estimated_rows: int = 1_000_000

    def __init__(self, table: str, estimated_rows: int = 1_000_000):
        super().__init__(kind="source")
        self.table = table
        self.estimated_rows = estimated_rows

    def __repr__(self):
        return f"source({self.table})"


@dataclass
class Filter(LogicalOp):
    predicate: str = ""
    selectivity: float = 0.1

    def __init__(self, child: LogicalOp, predicate: str, selectivity: float = 0.1):
        super().__init__(kind="filter", children=[child])
        self.predicate = predicate
        self.selectivity = selectivity

    def __repr__(self):
        return f"filter({self.predicate}, {self.children[0]})"


@dataclass
class Aggregate(LogicalOp):
    group_by: tuple = ()
    aggs: tuple = ()

    def __init__(self, child: LogicalOp, group_by, aggs):
        super().__init__(kind="aggregate", children=[child])
        self.group_by = tuple(group_by)
        self.aggs = tuple(aggs)

    def __repr__(self):
        return f"aggregate({self.group_by}, {self.children[0]})"


@dataclass
class Join(LogicalOp):
    join_key: tuple = ()

    def __init__(self, left: LogicalOp, right: LogicalOp, join_key):
        super().__init__(kind="join", children=[left, right])
        self.join_key = tuple(join_key)

    def __repr__(self):
        return f"join({self.children[0]}, {self.children[1]})"


__all__ = ["LogicalOp", "Source", "Filter", "Aggregate", "Join"]

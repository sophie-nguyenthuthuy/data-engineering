"""Base operator for the IVM dataflow graph.

Operators form a DAG.  Each operator:
  1. Receives a batch of Updates from upstream via handle().
  2. Computes an output batch via process() (pure, override in subclasses).
  3. Forwards the output batch to all downstream listeners.

The fluent builder methods (filter, project, group_by, window, join) let you
construct pipelines without manually wiring listeners.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ivm.types import Update


class Operator(ABC):

    def __init__(self):
        self._listeners: List[Callable[[List["Update"]], None]] = []
        self.name: Optional[str] = None

    # ------------------------------------------------------------------
    # Subclasses implement this
    # ------------------------------------------------------------------

    @abstractmethod
    def process(self, updates: List["Update"]) -> List["Update"]:
        """Transform input updates into output updates (pure computation)."""
        ...

    # ------------------------------------------------------------------
    # Plumbing
    # ------------------------------------------------------------------

    def handle(self, updates: List["Update"]) -> None:
        """Entry point: called by upstream operators or the engine."""
        if not updates:
            return
        out = self.process(updates)
        if out:
            self._emit(out)

    def _emit(self, updates: List["Update"]) -> None:
        for fn in self._listeners:
            fn(updates)

    def add_listener(self, fn: Callable[[List["Update"]], None]) -> None:
        self._listeners.append(fn)

    # ------------------------------------------------------------------
    # Fluent builder API
    # ------------------------------------------------------------------

    def pipe(self, op: "Operator") -> "Operator":
        """Wire self → op and return op for chaining."""
        self.add_listener(op.handle)
        return op

    def filter(self, predicate: Callable) -> "Operator":
        from ivm.operators.filter import FilterOperator
        return self.pipe(FilterOperator(predicate))

    def project(self, columns: Optional[List[str]] = None,
                transform: Optional[Callable] = None) -> "Operator":
        from ivm.operators.project import ProjectOperator
        return self.pipe(ProjectOperator(columns, transform))

    def group_by(self, key_columns: List[str], aggregates: dict) -> "Operator":
        from ivm.operators.group_by import GroupByOperator
        return self.pipe(GroupByOperator(key_columns, aggregates))

    def window(self, window_spec, aggregates: Optional[dict] = None,
               rank_fns: Optional[dict] = None) -> "Operator":
        from ivm.operators.window import WindowOperator
        return self.pipe(WindowOperator(window_spec, aggregates or {}, rank_fns or {}))

    def join(self, right: "Operator", left_key, right_key,
             join_type: str = "inner") -> "Operator":
        from ivm.operators.join import JoinOperator
        op = JoinOperator(left_key, right_key, join_type)
        self.add_listener(op.handle_left)
        right.add_listener(op.handle_right)
        return op

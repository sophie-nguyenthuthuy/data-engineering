"""Abstract engine base class and shared engine types."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, List, Set

from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    NotPredicate,
    OrPredicate,
    Predicate,
)


class EngineCapability(Enum):
    """Predicate types that an engine can evaluate natively."""

    COMPARISON = auto()
    IN = auto()
    BETWEEN = auto()
    LIKE = auto()
    IS_NULL = auto()
    AND = auto()
    OR = auto()
    NOT = auto()


@dataclass
class PushdownResult:
    """Result of attempting predicate pushdown.

    *pushed* predicates were translated to *native_filter* for engine-side evaluation.
    *residual* predicates must be evaluated by the query planner after the scan.
    """

    pushed: List[Predicate]
    residual: List[Predicate]
    native_filter: Any  # engine-specific: dict for Mongo, expression for Parquet, str for Postgres


class EngineBase(ABC):
    """Abstract base for all storage engines."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique engine identifier."""
        ...

    @property
    @abstractmethod
    def capabilities(self) -> Set[EngineCapability]:
        """Set of predicate types this engine can push down."""
        ...

    def can_push(self, pred: Predicate) -> bool:
        """Return True if *pred* can be fully pushed into this engine."""
        caps = self.capabilities

        if isinstance(pred, ComparisonPredicate):
            return EngineCapability.COMPARISON in caps

        if isinstance(pred, InPredicate):
            return EngineCapability.IN in caps

        if isinstance(pred, BetweenPredicate):
            return EngineCapability.BETWEEN in caps

        if isinstance(pred, LikePredicate):
            return EngineCapability.LIKE in caps

        if isinstance(pred, IsNullPredicate):
            return EngineCapability.IS_NULL in caps

        if isinstance(pred, AndPredicate):
            return EngineCapability.AND in caps and all(self.can_push(c) for c in pred.predicates)

        if isinstance(pred, OrPredicate):
            return EngineCapability.OR in caps and all(self.can_push(c) for c in pred.predicates)

        if isinstance(pred, NotPredicate):
            return EngineCapability.NOT in caps and self.can_push(pred.predicate)

        return False

    def pushdown_predicates(self, predicates: List[Predicate]) -> PushdownResult:
        """Split predicates into pushed (native) and residual (Python-side).

        Each predicate is independently tested via *can_push*. Pushed predicates
        are combined and translated via *translate_predicate*; residual predicates
        are returned as-is for post-scan filtering.
        """
        pushed: List[Predicate] = []
        residual: List[Predicate] = []

        for pred in predicates:
            if self.can_push(pred):
                pushed.append(pred)
            else:
                residual.append(pred)

        native_filter: Any = None
        if pushed:
            if len(pushed) == 1:
                native_filter = self.translate_predicate(pushed[0])
            else:
                native_filter = self.translate_predicate(AndPredicate(pushed))

        return PushdownResult(pushed=pushed, residual=residual, native_filter=native_filter)

    @abstractmethod
    def translate_predicate(self, pred: Predicate) -> Any:
        """Translate a predicate to the engine's native filter representation."""
        ...

    @abstractmethod
    def execute_scan(
        self, table_name: str, pushed_result: PushdownResult, columns: List[str]
    ) -> Any:
        """Execute a scan with the given pushdown result and return rows."""
        ...

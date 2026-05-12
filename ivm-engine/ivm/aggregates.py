"""Incremental aggregate functions.

Each Aggregate exposes:
  initial_state()  -> opaque state value
  add(state, value, diff) -> new state   (diff is +1 or -1)
  result(state)    -> the aggregate value to emit
  is_empty(state)  -> True when the group has zero rows (should be omitted)

All aggregates correctly handle retractions (diff = -1) because they track
full multiplicity, not just presence.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from typing import Any, Dict, Optional


class Aggregate(ABC):
    @abstractmethod
    def initial_state(self) -> Any: ...

    @abstractmethod
    def add(self, state: Any, value: Any, diff: int) -> Any: ...

    @abstractmethod
    def result(self, state: Any) -> Any: ...

    def is_empty(self, state: Any) -> bool:
        return False


# ---------------------------------------------------------------------------
# COUNT(*)
# ---------------------------------------------------------------------------

class Count(Aggregate):
    def initial_state(self) -> int:
        return 0

    def add(self, state: int, value: Any, diff: int) -> int:
        return state + diff

    def result(self, state: int) -> int:
        return max(0, state)

    def is_empty(self, state: int) -> bool:
        return state <= 0


# ---------------------------------------------------------------------------
# SUM(column)
# ---------------------------------------------------------------------------

class Sum(Aggregate):
    def __init__(self, column: str):
        self.column = column

    def initial_state(self) -> Dict:
        return {"sum": 0, "count": 0}

    def add(self, state: Dict, value: Any, diff: int) -> Dict:
        return {"sum": state["sum"] + diff * value, "count": state["count"] + diff}

    def result(self, state: Dict) -> Optional[float]:
        return state["sum"] if state["count"] > 0 else None

    def is_empty(self, state: Dict) -> bool:
        return state["count"] <= 0


# ---------------------------------------------------------------------------
# AVG(column)
# ---------------------------------------------------------------------------

class Avg(Aggregate):
    def __init__(self, column: str):
        self.column = column

    def initial_state(self) -> Dict:
        return {"sum": 0.0, "count": 0}

    def add(self, state: Dict, value: Any, diff: int) -> Dict:
        return {"sum": state["sum"] + diff * value, "count": state["count"] + diff}

    def result(self, state: Dict) -> Optional[float]:
        return state["sum"] / state["count"] if state["count"] > 0 else None

    def is_empty(self, state: Dict) -> bool:
        return state["count"] <= 0


# ---------------------------------------------------------------------------
# MIN / MAX — use a counter so retractions work correctly
# ---------------------------------------------------------------------------

class Min(Aggregate):
    """MIN with retraction support: tracks value → multiplicity."""

    def __init__(self, column: str):
        self.column = column

    def initial_state(self) -> Counter:
        return Counter()

    def add(self, state: Counter, value: Any, diff: int) -> Counter:
        nxt = Counter(state)
        nxt[value] += diff
        if nxt[value] <= 0:
            del nxt[value]
        return nxt

    def result(self, state: Counter) -> Optional[Any]:
        return min(state.keys()) if state else None

    def is_empty(self, state: Counter) -> bool:
        return not state


class Max(Aggregate):
    def __init__(self, column: str):
        self.column = column

    def initial_state(self) -> Counter:
        return Counter()

    def add(self, state: Counter, value: Any, diff: int) -> Counter:
        nxt = Counter(state)
        nxt[value] += diff
        if nxt[value] <= 0:
            del nxt[value]
        return nxt

    def result(self, state: Counter) -> Optional[Any]:
        return max(state.keys()) if state else None

    def is_empty(self, state: Counter) -> bool:
        return not state


# ---------------------------------------------------------------------------
# COUNT(DISTINCT column)
# ---------------------------------------------------------------------------

class CountDistinct(Aggregate):
    def __init__(self, column: str):
        self.column = column

    def initial_state(self) -> Counter:
        return Counter()

    def add(self, state: Counter, value: Any, diff: int) -> Counter:
        nxt = Counter(state)
        nxt[value] += diff
        if nxt[value] <= 0:
            del nxt[value]
        return nxt

    def result(self, state: Counter) -> int:
        return len(state)

    def is_empty(self, state: Counter) -> bool:
        return not state


# ---------------------------------------------------------------------------
# Convenience aliases
# ---------------------------------------------------------------------------

count = Count
count_distinct = CountDistinct


def sum_(column: str) -> Sum:
    return Sum(column)


def avg(column: str) -> Avg:
    return Avg(column)


def min_(column: str) -> Min:
    return Min(column)


def max_(column: str) -> Max:
    return Max(column)

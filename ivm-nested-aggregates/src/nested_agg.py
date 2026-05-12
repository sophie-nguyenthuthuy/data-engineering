"""IVM for nested aggregates: MAX(SUM(...) GROUP BY).

Example: SELECT MAX(daily_total) FROM (SELECT date, SUM(amount) FROM tx GROUP BY date)

Inner: per-date running sum.
Outer: MAX over the inner table. We track which date currently holds the MAX
to avoid O(n) recompute on every insert.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class MaxOfSum:
    _sums: dict = field(default_factory=dict)        # date -> sum
    _max_date: object = field(default=None)
    _max_value: float = field(default=float("-inf"))

    def insert(self, date, amount: float) -> tuple[float, object]:
        """Insert amount for date. Return (new_max_value, max_date)."""
        old = self._sums.get(date, 0.0)
        new = old + amount
        self._sums[date] = new
        # Maintain max
        if new > self._max_value:
            self._max_value = new
            self._max_date = date
        elif date == self._max_date and new < self._max_value:
            # Current max was THIS date; its sum decreased — recompute
            self._recompute_max()
        return self._max_value, self._max_date

    def delete(self, date, amount: float) -> tuple[float, object]:
        old = self._sums.get(date, 0.0)
        new = old - amount
        if new == 0:
            self._sums.pop(date, None)
        else:
            self._sums[date] = new
        if date == self._max_date:
            self._recompute_max()
        return self._max_value, self._max_date

    def _recompute_max(self) -> None:
        if not self._sums:
            self._max_value = float("-inf")
            self._max_date = None
            return
        best_k = max(self._sums, key=lambda k: self._sums[k])
        self._max_date = best_k
        self._max_value = self._sums[best_k]

    @property
    def max(self) -> tuple[float, object]:
        return self._max_value, self._max_date


__all__ = ["MaxOfSum"]

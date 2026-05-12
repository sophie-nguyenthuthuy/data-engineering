"""IVM for correlated subqueries (SQL example):

    SELECT * FROM orders o
    WHERE o.amount > (SELECT AVG(amount) FROM orders WHERE cust = o.cust)

Equivalent rewrite:
    WITH per_cust AS (SELECT cust, AVG(amount) AS avg FROM orders GROUP BY cust)
    SELECT o.* FROM orders o
    JOIN per_cust c ON o.cust = c.cust
    WHERE o.amount > c.avg

The CTE is flat-IVM-friendly. We maintain per-key AVG and the membership
set of qualifying rows.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from ivm.correlated.per_key_agg import PerKeyAvg


@dataclass(frozen=True, slots=True)
class _Row:
    customer: Any
    amount: float


@dataclass
class _Changes:
    added: list[_Row] = field(default_factory=list)
    removed: list[_Row] = field(default_factory=list)


class CorrelatedSubqueryIVM:
    """Tracks rows where amount > per-customer AVG(amount)."""

    def __init__(self) -> None:
        self.avgs = PerKeyAvg()
        self._all_rows: list[_Row] = []
        self._qualifying: set[tuple[Any, float, int]] = set()  # (cust, amount, slot)
        # We tag each row with a slot so duplicate amounts stay distinct
        self._next_slot = 0
        self._row_slots: dict[int, _Row] = {}
        self._lock = threading.RLock()

    def insert(self, customer: Any, amount: float) -> dict:
        with self._lock:
            old_avg = self.avgs.get(customer)
            self.avgs.insert(customer, amount)
            new_avg = self.avgs.get(customer)
            row = _Row(customer=customer, amount=amount)
            slot = self._next_slot
            self._next_slot += 1
            self._row_slots[slot] = row
            self._all_rows.append(row)

            added: list[_Row] = []
            removed: list[_Row] = []
            # Newly inserted row may itself qualify against new avg
            if amount > new_avg:
                self._qualifying.add((customer, amount, slot))
                added.append(row)
            # Recheck membership for ALL existing rows with this customer
            for s, r in list(self._row_slots.items()):
                if r.customer != customer or s == slot:
                    continue
                before = r.amount > old_avg
                after = r.amount > new_avg
                tup = (r.customer, r.amount, s)
                if not before and after:
                    self._qualifying.add(tup)
                    added.append(r)
                elif before and not after:
                    self._qualifying.discard(tup)
                    removed.append(r)
            return {"added": added, "removed": removed}

    def delete(self, customer: Any, amount: float) -> dict:
        with self._lock:
            old_avg = self.avgs.get(customer)
            # Find a slot matching this row
            victim_slot: int | None = None
            for s, r in self._row_slots.items():
                if r.customer == customer and r.amount == amount:
                    victim_slot = s
                    break
            if victim_slot is None:
                return {"added": [], "removed": []}
            row = self._row_slots.pop(victim_slot)
            self._all_rows.remove(row)
            self.avgs.delete(customer, amount)
            new_avg = self.avgs.get(customer)

            added: list[_Row] = []
            removed: list[_Row] = []
            tup = (row.customer, row.amount, victim_slot)
            if tup in self._qualifying:
                self._qualifying.discard(tup)
                removed.append(row)
            # Re-evaluate other rows
            for s, r in self._row_slots.items():
                if r.customer != customer:
                    continue
                before = r.amount > old_avg
                after = r.amount > new_avg
                rtup = (r.customer, r.amount, s)
                if not before and after:
                    self._qualifying.add(rtup)
                    added.append(r)
                elif before and not after:
                    self._qualifying.discard(rtup)
                    removed.append(r)
            return {"added": added, "removed": removed}

    def qualifying(self) -> list[tuple[Any, float]]:
        """All currently-qualifying rows (deduped by content)."""
        with self._lock:
            return [(c, a) for (c, a, _slot) in self._qualifying]


__all__ = ["CorrelatedSubqueryIVM"]

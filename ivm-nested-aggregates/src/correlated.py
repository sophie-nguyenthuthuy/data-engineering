"""IVM for correlated subqueries via rewrite to lateral join.

Example:
    SELECT o.* FROM orders o
    WHERE o.amount > (SELECT AVG(amount) FROM orders WHERE customer = o.customer)

Rewrite:
    WITH per_cust_avg AS (
      SELECT customer, AVG(amount) AS avg_amt FROM orders GROUP BY customer
    )
    SELECT o.* FROM orders o JOIN per_cust_avg c
      ON o.customer = c.customer
      WHERE o.amount > c.avg_amt

The CTE per_cust_avg is a flat IVM-friendly aggregate (running average per
customer). The outer is a simple inequality join.

We maintain both incrementally.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class PerCustomerAvg:
    """Running per-key sum + count; AVG = sum/count."""
    _sum: dict = field(default_factory=lambda: defaultdict(float))
    _cnt: dict = field(default_factory=lambda: defaultdict(int))

    def insert(self, customer, amount: float) -> tuple[float, float]:
        old = self.avg(customer)
        self._sum[customer] += amount
        self._cnt[customer] += 1
        return old, self.avg(customer)

    def delete(self, customer, amount: float) -> tuple[float, float]:
        old = self.avg(customer)
        if self._cnt[customer] <= 0:
            return old, old
        self._sum[customer] -= amount
        self._cnt[customer] -= 1
        if self._cnt[customer] == 0:
            self._sum.pop(customer, None)
            self._cnt.pop(customer, None)
        return old, self.avg(customer)

    def avg(self, customer) -> float:
        if self._cnt.get(customer, 0) == 0:
            return 0.0
        return self._sum[customer] / self._cnt[customer]


@dataclass
class CorrelatedSubqueryIVM:
    """Tracks 'orders where amount > avg(amount) per customer'."""
    avgs: PerCustomerAvg = field(default_factory=PerCustomerAvg)
    _orders: list = field(default_factory=list)  # (customer, amount)

    def insert(self, customer, amount: float) -> dict:
        """Insert order. Return {'added': [orders that now qualify],
                                  'removed': [orders that no longer qualify]}."""
        old_avg, new_avg = self.avgs.insert(customer, amount)
        self._orders.append((customer, amount))
        added, removed = [], []
        for (c, a) in self._orders:
            if c != customer:
                continue
            before = a > old_avg
            after = a > new_avg
            if not before and after:
                added.append((c, a))
            elif before and not after:
                removed.append((c, a))
        return {"added": added, "removed": removed}

    def qualifying(self) -> list:
        """All orders currently satisfying the predicate."""
        return [(c, a) for (c, a) in self._orders if a > self.avgs.avg(c)]


__all__ = ["PerCustomerAvg", "CorrelatedSubqueryIVM"]

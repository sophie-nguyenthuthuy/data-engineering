"""TPC-C-shaped workload generator.

TPC-C is the canonical OLTP benchmark. Each transaction touches a small
set of pages with locality:
  - new_order: 4-12 pages (customer, item, stock, order, ...)
  - payment:   3-5 pages
  - delivery:  10-15 pages
We approximate by generating page-access traces that match the locality
patterns without implementing the full transaction logic.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from disagg.core.page import PageId

if TYPE_CHECKING:
    from collections.abc import Iterator

# TPC-C transaction mix (standard percentages)
_MIX = [
    ("new_order", 0.45, (4, 12)),    # name, fraction, (min, max) pages
    ("payment",   0.43, (3, 5)),
    ("delivery",  0.04, (10, 15)),
    ("order_status", 0.04, (2, 5)),
    ("stock_level",  0.04, (5, 8)),
]


def tpcc_workload(
    n_warehouses: int = 10,
    n_transactions: int = 1000,
    seed: int = 0,
) -> Iterator[PageId]:
    """Yield a stream of page-access events that mimics TPC-C locality.

    Page numbering convention (toy):
      warehouse_pages:  [0 .. n_warehouses)
      customer_pages:   [1000 .. 1000 + n_warehouses * 100)
      stock_pages:      [10000 .. 10000 + n_warehouses * 1000)
    """
    rng = random.Random(seed)
    cdf: list[tuple[str, float, tuple[int, int]]] = []
    cum = 0.0
    for name, frac, span in _MIX:
        cum += frac
        cdf.append((name, cum, span))

    for _ in range(n_transactions):
        r = rng.random()
        tx_kind, _, (lo, hi) = next(t for t in cdf if r <= t[1])
        n_pages = rng.randint(lo, hi)
        warehouse = rng.randint(0, n_warehouses - 1)
        # Warehouse home page
        yield PageId(tenant=0, page_no=warehouse)
        # A few customer pages (per-warehouse locality)
        for _ in range(min(n_pages // 3, 4)):
            cust = rng.randint(0, 99)
            yield PageId(tenant=0, page_no=1000 + warehouse * 100 + cust)
        # Stock pages
        for _ in range(max(1, n_pages - n_pages // 3 - 1)):
            item = rng.randint(0, 999)
            yield PageId(tenant=0, page_no=10_000 + warehouse * 1000 + item)

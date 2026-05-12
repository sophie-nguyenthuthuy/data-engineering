"""Shadow-replay reward computation.

Given a layout and a query log, estimate the cost as the number of "pages"
that need to be scanned. We use a simple page model: rows physically sorted
by layout's index; each page holds N rows; a query reads a page if any row
in the page satisfies the predicate.
"""
from __future__ import annotations

import numpy as np

from .curves import z_order_index, hilbert_index
from .policy import Action
from .workload import Query


PAGE_ROWS = 64


def apply_layout(data: np.ndarray, cols: list, action: Action) -> np.ndarray:
    """Reorder rows by action; returns permutation of indices."""
    if action.kind == "noop":
        return np.arange(len(data))
    col_idx = [cols.index(c) for c in action.cols]
    sub = data[:, col_idx]
    if action.kind == "sortkey":
        return np.lexsort(sub.T[::-1])
    if action.kind == "zorder":
        idx = z_order_index(sub.astype(np.uint64))
        return np.argsort(idx)
    if action.kind == "hilbert":
        if sub.shape[1] != 2:
            # Fall back to z-order for non-2D Hilbert
            idx = z_order_index(sub.astype(np.uint64))
        else:
            idx = hilbert_index(sub.astype(np.uint64))
        return np.argsort(idx)
    return np.arange(len(data))


def pages_scanned(data: np.ndarray, perm: np.ndarray, cols: list, query: Query) -> int:
    """How many pages does this query read against the laid-out data?"""
    n = len(perm)
    if n == 0:
        return 0
    # Apply permutation; build per-page row range
    permuted = data[perm]
    pages = (n + PAGE_ROWS - 1) // PAGE_ROWS
    scanned = 0
    for p in range(pages):
        lo, hi = p * PAGE_ROWS, min((p + 1) * PAGE_ROWS, n)
        page_rows = permuted[lo:hi]
        # Does any row in this page satisfy the predicate?
        keep = np.ones(hi - lo, dtype=bool)
        for c, pred in query.predicates.items():
            j = cols.index(c)
            col_vals = page_rows[:, j]
            if pred[0] == "=":
                keep &= (col_vals == pred[1])
            else:
                keep &= (col_vals >= pred[1]) & (col_vals <= pred[2])
        if keep.any():
            scanned += 1
    return scanned


def expected_pages(data: np.ndarray, cols: list, action: Action,
                   workload: list[Query]) -> float:
    perm = apply_layout(data, cols, action)
    return np.mean([pages_scanned(data, perm, cols, q) for q in workload])


def reward(data: np.ndarray, cols: list, action: Action,
           workload: list[Query], io_cost: float = 100.0) -> float:
    """Reward = pages saved vs noop - (rewrite I/O if not noop)."""
    base = expected_pages(data, cols, Action("noop", ()), workload)
    new = expected_pages(data, cols, action, workload)
    saved = base - new
    if action.kind != "noop":
        saved -= io_cost / len(workload)
    return saved


__all__ = ["apply_layout", "pages_scanned", "expected_pages", "reward", "PAGE_ROWS"]

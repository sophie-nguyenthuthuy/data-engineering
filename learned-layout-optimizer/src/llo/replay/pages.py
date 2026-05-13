"""Shadow-replay reward computation under a page model.

A *page* is a contiguous block of ``PAGE_ROWS`` rows. A query "reads" a
page iff at least one row in the page satisfies every predicate. Layouts
that cluster matching rows reduce pages scanned for typical queries —
exactly the metric an analytics engine cares about.

The reward signal compares the action's pages-scanned to the noop
baseline and subtracts an amortised I/O cost when the action involves a
physical rewrite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from llo.curves.spacefill import hilbert_index, hilbert_index_nd, z_order_index

if TYPE_CHECKING:
    from numpy.typing import NDArray

    from llo.policy.bandit import Action
    from llo.workload.profile import Query

PAGE_ROWS = 64


def apply_layout(data: NDArray[np.integer], cols: list[str], action: Action) -> NDArray[np.int64]:
    """Return the row-permutation induced by ``action``.

    ``data`` is shape (n, n_cols). The returned permutation can be applied
    as ``data[perm]`` to obtain the physically reorganised table.
    """
    n = len(data)
    if action.kind == "noop":
        return np.arange(n, dtype=np.int64)
    if any(c not in cols for c in action.cols):
        raise ValueError(f"action references unknown columns {action.cols!r}")
    col_idx = [cols.index(c) for c in action.cols]
    sub = data[:, col_idx]
    if action.kind == "sortkey":
        sorted_perm: NDArray[np.int64] = np.lexsort(sub.T[::-1]).astype(np.int64)
        return sorted_perm
    if action.kind == "zorder":
        idx = z_order_index(sub.astype(np.int64))
        return np.argsort(idx).astype(np.int64)
    if action.kind == "hilbert":
        if sub.shape[1] == 2:
            idx = hilbert_index(sub.astype(np.int64))
        else:
            idx = hilbert_index_nd(sub.astype(np.int64))
        return np.argsort(idx).astype(np.int64)
    raise ValueError(f"unknown action kind {action.kind!r}")


def pages_scanned(
    data: NDArray[np.integer],
    perm: NDArray[np.int64],
    cols: list[str],
    query: Query,
) -> int:
    """Number of pages the query must read against the laid-out data."""
    n = len(perm)
    if n == 0:
        return 0
    permuted = data[perm]
    n_pages = (n + PAGE_ROWS - 1) // PAGE_ROWS
    scanned = 0
    for p in range(n_pages):
        lo, hi = p * PAGE_ROWS, min((p + 1) * PAGE_ROWS, n)
        page = permuted[lo:hi]
        keep = np.ones(hi - lo, dtype=bool)
        for c, pred in query.predicates.items():
            j = cols.index(c)
            vals = page[:, j]
            if pred[0] == "=":
                keep &= vals == pred[1]
            else:
                keep &= (vals >= pred[1]) & (vals <= pred[2])
            if not keep.any():
                break
        if keep.any():
            scanned += 1
    return scanned


def expected_pages(
    data: NDArray[np.integer],
    cols: list[str],
    action: Action,
    workload: list[Query],
) -> float:
    """Mean pages scanned across the workload under ``action``."""
    if not workload:
        return 0.0
    perm = apply_layout(data, cols, action)
    return float(np.mean([pages_scanned(data, perm, cols, q) for q in workload]))


def reward(
    data: NDArray[np.integer],
    cols: list[str],
    action: Action,
    workload: list[Query],
    io_cost: float = 100.0,
) -> float:
    """Amortised reward = pages-saved-vs-noop − rewrite cost per query."""
    if not workload:
        return 0.0
    base = expected_pages(data, cols, _noop(), workload)
    new = expected_pages(data, cols, action, workload)
    saved = base - new
    if action.kind != "noop":
        saved -= io_cost / len(workload)
    return saved


def _noop() -> Action:
    from llo.policy.bandit import Action  # local import → avoid cycle

    return Action("noop", ())


__all__ = ["PAGE_ROWS", "apply_layout", "expected_pages", "pages_scanned", "reward"]

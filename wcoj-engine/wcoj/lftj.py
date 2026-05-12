"""Leapfrog Triejoin (LFTJ) — worst-case optimal join algorithm.

Reference:
    Veldhuizen, T. L. (2014). Leapfrog triejoin: A simple, worst-case optimal
    join algorithm. ICDT 2014. https://arxiv.org/abs/1210.0481

Complexity: O(N^{rho*} log N) where rho* is the fractional edge cover number
of the query hypergraph — optimal up to the log factor.

Public API
----------
leapfrog_join(iterators)   – generator: intersection of N sorted iterators
lftj(query, var_order)     – run LFTJ on a JoinQuery, return result array
"""
from __future__ import annotations

from typing import Dict, Generator, List, Optional, Tuple

import numpy as np

from .query import JoinQuery, Relation
from .trie import TrieIterator


# ------------------------------------------------------------------ #
#  Core: leapfrog intersection of N sorted iterators at one depth     #
# ------------------------------------------------------------------ #

def leapfrog_join(iterators: List[TrieIterator]) -> Generator[int, None, None]:
    """Yield each integer in the intersection of N open trie iterators.

    All iterators must already be opened (open() called) and positioned at
    their first key.  The generator leaves iterators at_end when exhausted.

    Algorithm (Veldhuizen §2):
    - Maintain x_prime = current candidate (max seen so far).
    - Rotate through iterators seeking each to x_prime.
    - If an iterator overshoots, x_prime updates and the count resets.
    - Once all n iterators confirm x_prime, yield it and advance all.
    """
    n = len(iterators)
    if n == 0:
        return
    if any(it.at_end() for it in iterators):
        return

    # Seed x_prime as the current maximum key across all iterators.
    x_prime: int = max(it.key() for it in iterators)
    p: int = 0       # index of iterator we are about to seek
    count: int = 0   # consecutive iterators found at x_prime

    while True:
        iterators[p].seek(x_prime)
        if iterators[p].at_end():
            return

        curr = iterators[p].key()
        if curr > x_prime:
            # Overshot — new candidate, restart counting.
            x_prime = curr
            count = 1
        else:
            # Exactly at x_prime.
            count += 1

        p = (p + 1) % n

        if count == n:
            # All iterators agree on x_prime.
            yield x_prime

            # Advance every iterator past x_prime before the next round.
            for it in iterators:
                it.next()
            if any(it.at_end() for it in iterators):
                return

            x_prime = max(it.key() for it in iterators)
            count = 0


# ------------------------------------------------------------------ #
#  Leapfrog Triejoin                                                  #
# ------------------------------------------------------------------ #

def lftj(
    query: JoinQuery,
    var_order: Optional[List[str]] = None,
) -> np.ndarray:
    """Run Leapfrog Triejoin on *query* and return result tuples.

    Args:
        query:     The join query.
        var_order: Optional variable ordering.  If None, the query's
                   heuristic ordering is used (most-constrained first).

    Returns:
        2-D int64 numpy array of shape (n_results, len(var_order)).
        Column order matches *var_order*.
    """
    if var_order is None:
        var_order = query.variable_order()

    k = len(var_order)
    var_index: Dict[str, int] = {v: i for i, v in enumerate(var_order)}

    # Build TrieIterators — each relation's columns must be sorted in
    # global variable order (prepare_for_lftj does this).
    prepared = query.prepare_for_lftj(var_order)
    trie_iters: List[Tuple[TrieIterator, List[int]]] = []
    for rel in prepared:
        data = rel.data
        global_depths = [var_index[v] for v in rel.variables]
        it = TrieIterator(data)
        trie_iters.append((it, global_depths))

    # depth_iters[d] = list of TrieIterators that participate at global depth d.
    depth_iters: List[List[TrieIterator]] = [[] for _ in range(k)]
    for it, global_depths in trie_iters:
        for d in global_depths:
            depth_iters[d].append(it)

    results: List[Tuple] = []
    current: List[int] = [0] * k

    def recurse(depth: int) -> None:
        if depth == k:
            results.append(tuple(current))
            return

        iters = depth_iters[depth]
        if not iters:
            recurse(depth + 1)
            return

        for it in iters:
            it.open()

        # leapfrog_join yields each value in the intersection, advancing
        # iterators between yields.  open()/up() calls for deeper depths
        # happen between yields and are transparent to the generator because
        # they push/pop symmetric stack entries.
        for val in leapfrog_join(iters):
            current[depth] = val
            recurse(depth + 1)

        for it in iters:
            it.up()

    recurse(0)

    if not results:
        return np.empty((0, k), dtype=np.int64)
    return np.array(results, dtype=np.int64)

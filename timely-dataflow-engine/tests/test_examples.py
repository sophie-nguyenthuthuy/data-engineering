"""PageRank + belief propagation examples."""

from __future__ import annotations

from timely.examples.belief_propagation import belief_propagation
from timely.examples.pagerank import pagerank


def test_pagerank_converges():
    # Tiny graph: 0 → 1 → 2 → 0 (cycle of 3)
    edges = {0: [1], 1: [2], 2: [0]}
    ranks, iters = pagerank(edges, n_nodes=3)
    # All three should be (approximately) 1/3 since cycle is symmetric
    assert all(0.3 < r < 0.4 for r in ranks)
    assert iters > 0
    # Sum to ~1
    assert abs(sum(ranks) - 1.0) < 1e-2


def test_pagerank_directed_graph():
    # Hub-spoke: 1 → 0, 2 → 0
    edges = {0: [], 1: [0], 2: [0]}
    ranks, _ = pagerank(edges, n_nodes=3)
    # Node 0 receives all inbound → highest rank
    assert ranks[0] > ranks[1]
    assert ranks[0] > ranks[2]


def test_belief_propagation_converges():
    msgs, iters = belief_propagation(n_nodes=4)
    assert iters > 0
    # Convergent: small variance
    assert max(msgs) - min(msgs) < 0.5


def test_pagerank_respects_max_iter():
    edges = {0: [1], 1: [0]}
    _, iters = pagerank(edges, n_nodes=2, max_iter=5, tol=1e-100)
    assert iters <= 5

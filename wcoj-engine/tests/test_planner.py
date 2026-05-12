"""Tests for the query planner: cycle detection and algorithm selection."""
import numpy as np
import pytest

from wcoj import JoinQuery, Relation, execute, explain, is_acyclic


def make_edges(n=20, seed=7):
    rng = np.random.default_rng(seed)
    edges = set()
    for _ in range(n * 3):
        u, v = sorted(rng.integers(0, n, size=2))
        if u != v:
            edges.add((int(u), int(v)))
    return np.array(sorted(edges), dtype=np.int64)


class TestIsAcyclic:
    def test_triangle_is_cyclic(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["x", "z"], arr),
        ])
        assert not is_acyclic(q)

    def test_path_is_acyclic(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["z", "w"], arr),
        ])
        assert is_acyclic(q)

    def test_single_relation_is_acyclic(self):
        arr = make_edges()
        q = JoinQuery([Relation("R", ["x", "y"], arr)])
        assert is_acyclic(q)

    def test_chain_of_two_is_acyclic(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
        ])
        assert is_acyclic(q)

    def test_four_cycle_is_cyclic(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["a", "b"], arr),
            Relation("S", ["b", "c"], arr),
            Relation("T", ["c", "d"], arr),
            Relation("U", ["a", "d"], arr),
        ])
        assert not is_acyclic(q)


class TestExecute:
    def test_cyclic_uses_lftj(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["x", "z"], arr),
        ])
        result = execute(q)
        assert result.algorithm == "lftj"

    def test_acyclic_uses_hash_join(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
        ])
        result = execute(q)
        assert result.algorithm == "hash_join"

    def test_force_override(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["x", "z"], arr),
        ])
        result = execute(q, force="hash_join")
        assert result.algorithm == "hash_join"

    def test_results_not_empty_on_dense_graph(self):
        arr = np.array([(0,1),(0,2),(1,2),(1,3),(2,3),(0,3)], dtype=np.int64)
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["x", "z"], arr),
        ])
        result = execute(q)
        assert result.n_results > 0

    def test_explain_output(self):
        arr = make_edges()
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["x", "z"], arr),
        ])
        text = explain(q)
        assert "CYCLIC" in text
        assert "lftj" in text.lower() or "Leapfrog" in text

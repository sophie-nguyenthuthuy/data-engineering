"""Tests for Generic Join correctness and cross-validation with LFTJ."""
import numpy as np
import pytest

from wcoj import JoinQuery, Relation
from wcoj.generic_join import generic_join
from wcoj.lftj import lftj


def sorted_rows(arr):
    if len(arr) == 0:
        return arr
    return arr[np.lexsort(arr.T[::-1])]


def _triangle_query(edges):
    arr = np.array(edges, dtype=np.int64)
    return JoinQuery([
        Relation("R", ["x", "y"], arr),
        Relation("S", ["y", "z"], arr),
        Relation("T", ["x", "z"], arr),
    ])


class TestGenericJoin:
    def test_empty_input(self):
        arr = np.empty((0, 2), dtype=np.int64)
        q = JoinQuery([Relation("R", ["x", "y"], arr), Relation("S", ["y", "z"], arr)])
        assert len(generic_join(q)) == 0

    def test_triangle_no_result(self):
        edges = [(0, 1), (1, 2), (2, 3)]
        result = generic_join(_triangle_query(edges))
        assert len(result) == 0

    def test_triangle_single(self):
        edges = [(0, 1), (0, 2), (1, 2)]
        result = generic_join(_triangle_query(edges))
        assert len(result) >= 1

    def test_matches_lftj_k4(self):
        edges = [(0,1),(0,2),(0,3),(1,2),(1,3),(2,3)]
        q = _triangle_query(edges)
        var_order = q.variable_order()
        r_lftj = sorted_rows(lftj(q, var_order))
        r_gj   = sorted_rows(generic_join(q, var_order))
        assert np.array_equal(r_lftj, r_gj)

    def test_matches_lftj_random(self):
        from benchmarks.datasets import erdos_renyi
        edges = erdos_renyi(30, 0.2, seed=99)
        q = _triangle_query(edges)
        var_order = q.variable_order()
        r_lftj = sorted_rows(lftj(q, var_order))
        r_gj   = sorted_rows(generic_join(q, var_order))
        assert np.array_equal(r_lftj, r_gj), \
            f"Mismatch: LFTJ={len(r_lftj)} GJ={len(r_gj)}"

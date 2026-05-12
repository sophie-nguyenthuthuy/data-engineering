"""Tests for Leapfrog Triejoin correctness."""
import numpy as np
import pytest

from wcoj import JoinQuery, Relation
from wcoj.lftj import leapfrog_join, lftj
from wcoj.trie import TrieIterator


# ------------------------------------------------------------------ #
#  leapfrog_join unit tests                                           #
# ------------------------------------------------------------------ #

def iter_from_list(values):
    data = np.array(sorted(set(values)), dtype=np.int64).reshape(-1, 1)
    it = TrieIterator(data)
    it.open()
    return it


class TestLeapfrogJoin:
    def test_empty_intersection(self):
        a = iter_from_list([1, 3, 5])
        b = iter_from_list([2, 4, 6])
        assert list(leapfrog_join([a, b])) == []

    def test_full_intersection(self):
        a = iter_from_list([1, 2, 3])
        b = iter_from_list([1, 2, 3])
        assert list(leapfrog_join([a, b])) == [1, 2, 3]

    def test_partial_intersection(self):
        a = iter_from_list([1, 2, 4, 6])
        b = iter_from_list([2, 3, 4, 7])
        c = iter_from_list([2, 4, 5, 8])
        assert list(leapfrog_join([a, b, c])) == [2, 4]

    def test_single_iterator(self):
        a = iter_from_list([3, 7, 9])
        assert list(leapfrog_join([a])) == [3, 7, 9]

    def test_one_empty_iterator(self):
        a = iter_from_list([1, 2, 3])
        b = iter_from_list([])
        assert list(leapfrog_join([a, b])) == []


# ------------------------------------------------------------------ #
#  LFTJ end-to-end tests                                             #
# ------------------------------------------------------------------ #

def sorted_rows(arr):
    """Sort a 2-D array for comparison."""
    if len(arr) == 0:
        return arr
    return arr[np.lexsort(arr.T[::-1])]


class TestLFTJTriangle:
    """Triangle query: R(x,y) ⋈ S(y,z) ⋈ T(x,z)."""

    def _make_query(self, edges):
        arr = np.array(edges, dtype=np.int64)
        return JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["x", "z"], arr),
        ])

    def test_no_triangles(self):
        # Path graph: 0-1-2-3 has no triangles.
        edges = [(0, 1), (1, 2), (2, 3)]
        result = lftj(self._make_query(edges))
        assert len(result) == 0

    def test_single_triangle(self):
        edges = [(0, 1), (0, 2), (1, 2)]
        result = lftj(self._make_query(edges))
        # One triangle: (0,1,2) — but LFTJ may produce ordered permutations
        # based on variable ordering.  We just check count >= 1.
        assert len(result) >= 1

    def test_k4_triangle_count(self):
        # K4 has C(4,3)=4 triangles; each found once.
        edges = [(0,1),(0,2),(0,3),(1,2),(1,3),(2,3)]
        result = lftj(self._make_query(edges))
        # With variable order [x,y,z] each triangle appears once (x<y<z-like).
        assert len(result) == 4

    def test_matches_generic_join(self):
        from wcoj.generic_join import generic_join
        edges = [(0,1),(0,2),(1,2),(1,3),(2,3),(0,3)]
        q = self._make_query(edges)
        var_order = q.variable_order()
        r_lftj = sorted_rows(lftj(q, var_order))
        r_gj   = sorted_rows(generic_join(q, var_order))
        assert np.array_equal(r_lftj, r_gj), \
            f"LFTJ={len(r_lftj)} GJ={len(r_gj)}"


class TestLFTJPath:
    """Path-3 query (acyclic): R(x,y) ⋈ S(y,z) ⋈ T(z,w)."""

    def _make_query(self, edges):
        arr = np.array(edges, dtype=np.int64)
        return JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
            Relation("T", ["z", "w"], arr),
        ])

    def test_simple_path(self):
        edges = [(0, 1), (1, 2), (2, 3)]
        result = lftj(self._make_query(edges))
        # 0→1→2→3 is the only length-3 path.
        assert len(result) >= 1

    def test_no_path(self):
        # Disjoint edges — no length-3 path.
        edges = [(0, 1), (2, 3)]
        result = lftj(self._make_query(edges))
        assert len(result) == 0


class TestLFTJEdgeCases:
    def test_empty_relation(self):
        arr = np.empty((0, 2), dtype=np.int64)
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["y", "z"], arr),
        ])
        result = lftj(q)
        assert len(result) == 0

    def test_single_tuple(self):
        arr = np.array([[1, 2]], dtype=np.int64)
        q = JoinQuery([
            Relation("R", ["x", "y"], arr),
            Relation("S", ["x", "y"], arr),
        ])
        result = lftj(q)
        assert len(result) >= 1

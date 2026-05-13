"""Space-filling curve tests."""

from __future__ import annotations

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from llo.curves.spacefill import hilbert_index, hilbert_index_nd, z_order_index


def test_zorder_2x2_canonical():
    coords = np.array([[0, 0], [1, 0], [0, 1], [1, 1]], dtype=np.uint64)
    idx = z_order_index(coords)
    assert list(idx) == [0, 1, 2, 3]


def test_zorder_is_a_permutation_over_full_grid():
    # On a complete 4x4 grid (16 points) Z-order must produce {0..15}.
    coords = np.array([[x, y] for y in range(4) for x in range(4)], dtype=np.uint64)
    idx = z_order_index(coords, bits=2)
    assert sorted(idx.tolist()) == list(range(16))


def test_zorder_rejects_negative():
    with pytest.raises(ValueError):
        z_order_index(np.array([[-1, 0]], dtype=np.int64))


def test_zorder_rejects_too_large_coord():
    with pytest.raises(ValueError):
        z_order_index(np.array([[16, 0]], dtype=np.int64), bits=4)


def test_zorder_rejects_non_2d():
    with pytest.raises(ValueError):
        z_order_index(np.array([1, 2, 3], dtype=np.int64))


def test_zorder_rejects_bad_bits():
    with pytest.raises(ValueError):
        z_order_index(np.array([[0, 0]], dtype=np.int64), bits=0)
    with pytest.raises(ValueError):
        z_order_index(np.array([[0, 0]], dtype=np.int64), bits=33)


def test_hilbert_is_a_permutation_over_full_grid():
    coords = np.array([[x, y] for y in range(8) for x in range(8)], dtype=np.uint64)
    idx = hilbert_index(coords, bits=3)
    assert sorted(idx.tolist()) == list(range(64))


def test_hilbert_consecutive_keys_are_neighbours():
    """Defining property of the Hilbert curve: consecutive 1-D indices are
    grid-adjacent (Manhattan distance 1) in d-D."""
    bits = 3
    coords = np.array([[x, y] for y in range(8) for x in range(8)], dtype=np.uint64)
    idx = hilbert_index(coords, bits=bits)
    order = np.argsort(idx)
    pts = coords[order]
    diffs = np.abs(np.diff(pts.astype(np.int64), axis=0)).sum(axis=1)
    assert diffs.max() == 1


def test_hilbert_rejects_non_2d():
    with pytest.raises(ValueError):
        hilbert_index(np.array([[0, 0, 0]], dtype=np.int64))


def test_hilbert_nd_matches_2d_count():
    coords = np.array([[x, y] for y in range(4) for x in range(4)], dtype=np.uint64)
    idx = hilbert_index_nd(coords, bits=2)
    assert sorted(idx.tolist()) == list(range(16))


def test_hilbert_nd_three_d_is_permutation():
    bits = 2
    pts = np.array(
        [[x, y, z] for z in range(4) for y in range(4) for x in range(4)],
        dtype=np.uint64,
    )
    idx = hilbert_index_nd(pts, bits=bits)
    assert sorted(idx.tolist()) == list(range(64))


def test_hilbert_nd_rejects_too_many_bits():
    coords = np.zeros((1, 8), dtype=np.int64)
    with pytest.raises(ValueError):
        hilbert_index_nd(coords, bits=16)  # 8 * 16 = 128 > 63


@settings(max_examples=30, deadline=None)
@given(
    st.lists(
        st.tuples(st.integers(0, 31), st.integers(0, 31)),
        min_size=1,
        max_size=40,
    )
)
def test_zorder_is_deterministic(pts):
    """Same input → same output."""
    arr = np.array(pts, dtype=np.int64)
    a = z_order_index(arr, bits=5)
    b = z_order_index(arr, bits=5)
    assert np.array_equal(a, b)


@settings(max_examples=20, deadline=None)
@given(
    st.lists(
        st.tuples(st.integers(0, 15), st.integers(0, 15)),
        min_size=1,
        max_size=25,
        unique=True,
    )
)
def test_zorder_unique_on_unique_coords(pts):
    arr = np.array(pts, dtype=np.int64)
    idx = z_order_index(arr, bits=4)
    assert len(set(idx.tolist())) == len(pts)

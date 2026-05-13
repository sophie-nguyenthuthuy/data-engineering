"""Edge-case library tests."""

from __future__ import annotations

import math

from ace.edges.numeric import FLOAT_EDGES, INT_EDGES, numeric_edges
from ace.edges.strings import STRING_EDGES, string_edges
from ace.edges.timestamps import TIMESTAMP_EDGES, timestamp_edges


def test_int_edges_include_zero_and_overflow_boundaries():
    assert 0 in INT_EDGES
    assert 1 in INT_EDGES and -1 in INT_EDGES
    assert 2**31 in INT_EDGES and -(2**31) in INT_EDGES
    assert 2**63 - 1 in INT_EDGES


def test_float_edges_include_inf_nan_and_signed_zero():
    assert math.inf in FLOAT_EDGES
    assert -math.inf in FLOAT_EDGES
    assert any(math.isnan(x) for x in FLOAT_EDGES)
    # Both +0.0 and -0.0 are present (compare via repr to distinguish).
    reprs = {repr(x) for x in FLOAT_EDGES}
    assert "0.0" in reprs and "-0.0" in reprs


def test_numeric_edges_combines_int_and_float():
    edges = numeric_edges()
    assert len(edges) == len(INT_EDGES) + len(FLOAT_EDGES)


def test_string_edges_include_known_oddities():
    edges = string_edges()
    assert "" in edges
    assert "\x00" in edges
    assert any(len(s) > 1000 for s in edges)
    assert any("DROP TABLE" in s for s in edges)


def test_timestamp_edges_include_dst_and_y2k38_boundaries():
    edges = timestamp_edges()
    assert 0 in edges
    assert 2**31 - 1 in edges
    assert 2**31 in edges
    assert any(e > 2_000_000_000 for e in edges)


def test_edge_lists_are_immutable_tuples():
    assert isinstance(INT_EDGES, tuple)
    assert isinstance(FLOAT_EDGES, tuple)
    assert isinstance(STRING_EDGES, tuple)
    assert isinstance(TIMESTAMP_EDGES, tuple)

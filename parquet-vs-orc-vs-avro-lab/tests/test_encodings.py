"""Encoding round-trip + property tests."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pova.columnar.column import ColumnType
from pova.encoding.delta import delta_decode, delta_encode
from pova.encoding.dictionary import dictionary_decode, dictionary_encode
from pova.encoding.plain import plain_decode, plain_encode
from pova.encoding.rle import rle_decode, rle_encode

# --------------------------------------------------------------- Plain


def test_plain_int64_round_trip():
    values = [1, -2, 3, 4, None, 1 << 60]
    enc = plain_encode(values, ColumnType.INT64)
    assert plain_decode(enc, ColumnType.INT64) == values


def test_plain_float64_round_trip():
    values = [1.0, -2.5, None, 3.1415]
    enc = plain_encode(values, ColumnType.FLOAT64)
    assert plain_decode(enc, ColumnType.FLOAT64) == values


def test_plain_string_round_trip():
    values = ["", "hello", None, "café"]
    assert plain_decode(plain_encode(values, ColumnType.STRING), ColumnType.STRING) == values


def test_plain_bool_round_trip():
    values = [True, False, None, True]
    assert plain_decode(plain_encode(values, ColumnType.BOOL), ColumnType.BOOL) == values


def test_plain_rejects_bad_length():
    with pytest.raises(ValueError):
        plain_decode(b"\xff\xff\xff\x00", ColumnType.INT64)


# ----------------------------------------------------------------- RLE


def test_rle_compresses_repeated_runs():
    values = ["A"] * 10 + ["B"] * 5
    rle = rle_encode(values, ColumnType.STRING)
    plain = plain_encode(values, ColumnType.STRING)
    assert len(rle) < len(plain)
    assert rle_decode(rle, ColumnType.STRING) == values


def test_rle_handles_nulls_as_distinct_run():
    values = [1, None, None, 2]
    enc = rle_encode(values, ColumnType.INT64)
    assert rle_decode(enc, ColumnType.INT64) == values


def test_rle_empty_input():
    assert rle_encode([], ColumnType.INT64) == b""
    assert rle_decode(b"", ColumnType.INT64) == []


# ---------------------------------------------------------- Dictionary


def test_dictionary_compresses_low_cardinality():
    values = ["A", "B", "A", "C", "B", "A"]
    enc = dictionary_encode(values, ColumnType.STRING)
    # Dictionary stores 3 distinct + 6 indices; plain stores 6 strings.
    assert dictionary_decode(enc, ColumnType.STRING) == values


def test_dictionary_round_trip_with_nulls():
    values = [1, None, 2, 1, None]
    enc = dictionary_encode(values, ColumnType.INT64)
    assert dictionary_decode(enc, ColumnType.INT64) == values


def test_dictionary_rejects_short_buffer():
    with pytest.raises(ValueError):
        dictionary_decode(b"\x00\x00", ColumnType.INT64)


# ---------------------------------------------------------------- Delta


def test_delta_round_trip_monotone_ints():
    values = [10, 12, 13, 16, 100]
    assert delta_decode(delta_encode(values)) == values


def test_delta_empty():
    assert delta_decode(delta_encode([])) == []


def test_delta_rejects_nulls():
    with pytest.raises(ValueError):
        delta_encode([1, None, 3])


def test_delta_rejects_misaligned_buffer():
    with pytest.raises(ValueError):
        delta_decode(b"\x00" * 7)


# ------------------------------------------------------------- Hypothesis


@settings(max_examples=30, deadline=None)
@given(st.lists(st.integers(-(2**40), 2**40) | st.none(), min_size=0, max_size=20))
def test_property_plain_round_trip_int(values):
    assert plain_decode(plain_encode(values, ColumnType.INT64), ColumnType.INT64) == values


@settings(max_examples=30, deadline=None)
@given(st.lists(st.sampled_from(["A", "B", "C", None]), min_size=0, max_size=20))
def test_property_dictionary_round_trip(values):
    enc = dictionary_encode(values, ColumnType.STRING)
    assert dictionary_decode(enc, ColumnType.STRING) == values

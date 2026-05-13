"""Naming-convention tests."""

from __future__ import annotations

import datetime as dt

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from msc.naming import NamingConvention, StagedKey


def test_staged_key_round_trip():
    k = StagedKey(source="csv", dataset="orders", partition="2026/05/13", run_id="r1")
    assert StagedKey.parse(k.path()) == k


def test_staged_key_rejects_bad_partition():
    with pytest.raises(ValueError):
        StagedKey(source="csv", dataset="orders", partition="2026-05-13", run_id="r1")


def test_staged_key_rejects_empty_run_id():
    with pytest.raises(ValueError):
        StagedKey(source="csv", dataset="orders", partition="2026/05/13", run_id="")


def test_staged_key_rejects_slash_in_ext():
    with pytest.raises(ValueError):
        StagedKey(source="csv", dataset="orders", partition="2026/05/13", run_id="r1", ext="a/b")


def test_parse_rejects_malformed_path():
    with pytest.raises(ValueError):
        StagedKey.parse("not/enough/parts")


def test_parse_rejects_missing_extension():
    with pytest.raises(ValueError):
        StagedKey.parse("csv/orders/2026/05/13/r1noext")


def test_naming_normalises_source_and_dataset():
    when = dt.datetime(2026, 5, 13, 12, tzinfo=dt.timezone.utc)
    key = NamingConvention().make(source="HTTP-API", dataset="My Orders", when=when)
    assert key.source == "http_api"
    assert key.dataset == "my_orders"
    assert key.partition == "2026/05/13"


def test_naming_uses_utc_partition_when_no_when_given():
    key = NamingConvention().make(source="csv", dataset="orders")
    assert len(key.partition.split("/")) == 3


def test_naming_requires_timezone_aware_when():
    with pytest.raises(ValueError):
        NamingConvention().make(source="csv", dataset="orders", when=dt.datetime(2026, 5, 13, 12))


def test_naming_rejects_empty_source_or_dataset():
    with pytest.raises(ValueError):
        NamingConvention().make(source="", dataset="orders")
    with pytest.raises(ValueError):
        NamingConvention().make(source="csv", dataset="")


def test_naming_rejects_dataset_that_normalises_to_empty():
    with pytest.raises(ValueError):
        NamingConvention().make(source="csv", dataset="!!!")


def test_default_run_id_is_deterministic_for_same_inputs():
    when = dt.datetime(2026, 5, 13, 12, tzinfo=dt.timezone.utc)
    a = NamingConvention().make(source="csv", dataset="orders", when=when)
    b = NamingConvention().make(source="csv", dataset="orders", when=when)
    assert a.run_id == b.run_id


def test_default_run_id_changes_when_timestamp_changes():
    a = NamingConvention().make(
        source="csv",
        dataset="orders",
        when=dt.datetime(2026, 5, 13, 12, tzinfo=dt.timezone.utc),
    )
    b = NamingConvention().make(
        source="csv",
        dataset="orders",
        when=dt.datetime(2026, 5, 13, 13, tzinfo=dt.timezone.utc),
    )
    assert a.run_id != b.run_id


_ASCII_ALNUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


@settings(max_examples=40, deadline=None)
@given(
    st.text(alphabet=_ASCII_ALNUM, min_size=1, max_size=20),
    st.text(alphabet=_ASCII_ALNUM, min_size=1, max_size=20),
)
def test_property_naming_lowercases_and_replaces_punctuation(src, ds):
    key = NamingConvention().make(source=src, dataset=ds)
    assert key.source == key.source.lower()
    assert all(c.isascii() and (c.isalnum() or c == "_") for c in key.source)
    assert all(c.isascii() and (c.isalnum() or c == "_") for c in key.dataset)

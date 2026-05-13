"""Manifest tests."""

from __future__ import annotations

import datetime as dt

import pytest

from msc.manifest import Manifest, ManifestEntry, sha256_bytes


def test_manifest_creates_parent_dir(tmp_path):
    p = tmp_path / "a" / "b" / "manifest.jsonl"
    Manifest(path=p)
    assert p.exists()


def test_manifest_append_and_read(tmp_path):
    m = Manifest(path=tmp_path / "m.jsonl")
    e = m.record(
        staged_path="csv/orders/2026/05/13/run1.jsonl",
        source="csv",
        dataset="orders",
        run_id="run1",
        row_count=10,
        sha256="abc",
    )
    entries = m.entries()
    assert len(entries) == 1
    assert entries[0] == e


def test_manifest_record_rejects_naive_completed_at(tmp_path):
    m = Manifest(path=tmp_path / "m.jsonl")
    with pytest.raises(ValueError):
        m.record(
            staged_path="x",
            source="csv",
            dataset="orders",
            run_id="r1",
            row_count=1,
            sha256="abc",
            completed_at=dt.datetime(2026, 5, 13),
        )


def test_manifest_has_returns_true_for_known_triple(tmp_path):
    m = Manifest(path=tmp_path / "m.jsonl")
    m.record(
        staged_path="csv/orders/2026/05/13/r1.jsonl",
        source="csv",
        dataset="orders",
        run_id="r1",
        row_count=1,
        sha256="abc",
    )
    assert m.has("csv", "orders", "r1")
    assert not m.has("csv", "orders", "r2")


def test_manifest_latest_picks_max_completed_at(tmp_path):
    m = Manifest(path=tmp_path / "m.jsonl")
    early = dt.datetime(2026, 5, 13, 10, tzinfo=dt.timezone.utc)
    late = dt.datetime(2026, 5, 13, 12, tzinfo=dt.timezone.utc)
    m.record(
        staged_path="a",
        source="csv",
        dataset="orders",
        run_id="r1",
        row_count=1,
        sha256="aaa",
        completed_at=early,
    )
    m.record(
        staged_path="b",
        source="csv",
        dataset="orders",
        run_id="r2",
        row_count=2,
        sha256="bbb",
        completed_at=late,
    )
    latest = m.latest("csv", "orders")
    assert latest is not None
    assert latest.run_id == "r2"


def test_manifest_latest_none_for_unknown(tmp_path):
    m = Manifest(path=tmp_path / "m.jsonl")
    assert m.latest("csv", "orders") is None


def test_manifest_entry_key_tuple():
    e = ManifestEntry(
        staged_path="p",
        source="csv",
        dataset="orders",
        run_id="r1",
        row_count=1,
        sha256="abc",
        completed_at="2026-05-13T00:00:00+00:00",
    )
    assert e.key() == ("csv", "orders", "r1")


def test_sha256_bytes_matches_known_value():
    assert sha256_bytes(b"abc") == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )

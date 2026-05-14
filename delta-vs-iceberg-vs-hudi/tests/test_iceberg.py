"""Iceberg-style snapshot tests."""

from __future__ import annotations

import pytest

from tfl.delta.action import FileEntry
from tfl.iceberg.table import IcebergTable


def _file(path: str = "p", n: int = 10) -> FileEntry:
    return FileEntry(path=path, size_bytes=100, record_count=n)


def test_empty_table_has_no_files():
    t = IcebergTable()
    assert t.files_at() == []
    assert t.current() is None


def test_append_creates_snapshot():
    t = IcebergTable()
    sid = t.append([_file("a")])
    assert sid == 1
    assert t.current() == 1
    assert [f.path for f in t.files_at()] == ["a"]


def test_append_chains_parent_pointer():
    t = IcebergTable()
    s1 = t.append([_file("a")])
    s2 = t.append([_file("b")])
    snaps = t.snapshots()
    assert snaps[0].parent_id is None
    assert snaps[1].parent_id == s1
    assert s2 == 2


def test_delete_removes_file_from_subsequent_snapshots():
    t = IcebergTable()
    t.append([_file("a"), _file("b")])
    t.delete([_file("a")])
    assert {f.path for f in t.files_at()} == {"b"}


def test_overwrite_replaces_live_files():
    t = IcebergTable()
    t.append([_file("a"), _file("b")])
    t.overwrite([_file("c"), _file("d")])
    assert {f.path for f in t.files_at()} == {"c", "d"}


def test_rollback_does_not_truncate_history():
    t = IcebergTable()
    s1 = t.append([_file("a")])
    s2 = t.append([_file("b")])
    t.rollback(s1)
    assert t.current() == s1
    # Forward snapshot still accessible.
    assert {f.path for f in t.files_at(s2)} == {"a", "b"}


def test_rollback_unknown_raises():
    with pytest.raises(KeyError):
        IcebergTable().rollback(99)


def test_commit_rejects_empty_entries():
    t = IcebergTable()
    with pytest.raises(ValueError):
        t.append([])


def test_files_at_specific_snapshot():
    t = IcebergTable()
    s1 = t.append([_file("a")])
    t.append([_file("b")])
    assert {f.path for f in t.files_at(s1)} == {"a"}

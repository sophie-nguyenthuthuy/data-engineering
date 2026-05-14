"""Delta-style transaction-log tests."""

from __future__ import annotations

import threading

import pytest

from tfl.delta.action import Action, ActionType, FileEntry
from tfl.delta.table import DeltaConflict, DeltaTable


def _file(path: str = "p.parquet", n: int = 10) -> FileEntry:
    return FileEntry(path=path, size_bytes=100, record_count=n)


def test_file_entry_validates():
    with pytest.raises(ValueError):
        FileEntry(path="", size_bytes=0, record_count=0)
    with pytest.raises(ValueError):
        FileEntry(path="p", size_bytes=-1, record_count=0)
    with pytest.raises(ValueError):
        FileEntry(path="p", size_bytes=0, record_count=-1)


def test_action_validates_payload():
    with pytest.raises(ValueError):
        Action(ActionType.ADD)
    with pytest.raises(ValueError):
        Action(ActionType.REMOVE)
    with pytest.raises(ValueError):
        Action(ActionType.METADATA)


def test_initial_version_is_minus_one():
    assert DeltaTable().version() == -1


def test_commit_advances_version():
    t = DeltaTable()
    v0 = t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    v1 = t.commit([Action(ActionType.ADD, file=_file())], expected_version=v0)
    assert (v0, v1) == (0, 1)


def test_commit_rejects_empty_actions():
    with pytest.raises(ValueError):
        DeltaTable().commit([], expected_version=-1)


def test_concurrent_commit_conflict():
    t = DeltaTable()
    t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    with pytest.raises(DeltaConflict):
        t.commit([Action(ActionType.ADD, file=_file())], expected_version=-1)


def test_files_at_replays_add_remove():
    t = DeltaTable()
    v = t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    v = t.commit([Action(ActionType.ADD, file=_file("a"))], expected_version=v)
    v = t.commit([Action(ActionType.ADD, file=_file("b"))], expected_version=v)
    v = t.commit([Action(ActionType.REMOVE, file=_file("a"))], expected_version=v)
    paths = {f.path for f in t.files_at()}
    assert paths == {"b"}


def test_time_travel_to_version():
    t = DeltaTable()
    v0 = t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    v1 = t.commit([Action(ActionType.ADD, file=_file("a"))], expected_version=v0)
    v2 = t.commit([Action(ActionType.ADD, file=_file("b"))], expected_version=v1)
    assert {f.path for f in t.files_at(version=v1)} == {"a"}
    assert {f.path for f in t.files_at(version=v2)} == {"a", "b"}


def test_compaction_rewrites_files_in_one_commit():
    t = DeltaTable()
    v = t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    v = t.commit([Action(ActionType.ADD, file=_file("a"))], expected_version=v)
    v = t.commit([Action(ActionType.ADD, file=_file("b"))], expected_version=v)
    n_entries_before = t.n_log_entries()
    t.compact([_file("a"), _file("b")], _file("ab", n=20))
    assert t.n_log_entries() == n_entries_before + 1
    assert {f.path for f in t.files_at()} == {"ab"}


def test_compact_rejects_empty_input():
    t = DeltaTable()
    t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    with pytest.raises(ValueError):
        t.compact([], _file("x"))


def test_current_schema_id_tracks_metadata_commits():
    t = DeltaTable()
    t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    assert t.current_schema_id() == 0
    t.commit([Action(ActionType.METADATA, schema_id=1)], expected_version=t.version())
    assert t.current_schema_id() == 1


def test_log_paths_use_zero_padded_filenames():
    t = DeltaTable()
    t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    paths = t.log_paths()
    assert paths == ["_delta_log/00000000000000000000.json"]


def test_concurrent_writers_one_wins():
    """Two threads racing for v1: exactly one wins, the other sees DeltaConflict."""
    t = DeltaTable()
    t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)
    results: list[str] = []

    def writer(label: str) -> None:
        try:
            t.commit([Action(ActionType.ADD, file=_file(label))], expected_version=0)
            results.append(f"{label}:ok")
        except DeltaConflict:
            results.append(f"{label}:conflict")

    threads = [threading.Thread(target=writer, args=(name,)) for name in ("a", "b")]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    oks = [r for r in results if r.endswith(":ok")]
    confs = [r for r in results if r.endswith(":conflict")]
    assert len(oks) == 1
    assert len(confs) == 1

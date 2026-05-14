"""Table-level operations: append, time travel, schema evolution."""

from __future__ import annotations

import pytest

from lake.datafile import DataFile
from lake.schema import Field, FieldType, Schema, SchemaEvolutionError
from lake.snapshot import SnapshotOp
from lake.storage.inmemory import InMemoryStorage
from lake.table import Table, TableError


def _schema() -> Schema:
    return Schema(
        schema_id=0,
        fields=(
            Field(id=1, name="id", type=FieldType.LONG, required=True),
            Field(id=2, name="amount", type=FieldType.DOUBLE),
        ),
    )


def _table() -> Table:
    return Table.create(storage=InMemoryStorage(), location="demo/orders", initial_schema=_schema())


def _file(path: str, n: int = 10) -> DataFile:
    return DataFile(path=path, record_count=n, file_size_bytes=1024 * n)


# ---------------------------------------------------------- DataFile


def test_datafile_rejects_invalid_fields():
    with pytest.raises(ValueError):
        DataFile(path="", record_count=0, file_size_bytes=0)
    with pytest.raises(ValueError):
        DataFile(path="x", record_count=-1, file_size_bytes=0)
    with pytest.raises(ValueError):
        DataFile(path="x", record_count=0, file_size_bytes=-1)


def test_datafile_null_count_invariants():
    with pytest.raises(ValueError):
        DataFile(path="x", record_count=10, file_size_bytes=10, null_counts={1: 20})


# ----------------------------------------------------------- Table


def test_table_create_initial_state():
    t = _table()
    assert t.metadata.current_snapshot_id is None
    assert len(t.metadata.snapshots) == 0
    assert t.metadata_version == 1


def test_append_creates_snapshot_and_advances_version():
    t = _table()
    snap = t.append([_file("a.parquet")])
    assert snap.snapshot_id == 1
    assert snap.parent_id is None
    assert snap.op is SnapshotOp.APPEND
    assert t.metadata.current_snapshot_id == 1
    assert t.metadata_version == 2


def test_append_chains_parent_pointer():
    t = _table()
    s1 = t.append([_file("a.parquet")])
    s2 = t.append([_file("b.parquet")])
    assert s2.parent_id == s1.snapshot_id


def test_append_rejects_empty_input():
    t = _table()
    with pytest.raises(TableError):
        t.append([])


def test_files_at_replays_chain():
    t = _table()
    s1 = t.append([_file("a.parquet")])
    s2 = t.append([_file("b.parquet")])
    s3 = t.append([_file("c.parquet")])
    paths1 = {f.path for f in t.files_at(s1.snapshot_id)}
    paths2 = {f.path for f in t.files_at(s2.snapshot_id)}
    paths3 = {f.path for f in t.files_at(s3.snapshot_id)}
    assert paths1 == {"a.parquet"}
    assert paths2 == {"a.parquet", "b.parquet"}
    assert paths3 == {"a.parquet", "b.parquet", "c.parquet"}


def test_delete_removes_files_from_subsequent_snapshots():
    t = _table()
    f_a = _file("a.parquet")
    f_b = _file("b.parquet")
    t.append([f_a, f_b])
    snap = t.delete([f_a])
    assert snap.op is SnapshotOp.DELETE
    paths = {f.path for f in t.files_at(snap.snapshot_id)}
    assert paths == {"b.parquet"}


def test_overwrite_replaces_live_files():
    t = _table()
    t.append([_file("a.parquet"), _file("b.parquet")])
    snap = t.overwrite([_file("c.parquet"), _file("d.parquet")])
    assert snap.op is SnapshotOp.OVERWRITE
    paths = {f.path for f in t.files_at(snap.snapshot_id)}
    assert paths == {"c.parquet", "d.parquet"}


def test_rollback_changes_current_snapshot():
    t = _table()
    s1 = t.append([_file("a.parquet")])
    t.append([_file("b.parquet")])
    t.rollback(s1.snapshot_id)
    assert t.metadata.current_snapshot_id == s1.snapshot_id


def test_rollback_does_not_truncate_history():
    """Iceberg keeps the abandoned snapshots so a forward time-travel is possible."""
    t = _table()
    s1 = t.append([_file("a.parquet")])
    s2 = t.append([_file("b.parquet")])
    t.rollback(s1.snapshot_id)
    # The forward snapshot is still queryable.
    assert {f.path for f in t.files_at(s2.snapshot_id)} == {"a.parquet", "b.parquet"}


def test_evolve_schema_pushes_new_version():
    t = _table()
    new_schema = t.metadata.schema().add_column("country", FieldType.STRING)
    t.evolve_schema(new_schema)
    assert t.metadata.current_schema_id == 1
    assert len(t.metadata.schemas) == 2


def test_evolve_schema_rejects_duplicate_field_name():
    t = _table()
    with pytest.raises(SchemaEvolutionError):
        t.metadata.schema().add_column("amount", FieldType.STRING)


def test_metadata_and_manifest_round_trip_through_storage():
    """The metadata JSON references a manifest; that manifest carries the file paths."""
    storage = InMemoryStorage()
    t = Table.create(storage=storage, location="x", initial_schema=_schema())
    t.append([_file("a.parquet")])
    raw_meta = storage.get(f"metadata/{t.metadata.table_uuid}/v2.json")
    assert b"manifest_ids" in raw_meta
    snap = t.metadata.current_snapshot()
    assert snap is not None
    manifest_path = f"metadata/{t.metadata.table_uuid}/manifests/{snap.manifest_ids[0]}.json"
    assert b"a.parquet" in storage.get(manifest_path)


def test_overwrite_rejects_empty_table():
    t = _table()
    with pytest.raises(TableError):
        t.overwrite([])

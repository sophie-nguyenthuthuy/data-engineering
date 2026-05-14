# minio-iceberg-lakehouse

A from-scratch implementation of the **Iceberg table format** —
schemas with stable field ids and safe evolution, manifests, snapshots,
ACID via atomic metadata-pointer CAS, time-travel queries, and a
pluggable object-store backend.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Iceberg is "Parquet plus the manifest+snapshot machinery". The
production spec is hundreds of pages of Avro/Thrift, but the *ideas*
are small and worth reading in code:

- **Schema by field id** — rename or reorder a column, the data files
  are still readable because they're keyed by id, not position.
- **Snapshots** — every commit produces a new immutable pointer to a
  manifest list; time travel = pick any of them.
- **Atomic commit** — swap the table's metadata pointer with a
  compare-and-swap; concurrent writers either see each other's commit
  or get a `CASMismatch` and retry.

This package implements all three on a pluggable storage layer
(in-memory + local filesystem). The shape is exactly what you'd wire
against S3 / MinIO if you swapped the storage adapter.

## Components

| Module                          | Role                                                            |
| ------------------------------- | --------------------------------------------------------------- |
| `lake.schema`                   | `Schema` with field ids + safe evolution (`add/drop/rename/promote`) |
| `lake.datafile`                 | `DataFile` (path, record count, stats) — validated              |
| `lake.manifest`                 | `Manifest` (group of ADDED/EXISTING/DELETED file entries)       |
| `lake.snapshot`                 | `Snapshot` (parent + manifest ids + commit summary)             |
| `lake.metadata`                 | `TableMetadata` (schema history, snapshot history, pointers)   |
| `lake.storage.base`             | `Storage` ABC + `CASMismatch`                                   |
| `lake.storage.inmemory`         | RLock-guarded dict storage                                       |
| `lake.storage.local_fs`         | POSIX filesystem storage with atomic rename                     |
| `lake.table`                    | `Table.create / append / delete / overwrite / files_at / rollback / evolve_schema` |
| `lake.catalog`                  | Hive-like `(namespace, name) → metadata_path`                  |
| `lake.cli`                      | `lakectl info | demo`                                          |

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## CLI

```bash
lakectl info
lakectl demo
```

## Library

```python
from lake.schema           import Field, FieldType, Schema
from lake.datafile         import DataFile
from lake.storage.inmemory import InMemoryStorage
from lake.table            import Table

# 1. Create a table.
schema = Schema(
    schema_id=0,
    fields=(
        Field(id=1, name="id",     type=FieldType.LONG,   required=True),
        Field(id=2, name="amount", type=FieldType.DOUBLE),
    ),
)
table = Table.create(
    storage=InMemoryStorage(),
    location="lake/orders",
    initial_schema=schema,
)

# 2. Append.
snap1 = table.append([DataFile("p0.parquet", record_count=10, file_size_bytes=1024)])
snap2 = table.append([DataFile("p1.parquet", record_count=20, file_size_bytes=2048)])

# 3. Time travel.
files_at_s1 = table.files_at(snap1.snapshot_id)  # only p0.parquet
files_at_s2 = table.files_at(snap2.snapshot_id)  # p0 + p1

# 4. Schema evolution by id.
new_schema = table.metadata.schema().add_column("country", FieldType.STRING)
table.evolve_schema(new_schema)

# 5. Rollback — abandoned snapshots remain reachable for forward time-travel.
table.rollback(snap1.snapshot_id)
```

## ACID via metadata swap

Every commit (`append`, `delete`, `overwrite`, `evolve_schema`,
`rollback`) calls `storage.atomic_put(metadata_path, bytes,
expected_etag=...)`. If a concurrent writer wins the race, the loser
sees `CASMismatch` and the caller can rebuild the in-memory state from
the new metadata and retry. That single primitive is what makes
multi-writer Iceberg tables safe.

## Schema evolution

```python
schema_v0 = Schema(schema_id=0, fields=(
    Field(1, "id",     FieldType.LONG, required=True),
    Field(2, "amount", FieldType.DOUBLE),
))

# Safe — pure metadata, new id assigned, old data reads as NULL.
schema_v1 = schema_v0.add_column("country", FieldType.STRING)

# Safe — rename by id, no data rewrite.
schema_v2 = schema_v1.rename_column("amount", "total")

# Refused — double → int is lossy and would corrupt prior data.
schema_v2.promote_type("total", FieldType.INT)
# → SchemaEvolutionError
```

## Time travel

`Table.files_at(snap_id)` replays the chain of parent snapshots and
applies each manifest's `ADDED`/`DELETED` actions in order — the
result is exactly the set of files live at that commit, regardless of
what's happened on the table since.

## Quality

```bash
make test       # 50+ tests
make type       # mypy --strict
make lint
```

- **50+ tests** covering schema evolution, storage backends + CAS,
  catalog scoping, snapshot replay (append/delete/overwrite),
  rollback non-destructiveness, evolve_schema → metadata version bump.
- mypy `--strict` clean over 15 source files; ruff clean.
- Multi-stage slim Docker image, non-root `lake` user.
- Python 3.10 / 3.11 / 3.12 CI matrix.
- **Zero runtime dependencies.**

## License

MIT — see [LICENSE](LICENSE).

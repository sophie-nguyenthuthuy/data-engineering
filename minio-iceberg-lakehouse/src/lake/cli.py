"""``lakectl`` command-line interface."""

from __future__ import annotations

import argparse


def cmd_info(_args: argparse.Namespace) -> int:
    from lake import __version__

    print(f"minio-iceberg-lakehouse {__version__}")
    return 0


def cmd_demo(_args: argparse.Namespace) -> int:
    from lake.datafile import DataFile
    from lake.schema import Field, FieldType, Schema
    from lake.storage.inmemory import InMemoryStorage
    from lake.table import Table

    schema = Schema(
        schema_id=0,
        fields=(
            Field(id=1, name="id", type=FieldType.LONG, required=True),
            Field(id=2, name="amount", type=FieldType.DOUBLE),
        ),
    )
    table = Table.create(storage=InMemoryStorage(), location="demo/orders", initial_schema=schema)

    f1 = DataFile(path="orders/p0.parquet", record_count=10, file_size_bytes=1024)
    f2 = DataFile(path="orders/p1.parquet", record_count=20, file_size_bytes=2048)
    s1 = table.append([f1])
    s2 = table.append([f2])

    new_schema = table.metadata.schema().add_column("country", FieldType.STRING)
    table.evolve_schema(new_schema)

    print(f"snapshots = {[s.snapshot_id for s in table.metadata.snapshots]}")
    print(f"current   = {table.metadata.current_snapshot_id}")
    print(f"files@{s1.snapshot_id} = {[f.path for f in table.files_at(s1.snapshot_id)]}")
    print(f"files@{s2.snapshot_id} = {[f.path for f in table.files_at(s2.snapshot_id)]}")
    print(f"schema_id = {table.metadata.current_schema_id}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="lakectl")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("info").set_defaults(func=cmd_info)
    sub.add_parser("demo", help="create a table, append twice, evolve schema").set_defaults(
        func=cmd_demo
    )
    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

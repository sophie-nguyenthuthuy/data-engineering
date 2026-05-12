"""Tests for the schema catalog."""

import pytest
from federation.catalog import ColumnDef, SchemaCatalog, SourceType, TableSchema


def test_register_and_lookup():
    cat = SchemaCatalog()
    cat.register_source("postgres", SourceType.POSTGRES, {"host": "localhost"})
    schema = TableSchema(
        source="postgres", table="orders", source_type=SourceType.POSTGRES,
        columns=[ColumnDef("id", "int"), ColumnDef("total", "float")],
        estimated_rows=50_000,
    )
    cat.register_table(schema)

    retrieved = cat.get_table("postgres.orders")
    assert retrieved.qualified_name == "postgres.orders"
    assert retrieved.estimated_rows == 50_000
    assert retrieved.column_names() == ["id", "total"]


def test_unknown_table_raises():
    cat = SchemaCatalog()
    with pytest.raises(KeyError, match="postgres.missing"):
        cat.get_table("postgres.missing")


def test_unknown_source_raises():
    cat = SchemaCatalog()
    with pytest.raises(KeyError, match="nope"):
        cat.get_source_connection("nope")


def test_list_tables(catalog):
    tables = catalog.list_tables()
    assert "postgres.orders"   in tables
    assert "mongodb.users"     in tables
    assert "s3_parquet.events" in tables
    assert "rest_api.products" in tables


def test_from_yaml(tmp_path):
    yaml_text = """
sources:
  - name: pg
    type: postgres
    connection:
      host: localhost
      dbname: mydb

tables:
  - source: pg
    table: customers
    estimated_rows: 10000
    columns:
      - name: id
        type: int
      - name: email
        type: string
"""
    config_file = tmp_path / "catalog.yaml"
    config_file.write_text(yaml_text)

    cat = SchemaCatalog.from_yaml(config_file)
    assert "pg.customers" in cat.list_tables()
    schema = cat.get_table("pg.customers")
    assert schema.estimated_rows == 10_000
    assert schema.column_names() == ["id", "email"]

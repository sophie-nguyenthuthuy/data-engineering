"""Thin wrapper around pyiceberg + DuckDB for the Dagster assets."""
from __future__ import annotations

from typing import Iterable

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

from .config import (
    CATALOG_NAME,
    CATALOG_URI,
    NAMESPACE,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_SECRET_KEY,
    WAREHOUSE_PATH,
)


def get_catalog() -> Catalog:
    cat = SqlCatalog(
        CATALOG_NAME,
        **{
            "uri": CATALOG_URI,
            "warehouse": WAREHOUSE_PATH,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
    try:
        cat.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return cat


def upsert_table(name: str, table: pa.Table, partition_by: list[str] | None = None):
    """Create-or-append an Iceberg table from a PyArrow Table."""
    cat = get_catalog()
    full = f"{NAMESPACE}.{name}"
    try:
        ice = cat.load_table(full)
        ice.append(table)
    except NoSuchTableError:
        cat.create_table(full, schema=table.schema)
        cat.load_table(full).append(table)
    return full


def overwrite_table(name: str, table: pa.Table):
    cat = get_catalog()
    full = f"{NAMESPACE}.{name}"
    try:
        ice = cat.load_table(full)
        ice.overwrite(table)
    except NoSuchTableError:
        cat.create_table(full, schema=table.schema).append(table)
    return full


def scan(name: str) -> pa.Table:
    cat = get_catalog()
    return cat.load_table(f"{NAMESPACE}.{name}").scan().to_arrow()


def duckdb_connect() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection configured to read parquet from MinIO directly
    (used by the Streamlit dashboard for fast scans)."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    host = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET s3_minio (
          TYPE s3,
          KEY_ID '{S3_ACCESS_KEY}',
          SECRET '{S3_SECRET_KEY}',
          ENDPOINT '{host}',
          URL_STYLE 'path',
          USE_SSL false
        );
        """
    )
    return con


def register_iceberg_views(con: duckdb.DuckDBPyConnection, tables: Iterable[str]):
    """Materialize Iceberg tables as DuckDB views by scanning via pyiceberg."""
    cat = get_catalog()
    for t in tables:
        arrow = cat.load_table(f"{NAMESPACE}.{t}").scan().to_arrow()
        con.register(t, arrow)

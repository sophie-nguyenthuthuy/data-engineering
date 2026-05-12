"""DuckDB-backed persistence for extracted products and CPI data."""

from __future__ import annotations

from collections.abc import Iterable

import duckdb

from .config import settings
from .extract import Product
from .logging import get_logger

log = get_logger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR,
    url VARCHAR,
    title VARCHAR,
    brand VARCHAR,
    price DOUBLE,
    currency VARCHAR,
    category VARCHAR,
    fetch_time TIMESTAMP,
    source VARCHAR,
    PRIMARY KEY (product_id, fetch_time)
);

CREATE INDEX IF NOT EXISTS products_fetch_time ON products(fetch_time);
CREATE INDEX IF NOT EXISTS products_category ON products(category);

CREATE TABLE IF NOT EXISTS cpi (
    series_id VARCHAR,
    period VARCHAR,  -- YYYY-MM
    value DOUBLE,
    PRIMARY KEY (series_id, period)
);
"""


def connect() -> duckdb.DuckDBPyConnection:
    settings.ensure_dirs()
    con = duckdb.connect(str(settings.db_path))
    con.execute(SCHEMA)
    return con


def upsert_products(products: Iterable[Product]) -> int:
    products = list(products)
    if not products:
        return 0
    con = connect()
    con.executemany(
        """
        INSERT OR REPLACE INTO products
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (p.product_id, p.url, p.title, p.brand, p.price, p.currency,
             p.category, p.fetch_time, p.source)
            for p in products
        ],
    )
    log.info("store.products_upserted", count=len(products))
    return len(products)


def upsert_cpi(series_id: str, rows: list[tuple[str, float]]) -> int:
    if not rows:
        return 0
    con = connect()
    con.executemany(
        "INSERT OR REPLACE INTO cpi VALUES (?, ?, ?)",
        [(series_id, period, value) for period, value in rows],
    )
    log.info("store.cpi_upserted", series_id=series_id, count=len(rows))
    return len(rows)

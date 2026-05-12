"""SQLite layer. Upsert-by-PK so re-running ingest is idempotent."""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterable

from .config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS prices (
  asset       TEXT NOT NULL,
  ts          INTEGER NOT NULL,
  price       REAL NOT NULL,
  vs_currency TEXT NOT NULL,
  PRIMARY KEY (asset, ts, vs_currency)
);
CREATE INDEX IF NOT EXISTS idx_prices_asset_ts ON prices(asset, ts DESC);
"""


def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


@contextmanager
def connect():
    _ensure_parent(settings.db_path)
    conn = sqlite3.connect(settings.db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with connect() as c:
        c.executescript(SCHEMA)


def upsert_prices(rows: Iterable[tuple[str, int, float, str]]) -> int:
    """rows: (asset, ts_epoch, price, vs_currency)."""
    rows = list(rows)
    if not rows:
        return 0
    with connect() as c:
        c.executemany(
            "INSERT OR REPLACE INTO prices(asset, ts, price, vs_currency) VALUES (?, ?, ?, ?)",
            rows,
        )
    return len(rows)


def recent_prices(asset: str, vs_currency: str, limit: int = 500) -> list[dict]:
    with connect() as c:
        cur = c.execute(
            "SELECT ts, price FROM prices "
            "WHERE asset=? AND vs_currency=? ORDER BY ts DESC LIMIT ?",
            (asset, vs_currency, limit),
        )
        rows = [{"ts": ts, "price": price} for ts, price in cur.fetchall()]
    rows.reverse()
    return rows


def row_count() -> int:
    with connect() as c:
        (n,) = c.execute("SELECT COUNT(*) FROM prices").fetchone()
        return n

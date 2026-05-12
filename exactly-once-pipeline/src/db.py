"""Postgres connection pool and typed query helpers."""
from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Generator

import psycopg2
import psycopg2.extras
import psycopg2.pool
import structlog

from src.config import settings

log = structlog.get_logger(__name__)

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=settings.postgres_dsn,
        )
    return _pool


@contextmanager
def get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


@contextmanager
def transaction() -> Generator[psycopg2.extensions.cursor, None, None]:
    """Yield a cursor inside a committed (or rolled-back) transaction."""
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def execute(sql: str, params: tuple[Any, ...] | None = None) -> list[dict]:
    with transaction() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchall()  # type: ignore[return-value]
        except psycopg2.ProgrammingError:
            return []


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=str)

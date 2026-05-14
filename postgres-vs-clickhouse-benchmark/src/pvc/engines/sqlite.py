"""Stdlib-backed SQLite engine.

The reference engine — every CI has SQLite, so this is the one we
exercise tests against. An in-memory connection is plenty fast for
benchmark plumbing tests; for repeatable production runs the caller
supplies a file path.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any

from pvc.engines.base import Engine, EngineError


@dataclass
class SQLiteEngine(Engine):
    """SQLite engine using the stdlib ``sqlite3`` driver."""

    path: str = ":memory:"
    name: str = "sqlite"
    _conn: sqlite3.Connection | None = field(default=None, init=False, repr=False)

    def setup(self, ddl: list[str], inserts: list[tuple[str, list[tuple[Any, ...]]]]) -> None:
        if self._conn is not None:
            raise EngineError("engine already set up")
        conn = sqlite3.connect(self.path)
        try:
            for stmt in ddl:
                conn.execute(stmt)
            for sql, rows in inserts:
                if rows:
                    conn.executemany(sql, rows)
            conn.commit()
        except sqlite3.Error as exc:
            conn.close()
            raise EngineError(f"setup failed: {exc}") from exc
        self._conn = conn

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        if self._conn is None:
            raise EngineError("engine not set up")
        try:
            cur = self._conn.execute(sql)
        except sqlite3.Error as exc:
            raise EngineError(f"query failed: {exc}") from exc
        return list(cur.fetchall())

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


__all__ = ["SQLiteEngine"]

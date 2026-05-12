"""PostgreSQL connection management."""
from __future__ import annotations
import contextlib
import logging
from typing import Any, Generator

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConnection, cursor as PGCursor

logger = logging.getLogger(__name__)


class DBConfig:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "imdb",
        user: str = "postgres",
        password: str = "postgres",
        application_name: str = "cle-optimizer",
    ) -> None:
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.application_name = application_name

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password} "
            f"application_name={self.application_name}"
        )

    @classmethod
    def from_env(cls) -> "DBConfig":
        import os
        return cls(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432")),
            dbname=os.getenv("PG_DBNAME", "imdb"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", "postgres"),
        )


class ConnectionPool:
    """Simple single-connection wrapper (use pgbouncer for real pooling)."""

    def __init__(self, config: DBConfig) -> None:
        self.config = config
        self._conn: PGConnection | None = None

    def _connect(self) -> PGConnection:
        conn = psycopg2.connect(self.config.dsn)
        conn.autocommit = True
        logger.debug("Connected to %s/%s", self.config.host, self.config.dbname)
        return conn

    @property
    def conn(self) -> PGConnection:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    @contextlib.contextmanager
    def cursor(self, factory=None) -> Generator[PGCursor, None, None]:
        cur = self.conn.cursor(cursor_factory=factory)
        try:
            yield cur
        finally:
            cur.close()

    def execute(self, sql: str, params: tuple = ()) -> list[Any]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return []

    def set_timeout(self, ms: int) -> None:
        self.execute(f"SET statement_timeout = {ms}")

    def reset_timeout(self) -> None:
        self.execute("RESET statement_timeout")

    def enable_hint_plan(self) -> None:
        """Load pg_hint_plan if available."""
        try:
            self.execute("LOAD 'pg_hint_plan'")
            self.execute("SET pg_hint_plan.enable_hint = on")
            logger.info("pg_hint_plan enabled")
        except Exception as e:
            logger.warning("pg_hint_plan not available: %s", e)

    def get_pg_version(self) -> str:
        rows = self.execute("SELECT version()")
        return rows[0][0] if rows else "unknown"

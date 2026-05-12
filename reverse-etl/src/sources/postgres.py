from typing import Any
import psycopg2
import psycopg2.extras
from ..settings import settings
from ..logger import get_logger
from .base import BaseSource

logger = get_logger(__name__)


class PostgresSource(BaseSource):
    def __init__(self, params: dict[str, Any]) -> None:
        super().__init__(params)
        self._conn_params = {
            "host": params.get("host", settings.postgres_host),
            "port": int(params.get("port", settings.postgres_port)),
            "dbname": params.get("database", settings.postgres_db),
            "user": params.get("user", settings.postgres_user),
            "password": params.get("password", settings.postgres_password),
        }

    def fetch(self, query: str, query_params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        logger.debug(f"PostgresSource: executing query")
        with psycopg2.connect(**self._conn_params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, query_params or {})
                rows = [dict(row) for row in cur.fetchall()]
        logger.info(f"PostgresSource: fetched {len(rows)} rows")
        return rows

from typing import Any
from ..settings import settings
from ..logger import get_logger
from .base import BaseSource

logger = get_logger(__name__)


class SnowflakeSource(BaseSource):
    def __init__(self, params: dict[str, Any]) -> None:
        super().__init__(params)
        self._conn_params = {
            "account": params.get("account", settings.snowflake_account),
            "user": params.get("user", settings.snowflake_user),
            "password": params.get("password", settings.snowflake_password),
            "database": params.get("database", settings.snowflake_database),
            "warehouse": params.get("warehouse", settings.snowflake_warehouse),
            "schema": params.get("schema", settings.snowflake_schema),
        }

    def fetch(self, query: str, query_params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        import snowflake.connector
        logger.debug("SnowflakeSource: executing query")
        with snowflake.connector.connect(**self._conn_params) as conn:
            cur = conn.cursor(snowflake.connector.DictCursor)
            cur.execute(query, query_params or {})
            rows = cur.fetchall()
        logger.info(f"SnowflakeSource: fetched {len(rows)} rows")
        return rows

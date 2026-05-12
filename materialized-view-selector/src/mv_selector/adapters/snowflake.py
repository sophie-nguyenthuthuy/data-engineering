"""
Snowflake adapter.

Requires:  snowflake-connector-python >= 3.0
           Connection params via environment or explicit kwargs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from ..models import CandidateView, MaterializedView, QueryRecord, Warehouse
from .base import BaseAdapter

try:
    import snowflake.connector
    _SF_AVAILABLE = True
except ImportError:
    _SF_AVAILABLE = False


def _require_sf() -> None:
    if not _SF_AVAILABLE:
        raise ImportError(
            "snowflake-connector-python is not installed. "
            "Run: pip install snowflake-connector-python"
        )


class SnowflakeAdapter(BaseAdapter):
    """
    Parameters
    ----------
    account, user, password, warehouse, database, schema
        Standard Snowflake connection parameters.  Alternatively pass
        `private_key_path` for key-pair auth.
    """

    warehouse = Warehouse.SNOWFLAKE

    def __init__(
        self,
        account: str,
        user: str,
        password: Optional[str] = None,
        warehouse_name: str = "COMPUTE_WH",
        database: str = "ANALYTICS",
        schema: str = "PUBLIC",
        private_key_path: Optional[str] = None,
        **extra_kwargs: Any,
    ) -> None:
        _require_sf()
        self.account = account
        self.user = user
        self.password = password
        self.warehouse_name = warehouse_name
        self.database = database
        self.schema = schema
        self.private_key_path = private_key_path
        self._extra = extra_kwargs
        self._conn: Optional[Any] = None

    @property
    def conn(self) -> Any:
        if self._conn is None or self._conn.is_closed():
            params: dict[str, Any] = dict(
                account=self.account,
                user=self.user,
                warehouse=self.warehouse_name,
                database=self.database,
                schema=self.schema,
            )
            if self.password:
                params["password"] = self.password
            if self.private_key_path:
                params["private_key_path"] = self.private_key_path
            params.update(self._extra)
            self._conn = snowflake.connector.connect(**params)
        return self._conn

    def _execute(self, sql: str, params: Optional[list] = None) -> list[dict]:
        cur = self.conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql, params or [])
        return cur.fetchall()

    # ------------------------------------------------------------------
    # Worklog
    # ------------------------------------------------------------------

    def fetch_query_history(
        self,
        since: datetime,
        limit: int = 10_000,
    ) -> list[QueryRecord]:
        sql = """
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            START_TIME,
            EXECUTION_TIME,
            BYTES_SCANNED,
            CREDITS_USED_CLOUD_SERVICES,
            USER_NAME
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            DATEADD('hour', -720, CURRENT_TIMESTAMP()),
            CURRENT_TIMESTAMP()
        ))
        WHERE START_TIME >= %s
          AND QUERY_TYPE = 'SELECT'
          AND EXECUTION_STATUS = 'SUCCESS'
        ORDER BY START_TIME DESC
        LIMIT %s
        """
        rows = self._execute(sql, [since, limit])
        records: list[QueryRecord] = []
        for row in rows:
            credits = float(row.get("CREDITS_USED_CLOUD_SERVICES") or 0)
            cost_usd = credits * 2.0  # ~$2/credit
            records.append(
                QueryRecord(
                    query_id=str(row["QUERY_ID"]),
                    sql=row.get("QUERY_TEXT") or "",
                    warehouse=Warehouse.SNOWFLAKE,
                    executed_at=row["START_TIME"].replace(tzinfo=timezone.utc),
                    duration_ms=int(row.get("EXECUTION_TIME") or 0),
                    bytes_processed=int(row.get("BYTES_SCANNED") or 0),
                    cost_usd=cost_usd,
                    user=row.get("USER_NAME"),
                    project_or_account=self.account,
                    dataset_or_schema=f"{self.database}.{self.schema}",
                )
            )
        return records

    # ------------------------------------------------------------------
    # View lifecycle
    # ------------------------------------------------------------------

    def create_view(
        self,
        candidate: CandidateView,
        dataset_or_schema: str,
    ) -> MaterializedView:
        parts = dataset_or_schema.split(".")
        db = parts[0] if len(parts) > 1 else self.database
        schema = parts[-1]
        fqn = f"{db}.{schema}.{candidate.name.upper()}"
        ddl = (
            f"CREATE OR REPLACE MATERIALIZED VIEW {fqn} AS\n"
            f"{candidate.sql}"
        )
        self._execute(ddl)
        return MaterializedView(
            candidate=candidate,
            warehouse=Warehouse.SNOWFLAKE,
            created_at=datetime.now(timezone.utc),
            fqn=fqn,
        )

    def refresh_view(self, view: MaterializedView) -> MaterializedView:
        # Snowflake MVs refresh automatically; force via SUSPEND+RESUME
        self._execute(f"ALTER MATERIALIZED VIEW {view.fqn} SUSPEND")
        self._execute(f"ALTER MATERIALIZED VIEW {view.fqn} RESUME")
        view.last_refreshed_at = datetime.now(timezone.utc)
        view.refresh_count += 1
        return view

    def drop_view(self, view: MaterializedView) -> None:
        self._execute(f"DROP MATERIALIZED VIEW IF EXISTS {view.fqn}")
        view.is_active = False

    # ------------------------------------------------------------------
    # Cost measurement
    # ------------------------------------------------------------------

    def measure_savings(
        self,
        view: MaterializedView,
        since: datetime,
    ) -> float:
        table_conditions = " OR ".join(
            f"LOWER(QUERY_TEXT) LIKE '%{t.lower()}%'"
            for t in view.candidate.referenced_tables
        )
        created_str = view.created_at.strftime("%Y-%m-%d %H:%M:%S")
        since_str = since.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        WITH after_mv AS (
            SELECT AVG(BYTES_SCANNED) AS avg_bytes
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                TO_TIMESTAMP_LTZ('{created_str}'),
                CURRENT_TIMESTAMP()
            ))
            WHERE QUERY_TYPE = 'SELECT'
              AND EXECUTION_STATUS = 'SUCCESS'
              AND ({table_conditions})
        ),
        before_mv AS (
            SELECT AVG(BYTES_SCANNED) AS avg_bytes
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                TO_TIMESTAMP_LTZ('{since_str}'),
                TO_TIMESTAMP_LTZ('{created_str}')
            ))
            WHERE QUERY_TYPE = 'SELECT'
              AND EXECUTION_STATUS = 'SUCCESS'
              AND ({table_conditions})
        )
        SELECT
            GREATEST(0, b.avg_bytes - a.avg_bytes) * 2.0 / 1e12 AS estimated_savings_usd
        FROM before_mv b, after_mv a
        """
        rows = self._execute(sql)
        if rows and rows[0].get("ESTIMATED_SAVINGS_USD") is not None:
            return float(rows[0]["ESTIMATED_SAVINGS_USD"])
        return 0.0

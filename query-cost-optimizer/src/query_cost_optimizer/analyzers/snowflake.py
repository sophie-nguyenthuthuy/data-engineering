"""Snowflake query-history analyzer using ACCOUNT_USAGE.QUERY_HISTORY."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from ..models import Platform, QueryRecord, TableStats

logger = logging.getLogger(__name__)

# Serverless credit pricing approximation: $3 / credit, ~1 credit = 3200 seconds of compute
# We estimate from execution_time and credits_used_cloud_services when available.
_DEFAULT_CREDIT_PRICE_USD = 3.0


class SnowflakeAnalyzer:
    """Pull query history from Snowflake ACCOUNT_USAGE and build TableStats."""

    def __init__(
        self,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        warehouse: str | None = None,
        database: str = "SNOWFLAKE",
        schema: str = "ACCOUNT_USAGE",
        history_days: int = 30,
        credit_price_usd: float = _DEFAULT_CREDIT_PRICE_USD,
    ) -> None:
        try:
            import snowflake.connector  # type: ignore
        except ImportError as e:
            raise ImportError(
                "snowflake-connector-python is required. "
                "Install it with: pip install 'query-cost-optimizer[snowflake]'"
            ) from e

        import snowflake.connector

        self.history_days = history_days
        self.credit_price_usd = credit_price_usd
        self._conn = snowflake.connector.connect(
            account=account or os.environ["SNOWFLAKE_ACCOUNT"],
            user=user or os.environ["SNOWFLAKE_USER"],
            password=password or os.environ.get("SNOWFLAKE_PASSWORD", ""),
            warehouse=warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
            database=database,
            schema=schema,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_query_history(self) -> list[QueryRecord]:
        """Return recent queries from ACCOUNT_USAGE.QUERY_HISTORY."""
        sql = f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            USER_NAME,
            START_TIME,
            END_TIME,
            BYTES_SCANNED,
            BYTES_WRITTEN,
            EXECUTION_TIME,
            CREDITS_USED_CLOUD_SERVICES,
            TABLES_REFERENCED_BY_QUERY
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE
            START_TIME >= DATEADD(DAY, -{self.history_days}, CURRENT_TIMESTAMP())
            AND EXECUTION_STATUS = 'SUCCESS'
            AND QUERY_TYPE = 'SELECT'
        ORDER BY BYTES_SCANNED DESC NULLS LAST
        LIMIT 50000
        """
        logger.info("Fetching Snowflake query history (last %d days)…", self.history_days)
        cursor = self._conn.cursor()
        cursor.execute(sql)
        cols = [d[0].lower() for d in cursor.description]
        records: list[QueryRecord] = []
        for raw in cursor.fetchall():
            row = dict(zip(cols, raw))
            credits = row.get("credits_used_cloud_services") or 0
            cost = float(credits) * self.credit_price_usd
            tables = self._parse_tables(row.get("tables_referenced_by_query"))
            records.append(
                QueryRecord(
                    query_id=row["query_id"],
                    query_text=row["query_text"] or "",
                    user=row["user_name"] or "",
                    start_time=row["start_time"],
                    end_time=row["end_time"],
                    bytes_processed=int(row.get("bytes_scanned") or 0),
                    bytes_billed=int(row.get("bytes_written") or 0),
                    elapsed_ms=int(row.get("execution_time") or 0),
                    tables_referenced=tables,
                    platform=Platform.SNOWFLAKE,
                    cost_usd=cost,
                )
            )
        logger.info("Fetched %d query records from Snowflake.", len(records))
        return records

    def build_table_stats(self, records: list[QueryRecord]) -> list[TableStats]:
        """Aggregate per-table usage from query records."""
        from ..sql_parser import extract_filter_columns, extract_join_columns, extract_group_by_columns

        stats_map: dict[str, TableStats] = {}
        for rec in records:
            for tbl in rec.tables_referenced:
                if tbl not in stats_map:
                    stats_map[tbl] = TableStats(table_id=tbl, platform=Platform.SNOWFLAKE)
                s = stats_map[tbl]
                s.query_count += 1
                s.total_bytes_scanned += rec.bytes_processed
                s.total_cost_usd += rec.cost_usd
                for col in extract_filter_columns(rec.query_text, tbl):
                    if col not in s.filter_columns:
                        s.filter_columns.append(col)
                for col in extract_join_columns(rec.query_text, tbl):
                    if col not in s.join_columns:
                        s.join_columns.append(col)
                for col in extract_group_by_columns(rec.query_text):
                    if col not in s.group_by_columns:
                        s.group_by_columns.append(col)

        return sorted(stats_map.values(), key=lambda x: x.total_cost_usd, reverse=True)

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_tables(raw: str | None) -> list[str]:
        """Parse the semi-colon-separated table list Snowflake returns."""
        if not raw:
            return []
        return [t.strip() for t in raw.split(";") if t.strip()]

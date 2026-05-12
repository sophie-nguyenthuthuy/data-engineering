"""BigQuery query-history analyzer using INFORMATION_SCHEMA."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from ..models import Platform, QueryRecord, TableStats

logger = logging.getLogger(__name__)

# On-demand pricing: $6.25 per TiB scanned (as of 2025)
_BQ_COST_PER_TIB = 6.25
_BYTES_PER_TIB = 1024 ** 4


class BigQueryAnalyzer:
    """Pull query history from BigQuery INFORMATION_SCHEMA and build TableStats."""

    def __init__(
        self,
        project_id: str | None = None,
        history_days: int = 30,
    ) -> None:
        try:
            from google.cloud import bigquery  # type: ignore
        except ImportError as e:
            raise ImportError(
                "google-cloud-bigquery is required. "
                "Install it with: pip install 'query-cost-optimizer[bigquery]'"
            ) from e

        self.project_id = project_id or os.environ["BQ_PROJECT_ID"]
        self.history_days = history_days
        self._client = bigquery.Client(project=self.project_id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_query_history(self) -> list[QueryRecord]:
        """Return recent completed queries from INFORMATION_SCHEMA.JOBS."""
        sql = f"""
        SELECT
            job_id,
            query,
            user_email,
            creation_time,
            end_time,
            total_bytes_processed,
            total_bytes_billed,
            TIMESTAMP_DIFF(end_time, creation_time, MILLISECOND) AS elapsed_ms,
            referenced_tables
        FROM `region-us`.INFORMATION_SCHEMA.JOBS
        WHERE
            creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.history_days} DAY)
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND total_bytes_processed IS NOT NULL
        ORDER BY total_bytes_processed DESC
        LIMIT 50000
        """
        logger.info("Fetching BigQuery job history (last %d days)…", self.history_days)
        rows = list(self._client.query(sql).result())
        records: list[QueryRecord] = []
        for row in rows:
            tables = []
            if row.referenced_tables:
                for t in row.referenced_tables:
                    tables.append(f"{t.get('projectId','')}.{t.get('datasetId','')}.{t.get('tableId','')}")
            cost = (row.total_bytes_billed or 0) / _BYTES_PER_TIB * _BQ_COST_PER_TIB
            records.append(
                QueryRecord(
                    query_id=row.job_id,
                    query_text=row.query or "",
                    user=row.user_email or "",
                    start_time=row.creation_time,
                    end_time=row.end_time,
                    bytes_processed=row.total_bytes_processed or 0,
                    bytes_billed=row.total_bytes_billed or 0,
                    elapsed_ms=row.elapsed_ms or 0,
                    tables_referenced=tables,
                    platform=Platform.BIGQUERY,
                    cost_usd=cost,
                )
            )
        logger.info("Fetched %d query records from BigQuery.", len(records))
        return records

    def fetch_table_metadata(self, dataset_ids: list[str] | None = None) -> dict[str, dict]:
        """Return row count + size for tables in the project."""
        meta: dict[str, dict] = {}
        datasets = dataset_ids or self._list_datasets()
        for ds in datasets:
            sql = f"""
            SELECT
                table_id,
                row_count,
                size_bytes
            FROM `{self.project_id}.{ds}.__TABLES__`
            """
            try:
                for row in self._client.query(sql).result():
                    key = f"{self.project_id}.{ds}.{row.table_id}"
                    meta[key] = {
                        "row_count": row.row_count or 0,
                        "size_bytes": row.size_bytes or 0,
                    }
            except Exception:
                logger.debug("Could not fetch metadata for dataset %s", ds)
        return meta

    def build_table_stats(self, records: list[QueryRecord]) -> list[TableStats]:
        """Aggregate per-table usage from query records."""
        from ..sql_parser import extract_filter_columns, extract_join_columns, extract_group_by_columns

        stats_map: dict[str, TableStats] = {}
        for rec in records:
            for tbl in rec.tables_referenced:
                if tbl not in stats_map:
                    stats_map[tbl] = TableStats(table_id=tbl, platform=Platform.BIGQUERY)
                s = stats_map[tbl]
                s.query_count += 1
                s.total_bytes_scanned += rec.bytes_processed
                s.total_cost_usd += rec.cost_usd
                # Parse SQL for column usage
                for col in extract_filter_columns(rec.query_text, tbl):
                    if col not in s.filter_columns:
                        s.filter_columns.append(col)
                for col in extract_join_columns(rec.query_text, tbl):
                    if col not in s.join_columns:
                        s.join_columns.append(col)
                for col in extract_group_by_columns(rec.query_text):
                    if col not in s.group_by_columns:
                        s.group_by_columns.append(col)

        # Enrich with table metadata
        table_meta = self.fetch_table_metadata()
        for tbl, s in stats_map.items():
            if tbl in table_meta:
                s.row_count = table_meta[tbl]["row_count"]
                s.size_bytes = table_meta[tbl]["size_bytes"]

        return sorted(stats_map.values(), key=lambda x: x.total_cost_usd, reverse=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _list_datasets(self) -> list[str]:
        return [ds.dataset_id for ds in self._client.list_datasets()]

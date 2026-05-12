"""
BigQuery adapter.

Requires:  google-cloud-bigquery >= 3.0
           GOOGLE_APPLICATION_CREDENTIALS or ADC configured.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

from ..models import CandidateView, MaterializedView, QueryRecord, Warehouse
from .base import BaseAdapter

try:
    from google.cloud import bigquery
    from google.cloud.bigquery import QueryJobConfig
    _BQ_AVAILABLE = True
except ImportError:
    _BQ_AVAILABLE = False


def _require_bq() -> None:
    if not _BQ_AVAILABLE:
        raise ImportError(
            "google-cloud-bigquery is not installed. "
            "Run: pip install google-cloud-bigquery"
        )


class BigQueryAdapter(BaseAdapter):
    """
    Parameters
    ----------
    project : str
        GCP project id (e.g. "my-project")
    location : str
        BQ location (e.g. "US", "EU")
    """

    warehouse = Warehouse.BIGQUERY

    def __init__(self, project: str, location: str = "US") -> None:
        _require_bq()
        self.project = project
        self.location = location
        self._client: Optional["bigquery.Client"] = None

    @property
    def client(self) -> "bigquery.Client":
        if self._client is None:
            self._client = bigquery.Client(
                project=self.project, location=self.location
            )
        return self._client

    # ------------------------------------------------------------------
    # Worklog
    # ------------------------------------------------------------------

    def fetch_query_history(
        self,
        since: datetime,
        limit: int = 10_000,
    ) -> list[QueryRecord]:
        since_str = since.strftime("%Y-%m-%d %H:%M:%S UTC")
        sql = f"""
        SELECT
            job_id,
            query,
            creation_time,
            total_slot_ms,
            total_bytes_processed,
            total_bytes_billed,
            user_email,
            ROUND(total_bytes_billed * 5.0 / 1e12, 6) AS cost_usd
        FROM
            `{self.project}`.`region-{self.location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE
            creation_time >= TIMESTAMP('{since_str}')
            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL
            AND statement_type = 'SELECT'
        ORDER BY creation_time DESC
        LIMIT {limit}
        """
        rows = list(self.client.query(sql).result())
        records: list[QueryRecord] = []
        for row in rows:
            records.append(
                QueryRecord(
                    query_id=row.job_id,
                    sql=row.query or "",
                    warehouse=Warehouse.BIGQUERY,
                    executed_at=row.creation_time.replace(tzinfo=timezone.utc),
                    duration_ms=int(row.total_slot_ms or 0),
                    bytes_processed=int(row.total_bytes_processed or 0),
                    cost_usd=float(row.cost_usd or 0.0),
                    user=row.user_email,
                    project_or_account=self.project,
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
        fqn = f"{self.project}.{dataset_or_schema}.{candidate.name}"
        ddl = f"CREATE MATERIALIZED VIEW IF NOT EXISTS `{fqn}` AS\n{candidate.sql}"
        self.client.query(ddl).result()

        return MaterializedView(
            candidate=candidate,
            warehouse=Warehouse.BIGQUERY,
            created_at=datetime.now(timezone.utc),
            fqn=fqn,
        )

    def refresh_view(self, view: MaterializedView) -> MaterializedView:
        # BigQuery MVs refresh automatically; manual refresh via a full rebuild
        ddl = (
            f"CREATE OR REPLACE MATERIALIZED VIEW `{view.fqn}` AS\n"
            f"{view.candidate.sql}"
        )
        self.client.query(ddl).result()
        view.last_refreshed_at = datetime.now(timezone.utc)
        view.refresh_count += 1
        return view

    def drop_view(self, view: MaterializedView) -> None:
        self.client.query(
            f"DROP MATERIALIZED VIEW IF EXISTS `{view.fqn}`"
        ).result()
        view.is_active = False

    # ------------------------------------------------------------------
    # Cost measurement
    # ------------------------------------------------------------------

    def measure_savings(
        self,
        view: MaterializedView,
        since: datetime,
    ) -> float:
        """
        Approximate savings: queries that referenced the MV name after its
        creation vs. their pre-MV average cost.

        BigQuery doesn't expose per-job MV hit flags directly, so we proxy
        savings as (avg bytes processed before MV) – (avg after) × $5/TB
        for queries that match the view's referenced tables.
        """
        since_str = since.strftime("%Y-%m-%d %H:%M:%S UTC")
        created_str = view.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")

        # Queries touching the same tables, after view creation
        table_filter = " OR ".join(
            f"LOWER(query) LIKE '%{t.lower()}%'"
            for t in view.candidate.referenced_tables
        )
        sql = f"""
        WITH after AS (
            SELECT AVG(total_bytes_billed) AS avg_bytes
            FROM `{self.project}`.`region-{self.location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE creation_time BETWEEN TIMESTAMP('{created_str}') AND CURRENT_TIMESTAMP()
              AND ({table_filter})
              AND state = 'DONE' AND statement_type = 'SELECT'
        ),
        before AS (
            SELECT AVG(total_bytes_billed) AS avg_bytes
            FROM `{self.project}`.`region-{self.location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE creation_time BETWEEN TIMESTAMP('{since_str}') AND TIMESTAMP('{created_str}')
              AND ({table_filter})
              AND state = 'DONE' AND statement_type = 'SELECT'
        )
        SELECT
            GREATEST(0, b.avg_bytes - a.avg_bytes) * 5.0 / 1e12 AS estimated_savings_usd
        FROM before b, after a
        """
        rows = list(self.client.query(sql).result())
        if rows and rows[0].estimated_savings_usd is not None:
            return float(rows[0].estimated_savings_usd)
        return 0.0

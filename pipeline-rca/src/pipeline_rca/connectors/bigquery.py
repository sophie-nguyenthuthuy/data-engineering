"""BigQuery connector (requires google-cloud-bigquery extra)."""

from __future__ import annotations

from datetime import datetime

from pipeline_rca.connectors.base import BaseConnector
from pipeline_rca.models import MetricPoint


class BigQueryConnector(BaseConnector):
    def __init__(self, project: str, dataset: str) -> None:
        try:
            from google.cloud import bigquery  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "Install the BigQuery extra: pip install 'pipeline-rca[bigquery]'"
            ) from exc

        self._project = project
        self._dataset = dataset
        self._client = bigquery.Client(project=project)

    def fetch_series(
        self,
        query: str,
        start_date: datetime,
        end_date: datetime,
        **kwargs: object,
    ) -> list[MetricPoint]:
        rendered = query.format(
            dataset=self._dataset,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
            **kwargs,
        )
        rows = self._client.query(rendered).result()
        return [
            MetricPoint(timestamp=datetime.combine(row[0], datetime.min.time()), value=float(row[1]))
            for row in rows
        ]

    def fetch_columns(self, table: str) -> list[dict[str, str]]:
        ref = self._client.get_table(f"{self._project}.{self._dataset}.{table}")
        return [{"name": f.name, "type": f.field_type} for f in ref.schema]

    def close(self) -> None:
        self._client.close()

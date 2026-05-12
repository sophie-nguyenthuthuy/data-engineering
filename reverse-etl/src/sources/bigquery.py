from typing import Any
from ..settings import settings
from ..logger import get_logger
from .base import BaseSource

logger = get_logger(__name__)


class BigQuerySource(BaseSource):
    def __init__(self, params: dict[str, Any]) -> None:
        super().__init__(params)
        self._project = params.get("project", settings.bigquery_project)
        self._credentials_file = params.get("credentials_file", settings.bigquery_credentials_file)

    def _get_client(self):
        from google.cloud import bigquery
        from google.oauth2 import service_account

        if self._credentials_file:
            creds = service_account.Credentials.from_service_account_file(
                self._credentials_file,
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
            )
            return bigquery.Client(project=self._project, credentials=creds)
        # falls back to application default credentials
        return bigquery.Client(project=self._project)

    def fetch(self, query: str, query_params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        logger.debug("BigQuerySource: executing query")
        client = self._get_client()
        job = client.query(query)
        rows = [dict(row) for row in job.result()]
        logger.info(f"BigQuerySource: fetched {len(rows)} rows")
        return rows

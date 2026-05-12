"""Abstract connector interface for fetching metric series from a data warehouse."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from pipeline_rca.models import MetricPoint


class BaseConnector(ABC):
    """Fetch a named metric series from a warehouse."""

    @abstractmethod
    def fetch_series(
        self,
        query: str,
        start_date: datetime,
        end_date: datetime,
        **kwargs: object,
    ) -> list[MetricPoint]:
        """Execute *query* and return a list of (timestamp, value) points."""
        ...

    @abstractmethod
    def fetch_columns(self, table: str) -> list[dict[str, str]]:
        """Return the current columns of *table* as [{"name": ..., "type": ...}]."""
        ...

    def close(self) -> None:
        """Release any held connections."""

"""Abstract adapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from ..models import CandidateView, MaterializedView, QueryRecord, Warehouse


class BaseAdapter(ABC):
    """
    One adapter per warehouse.  Adapters are responsible for:
      • Pulling job / query history to build a worklog
      • Creating / refreshing / dropping materialized views
      • Reporting actual bytes / cost saved after a view is live
    """

    warehouse: Warehouse

    # ------------------------------------------------------------------
    # Worklog
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_query_history(
        self,
        since: datetime,
        limit: int = 10_000,
    ) -> list[QueryRecord]:
        """Return query records executed since `since`."""

    # ------------------------------------------------------------------
    # View lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def create_view(
        self,
        candidate: CandidateView,
        dataset_or_schema: str,
    ) -> MaterializedView:
        """CREATE MATERIALIZED VIEW … and return a MaterializedView."""

    @abstractmethod
    def refresh_view(self, view: MaterializedView) -> MaterializedView:
        """Trigger a manual refresh and update last_refreshed_at."""

    @abstractmethod
    def drop_view(self, view: MaterializedView) -> None:
        """DROP MATERIALIZED VIEW …"""

    # ------------------------------------------------------------------
    # Cost measurement
    # ------------------------------------------------------------------

    @abstractmethod
    def measure_savings(
        self,
        view: MaterializedView,
        since: datetime,
    ) -> float:
        """
        Estimate actual USD saved by `view` since `since`.

        Implementations query the warehouse's information_schema /
        query history to compare cost for queries that hit the view
        against the estimated cost if they had scanned the base tables.
        """

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def fqn(self, dataset_or_schema: str, name: str) -> str:
        """Build a fully-qualified view name."""
        return f"{dataset_or_schema}.{name}"

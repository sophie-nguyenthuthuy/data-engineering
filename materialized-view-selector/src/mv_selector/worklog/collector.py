"""Pull fresh query history from a warehouse adapter and store it."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from ..adapters.base import BaseAdapter
from ..models import Warehouse
from .store import WorklogStore


class WorklogCollector:
    def __init__(
        self,
        adapter: BaseAdapter,
        store: Optional[WorklogStore] = None,
        lookback_days: int = 30,
    ) -> None:
        self.adapter = adapter
        self.store = store or WorklogStore()
        self.lookback_days = lookback_days

    def collect(self, since: Optional[datetime] = None) -> int:
        if since is None:
            since = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        records = self.adapter.fetch_query_history(since=since)
        upserted = self.store.upsert(records)
        self.store.record_ingestion(self.adapter.warehouse, len(records))
        return upserted

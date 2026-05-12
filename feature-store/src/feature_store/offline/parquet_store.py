"""
Offline store — Parquet-backed feature history for training data generation.

Partition layout:
  {base_path}/{feature_group}/date={YYYY-MM-DD}/{uuid}.parquet

Point-in-time correctness: each row carries event_timestamp so training
joins can retrieve the feature value that was valid at prediction time,
preventing label leakage.
"""
from __future__ import annotations

import io
import threading
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import structlog

log = structlog.get_logger(__name__)

# Schema column names
_COL_ENTITY = "entity_id"
_COL_GROUP = "feature_group"
_COL_EVENT_TS = "event_timestamp"
_COL_WRITE_TS = "write_timestamp"
_COL_DATE = "date"


class OfflineStore:
    """
    Append-only offline store. Writes are buffered and flushed to Parquet
    in row groups for efficient predicate pushdown during training reads.
    """

    def __init__(
        self,
        base_path: str | Path = "./data/offline",
        write_batch_size: int = 10_000,
        row_group_size: int = 131_072,
        compression: str = "snappy",
    ) -> None:
        self._base = Path(base_path)
        self._base.mkdir(parents=True, exist_ok=True)
        self._write_batch_size = write_batch_size
        self._row_group_size = row_group_size
        self._compression = compression
        # In-memory buffer: group -> list of row dicts
        self._buffers: dict[str, list[dict]] = defaultdict(list)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    def write(
        self,
        group: str,
        entity_id: str,
        features: dict[str, Any],
        event_timestamp: datetime | None = None,
    ) -> None:
        ts = event_timestamp or datetime.now(timezone.utc)
        row = {
            _COL_ENTITY: entity_id,
            _COL_GROUP: group,
            _COL_EVENT_TS: ts,
            _COL_WRITE_TS: datetime.now(timezone.utc),
            _COL_DATE: ts.date().isoformat(),
            **features,
        }
        with self._lock:
            self._buffers[group].append(row)
            if len(self._buffers[group]) >= self._write_batch_size:
                self._flush_group(group)

    def write_batch(
        self,
        group: str,
        records: list[tuple[str, dict[str, Any], datetime | None]],
    ) -> None:
        """Bulk write — (entity_id, features, event_timestamp) tuples."""
        now = datetime.now(timezone.utc)
        rows = []
        for entity_id, features, event_ts in records:
            ts = event_ts or now
            rows.append(
                {
                    _COL_ENTITY: entity_id,
                    _COL_GROUP: group,
                    _COL_EVENT_TS: ts,
                    _COL_WRITE_TS: now,
                    _COL_DATE: ts.date().isoformat(),
                    **features,
                }
            )
        with self._lock:
            self._buffers[group].extend(rows)
            if len(self._buffers[group]) >= self._write_batch_size:
                self._flush_group(group)

    def flush(self, group: str | None = None) -> None:
        with self._lock:
            groups = [group] if group else list(self._buffers.keys())
            for g in groups:
                if self._buffers[g]:
                    self._flush_group(g)

    def _flush_group(self, group: str) -> None:
        rows = self._buffers[group]
        if not rows:
            return
        self._buffers[group] = []
        # Partition rows by date so cross-day batches land in the right directories
        by_date: dict[str, list[dict]] = {}
        for row in rows:
            by_date.setdefault(row[_COL_DATE], []).append(row)
        for date_str, date_rows in by_date.items():
            partition_dir = self._base / f"feature_group={group}" / f"date={date_str}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            out_path = partition_dir / f"{uuid.uuid4().hex}.parquet"
            df = pd.DataFrame(date_rows)
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(
                table,
                out_path,
                row_group_size=self._row_group_size,
                compression=self._compression,
            )
            log.info("flushed offline partition", group=group, rows=len(date_rows), path=str(out_path))

    # ------------------------------------------------------------------ #
    # Read — point-in-time correct retrieval                               #
    # ------------------------------------------------------------------ #

    def read(
        self,
        group: str,
        start_date: date | None = None,
        end_date: date | None = None,
        entity_ids: list[str] | None = None,
        feature_names: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Read feature history with optional filters.
        Uses Arrow dataset API for predicate pushdown into Parquet files.
        """
        group_path = self._base / f"feature_group={group}"
        if not group_path.exists():
            return pd.DataFrame()

        dataset = ds.dataset(group_path, format="parquet", partitioning="hive")
        filters = self._build_filters(start_date, end_date, entity_ids)
        cols = self._build_col_list(feature_names)

        table = dataset.to_table(filter=filters, columns=cols)
        return table.to_pandas()

    def point_in_time_join(
        self,
        entity_df: pd.DataFrame,
        group: str,
        feature_names: list[str] | None = None,
        timestamp_col: str = "label_timestamp",
    ) -> pd.DataFrame:
        """
        For each row in entity_df (entity_id + label_timestamp), find the most
        recent feature value that was recorded BEFORE label_timestamp.

        This prevents training-serving skew / label leakage.
        """
        entity_ids = entity_df["entity_id"].unique().tolist()
        hist = self.read(group, entity_ids=entity_ids, feature_names=feature_names)
        if hist.empty:
            return entity_df

        hist = hist.sort_values(_COL_EVENT_TS)
        merged = pd.merge_asof(
            entity_df.sort_values(timestamp_col),
            hist,
            left_on=timestamp_col,
            right_on=_COL_EVENT_TS,
            left_by="entity_id",
            right_by=_COL_ENTITY,
            direction="backward",
        )
        return merged

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _build_filters(
        start_date: date | None,
        end_date: date | None,
        entity_ids: list[str] | None,
    ) -> ds.Expression | None:
        exprs = []
        if start_date:
            exprs.append(ds.field("date") >= start_date.isoformat())
        if end_date:
            exprs.append(ds.field("date") <= end_date.isoformat())
        if entity_ids:
            exprs.append(ds.field(_COL_ENTITY).isin(entity_ids))
        if not exprs:
            return None
        result = exprs[0]
        for e in exprs[1:]:
            result = result & e
        return result

    @staticmethod
    def _build_col_list(feature_names: list[str] | None) -> list[str] | None:
        if feature_names is None:
            return None
        return [_COL_ENTITY, _COL_EVENT_TS, _COL_DATE] + feature_names

    def list_partitions(self, group: str) -> list[str]:
        group_path = self._base / f"feature_group={group}"
        if not group_path.exists():
            return []
        return sorted(p.name for p in group_path.iterdir() if p.is_dir())

    def get_stats(self, group: str) -> dict:
        df = self.read(group)
        if df.empty:
            return {"row_count": 0}
        return {
            "row_count": len(df),
            "entity_count": df[_COL_ENTITY].nunique(),
            "date_range": [df[_COL_DATE].min(), df[_COL_DATE].max()],
        }

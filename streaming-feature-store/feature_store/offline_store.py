"""
Offline (batch) feature store backed by Parquet files.
Used by the training pipeline and the nightly drift comparator.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class OfflineStore:
    def __init__(self, base_path: str | None = None) -> None:
        self.base_path = Path(base_path or os.getenv("OFFLINE_STORE_PATH", "/data/offline"))
        self.base_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write_batch(self, df: pd.DataFrame, partition: str = "latest") -> Path:
        """Append a batch of feature rows to a named partition."""
        dest = self.base_path / f"{partition}.parquet"
        table = pa.Table.from_pandas(df, preserve_index=False)
        if dest.exists():
            existing = pq.read_table(dest)
            table = pa.concat_tables([existing, table])
        pq.write_table(table, dest)
        return dest

    def write_stats(self, stats: dict, name: str) -> Path:
        """Persist summary statistics as a single-row Parquet file."""
        df = pd.DataFrame([stats])
        dest = self.base_path / f"stats_{name}.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), dest)
        return dest

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read_partition(self, partition: str = "latest") -> pd.DataFrame:
        dest = self.base_path / f"{partition}.parquet"
        if not dest.exists():
            return pd.DataFrame()
        return pq.read_table(dest).to_pandas()

    def read_stats(self, name: str) -> dict:
        dest = self.base_path / f"stats_{name}.parquet"
        if not dest.exists():
            return {}
        return pq.read_table(dest).to_pandas().iloc[0].to_dict()

    def list_partitions(self) -> list[str]:
        return [
            p.stem
            for p in self.base_path.glob("*.parquet")
            if not p.stem.startswith("stats_")
        ]

    def iter_recent(self, n_partitions: int = 7) -> Iterator[pd.DataFrame]:
        """Yield DataFrames from the most recent n partitions (by mtime)."""
        parts = sorted(
            [p for p in self.base_path.glob("*.parquet") if not p.stem.startswith("stats_")],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for path in parts[:n_partitions]:
            yield pq.read_table(path).to_pandas()

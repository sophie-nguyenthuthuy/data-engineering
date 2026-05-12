"""
Batch processor: computes features from a DataFrame of raw events
using the same transformation functions as the streaming path.

Typical usage:
    processor = BatchProcessor()
    features_df = processor.process(raw_df)
    processor.save_training_snapshot(features_df, label="2024-01-01")
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from feature_store.offline_store import OfflineStore
from feature_store.registry import FeatureRegistry
from feature_store.transformations import build_registry


class BatchProcessor:
    def __init__(
        self,
        registry: FeatureRegistry | None = None,
        offline_store: OfflineStore | None = None,
    ) -> None:
        self.registry = registry or build_registry()
        self.offline_store = offline_store or OfflineStore()

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def process(self, raw_df: pd.DataFrame, context: dict | None = None) -> pd.DataFrame:
        """
        Apply every registered feature to each row of raw_df.
        Returns a new DataFrame with one column per feature plus
        entity/timestamp passthrough columns.
        """
        ctx = context or {}
        records = raw_df.to_dict(orient="records")

        rows = []
        for record in records:
            feature_row: dict = {
                "entity_id": record.get("user_id", ""),
                "event_timestamp": record.get("timestamp", datetime.now(tz=timezone.utc)),
            }
            for feat in self.registry.all_features():
                feature_row[feat.name] = feat.compute(record, ctx)
            rows.append(feature_row)

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Compute global stats needed for z-score at serving time
    # ------------------------------------------------------------------

    def compute_global_stats(self, raw_df: pd.DataFrame) -> dict:
        stats = {
            "amount_mean": float(raw_df["amount"].mean()),
            "amount_stddev": float(raw_df["amount"].std()),
        }
        return stats

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def save_training_snapshot(
        self,
        features_df: pd.DataFrame,
        label: str | None = None,
    ) -> None:
        """Write feature DataFrame to the offline store as the training snapshot."""
        partition = f"training_{label or datetime.now(tz=timezone.utc).date()}"
        self.offline_store.write_batch(features_df, partition=partition)

    def run_full_pipeline(
        self,
        raw_df: pd.DataFrame,
        label: str | None = None,
    ) -> pd.DataFrame:
        """Convenience: compute features, persist, and return."""
        context = self.compute_global_stats(raw_df)
        features_df = self.process(raw_df, context=context)
        self.offline_store.write_stats(context, "global")
        self.save_training_snapshot(features_df, label=label)
        return features_df

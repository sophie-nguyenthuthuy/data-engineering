"""
Online ↔ Offline consistency utilities.

1. Backfill: hydrate online store from historical Parquet (e.g. cold start)
2. Audit:    compare online vs offline to detect drift
3. Snapshot: export current online state to Parquet for point-in-time restore

Training ↔ serving consistency is enforced structurally:
  - Both stores are written by the same Kafka consumer within one batch.
  - Kafka offset is committed only after both writes succeed.
  - point_in_time_join() in OfflineStore prevents label leakage.
"""
from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import structlog

from feature_store.offline.parquet_store import OfflineStore
from feature_store.online.redis_store import OnlineStore
from feature_store.registry.feature_registry import FeatureRegistry

log = structlog.get_logger(__name__)


class ConsistencyManager:
    def __init__(
        self,
        online: OnlineStore,
        offline: OfflineStore,
        registry: FeatureRegistry,
    ) -> None:
        self._online = online
        self._offline = offline
        self._registry = registry

    # ------------------------------------------------------------------ #
    # Backfill: Parquet → Redis                                            #
    # ------------------------------------------------------------------ #

    def backfill_online(
        self,
        group: str,
        as_of_date: date | None = None,
        entity_ids: list[str] | None = None,
        batch_size: int = 1000,
    ) -> int:
        """
        Hydrate Redis from Parquet for cold-start or recovery scenarios.
        Loads the LATEST feature value per entity as of as_of_date.
        Returns count of entities written.
        """
        log.info("backfill started", group=group, as_of=str(as_of_date))
        df = self._offline.read(
            group,
            end_date=as_of_date,
            entity_ids=entity_ids,
        )
        if df.empty:
            log.warning("no offline data found", group=group)
            return 0

        feature_cols = [
            c for c in df.columns
            if c not in ("entity_id", "feature_group", "event_timestamp", "write_timestamp", "date")
        ]
        # Latest value per entity
        latest = (
            df.sort_values("event_timestamp")
            .groupby("entity_id")[feature_cols]
            .last()
            .reset_index()
        )
        ttl = self._get_ttl(group)
        written = 0
        for chunk_start in range(0, len(latest), batch_size):
            chunk = latest.iloc[chunk_start : chunk_start + batch_size]
            records = [
                (row["entity_id"], {c: row[c] for c in feature_cols if pd.notna(row[c])})
                for _, row in chunk.iterrows()
            ]
            self._online.put_batch(group, records, ttl_seconds=ttl)
            written += len(records)
            log.info("backfill progress", group=group, written=written, total=len(latest))

        log.info("backfill complete", group=group, entities=written)
        return written

    # ------------------------------------------------------------------ #
    # Snapshot: Redis → Parquet                                            #
    # ------------------------------------------------------------------ #

    def snapshot_online(
        self,
        group: str,
        entity_ids: list[str],
        label: str = "snapshot",
    ) -> int:
        """
        Dump current Redis state for a set of entities to Parquet.
        Useful for saving model serving state for debugging or auditing.
        """
        ts = datetime.now(timezone.utc)
        records = self._online.get_batch(group, entity_ids)
        valid = [(eid, feats) for eid, feats in records.items() if feats is not None]
        if not valid:
            return 0
        self._offline.write_batch(
            group,
            [(eid, {**feats, "__snapshot_label__": label}, ts) for eid, feats in valid],
        )
        self._offline.flush(group)
        log.info("snapshot written", group=group, entities=len(valid), label=label)
        return len(valid)

    # ------------------------------------------------------------------ #
    # Audit: compare online vs offline                                     #
    # ------------------------------------------------------------------ #

    def audit(
        self,
        group: str,
        entity_ids: list[str],
        tolerance: float = 1e-4,
    ) -> dict:
        """
        Compare the online (Redis) values against the latest offline (Parquet)
        values for the same entities.  Returns a drift report.
        """
        online_data = self._online.get_batch(group, entity_ids)
        offline_df = self._offline.read(group, entity_ids=entity_ids)

        if offline_df.empty:
            return {"status": "no_offline_data", "entities_checked": 0}

        feature_cols = [
            c for c in offline_df.columns
            if c not in ("entity_id", "feature_group", "event_timestamp", "write_timestamp", "date")
        ]
        latest_offline = (
            offline_df.sort_values("event_timestamp")
            .groupby("entity_id")[feature_cols]
            .last()
        )

        drifted = []
        for entity_id in entity_ids:
            online_feats = online_data.get(entity_id)
            if online_feats is None or entity_id not in latest_offline.index:
                continue
            offline_feats = latest_offline.loc[entity_id].to_dict()
            for feat, online_val in online_feats.items():
                offline_val = offline_feats.get(feat)
                if offline_val is None:
                    continue
                try:
                    if abs(float(online_val) - float(offline_val)) > tolerance:
                        drifted.append({
                            "entity_id": entity_id,
                            "feature": feat,
                            "online": online_val,
                            "offline": offline_val,
                        })
                except (TypeError, ValueError):
                    if online_val != offline_val:
                        drifted.append({
                            "entity_id": entity_id,
                            "feature": feat,
                            "online": online_val,
                            "offline": offline_val,
                        })

        return {
            "status": "ok" if not drifted else "drift_detected",
            "entities_checked": len(entity_ids),
            "drift_count": len(drifted),
            "drifted_features": drifted[:50],  # cap response size
        }

    def _get_ttl(self, group: str) -> int:
        try:
            return self._registry.get_group(group).ttl_seconds
        except KeyError:
            return 86400

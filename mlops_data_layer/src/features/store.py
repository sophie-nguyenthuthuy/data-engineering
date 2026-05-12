from __future__ import annotations
import json
from collections import deque
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import redis.asyncio as aioredis
import structlog

from ..config import settings
from ..models import FeatureVector, FeatureStats, TrainingSnapshot, FeatureType
from .registry import FeatureRegistry

log = structlog.get_logger(__name__)

_SERVING_BUFFER_KEY = "mlops:serving_buffer:{model_name}"
_SNAPSHOT_KEY = "mlops:snapshot:{model_name}:{version}"
_VECTOR_KEY = "mlops:vector:{entity_id}:{model_name}"


class FeatureStore:
    """
    Two-tier feature store:
    - Online tier  : Redis (low-latency read/write for serving vectors)
    - Reference tier: Redis sorted set acting as a sliding window of
                      serving observations used for drift detection.

    Training snapshots are also cached in Redis (serialised JSON) and
    persisted to Postgres via the repository layer.
    """

    def __init__(self, redis_client: aioredis.Redis, registry: FeatureRegistry) -> None:
        self._redis = redis_client
        self._registry = registry

    # ------------------------------------------------------------------
    # Online serving tier
    # ------------------------------------------------------------------

    async def write_vector(self, vector: FeatureVector) -> None:
        """Cache a feature vector for an entity (used at serving time)."""
        key = _VECTOR_KEY.format(entity_id=vector.entity_id, model_name=vector.model_name)
        await self._redis.setex(
            key,
            settings.feature_cache_ttl_seconds,
            vector.model_dump_json(),
        )
        # Append to rolling serving buffer for drift evaluation
        await self._append_to_serving_buffer(vector)

    async def read_vector(self, entity_id: str, model_name: str) -> FeatureVector | None:
        key = _VECTOR_KEY.format(entity_id=entity_id, model_name=model_name)
        raw = await self._redis.get(key)
        if not raw:
            return None
        return FeatureVector.model_validate_json(raw)

    # ------------------------------------------------------------------
    # Serving buffer (sliding window for drift evaluation)
    # ------------------------------------------------------------------

    async def _append_to_serving_buffer(self, vector: FeatureVector) -> None:
        buf_key = _SERVING_BUFFER_KEY.format(model_name=vector.model_name)
        score = vector.created_at.timestamp()
        await self._redis.zadd(buf_key, {vector.model_dump_json(): score})
        # Trim to last N seconds (feature_store_window_days)
        cutoff = (datetime.utcnow() - timedelta(days=settings.feature_store_window_days)).timestamp()
        await self._redis.zremrangebyscore(buf_key, "-inf", cutoff)

    async def get_serving_window(
        self, model_name: str, limit: int | None = None
    ) -> pd.DataFrame:
        """Return recent serving vectors as a DataFrame for drift checks."""
        buf_key = _SERVING_BUFFER_KEY.format(model_name=model_name)
        n = limit or settings.drift_eval_window_size
        raw_items = await self._redis.zrange(buf_key, -n, -1)
        if not raw_items:
            return pd.DataFrame()
        records = []
        for raw in raw_items:
            vec = FeatureVector.model_validate_json(raw)
            records.append(vec.features)
        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Training snapshots
    # ------------------------------------------------------------------

    async def save_training_snapshot(self, snapshot: TrainingSnapshot) -> None:
        key = _SNAPSHOT_KEY.format(
            model_name=snapshot.model_name, version=snapshot.model_version
        )
        await self._redis.set(key, snapshot.model_dump_json())
        log.info(
            "training_snapshot_saved",
            model=snapshot.model_name,
            version=snapshot.model_version,
            features=len(snapshot.feature_stats),
        )

    async def get_training_snapshot(
        self, model_name: str, model_version: str
    ) -> TrainingSnapshot | None:
        key = _SNAPSHOT_KEY.format(model_name=model_name, version=model_version)
        raw = await self._redis.get(key)
        if not raw:
            return None
        return TrainingSnapshot.model_validate_json(raw)

    # ------------------------------------------------------------------
    # Compute stats from a DataFrame
    # ------------------------------------------------------------------

    def compute_stats(self, df: pd.DataFrame, source_label: str = "") -> list[FeatureStats]:
        stats: list[FeatureStats] = []
        for col in df.columns:
            fd = self._registry.get(col)
            ftype = fd.feature_type if fd else _infer_type(df[col])
            stats.append(_compute_column_stats(df[col], col, ftype))
        return stats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infer_type(series: pd.Series) -> FeatureType:
    if pd.api.types.is_bool_dtype(series):
        return FeatureType.BOOLEAN
    if pd.api.types.is_numeric_dtype(series):
        return FeatureType.NUMERICAL
    return FeatureType.CATEGORICAL


def _compute_column_stats(series: pd.Series, name: str, ftype: FeatureType) -> FeatureStats:
    import numpy as np
    total = len(series)
    null_count = int(series.isna().sum())
    null_fraction = null_count / total if total else 0.0

    base = dict(
        feature_name=name,
        feature_type=ftype,
        count=total,
        null_count=null_count,
        null_fraction=null_fraction,
    )

    clean = series.dropna()

    if ftype == FeatureType.NUMERICAL and len(clean) > 0:
        vals = clean.astype(float)
        counts, edges = np.histogram(vals, bins=min(20, len(clean.unique())))
        return FeatureStats(
            **base,
            mean=float(vals.mean()),
            std=float(vals.std()),
            min=float(vals.min()),
            max=float(vals.max()),
            p25=float(np.percentile(vals, 25)),
            p50=float(np.percentile(vals, 50)),
            p75=float(np.percentile(vals, 75)),
            p95=float(np.percentile(vals, 95)),
            p99=float(np.percentile(vals, 99)),
            histogram_edges=edges.tolist(),
            histogram_counts=counts.tolist(),
        )

    if ftype == FeatureType.CATEGORICAL and len(clean) > 0:
        vc = clean.astype(str).value_counts()
        return FeatureStats(
            **base,
            value_counts=vc.to_dict(),
            cardinality=int(vc.shape[0]),
            top_value=str(vc.index[0]) if len(vc) else None,
        )

    return FeatureStats(**base)

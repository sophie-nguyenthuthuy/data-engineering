"""Engine samplers and stats builder for calibrating the cost model."""
from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dqp.cost.statistics import ColumnStats, Histogram, StatsRegistry, TableStats


@dataclass
class SamplingConfig:
    """Configuration for how much data to sample."""

    sample_fraction: float = 0.01
    min_sample_rows: int = 1_000
    max_sample_rows: int = 100_000


class SamplerBase(ABC):
    """Abstract sampler that returns rows as plain dicts."""

    @abstractmethod
    def sample(self, table_name: str, fraction: float) -> List[Dict[str, Any]]:
        """Return a random sample of *fraction* of rows from *table_name*."""
        ...


class MongoSampler(SamplerBase):
    """Samples a MongoDB collection using the $sample aggregation stage."""

    def __init__(self, db: Any, collection_name: str) -> None:
        try:
            import pymongo  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "pymongo is required for MongoSampler. Install it with: pip install pymongo"
            ) from exc
        self._db = db
        self._collection_name = collection_name

    def sample(self, table_name: str, fraction: float) -> List[Dict[str, Any]]:
        collection = self._db[table_name]
        # Estimate count to compute sample size
        count = collection.estimated_document_count()
        n = max(1, int(count * fraction))
        pipeline = [{"$sample": {"size": n}}]
        docs = list(collection.aggregate(pipeline))
        # Remove MongoDB's _id for uniform processing
        for doc in docs:
            doc.pop("_id", None)
        return docs


class ParquetSampler(SamplerBase):
    """Samples a Parquet dataset by reading all rows and random-selecting."""

    def __init__(self, path: str) -> None:
        try:
            import pyarrow.dataset as ds  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "pyarrow is required for ParquetSampler. Install it with: pip install pyarrow"
            ) from exc
        self._path = path

    def sample(self, table_name: str, fraction: float) -> List[Dict[str, Any]]:
        import pyarrow.dataset as ds

        dataset = ds.dataset(self._path, format="parquet")
        table = dataset.to_table()
        n_rows = len(table)
        n_sample = max(1, int(n_rows * fraction))
        indices = random.sample(range(n_rows), min(n_sample, n_rows))
        sampled = table.take(indices)
        return sampled.to_pylist()


class PostgresSampler(SamplerBase):
    """Samples a Postgres table using TABLESAMPLE BERNOULLI."""

    def __init__(self, conn_string: str) -> None:
        try:
            import psycopg2  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "psycopg2-binary is required for PostgresSampler. "
                "Install it with: pip install psycopg2-binary"
            ) from exc
        self._conn_string = conn_string

    def sample(self, table_name: str, fraction: float) -> List[Dict[str, Any]]:
        import psycopg2
        import psycopg2.extras

        pct = min(max(fraction * 100, 0.0001), 100.0)
        conn = psycopg2.connect(self._conn_string)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f'SELECT * FROM "{table_name}" TABLESAMPLE BERNOULLI (%s)', (pct,)
                )
                rows = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()
        return rows


# ---------------------------------------------------------------------------
# Stats builder
# ---------------------------------------------------------------------------

_N_HISTOGRAM_BUCKETS = 20


class StatsBuilder:
    """Builds TableStats from a sample and registers them in a StatsRegistry."""

    def __init__(
        self,
        sampler: SamplerBase,
        registry: StatsRegistry,
        config: Optional[SamplingConfig] = None,
    ) -> None:
        self._sampler = sampler
        self._registry = registry
        self._config = config or SamplingConfig()

    def build_stats(self, table_name: str, columns: List[str]) -> TableStats:
        """Sample the table and compute per-column statistics."""
        cfg = self._config
        rows = self._sampler.sample(table_name, cfg.sample_fraction)

        # Clamp sample size
        if len(rows) > cfg.max_sample_rows:
            rows = random.sample(rows, cfg.max_sample_rows)

        n = len(rows)
        column_stats: Dict[str, ColumnStats] = {}

        for col in columns:
            values = [row.get(col) for row in rows if col in row]
            if not values:
                continue
            null_count = sum(1 for v in values if v is None)
            null_fraction = null_count / len(values) if values else 0.0
            non_null = [v for v in values if v is not None]
            distinct_count = len(set(str(v) for v in non_null)) if non_null else 0

            # Numeric stats
            numeric_vals: List[float] = []
            for v in non_null:
                try:
                    numeric_vals.append(float(v))
                except (TypeError, ValueError):
                    pass

            min_value: Optional[float] = min(numeric_vals) if numeric_vals else None
            max_value: Optional[float] = max(numeric_vals) if numeric_vals else None

            histogram: Optional[Histogram] = None
            if len(numeric_vals) >= _N_HISTOGRAM_BUCKETS * 2:
                histogram = _build_equidepth_histogram(numeric_vals, _N_HISTOGRAM_BUCKETS)

            column_stats[col] = ColumnStats(
                column=col,
                null_fraction=null_fraction,
                distinct_count=distinct_count,
                min_value=min_value,
                max_value=max_value,
                histogram=histogram,
            )

        # Estimate true row count from sample
        # (we don't know the real count, so we scale up by 1/fraction)
        est_row_count = max(n, cfg.min_sample_rows)

        stats = TableStats(
            table_name=table_name,
            row_count=est_row_count,
            column_stats=column_stats,
        )
        self._registry.set_table_stats(stats)
        return stats


def _build_equidepth_histogram(values: List[float], n_buckets: int) -> Histogram:
    """Build an equi-depth histogram from numeric values."""
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    bucket_size = n / n_buckets

    boundaries: List[float] = [sorted_vals[0]]
    frequencies: List[float] = []

    for i in range(1, n_buckets + 1):
        idx = min(int(i * bucket_size), n - 1)
        boundaries.append(sorted_vals[idx])

    # Compute frequency (fraction of total) per bucket
    freq_per_bucket = 1.0 / n_buckets
    frequencies = [freq_per_bucket] * n_buckets

    return Histogram(boundaries=boundaries, frequencies=frequencies)

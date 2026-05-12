from __future__ import annotations
import numpy as np
import pandas as pd

from ..models import FeatureStats, FeatureType
from ..features.store import _compute_column_stats, _infer_type
from ..features.registry import FeatureRegistry


class DataProfiler:
    """
    Compute a rich statistical profile of any DataFrame.
    Used both at training time (to capture reference stats)
    and at serving time (to compute live stats for skew comparison).
    """

    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry

    def profile(self, df: pd.DataFrame) -> list[FeatureStats]:
        stats: list[FeatureStats] = []
        for col in df.columns:
            fd = self._registry.get(col)
            ftype = fd.feature_type if fd else _infer_type(df[col])
            stats.append(_compute_column_stats(df[col], col, ftype))
        return stats

    def compare(
        self,
        training_stats: list[FeatureStats],
        serving_stats: list[FeatureStats],
    ) -> list[dict]:
        """
        Quick side-by-side comparison of training vs serving stats.
        Returns a list of per-feature comparison dicts.
        """
        serving_map = {s.feature_name: s for s in serving_stats}
        results = []
        for ts in training_stats:
            ss = serving_map.get(ts.feature_name)
            entry: dict = {"feature": ts.feature_name, "type": ts.feature_type}
            if ss is None:
                entry["status"] = "missing_in_serving"
                results.append(entry)
                continue
            if ts.feature_type == FeatureType.NUMERICAL and ts.mean is not None and ss.mean is not None:
                delta_mean = abs((ss.mean - ts.mean) / max(abs(ts.mean), 1e-6))
                delta_std = abs((ss.std or 0) - (ts.std or 0)) / max(abs(ts.std or 1), 1e-6)
                entry.update({
                    "train_mean": ts.mean,
                    "serve_mean": ss.mean,
                    "relative_mean_shift": delta_mean,
                    "train_std": ts.std,
                    "serve_std": ss.std,
                    "relative_std_shift": delta_std,
                    "null_fraction_train": ts.null_fraction,
                    "null_fraction_serve": ss.null_fraction,
                })
            elif ts.feature_type == FeatureType.CATEGORICAL:
                entry.update({
                    "cardinality_train": ts.cardinality,
                    "cardinality_serve": ss.cardinality,
                    "top_train": ts.top_value,
                    "top_serve": ss.top_value,
                    "null_fraction_train": ts.null_fraction,
                    "null_fraction_serve": ss.null_fraction,
                })
            results.append(entry)
        return results

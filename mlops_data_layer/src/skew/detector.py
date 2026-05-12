from __future__ import annotations
import uuid
import numpy as np
import pandas as pd
import structlog

from ..config import settings
from ..models import (
    FeatureType,
    TrainingSnapshot,
    SkewReport,
    SkewFeatureResult,
    DriftStatus,
)
from ..features.registry import FeatureRegistry
from ..drift.tests import DriftTestSuite

log = structlog.get_logger(__name__)


class SkewDetector:
    """
    Training / Serving Skew Detector.

    Skew is the systematic difference between the feature distribution
    at training time and at serving time.  Unlike temporal drift (which
    compares two serving windows), skew always compares against the fixed
    training reference snapshot.

    For each feature the detector:
    - Computes PSI between training and current serving distribution
    - For numerical features: also computes relative mean/std shift
    - For categorical: also runs a Chi2 test
    Flags features with PSI > ``settings.skew_psi_threshold``.
    """

    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry

    def detect(
        self,
        snapshot: TrainingSnapshot,
        serving_df: pd.DataFrame,
    ) -> SkewReport:
        if serving_df.empty:
            return SkewReport(
                model_name=snapshot.model_name,
                model_version=snapshot.model_version,
                snapshot_id=snapshot.snapshot_id,
                serving_window_size=0,
                overall_status=DriftStatus.INSUFFICIENT_DATA,
                skewed_feature_count=0,
                total_feature_count=0,
            )

        ref_stats = snapshot.stats_by_name()
        feature_results: list[SkewFeatureResult] = []

        for col in serving_df.columns:
            fd = self._registry.get(col)
            if fd is None:
                continue
            ref = ref_stats.get(col)
            if ref is None:
                continue
            result = self._check_feature(col, fd.feature_type, ref, serving_df[col])
            feature_results.append(result)

        skewed = [r for r in feature_results if r.status == DriftStatus.DRIFT_DETECTED]
        overall = (
            DriftStatus.DRIFT_DETECTED if skewed
            else DriftStatus.NO_DRIFT if feature_results
            else DriftStatus.INSUFFICIENT_DATA
        )

        report = SkewReport(
            report_id=str(uuid.uuid4()),
            model_name=snapshot.model_name,
            model_version=snapshot.model_version,
            snapshot_id=snapshot.snapshot_id,
            serving_window_size=len(serving_df),
            overall_status=overall,
            skewed_feature_count=len(skewed),
            total_feature_count=len(feature_results),
            feature_results=feature_results,
        )
        log.info(
            "skew_detection_complete",
            model=snapshot.model_name,
            status=overall,
            skewed=len(skewed),
            total=len(feature_results),
        )
        return report

    # ------------------------------------------------------------------
    # Per-feature
    # ------------------------------------------------------------------

    def _check_feature(
        self, name: str, ftype: FeatureType, ref, serving: pd.Series
    ) -> SkewFeatureResult:
        serving_clean = serving.dropna()
        if len(serving_clean) < 10:
            return SkewFeatureResult(
                feature_name=name, status=DriftStatus.INSUFFICIENT_DATA
            )

        if ftype == FeatureType.NUMERICAL:
            return self._numerical_skew(name, ref, serving_clean.astype(float))
        elif ftype == FeatureType.CATEGORICAL:
            return self._categorical_skew(name, ref, serving_clean.astype(str))
        return SkewFeatureResult(feature_name=name, status=DriftStatus.NO_DRIFT)

    def _numerical_skew(self, name: str, ref, serving: pd.Series) -> SkewFeatureResult:
        from ..drift.detector import _reconstruct_ref_samples
        ref_vals = _reconstruct_ref_samples(ref)
        serving_vals = serving.values

        psi = DriftTestSuite.psi(ref_vals, serving_vals, threshold=settings.skew_psi_threshold)

        serving_mean = float(serving_vals.mean())
        serving_std = float(serving_vals.std())
        train_mean = ref.mean or 0.0
        train_std = ref.std or 1.0
        rel_mean_shift = abs(serving_mean - train_mean) / max(abs(train_mean), 1e-6)

        skewed = psi.drifted or rel_mean_shift > 0.3
        explanation = ""
        if psi.drifted:
            explanation += f"PSI={psi.score:.3f}>{settings.skew_psi_threshold}"
        if rel_mean_shift > 0.3:
            explanation += f" | mean_shift={rel_mean_shift:.2%}"

        return SkewFeatureResult(
            feature_name=name,
            status=DriftStatus.DRIFT_DETECTED if skewed else DriftStatus.NO_DRIFT,
            psi_score=psi.score,
            training_mean=train_mean,
            serving_mean=serving_mean,
            training_std=train_std,
            serving_std=serving_std,
            relative_mean_shift=rel_mean_shift,
            explanation=explanation or "No skew",
        )

    def _categorical_skew(self, name: str, ref, serving: pd.Series) -> SkewFeatureResult:
        if not ref.value_counts:
            return SkewFeatureResult(feature_name=name, status=DriftStatus.INSUFFICIENT_DATA)
        ref_series = pd.Series(ref.value_counts)
        chi2 = DriftTestSuite.chi2_test(ref_series, serving, pvalue_threshold=0.05)
        skewed = chi2.drifted
        return SkewFeatureResult(
            feature_name=name,
            status=DriftStatus.DRIFT_DETECTED if skewed else DriftStatus.NO_DRIFT,
            explanation=f"Chi2 p={chi2.pvalue:.4f}" if skewed else "No skew",
        )

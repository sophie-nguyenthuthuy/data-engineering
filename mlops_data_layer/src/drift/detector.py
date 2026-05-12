from __future__ import annotations
import uuid
import structlog
import numpy as np
import pandas as pd

from ..config import settings
from ..models import (
    FeatureType,
    TrainingSnapshot,
    DriftReport,
    DriftStatus,
    FeatureDriftResult,
)
from ..features.registry import FeatureRegistry
from .tests import DriftTestSuite

log = structlog.get_logger(__name__)


class DriftDetector:
    """
    Compares a window of live serving data against a training reference
    snapshot using KS, PSI, JS (numerical) or Chi2 + PSI (categorical).

    A feature is marked DRIFT_DETECTED if any of the active tests exceeds
    its configured threshold.  Overall report status is DRIFT_DETECTED when
    ≥ ``settings.retrain_min_drift_features`` features have drifted.
    """

    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry
        self._tests = DriftTestSuite()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def detect(
        self,
        snapshot: TrainingSnapshot,
        serving_df: pd.DataFrame,
    ) -> DriftReport:
        ref_stats = snapshot.stats_by_name()
        feature_results: list[FeatureDriftResult] = []

        for col in serving_df.columns:
            fd = self._registry.get(col)
            if fd is None:
                continue

            ref = ref_stats.get(col)
            if ref is None:
                log.warning("no_reference_stats", feature=col)
                continue

            result = self._check_feature(col, fd.feature_type, ref, serving_df[col])
            feature_results.append(result)

        drifted = [r for r in feature_results if r.status == DriftStatus.DRIFT_DETECTED]
        warnings = [r for r in feature_results if r.status == DriftStatus.WARNING]
        total = len(feature_results)

        if not feature_results:
            overall = DriftStatus.INSUFFICIENT_DATA
        elif len(drifted) >= settings.retrain_min_drift_features:
            overall = DriftStatus.DRIFT_DETECTED
        elif warnings:
            overall = DriftStatus.WARNING
        else:
            overall = DriftStatus.NO_DRIFT

        avg_drift = (
            sum(r.drift_magnitude for r in feature_results) / total if total else 0.0
        )
        triggers = overall == DriftStatus.DRIFT_DETECTED

        report = DriftReport(
            report_id=str(uuid.uuid4()),
            model_name=snapshot.model_name,
            model_version=snapshot.model_version,
            reference_snapshot_id=snapshot.snapshot_id,
            window_size=len(serving_df),
            overall_status=overall,
            drifted_feature_count=len(drifted),
            total_feature_count=total,
            drift_score=avg_drift,
            feature_results=feature_results,
            triggers_retraining=triggers,
        )
        log.info(
            "drift_detection_complete",
            model=snapshot.model_name,
            status=overall,
            drifted=len(drifted),
            total=total,
            window=len(serving_df),
        )
        return report

    # ------------------------------------------------------------------
    # Per-feature tests
    # ------------------------------------------------------------------

    def _check_feature(
        self,
        name: str,
        ftype: FeatureType,
        ref_stats,
        serving_series: pd.Series,
    ) -> FeatureDriftResult:
        serving_clean = serving_series.dropna()
        if len(serving_clean) < 10:
            return FeatureDriftResult(
                feature_name=name,
                status=DriftStatus.INSUFFICIENT_DATA,
                explanation="Not enough serving data",
            )

        if ftype == FeatureType.NUMERICAL:
            return self._numerical_check(name, ref_stats, serving_clean.astype(float).values)
        elif ftype == FeatureType.CATEGORICAL:
            return self._categorical_check(name, ref_stats, serving_clean.astype(str))
        else:
            return FeatureDriftResult(
                feature_name=name,
                status=DriftStatus.NO_DRIFT,
                explanation=f"Type {ftype} not checked",
            )

    def _numerical_check(
        self, name: str, ref, serving: np.ndarray
    ) -> FeatureDriftResult:
        ref_vals = _reconstruct_ref_samples(ref)
        if len(ref_vals) < 5:
            return FeatureDriftResult(feature_name=name, status=DriftStatus.INSUFFICIENT_DATA)

        ks = DriftTestSuite.ks_test(ref_vals, serving, settings.ks_pvalue_threshold)
        psi = DriftTestSuite.psi(ref_vals, serving, threshold=settings.psi_threshold)
        js = DriftTestSuite.js_divergence(ref_vals, serving, threshold=settings.js_threshold)

        any_drift = ks.drifted or psi.drifted or js.drifted
        any_warn = (ks.pvalue < 0.10) or (psi.score > settings.psi_threshold * 0.5)

        status = (
            DriftStatus.DRIFT_DETECTED if any_drift
            else DriftStatus.WARNING if any_warn
            else DriftStatus.NO_DRIFT
        )

        magnitude = min(1.0, (
            (1 - ks.pvalue) * 0.4
            + min(psi.score / (settings.psi_threshold * 2), 1.0) * 0.3
            + min(js.divergence / settings.js_threshold, 1.0) * 0.3
        ))

        explanations = []
        if ks.drifted:
            explanations.append(f"KS p={ks.pvalue:.4f} < {settings.ks_pvalue_threshold}")
        if psi.drifted:
            explanations.append(f"PSI={psi.score:.3f} > {settings.psi_threshold}")
        if js.drifted:
            explanations.append(f"JS={js.divergence:.3f} > {settings.js_threshold}")

        return FeatureDriftResult(
            feature_name=name,
            status=status,
            ks_statistic=ks.statistic,
            ks_pvalue=ks.pvalue,
            psi_score=psi.score,
            js_divergence=js.divergence,
            explanation="; ".join(explanations) or "No drift",
            drift_magnitude=magnitude,
        )

    def _categorical_check(
        self, name: str, ref, serving: pd.Series
    ) -> FeatureDriftResult:
        if not ref.value_counts:
            return FeatureDriftResult(feature_name=name, status=DriftStatus.INSUFFICIENT_DATA)

        ref_series = pd.Series(
            {k: v for k, v in ref.value_counts.items()}
        ).reindex(serving.unique(), fill_value=0)

        chi2 = DriftTestSuite.chi2_test(ref_series, serving, settings.ks_pvalue_threshold)

        # PSI for categorical: treat category fractions as distributions
        ref_total = sum(ref.value_counts.values())
        all_cats = sorted(set(ref.value_counts.keys()) | set(serving.unique()))
        ref_arr = np.array([ref.value_counts.get(c, 0) / max(ref_total, 1) for c in all_cats])
        cur_total = len(serving)
        cur_arr = np.array([(serving == c).sum() / max(cur_total, 1) for c in all_cats])
        psi_score = float(np.sum((cur_arr - ref_arr) * np.log(np.clip(cur_arr, 1e-6, None) / np.clip(ref_arr, 1e-6, None))))

        any_drift = chi2.drifted or psi_score > settings.psi_threshold
        status = DriftStatus.DRIFT_DETECTED if any_drift else DriftStatus.NO_DRIFT
        explanations = []
        if chi2.drifted:
            explanations.append(f"Chi2 p={chi2.pvalue:.4f}")
        if psi_score > settings.psi_threshold:
            explanations.append(f"PSI={psi_score:.3f}")

        magnitude = min(1.0, (1 - chi2.pvalue) * 0.5 + min(psi_score / (settings.psi_threshold * 2), 1.0) * 0.5)

        return FeatureDriftResult(
            feature_name=name,
            status=status,
            chi2_statistic=chi2.statistic,
            chi2_pvalue=chi2.pvalue,
            psi_score=psi_score,
            explanation="; ".join(explanations) or "No drift",
            drift_magnitude=magnitude,
        )


def _reconstruct_ref_samples(ref_stats) -> np.ndarray:
    """Reconstruct approximate reference samples from histogram."""
    if ref_stats.histogram_edges and ref_stats.histogram_counts:
        edges = np.array(ref_stats.histogram_edges)
        counts = np.array(ref_stats.histogram_counts)
        midpoints = (edges[:-1] + edges[1:]) / 2
        return np.repeat(midpoints, counts.astype(int))
    if ref_stats.mean is not None and ref_stats.std is not None:
        rng = np.random.default_rng(42)
        return rng.normal(ref_stats.mean, max(ref_stats.std, 1e-6), 200)
    return np.array([])

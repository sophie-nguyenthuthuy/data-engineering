"""
Statistical drift detector: compares training-time feature distributions
against production distributions captured from the online ring-buffer.

Metrics used:
  - Continuous features: Kolmogorov-Smirnov test + Population Stability Index (PSI)
  - Categorical features: Chi-squared test + Jensen-Shannon divergence

A feature is flagged as drifted when:
  - PSI > threshold_psi  (default 0.2)  OR
  - KS p-value < threshold_ks  (default 0.05)  OR
  - JS divergence > threshold_js  (default 0.1)
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import jensenshannon

from feature_store.registry import FeatureType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PSI helpers
# ---------------------------------------------------------------------------

def _psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index between two continuous distributions."""
    combined = np.concatenate([expected, actual])
    breakpoints = np.linspace(combined.min(), combined.max(), bins + 1)
    breakpoints[0] -= 1e-9
    breakpoints[-1] += 1e-9

    exp_pct, _ = np.histogram(expected, bins=breakpoints)
    act_pct, _ = np.histogram(actual, bins=breakpoints)

    exp_pct = exp_pct / len(expected)
    act_pct = act_pct / len(actual)

    # Avoid log(0)
    exp_pct = np.where(exp_pct == 0, 1e-9, exp_pct)
    act_pct = np.where(act_pct == 0, 1e-9, act_pct)

    return float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)))


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class FeatureDriftResult:
    feature_name: str
    feature_type: str
    drifted: bool
    psi: float | None = None
    ks_statistic: float | None = None
    ks_pvalue: float | None = None
    js_divergence: float | None = None
    chi2_statistic: float | None = None
    chi2_pvalue: float | None = None
    training_n: int = 0
    production_n: int = 0
    details: str = ""


@dataclass
class DriftReport:
    generated_at: float
    drifted_features: list[str]
    feature_results: list[FeatureDriftResult]
    overall_drift_score: float  # fraction of features drifted
    retraining_triggered: bool = False

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------

class DriftDetector:
    def __init__(
        self,
        threshold_psi: float | None = None,
        threshold_ks: float | None = None,
        threshold_js: float | None = None,
    ) -> None:
        self.threshold_psi = threshold_psi or float(
            os.getenv("DRIFT_THRESHOLD_PSI", "0.2")
        )
        self.threshold_ks = threshold_ks or float(
            os.getenv("DRIFT_THRESHOLD_KS", "0.05")
        )
        self.threshold_js = threshold_js or float(
            os.getenv("DRIFT_THRESHOLD_JS", "0.1")
        )

    # ------------------------------------------------------------------
    # Per-feature detection
    # ------------------------------------------------------------------

    def _check_continuous(
        self,
        name: str,
        training_vals: list[float],
        production_vals: list[float],
    ) -> FeatureDriftResult:
        tr = np.array(training_vals, dtype=float)
        pr = np.array(production_vals, dtype=float)

        ks_stat, ks_p = stats.ks_2samp(tr, pr)
        psi_val = _psi(tr, pr)

        drifted = bool((psi_val > self.threshold_psi) or (ks_p < self.threshold_ks))
        details_parts = []
        if psi_val > self.threshold_psi:
            details_parts.append(f"PSI {psi_val:.4f} > {self.threshold_psi}")
        if ks_p < self.threshold_ks:
            details_parts.append(f"KS p-value {ks_p:.4f} < {self.threshold_ks}")

        return FeatureDriftResult(
            feature_name=name,
            feature_type=FeatureType.CONTINUOUS.value,
            drifted=drifted,
            psi=round(psi_val, 6),
            ks_statistic=round(float(ks_stat), 6),
            ks_pvalue=round(float(ks_p), 6),
            training_n=len(tr),
            production_n=len(pr),
            details="; ".join(details_parts) if details_parts else "no drift",
        )

    def _check_categorical(
        self,
        name: str,
        training_vals: list,
        production_vals: list,
    ) -> FeatureDriftResult:
        # Build aligned frequency arrays over union of categories
        all_cats = list(set(training_vals) | set(production_vals))
        tr_counts = np.array([training_vals.count(c) for c in all_cats], dtype=float)
        pr_counts = np.array([production_vals.count(c) for c in all_cats], dtype=float)

        tr_pct = tr_counts / tr_counts.sum()
        pr_pct = pr_counts / pr_counts.sum()

        js_div = float(jensenshannon(tr_pct, pr_pct))

        # Chi-squared (add small epsilon to avoid zero expected)
        tr_expected = tr_pct * len(production_vals)
        tr_expected = np.where(tr_expected == 0, 1e-9, tr_expected)
        chi2_stat, chi2_p = stats.chisquare(pr_counts, f_exp=tr_expected)

        drifted = bool((js_div > self.threshold_js) or (chi2_p < self.threshold_ks))
        details_parts = []
        if js_div > self.threshold_js:
            details_parts.append(f"JS divergence {js_div:.4f} > {self.threshold_js}")
        if chi2_p < self.threshold_ks:
            details_parts.append(f"Chi2 p-value {chi2_p:.4f} < {self.threshold_ks}")

        return FeatureDriftResult(
            feature_name=name,
            feature_type=FeatureType.CATEGORICAL.value,
            drifted=drifted,
            js_divergence=round(js_div, 6),
            chi2_statistic=round(float(chi2_stat), 6),
            chi2_pvalue=round(float(chi2_p), 6),
            training_n=len(training_vals),
            production_n=len(production_vals),
            details="; ".join(details_parts) if details_parts else "no drift",
        )

    # ------------------------------------------------------------------
    # Full report
    # ------------------------------------------------------------------

    def compare(
        self,
        training_df: pd.DataFrame,
        production_values: dict[str, list],
        feature_types: dict[str, FeatureType],
    ) -> DriftReport:
        """
        Compare training distribution (DataFrame) to production values
        (dict of feature_name -> list of recent values from ring-buffer).
        """
        results: list[FeatureDriftResult] = []

        for feature_name, ftype in feature_types.items():
            if feature_name not in training_df.columns:
                logger.debug("Skipping %s: not in training DataFrame", feature_name)
                continue

            prod_vals = production_values.get(feature_name, [])
            if len(prod_vals) < 30:
                logger.debug(
                    "Skipping %s: only %d production samples", feature_name, len(prod_vals)
                )
                continue

            train_vals = training_df[feature_name].dropna().tolist()
            if len(train_vals) < 30:
                continue

            if ftype == FeatureType.CONTINUOUS:
                result = self._check_continuous(feature_name, train_vals, prod_vals)
            else:
                result = self._check_categorical(
                    feature_name,
                    [str(v) for v in train_vals],
                    [str(v) for v in prod_vals],
                )

            results.append(result)
            if result.drifted:
                logger.warning("DRIFT DETECTED: %s — %s", feature_name, result.details)

        drifted_names = [r.feature_name for r in results if r.drifted]
        overall_score = len(drifted_names) / len(results) if results else 0.0

        return DriftReport(
            generated_at=time.time(),
            drifted_features=drifted_names,
            feature_results=results,
            overall_drift_score=round(overall_score, 4),
        )

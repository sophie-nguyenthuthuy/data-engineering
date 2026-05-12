"""
Interrupted Time Series (ITS) causal impact analysis.

Given a metric time-series and a hypothesised intervention point (when an
upstream change occurred), this module:

1. Splits the series into a pre-period (control) and post-period (test).
2. Fits a segmented linear regression with:
      y = β0 + β1·t + β2·D + β3·D·t
   where D = 1 after the intervention ("level change") and
   β3 captures the slope change.
3. Constructs a counterfactual (no-intervention prediction) for the post-period.
4. Computes the absolute and relative causal effect with a confidence interval.
5. Returns a p-value for the level-change coefficient (β2) and flags
   significance at the configured α level.

Reference: Interrupted time series regression for the evaluation of public
health interventions: a tutorial. (Lopez Bernal et al., IJE 2017)
"""

from __future__ import annotations

import logging
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats

from pipeline_rca.models import CausalEstimate, MetricPoint, SchemaChange

logger = logging.getLogger(__name__)


def _build_design_matrix(
    series: pd.DataFrame, intervention_idx: int
) -> tuple[np.ndarray, np.ndarray]:
    """
    Build X (design matrix) and y for the ITS regression.

    Columns of X: [1, t, D, D*t]
    """
    n = len(series)
    t = np.arange(n, dtype=float)
    D = np.where(t >= intervention_idx, 1.0, 0.0)
    D_t = D * (t - intervention_idx)
    X = np.column_stack([np.ones(n), t, D, D_t])
    y = series["v"].to_numpy(dtype=float)
    return X, y


class ITSAnalyzer:
    """
    Run an ITS analysis for one (metric, candidate-change) pair.

    Parameters
    ----------
    confidence_level : float
        Width of the CI, default 0.95.
    min_pre_periods : int
        Minimum number of pre-intervention data points required.
    """

    def __init__(
        self, confidence_level: float = 0.95, min_pre_periods: int = 7
    ) -> None:
        self.confidence_level = confidence_level
        self.min_pre_periods = min_pre_periods
        self._alpha = 1.0 - confidence_level

    def analyze(
        self,
        metric_series: list[MetricPoint],
        intervention_at: datetime,
        candidate_label: str,
        change: SchemaChange | None = None,
    ) -> CausalEstimate | None:
        """
        Estimate the causal effect of a change that occurred at *intervention_at*.

        Returns None if the series is too short for a meaningful analysis.
        """
        df = pd.DataFrame(
            {"ts": [p.timestamp for p in metric_series], "v": [p.value for p in metric_series]}
        ).sort_values("ts").reset_index(drop=True)

        # Find intervention index
        int_idx = int(df["ts"].searchsorted(intervention_at))
        n_pre = int_idx
        n_post = len(df) - int_idx

        if n_pre < self.min_pre_periods:
            logger.warning(
                "Only %d pre-period points for %s; need %d – skipping",
                n_pre,
                candidate_label,
                self.min_pre_periods,
            )
            return None

        if n_post < 1:
            logger.warning("Intervention at %s is after the series end – skipping", intervention_at)
            return None

        X, y = _build_design_matrix(df, int_idx)

        # Fit segmented regression on the FULL series (Lopez Bernal 2017 standard ITS).
        # X columns: [1, t, D, D·(t−t₀)]  — D=1 in post-period.
        # β₂ (level change) and its SE are identifiable because both pre and post
        # periods are included in the fit.
        try:
            coeffs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        except np.linalg.LinAlgError as exc:
            logger.error("Linear algebra failure for %s: %s", candidate_label, exc)
            return None

        # Counterfactual: suppress the intervention terms (D=0, D·t=0)
        X_counter = X.copy()
        X_counter[:, 2] = 0.0
        X_counter[:, 3] = 0.0
        y_counter = X_counter @ coeffs

        y_post_obs = y[n_pre:]
        y_post_counter = y_counter[n_pre:]

        # Causal effect = mean(observed) - mean(counterfactual) in post-period
        abs_effect = float(np.mean(y_post_obs) - np.mean(y_post_counter))
        baseline_counter_mean = float(np.mean(y_post_counter))
        rel_effect = abs_effect / baseline_counter_mean if baseline_counter_mean != 0 else 0.0

        # Inference on β₂ using full-series OLS residuals
        p_value, ci = self._inference(X, y, coeffs, len(df), int_idx)

        is_sig = p_value <= self._alpha and abs(rel_effect) > 0.0

        counterfactual_points = [
            MetricPoint(timestamp=df.iloc[i]["ts"], value=float(y_counter[i]))
            for i in range(n_pre, len(df))
        ]

        logger.debug(
            "ITS [%s] abs_effect=%.2f rel_effect=%.2f%% p=%.4f sig=%s",
            candidate_label,
            abs_effect,
            rel_effect * 100,
            p_value,
            is_sig,
        )

        return CausalEstimate(
            candidate=candidate_label,
            change=change,
            effect_size=abs(rel_effect),
            absolute_effect=abs_effect,
            p_value=float(p_value),
            confidence_interval=(float(ci[0]), float(ci[1])),
            is_significant=is_sig,
            counterfactual=counterfactual_points,
        )

    def _inference(
        self,
        X: np.ndarray,
        y: np.ndarray,
        coeffs: np.ndarray,
        n: int,
        int_idx: int,
    ) -> tuple[float, tuple[float, float]]:
        """
        Compute p-value and CI for the level-change coefficient (β2).

        Uses OLS standard errors derived from the full-series residuals.
        """
        n, k = X.shape
        dof = max(n - k, 1)
        y_hat = X @ coeffs
        residuals = y - y_hat
        s2 = np.dot(residuals, residuals) / dof

        try:
            XtX_inv = np.linalg.inv(X.T @ X)
        except np.linalg.LinAlgError:
            return 1.0, (0.0, 0.0)

        se = np.sqrt(s2 * np.diag(XtX_inv))

        beta2 = coeffs[2]
        se2 = se[2] if len(se) > 2 else 1.0

        t_stat = beta2 / se2 if se2 > 0 else 0.0
        p_value = float(2 * stats.t.sf(abs(t_stat), df=dof))

        t_crit = float(stats.t.ppf(1 - self._alpha / 2, df=dof))
        ci = (beta2 - t_crit * se2, beta2 + t_crit * se2)

        return p_value, ci


def rank_candidates(estimates: list[CausalEstimate]) -> list[CausalEstimate]:
    """
    Sort estimates by (is_significant DESC, effect_size DESC, p_value ASC).
    Significant candidates with larger relative effect come first.
    """
    return sorted(
        estimates,
        key=lambda e: (not e.is_significant, -e.effect_size, e.p_value),
    )

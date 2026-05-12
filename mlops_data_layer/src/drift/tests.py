from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from scipy import stats


@dataclass
class KSResult:
    statistic: float
    pvalue: float
    drifted: bool


@dataclass
class PSIResult:
    score: float
    drifted: bool
    bin_contributions: list[float] = field(default_factory=list)


@dataclass
class JSResult:
    divergence: float
    drifted: bool


@dataclass
class Chi2Result:
    statistic: float
    pvalue: float
    drifted: bool


class DriftTestSuite:
    """
    Statistical tests for distribution drift.

    All tests follow the same interface:
        test(reference, current, threshold) → Result

    reference / current are 1-D array-like of the same feature.
    """

    # ── Kolmogorov-Smirnov (continuous / numerical) ──────────────────────────

    @staticmethod
    def ks_test(
        reference: np.ndarray,
        current: np.ndarray,
        pvalue_threshold: float = 0.05,
    ) -> KSResult:
        """
        Two-sample KS test.
        Small p-value (< threshold) → distributions differ → drift detected.
        """
        ref = np.asarray(reference, dtype=float)
        cur = np.asarray(current, dtype=float)
        if len(ref) < 5 or len(cur) < 5:
            return KSResult(statistic=0.0, pvalue=1.0, drifted=False)
        stat, pval = stats.ks_2samp(ref, cur)
        return KSResult(statistic=float(stat), pvalue=float(pval), drifted=pval < pvalue_threshold)

    # ── Population Stability Index ────────────────────────────────────────────

    @staticmethod
    def psi(
        reference: np.ndarray,
        current: np.ndarray,
        bins: int = 10,
        threshold: float = 0.2,
    ) -> PSIResult:
        """
        PSI measures how much a distribution has shifted from a reference.
        PSI < 0.1   → no significant change
        PSI 0.1–0.2 → moderate change (warning)
        PSI > 0.2   → significant change (drift)
        """
        ref = np.asarray(reference, dtype=float)
        cur = np.asarray(current, dtype=float)
        if len(ref) == 0 or len(cur) == 0:
            return PSIResult(score=0.0, drifted=False)

        # Build bins from reference distribution
        breakpoints = np.percentile(ref, np.linspace(0, 100, bins + 1))
        breakpoints = np.unique(breakpoints)
        if len(breakpoints) < 2:
            return PSIResult(score=0.0, drifted=False)

        def _bin_fractions(arr: np.ndarray) -> np.ndarray:
            counts, _ = np.histogram(arr, bins=breakpoints)
            fracs = counts / len(arr)
            # Avoid log(0) — clip to tiny positive value
            return np.clip(fracs, 1e-6, None)

        ref_fracs = _bin_fractions(ref)
        cur_fracs = _bin_fractions(cur)
        contributions = (cur_fracs - ref_fracs) * np.log(cur_fracs / ref_fracs)
        score = float(np.sum(contributions))
        return PSIResult(
            score=score,
            drifted=score > threshold,
            bin_contributions=contributions.tolist(),
        )

    # ── Jensen-Shannon Divergence ────────────────────────────────────────────

    @staticmethod
    def js_divergence(
        reference: np.ndarray,
        current: np.ndarray,
        bins: int = 20,
        threshold: float = 0.1,
    ) -> JSResult:
        """
        JS divergence is symmetric and bounded [0, 1].
        Values near 0 → distributions are similar.
        Values near 1 → completely different.
        """
        ref = np.asarray(reference, dtype=float)
        cur = np.asarray(current, dtype=float)
        if len(ref) == 0 or len(cur) == 0:
            return JSResult(divergence=0.0, drifted=False)

        combined = np.concatenate([ref, cur])
        bin_edges = np.linspace(combined.min(), combined.max(), bins + 1)

        def _prob(arr):
            counts, _ = np.histogram(arr, bins=bin_edges)
            p = counts.astype(float) + 1e-10   # Laplace smoothing
            return p / p.sum()

        p = _prob(ref)
        q = _prob(cur)
        m = 0.5 * (p + q)
        jsd = float(0.5 * np.sum(p * np.log(p / m)) + 0.5 * np.sum(q * np.log(q / m)))
        jsd = max(0.0, min(1.0, jsd))
        return JSResult(divergence=jsd, drifted=jsd > threshold)

    # ── Chi-Squared (categorical) ─────────────────────────────────────────────

    @staticmethod
    def chi2_test(
        reference: pd.Series,
        current: pd.Series,
        pvalue_threshold: float = 0.05,
    ) -> Chi2Result:
        """
        Chi-squared goodness-of-fit.
        Tests whether the *current* category distribution matches *reference*.
        """
        all_cats = sorted(set(reference.astype(str).unique()) | set(current.astype(str).unique()))
        ref_counts = reference.astype(str).value_counts().reindex(all_cats, fill_value=0)
        cur_counts = current.astype(str).value_counts().reindex(all_cats, fill_value=0)

        total_ref = ref_counts.sum()
        total_cur = cur_counts.sum()
        if total_ref == 0 or total_cur == 0:
            return Chi2Result(statistic=0.0, pvalue=1.0, drifted=False)

        # Scale reference counts to current total (expected frequencies)
        expected = ref_counts / total_ref * total_cur
        # Drop categories with very low expected frequency (chi2 assumption)
        mask = expected >= 5
        if mask.sum() < 2:
            return Chi2Result(statistic=0.0, pvalue=1.0, drifted=False)

        stat, pval = stats.chisquare(
            f_obs=cur_counts[mask],
            f_exp=expected[mask],
        )
        return Chi2Result(
            statistic=float(stat),
            pvalue=float(pval),
            drifted=pval < pvalue_threshold,
        )

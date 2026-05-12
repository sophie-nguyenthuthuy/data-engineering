"""Tests for the ITS causal impact analyzer."""

from datetime import datetime, timedelta

import numpy as np
import pytest

from pipeline_rca.analysis.causal_impact import ITSAnalyzer, rank_candidates
from pipeline_rca.models import CausalEstimate, MetricPoint
from pipeline_rca.monitors.metric_monitor import build_synthetic_degradation


def _series_with_drop(baseline: int = 14, drop: float = 0.40, noise: float = 0.02) -> list[MetricPoint]:
    return build_synthetic_degradation(baseline_days=baseline, eval_days=5, drop_pct=drop, noise_pct=noise)


class TestITSAnalyzer:
    def setup_method(self):
        self.analyzer = ITSAnalyzer(confidence_level=0.95, min_pre_periods=7)

    def test_returns_estimate_for_known_drop(self):
        series = _series_with_drop(baseline=14, drop=0.40)
        intervention_at = series[14].timestamp - timedelta(hours=1)
        est = self.analyzer.analyze(series, intervention_at, "table.col [drop]")
        assert est is not None
        assert est.effect_size > 0.0

    def test_significant_for_large_drop(self):
        series = _series_with_drop(baseline=14, drop=0.50, noise=0.01)
        intervention_at = series[14].timestamp
        est = self.analyzer.analyze(series, intervention_at, "big_drop")
        assert est is not None
        assert est.is_significant

    def test_returns_none_if_too_few_pre_periods(self):
        series = _series_with_drop(baseline=4)
        intervention_at = series[4].timestamp
        est = self.analyzer.analyze(series, intervention_at, "short")
        assert est is None

    def test_counterfactual_populated(self):
        series = _series_with_drop(baseline=14)
        intervention_at = series[14].timestamp
        est = self.analyzer.analyze(series, intervention_at, "x")
        assert est is not None
        assert len(est.counterfactual) > 0

    def test_confidence_interval_ordering(self):
        series = _series_with_drop(baseline=14, drop=0.30)
        intervention_at = series[14].timestamp
        est = self.analyzer.analyze(series, intervention_at, "ci_test")
        assert est is not None
        lo, hi = est.confidence_interval
        assert lo <= hi

    def test_rank_candidates_significant_first(self):
        sig = CausalEstimate("a", None, 0.3, -300, 0.01, (-350, -250), True)
        not_sig = CausalEstimate("b", None, 0.1, -100, 0.20, (-120, -80), False)
        ranked = rank_candidates([not_sig, sig])
        assert ranked[0].candidate == "a"

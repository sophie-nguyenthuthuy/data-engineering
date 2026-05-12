"""Tests for MetricMonitor."""

from datetime import datetime, timedelta

import pytest

from pipeline_rca.models import DegradationKind, MetricPoint
from pipeline_rca.monitors.metric_monitor import MetricMonitor, build_synthetic_degradation


def _flat_series(n: int, value: float = 1000.0) -> list[MetricPoint]:
    base = datetime(2024, 1, 1)
    return [MetricPoint(timestamp=base + timedelta(days=i), value=value) for i in range(n)]


class TestMetricMonitor:
    def test_no_degradation_flat_series(self):
        series = _flat_series(20, 1000.0)
        monitor = MetricMonitor("m", degradation_threshold=0.15, baseline_window_days=14, evaluation_window_days=3)
        assert monitor.check(series) is None

    def test_detects_drop(self):
        series = build_synthetic_degradation(baseline_days=14, eval_days=3, drop_pct=0.40, noise_pct=0.01)
        monitor = MetricMonitor("m", degradation_threshold=0.10, baseline_window_days=14, evaluation_window_days=3, z_threshold=2.0)
        result = monitor.check(series)
        assert result is not None
        assert result.kind == DegradationKind.DROP
        assert result.relative_change < -0.30

    def test_detects_spike(self):
        series = build_synthetic_degradation(baseline_days=14, eval_days=3, drop_pct=-0.40, noise_pct=0.01)
        monitor = MetricMonitor("m", degradation_threshold=0.10, baseline_window_days=14, evaluation_window_days=3, z_threshold=2.0)
        result = monitor.check(series)
        assert result is not None
        assert result.kind == DegradationKind.SPIKE

    def test_insufficient_data_returns_none(self):
        series = _flat_series(5)
        monitor = MetricMonitor("m", baseline_window_days=14, evaluation_window_days=3)
        assert monitor.check(series) is None

    def test_synthetic_series_length(self):
        series = build_synthetic_degradation(baseline_days=14, eval_days=3)
        assert len(series) == 17

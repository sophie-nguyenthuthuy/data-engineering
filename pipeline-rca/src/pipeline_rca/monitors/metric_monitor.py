"""Detect degradation in downstream metrics using rolling z-score and threshold checks."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Sequence

import numpy as np
import pandas as pd

from pipeline_rca.models import DegradationKind, MetricDegradation, MetricPoint

logger = logging.getLogger(__name__)


class MetricMonitor:
    """
    Watches a time-series metric and flags statistically significant degradation.

    Detection strategy:
    1. Rolling z-score: flag if the latest window mean deviates > `z_threshold` σ
       from the baseline window.
    2. Hard threshold: flag if relative change vs baseline mean exceeds
       `degradation_threshold`.
    Both conditions must be True to emit a degradation event (reduces noise).
    """

    def __init__(
        self,
        metric_name: str,
        degradation_threshold: float = 0.15,
        baseline_window_days: int = 14,
        evaluation_window_days: int = 3,
        z_threshold: float = 2.5,
    ) -> None:
        self.metric_name = metric_name
        self.degradation_threshold = degradation_threshold
        self.baseline_window_days = baseline_window_days
        self.evaluation_window_days = evaluation_window_days
        self.z_threshold = z_threshold

    def check(
        self, series: Sequence[MetricPoint]
    ) -> MetricDegradation | None:
        """Return a MetricDegradation if the series tail looks degraded, else None."""
        if len(series) < self.baseline_window_days + self.evaluation_window_days:
            logger.warning(
                "Not enough data for %s (need %d points, got %d)",
                self.metric_name,
                self.baseline_window_days + self.evaluation_window_days,
                len(series),
            )
            return None

        df = pd.DataFrame(
            {"ts": [p.timestamp for p in series], "v": [p.value for p in series]}
        ).sort_values("ts").reset_index(drop=True)

        baseline = df.iloc[: -self.evaluation_window_days]
        evaluation = df.iloc[-self.evaluation_window_days :]

        baseline_mean = baseline["v"].mean()
        baseline_std = baseline["v"].std(ddof=1)
        eval_mean = evaluation["v"].mean()

        if baseline_std == 0:
            logger.debug("Zero std in baseline for %s; skipping z-score check", self.metric_name)
            z_score = 0.0
        else:
            z_score = (eval_mean - baseline_mean) / baseline_std

        relative_change = (eval_mean - baseline_mean) / baseline_mean if baseline_mean != 0 else 0.0
        abs_change = abs(relative_change)

        is_significant_z = abs(z_score) >= self.z_threshold
        is_significant_threshold = abs_change >= self.degradation_threshold

        if not (is_significant_z and is_significant_threshold):
            return None

        kind = DegradationKind.DROP if relative_change < 0 else DegradationKind.SPIKE

        logger.info(
            "Degradation detected for %s: %s %.1f%% (z=%.2f)",
            self.metric_name,
            kind.value,
            abs_change * 100,
            z_score,
        )

        return MetricDegradation(
            metric_name=self.metric_name,
            detected_at=datetime.utcnow(),
            kind=kind,
            observed_value=float(eval_mean),
            baseline_value=float(baseline_mean),
            relative_change=float(relative_change),
            series=list(series),
        )

    def check_null_rate(
        self,
        series: Sequence[MetricPoint],
        null_count_series: Sequence[MetricPoint],
    ) -> MetricDegradation | None:
        """Detect a spike in null rate relative to row count."""
        if len(series) != len(null_count_series) or len(series) < 4:
            return None

        rates = [
            MetricPoint(
                timestamp=p.timestamp,
                value=n.value / p.value if p.value > 0 else 0.0,
            )
            for p, n in zip(series, null_count_series)
        ]
        result = self.check(rates)
        if result is not None:
            result.kind = DegradationKind.NULL_INCREASE
        return result


def build_synthetic_degradation(
    baseline_days: int = 14,
    eval_days: int = 3,
    baseline_mean: float = 1000.0,
    noise_pct: float = 0.05,
    drop_pct: float = 0.30,
    start: datetime | None = None,
) -> list[MetricPoint]:
    """Utility to generate synthetic data with a known drop for testing."""
    rng = np.random.default_rng(42)
    start = start or (datetime.utcnow() - timedelta(days=baseline_days + eval_days))
    points: list[MetricPoint] = []

    for i in range(baseline_days + eval_days):
        ts = start + timedelta(days=i)
        noise = rng.normal(0, baseline_mean * noise_pct)
        if i >= baseline_days:
            value = baseline_mean * (1 - drop_pct) + noise
        else:
            value = baseline_mean + noise
        points.append(MetricPoint(timestamp=ts, value=max(0.0, value)))

    return points

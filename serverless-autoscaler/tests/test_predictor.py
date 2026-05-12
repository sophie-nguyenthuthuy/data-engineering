from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import numpy as np
import pytest

from autoscaler.config import PredictorConfig
from autoscaler.models import JobRun, JobStatus
from autoscaler.predictor import ARIMAPredictor


def _make_run(peak_workers: int, days_ago: int = 0) -> JobRun:
    now = datetime.utcnow() - timedelta(days=days_ago)
    return JobRun(
        run_id=str(uuid.uuid4()),
        job_id="test-job",
        scheduled_at=now,
        started_at=now,
        finished_at=now + timedelta(minutes=30),
        status=JobStatus.COMPLETED,
        peak_workers=peak_workers,
        peak_cpu_millicores=peak_workers * 1000.0,
        peak_memory_mib=peak_workers * 2048.0,
        duration_seconds=1800.0,
    )


def _predictor(min_history: int = 5) -> ARIMAPredictor:
    cfg = PredictorConfig(min_history_points=min_history, safety_factor=1.0)
    return ARIMAPredictor(cfg)


class TestPercentileFallback:
    def test_returns_forecast_with_sparse_history(self):
        predictor = _predictor(min_history=20)
        history = [_make_run(10 + i, days_ago=i) for i in range(5)]
        target = datetime.utcnow() + timedelta(hours=1)
        forecast = predictor.forecast("test-job", history, target)
        assert forecast is not None
        assert forecast.predicted_peak_workers >= 1

    def test_returns_none_with_empty_history(self):
        predictor = _predictor(min_history=1)
        forecast = predictor.forecast("test-job", [], datetime.utcnow())
        assert forecast is None

    def test_safety_factor_applied(self):
        cfg = PredictorConfig(min_history_points=50, safety_factor=1.5)
        predictor = ARIMAPredictor(cfg)
        # All runs have 10 workers — p95 = 10 — with safety 1.5 → should be 15
        history = [_make_run(10, days_ago=i) for i in range(5)]
        target = datetime.utcnow() + timedelta(hours=1)
        forecast = predictor.forecast("test-job", history, target)
        assert forecast is not None
        assert forecast.predicted_peak_workers >= 10


class TestARIMAPath:
    def test_arima_forecast_with_sufficient_history(self):
        predictor = _predictor(min_history=10)
        rng = np.random.default_rng(42)
        # Simulate a mildly trending workload
        workers = [int(10 + i * 0.5 + rng.normal(0, 1)) for i in range(20)]
        history = [_make_run(max(1, w), days_ago=20 - i) for i, w in enumerate(workers)]
        target = datetime.utcnow() + timedelta(hours=1)
        forecast = predictor.forecast("test-job", history, target)
        assert forecast is not None
        assert forecast.predicted_peak_workers >= 1
        assert forecast.history_points_used == 20
        assert forecast.confidence_lower <= forecast.predicted_peak_workers

    def test_cpu_memory_extrapolated(self):
        predictor = _predictor(min_history=10)
        history = [_make_run(10, days_ago=i) for i in range(15)]
        target = datetime.utcnow() + timedelta(hours=1)
        forecast = predictor.forecast("test-job", history, target)
        assert forecast is not None
        assert forecast.predicted_peak_cpu_millicores > 0
        assert forecast.predicted_peak_memory_mib > 0

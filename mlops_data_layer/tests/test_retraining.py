"""Tests for the RetrainingTriggerEngine."""
from __future__ import annotations
import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

from src.retraining.trigger import RetrainingTriggerEngine
from src.models import (
    DriftReport, SkewReport, DriftStatus, FeatureDriftResult, SkewFeatureResult,
    RetrainingJobStatus, TriggerReason, ValidatorBackend,
)


def _drift_report(status: DriftStatus, n_drifted: int = 2) -> DriftReport:
    features = [
        FeatureDriftResult(
            feature_name=f"feat_{i}",
            status=DriftStatus.DRIFT_DETECTED if i < n_drifted else DriftStatus.NO_DRIFT,
            drift_magnitude=0.7 if i < n_drifted else 0.1,
        )
        for i in range(4)
    ]
    return DriftReport(
        report_id="report-1",
        model_name="fraud_model",
        model_version="v1",
        reference_snapshot_id="snap-1",
        window_size=500,
        overall_status=status,
        drifted_feature_count=n_drifted,
        total_feature_count=4,
        drift_score=0.6 if status == DriftStatus.DRIFT_DETECTED else 0.05,
        feature_results=features,
        triggers_retraining=(status == DriftStatus.DRIFT_DETECTED),
    )


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.setex = AsyncMock()
    redis.publish = AsyncMock()
    return redis


@pytest.fixture
def engine(mock_redis) -> RetrainingTriggerEngine:
    return RetrainingTriggerEngine(mock_redis)


class TestRetrainingTriggerEngine:
    @pytest.mark.asyncio
    async def test_no_trigger_when_no_drift(self, engine):
        report = _drift_report(DriftStatus.NO_DRIFT, n_drifted=0)
        job = await engine.evaluate_drift(report)
        assert job is None

    @pytest.mark.asyncio
    async def test_triggers_on_drift(self, engine, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)  # no cooldown
        report = _drift_report(DriftStatus.DRIFT_DETECTED, n_drifted=3)
        job = await engine.evaluate_drift(report)
        assert job is not None
        assert job.status == RetrainingJobStatus.DISPATCHED
        assert job.trigger.reason == TriggerReason.DATA_DRIFT

    @pytest.mark.asyncio
    async def test_cooldown_skips_trigger(self, engine, mock_redis):
        # Simulate an active cooldown — last trigger was 10 seconds ago
        from src.config import settings
        recent = datetime.utcnow().isoformat()
        mock_redis.get = AsyncMock(return_value=recent.encode())
        report = _drift_report(DriftStatus.DRIFT_DETECTED, n_drifted=3)
        job = await engine.evaluate_drift(report)
        assert job is not None
        assert job.status == RetrainingJobStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_manual_trigger_bypasses_cooldown(self, engine, mock_redis):
        recent = datetime.utcnow().isoformat()
        mock_redis.get = AsyncMock(return_value=recent.encode())
        job = await engine.trigger_manual("fraud_model", "v1", "test run")
        # Manual trigger calls _dispatch directly (no cooldown check)
        assert job.status == RetrainingJobStatus.DISPATCHED
        assert job.trigger.reason == TriggerReason.MANUAL

    @pytest.mark.asyncio
    async def test_publishes_to_redis_on_dispatch(self, engine, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        report = _drift_report(DriftStatus.DRIFT_DETECTED)
        await engine.evaluate_drift(report)
        mock_redis.publish.assert_called()

    @pytest.mark.asyncio
    async def test_job_record_persisted(self, engine, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        report = _drift_report(DriftStatus.DRIFT_DETECTED)
        job = await engine.evaluate_drift(report)
        # setex should have been called to persist the job
        mock_redis.setex.assert_called()
        call_args = mock_redis.setex.call_args_list
        keys_written = [str(c[0][0]) for c in call_args]
        assert any(job.job_id in k for k in keys_written)

    @pytest.mark.asyncio
    async def test_get_job_returns_none_for_unknown(self, engine, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        result = await engine.get_job("nonexistent-id")
        assert result is None

    @pytest.mark.asyncio
    async def test_drifted_features_captured(self, engine, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        report = _drift_report(DriftStatus.DRIFT_DETECTED, n_drifted=2)
        job = await engine.evaluate_drift(report)
        assert len(job.trigger.drifted_features) == 2
        assert "feat_0" in job.trigger.drifted_features
        assert "feat_1" in job.trigger.drifted_features

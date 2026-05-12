from __future__ import annotations

import uuid
from datetime import datetime

import pytest

from autoscaler.config import CostConfig, MetricsStoreConfig
from autoscaler.cost_tracker import CostTracker
from autoscaler.metrics_store import MetricsStore
from autoscaler.models import JobRun, JobStatus


@pytest.fixture
def store(tmp_path):
    cfg = MetricsStoreConfig(db_url=f"sqlite:///{tmp_path}/test.db")
    return MetricsStore(cfg)


@pytest.fixture
def tracker(store):
    cfg = CostConfig(
        cold_start_seconds=120.0,
        worker_cost_per_hour=0.096,
        idle_prewarm_cost_factor=0.25,
    )
    return CostTracker(cfg, store)


def _run(job_id: str = "j1") -> JobRun:
    return JobRun(
        run_id=str(uuid.uuid4()),
        job_id=job_id,
        scheduled_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        finished_at=datetime.utcnow(),
        status=JobStatus.COMPLETED,
        cold_start_avoided=True,
    )


class TestCostTracker:
    def test_net_saving_positive_for_reasonable_params(self, tracker):
        run = _run()
        record = tracker.record(run, workers_prewarmed=5, prewarm_lead_time_seconds=300)
        # With default params cold-start savings >> prewarm idle cost
        assert record.net_saving_usd > 0

    def test_net_saving_stored_and_retrievable(self, tracker, store):
        run = _run()
        tracker.record(run, workers_prewarmed=3, prewarm_lead_time_seconds=300)
        total = store.total_net_savings_usd()
        assert total > 0

    def test_report_structure(self, tracker):
        for i in range(3):
            tracker.record(_run(f"job-{i}"), workers_prewarmed=2, prewarm_lead_time_seconds=300)
        report = tracker.report()
        assert "total_net_saving_usd" in report
        assert "by_job" in report
        assert len(report["by_job"]) == 3

    def test_cost_formula_correctness(self, tracker):
        workers = 10
        lead_time = 300.0
        cost_per_sec = 0.096 / 3600
        expected_avoided = workers * 120.0 * cost_per_sec
        expected_idle = workers * 0.25 * lead_time * cost_per_sec
        run = _run()
        record = tracker.record(run, workers_prewarmed=workers, prewarm_lead_time_seconds=lead_time)
        assert abs(record.cold_start_avoided_cost_usd - expected_avoided) < 1e-9
        assert abs(record.prewarm_idle_cost_usd - expected_idle) < 1e-9

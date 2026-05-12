from __future__ import annotations

import uuid
from datetime import datetime

import pytest

from autoscaler.config import MetricsStoreConfig
from autoscaler.metrics_store import MetricsStore
from autoscaler.models import (
    ColdStartSavingRecord,
    JobRun,
    JobStatus,
    ScalingAction,
)


@pytest.fixture
def store(tmp_path):
    cfg = MetricsStoreConfig(db_url=f"sqlite:///{tmp_path}/test.db")
    return MetricsStore(cfg)


def _run(job_id: str = "j1", status: JobStatus = JobStatus.COMPLETED) -> JobRun:
    return JobRun(
        run_id=str(uuid.uuid4()),
        job_id=job_id,
        scheduled_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        finished_at=datetime.utcnow(),
        status=status,
        peak_workers=5,
        duration_seconds=600.0,
    )


class TestJobRuns:
    def test_upsert_and_retrieve(self, store):
        r = _run()
        store.upsert_run(r)
        fetched = store.get_run(r.run_id)
        assert fetched is not None
        assert fetched.job_id == r.job_id
        assert fetched.peak_workers == 5

    def test_only_completed_returned(self, store):
        store.upsert_run(_run(status=JobStatus.RUNNING))
        store.upsert_run(_run(status=JobStatus.COMPLETED))
        completed = store.get_completed_runs("j1")
        assert len(completed) == 1
        assert completed[0].status == JobStatus.COMPLETED

    def test_upsert_updates_existing(self, store):
        r = _run()
        store.upsert_run(r)
        r.peak_workers = 20
        store.upsert_run(r)
        fetched = store.get_run(r.run_id)
        assert fetched.peak_workers == 20


class TestScalingActions:
    def test_record_scaling_action(self, store):
        action = ScalingAction(
            job_id="j1",
            hpa_target="my-hpa",
            namespace="default",
            action_at=datetime.utcnow(),
            min_replicas_before=1,
            min_replicas_after=5,
            max_replicas_before=10,
            max_replicas_after=20,
            reason="prewarm",
        )
        # Should not raise
        store.record_scaling_action(action)


class TestColdStartSavings:
    def test_total_savings(self, store):
        rec = ColdStartSavingRecord(
            job_id="j1",
            run_id=str(uuid.uuid4()),
            recorded_at=datetime.utcnow(),
            workers_prewarmed=5,
            cold_start_seconds_saved=600.0,
            prewarm_idle_cost_usd=0.002,
            cold_start_avoided_cost_usd=0.01,
        )
        store.record_saving(rec)
        total = store.total_net_savings_usd()
        assert abs(total - 0.008) < 1e-6

    def test_savings_by_job(self, store):
        for job_id, savings in [("a", 0.05), ("b", 0.02)]:
            store.record_saving(
                ColdStartSavingRecord(
                    job_id=job_id,
                    run_id=str(uuid.uuid4()),
                    recorded_at=datetime.utcnow(),
                    workers_prewarmed=1,
                    cold_start_seconds_saved=120.0,
                    prewarm_idle_cost_usd=0.0,
                    cold_start_avoided_cost_usd=savings,
                )
            )
        by_job = store.savings_by_job()
        assert set(by_job.keys()) == {"a", "b"}

#!/usr/bin/env python3
"""
Simulate historical job runs to seed the metrics store for development and demos.

Usage:
    python scripts/simulate_jobs.py --jobs 3 --runs-per-job 30 --db sqlite:///dev.db
"""
from __future__ import annotations

import argparse
import random
import uuid
from datetime import datetime, timedelta

from autoscaler.config import MetricsStoreConfig
from autoscaler.metrics_store import MetricsStore
from autoscaler.models import JobRun, JobStatus


def simulate(n_jobs: int, runs_per_job: int, db_url: str) -> None:
    store = MetricsStore(MetricsStoreConfig(db_url=db_url))

    for j in range(n_jobs):
        job_id = f"simulated-job-{j}"
        base_workers = random.randint(4, 20)
        print(f"Simulating {runs_per_job} runs for {job_id} (base_workers={base_workers})")

        for r in range(runs_per_job):
            days_ago = runs_per_job - r
            scheduled = datetime.utcnow() - timedelta(days=days_ago)
            # Add slight upward trend + noise
            trend = r * 0.3
            noise = random.gauss(0, 1.5)
            workers = max(1, int(base_workers + trend + noise))

            run = JobRun(
                run_id=str(uuid.uuid4()),
                job_id=job_id,
                scheduled_at=scheduled,
                started_at=scheduled + timedelta(seconds=random.uniform(5, 30)),
                finished_at=scheduled + timedelta(minutes=random.uniform(15, 90)),
                status=JobStatus.COMPLETED,
                peak_workers=workers,
                peak_cpu_millicores=workers * random.uniform(800, 1200),
                peak_memory_mib=workers * random.uniform(1800, 2400),
                avg_workers=workers * 0.7,
                duration_seconds=random.uniform(900, 5400),
                cold_start_avoided=random.random() > 0.5,
            )
            store.upsert_run(run)

    print(f"\nSeeded {n_jobs * runs_per_job} job runs into {db_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=3)
    parser.add_argument("--runs-per-job", type=int, default=30)
    parser.add_argument("--db", default="sqlite:///dev.db")
    args = parser.parse_args()
    simulate(args.jobs, args.runs_per_job, args.db)

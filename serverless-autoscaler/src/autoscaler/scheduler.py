from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import yaml
from croniter import croniter

from .config import SchedulerConfig
from .cost_tracker import CostTracker
from .hpa_client import HPAClient
from .metrics_store import MetricsStore
from .models import JobDefinition, JobRun, JobStatus, JobType
from .predictor import ARIMAPredictor
from .telemetry import Telemetry

logger = logging.getLogger(__name__)


class PredictiveScheduler:
    """
    Main control loop.

    Every poll_interval_seconds:
      1. Load job definitions from registry YAML.
      2. For each job, find the next scheduled run.
      3. If the run starts within prewarm_lead_time, fetch history and
         compute an ARIMA forecast, then prewarm HPA.
      4. For jobs currently running, fetch live metrics and adjust HPA
         mid-run if actual usage diverges from forecast.
      5. On job completion, restore HPA defaults and record cost savings.
    """

    def __init__(
        self,
        cfg: SchedulerConfig,
        predictor: ARIMAPredictor,
        hpa: HPAClient,
        store: MetricsStore,
        cost: CostTracker,
        telemetry: Telemetry,
    ) -> None:
        self._cfg = cfg
        self._predictor = predictor
        self._hpa = hpa
        self._store = store
        self._cost = cost
        self._telemetry = telemetry
        self._active_runs: dict[str, JobRun] = {}   # run_id → JobRun
        self._prewarmed_jobs: set[str] = set()       # job_ids already prewarmed

    # ------------------------------------------------------------------ #
    #  Entry point                                                         #
    # ------------------------------------------------------------------ #

    def run_forever(self) -> None:
        logger.info("Scheduler started, poll interval=%ds", self._cfg.poll_interval_seconds)
        while True:
            try:
                self._tick()
            except Exception:
                logger.exception("Unhandled error in scheduler tick")
            time.sleep(self._cfg.poll_interval_seconds)

    def _tick(self) -> None:
        jobs = self._load_jobs()
        now = datetime.utcnow()

        for job in jobs:
            try:
                self._process_job(job, now)
            except Exception:
                logger.exception("Error processing job=%s", job.job_id)

    # ------------------------------------------------------------------ #
    #  Per-job logic                                                       #
    # ------------------------------------------------------------------ #

    def _process_job(self, job: JobDefinition, now: datetime) -> None:
        next_run_dt = self._next_run(job.cron_expression, now)
        seconds_until = (next_run_dt - now).total_seconds()

        # --- Prewarm window -----------------------------------------------
        if (
            0 < seconds_until <= self._cfg.prewarm_lead_time_seconds
            and job.job_id not in self._prewarmed_jobs
        ):
            self._prewarm(job, next_run_dt)

        # --- Start tracking if run window just opened ---------------------
        if seconds_until <= 0 and job.job_id not in {
            r.job_id for r in self._active_runs.values()
        }:
            self._start_run(job, next_run_dt)

        # --- Mid-run adjustment -------------------------------------------
        for run in list(self._active_runs.values()):
            if run.job_id == job.job_id:
                self._mid_run_check(job, run)

    def _prewarm(self, job: JobDefinition, target_start: datetime) -> None:
        history = self._store.get_completed_runs(job.job_id)
        forecast = self._predictor.forecast(job.job_id, history, target_start)
        if forecast is None:
            logger.warning("job=%s no forecast available, skipping prewarm", job.job_id)
            return

        action = self._hpa.prewarm(
            job_id=job.job_id,
            hpa_name=job.hpa_target,
            target_min=forecast.predicted_peak_workers,
            target_max=forecast.confidence_upper,
            namespace=job.namespace,
        )
        if action:
            self._store.record_scaling_action(action)
            self._prewarmed_jobs.add(job.job_id)
            self._telemetry.prewarm_total.labels(job_id=job.job_id).inc()
            self._telemetry.predicted_workers.labels(job_id=job.job_id).set(
                forecast.predicted_peak_workers
            )
            logger.info(
                "Prewarmed job=%s workers=%d target_start=%s",
                job.job_id, forecast.predicted_peak_workers, target_start.isoformat(),
            )

    def _start_run(self, job: JobDefinition, scheduled_at: datetime) -> None:
        run = JobRun(
            run_id=str(uuid.uuid4()),
            job_id=job.job_id,
            scheduled_at=scheduled_at,
            started_at=datetime.utcnow(),
            finished_at=None,
            status=JobStatus.RUNNING,
            cold_start_avoided=job.job_id in self._prewarmed_jobs,
        )
        self._active_runs[run.run_id] = run
        self._store.upsert_run(run)
        self._telemetry.active_jobs.labels(job_id=job.job_id).set(1)
        logger.info("Started tracking run=%s job=%s", run.run_id, job.job_id)

    def _mid_run_check(self, job: JobDefinition, run: JobRun) -> None:
        """
        Stubbed: in production this would query the k8s metrics server or
        Prometheus to get live CPU/memory, compare to forecast, and patch HPA.
        """
        live = self._hpa.get_hpa(job.hpa_target, job.namespace)
        current = live["current_replicas"]
        desired = live["desired_replicas"]

        if desired > live["max_replicas"] * 0.9:
            # Approaching ceiling — expand headroom
            new_max = min(int(live["max_replicas"] * 1.3), 200)
            action = self._hpa.adjust_mid_run(
                job_id=job.job_id,
                hpa_name=job.hpa_target,
                new_min=live["min_replicas"],
                new_max=new_max,
                namespace=job.namespace,
                reason="mid_run_headroom_expansion",
            )
            if action:
                self._store.record_scaling_action(action)
                self._telemetry.scaling_actions.labels(
                    job_id=job.job_id, reason="headroom_expansion"
                ).inc()

    def complete_run(
        self,
        run_id: str,
        peak_workers: int,
        peak_cpu: float,
        peak_mem: float,
        job: JobDefinition,
    ) -> None:
        run = self._active_runs.pop(run_id, None)
        if run is None:
            return
        run.status = JobStatus.COMPLETED
        run.finished_at = datetime.utcnow()
        run.peak_workers = peak_workers
        run.peak_cpu_millicores = peak_cpu
        run.peak_memory_mib = peak_mem
        run.duration_seconds = run.wall_seconds
        self._store.upsert_run(run)

        if run.cold_start_avoided:
            lead = self._cfg.prewarm_lead_time_seconds
            self._cost.record(run, peak_workers, lead)

        restore = self._hpa.restore_defaults(
            job_id=job.job_id,
            hpa_name=job.hpa_target,
            namespace=job.namespace,
        )
        if restore:
            self._store.record_scaling_action(restore)

        self._prewarmed_jobs.discard(job.job_id)
        self._telemetry.active_jobs.labels(job_id=job.job_id).set(0)
        self._telemetry.job_duration.labels(job_id=job.job_id).observe(
            run.duration_seconds or 0
        )
        logger.info(
            "Completed run=%s job=%s duration=%.1fs",
            run_id, job.job_id, run.duration_seconds or 0,
        )

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _load_jobs(self) -> list[JobDefinition]:
        try:
            with open(self._cfg.job_registry_path) as f:
                raw = yaml.safe_load(f)
            return [
                JobDefinition(
                    job_id=j["job_id"],
                    name=j["name"],
                    job_type=JobType(j.get("type", "spark")),
                    hpa_target=j["hpa_target"],
                    cron_expression=j["cron"],
                    namespace=j.get("namespace", "default"),
                    tags=j.get("tags", {}),
                )
                for j in (raw or {}).get("jobs", [])
            ]
        except FileNotFoundError:
            logger.debug("Job registry not found at %s", self._cfg.job_registry_path)
            return []
        except Exception:
            logger.exception("Failed to load job registry")
            return []

    @staticmethod
    def _next_run(cron_expr: str, after: datetime) -> datetime:
        return croniter(cron_expr, after).get_next(datetime)

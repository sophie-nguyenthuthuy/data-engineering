from __future__ import annotations

import logging
from datetime import datetime

from .config import CostConfig
from .metrics_store import MetricsStore
from .models import ColdStartSavingRecord, JobRun

logger = logging.getLogger(__name__)


class CostTracker:
    """
    Records and reports cold-start cost savings achieved by predictive warming.

    Saving model:
      avoided_cost  = workers_prewarmed * cold_start_time * worker_cost_per_second
      prewarm_cost  = workers_prewarmed * prewarm_idle_fraction * lead_time * worker_cost_per_second
      net_saving    = avoided_cost - prewarm_cost
    """

    def __init__(self, cfg: CostConfig, store: MetricsStore) -> None:
        self._cfg = cfg
        self._store = store

    def record(
        self,
        run: JobRun,
        workers_prewarmed: int,
        prewarm_lead_time_seconds: float,
    ) -> ColdStartSavingRecord:
        worker_cost_per_second = self._cfg.worker_cost_per_hour / 3600.0

        cold_start_avoided_cost = (
            workers_prewarmed
            * self._cfg.cold_start_seconds
            * worker_cost_per_second
        )
        prewarm_idle_cost = (
            workers_prewarmed
            * self._cfg.idle_prewarm_cost_factor
            * prewarm_lead_time_seconds
            * worker_cost_per_second
        )

        record = ColdStartSavingRecord(
            job_id=run.job_id,
            run_id=run.run_id,
            recorded_at=datetime.utcnow(),
            workers_prewarmed=workers_prewarmed,
            cold_start_seconds_saved=self._cfg.cold_start_seconds * workers_prewarmed,
            prewarm_idle_cost_usd=prewarm_idle_cost,
            cold_start_avoided_cost_usd=cold_start_avoided_cost,
        )
        self._store.record_saving(record)
        logger.info(
            "cost_tracker job=%s run=%s workers=%d net_saving=$%.4f",
            run.job_id,
            run.run_id,
            workers_prewarmed,
            record.net_saving_usd,
        )
        return record

    def report(self) -> dict:
        total = self._store.total_net_savings_usd()
        by_job = self._store.savings_by_job()
        return {
            "total_net_saving_usd": round(total, 4),
            "by_job": {k: round(v, 4) for k, v in sorted(by_job.items(), key=lambda x: -x[1])},
        }

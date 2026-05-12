"""
ViewScheduler — orchestrates the full optimization → create → measure → calibrate loop.

Typical workflow (run as a cron job, e.g. daily):

    scheduler = ViewScheduler(adapter=bq_adapter, config=cfg)
    scheduler.run_cycle()

Each cycle:
  1. Collect fresh query history.
  2. Analyse workload → candidate views.
  3. Refresh cost-model estimates (with calibration).
  4. Run greedy + SA optimizer.
  5. Diff current live views vs. new selection.
  6. Create new views, drop stale ones.
  7. Measure actual savings from existing views, feed calibration.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .adapters.base import BaseAdapter
from .cost_model import CalibrationStore, CostModel, PricingConfig
from .models import CandidateView, MaterializedView, OptimizationResult
from .optimizer import AnnealingSelector, GreedySelector
from .query_analyzer import QueryAnalyzer
from .worklog import WorklogCollector, WorklogStore

log = logging.getLogger(__name__)


@dataclass
class SchedulerConfig:
    # Storage budget for all materialized views combined (bytes)
    budget_bytes: int = 500 * 1024 * 1024 * 1024  # 500 GB

    # Dataset / schema where views are created
    target_dataset_or_schema: str = "analytics.mv_auto"

    # How many days of history to analyse
    lookback_days: int = 30

    # Minimum times a pattern must appear to be a candidate
    min_query_frequency: int = 3

    # Drop a view if its calibrated benefit < this USD/month
    min_net_benefit_usd: float = 1.0

    # SA tuning
    sa_max_iterations: int = 50_000
    sa_cooling_rate: float = 0.9995

    # State persistence
    state_path: Path = Path(".mv_state.json")
    calibration_path: Path = Path(".mv_calibration.json")
    worklog_path: Path = Path(".worklog.db")


class ViewScheduler:
    def __init__(
        self,
        adapter: BaseAdapter,
        config: Optional[SchedulerConfig] = None,
        pricing: Optional[PricingConfig] = None,
    ) -> None:
        self.adapter = adapter
        self.cfg = config or SchedulerConfig()
        self.store = WorklogStore(self.cfg.worklog_path)
        self.calibration = CalibrationStore(self.cfg.calibration_path)
        self.cost_model = CostModel(
            pricing=pricing,
            calibration_store=self.calibration,
        )
        self._live_views: dict[str, MaterializedView] = self._load_state()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_cycle(self) -> OptimizationResult:
        log.info("=== Starting optimization cycle ===")

        # 1. Collect history
        collected = WorklogCollector(
            self.adapter, self.store, self.cfg.lookback_days
        ).collect()
        log.info("Collected %d new query records", collected)

        # 2. Analyse
        workload = self.store.load(self.adapter.warehouse)
        log.info("Analysing %d unique queries", len(workload))
        analyzer = QueryAnalyzer(
            min_query_frequency=self.cfg.min_query_frequency,
        )
        candidates = analyzer.analyse(workload)
        log.info("Found %d candidate views", len(candidates))

        # 3. Refresh cost estimates
        candidates = self.cost_model.refresh_estimates(
            candidates, self.adapter.warehouse
        )

        # 4. Optimize
        result = self._optimize(candidates)
        log.info(
            "Selected %d views  estimated net benefit $%.2f/mo",
            len(result.selected),
            result.net_benefit_usd,
        )

        # 5+6. Apply diff
        self._apply_diff(result.selected)

        # 7. Measure savings from live views
        self._calibrate_live_views()

        log.info("=== Cycle complete ===")
        return result

    def status(self) -> dict:
        return {
            "live_views": len(self._live_views),
            "worklog": self.store.stats(),
            "calibration": self.calibration.summary(),
            "views": [
                {
                    "fqn": v.fqn,
                    "created_at": v.created_at.isoformat(),
                    "actual_savings_usd": round(v.actual_savings_usd, 4),
                    "predicted_savings_usd": round(
                        v.candidate.estimated_benefit_usd, 4
                    ),
                    "refresh_count": v.refresh_count,
                }
                for v in self._live_views.values()
            ],
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _optimize(self, candidates: list[CandidateView]) -> OptimizationResult:
        greedy = GreedySelector()
        greedy_result = greedy.select(candidates, self.cfg.budget_bytes)

        sa = AnnealingSelector(
            cooling_rate=self.cfg.sa_cooling_rate,
            max_iterations=self.cfg.sa_max_iterations,
        )
        return sa.select(
            candidates,
            self.cfg.budget_bytes,
            greedy_seed=greedy_result.selected,
        )

    def _apply_diff(self, selected: list[CandidateView]) -> None:
        selected_ids = {c.view_id for c in selected}
        live_ids = set(self._live_views.keys())

        to_create = [c for c in selected if c.view_id not in live_ids]
        to_drop_ids = live_ids - selected_ids

        # Drop views no longer in selection
        for vid in to_drop_ids:
            view = self._live_views[vid]
            # Keep if still profitable (calibration may make a view worth keeping)
            if view.actual_savings_usd > self.cfg.min_net_benefit_usd:
                log.info("Keeping %s despite deselection (actual savings $%.2f)", view.fqn, view.actual_savings_usd)
                continue
            log.info("Dropping view %s", view.fqn)
            try:
                self.adapter.drop_view(view)
            except Exception as exc:
                log.warning("Failed to drop %s: %s", view.fqn, exc)
            del self._live_views[vid]

        # Create new views
        for candidate in to_create:
            log.info("Creating view %s", candidate.name)
            try:
                mv = self.adapter.create_view(
                    candidate, self.cfg.target_dataset_or_schema
                )
                self._live_views[candidate.view_id] = mv
            except Exception as exc:
                log.warning("Failed to create %s: %s", candidate.name, exc)

        self._save_state()

    def _calibrate_live_views(self) -> None:
        since = datetime.now(timezone.utc) - timedelta(days=self.cfg.lookback_days)
        for view in list(self._live_views.values()):
            try:
                actual = self.adapter.measure_savings(view, since)
                self.cost_model.record_actual(view, actual)
                view.actual_savings_usd = actual
                log.info(
                    "%s  actual=$%.4f  predicted=$%.4f",
                    view.fqn,
                    actual,
                    view.candidate.estimated_benefit_usd,
                )
            except Exception as exc:
                log.warning("Could not measure savings for %s: %s", view.fqn, exc)
        self._save_state()

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> dict[str, MaterializedView]:
        if not self.cfg.state_path.exists():
            return {}
        try:
            raw = json.loads(self.cfg.state_path.read_text())
            result: dict[str, MaterializedView] = {}
            for vid, d in raw.items():
                cand_d = d.pop("candidate")
                candidate = CandidateView(**cand_d)
                d["candidate"] = candidate
                d["warehouse"] = d["warehouse"]
                d["created_at"] = datetime.fromisoformat(d["created_at"])
                if d.get("last_refreshed_at"):
                    d["last_refreshed_at"] = datetime.fromisoformat(
                        d["last_refreshed_at"]
                    )
                result[vid] = MaterializedView(**d)
            return result
        except Exception as exc:
            log.warning("Could not load state: %s", exc)
            return {}

    def _save_state(self) -> None:
        def _serialise(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Not serialisable: {type(obj)}")

        data = {}
        for vid, v in self._live_views.items():
            entry = {
                "candidate": {
                    "sql": v.candidate.sql,
                    "name": v.candidate.name,
                    "view_id": v.candidate.view_id,
                    "referenced_tables": v.candidate.referenced_tables,
                    "benefiting_query_ids": v.candidate.benefiting_query_ids,
                    "estimated_storage_bytes": v.candidate.estimated_storage_bytes,
                    "estimated_maintenance_cost_usd": v.candidate.estimated_maintenance_cost_usd,
                    "estimated_benefit_usd": v.candidate.estimated_benefit_usd,
                },
                "warehouse": v.warehouse.value,
                "created_at": v.created_at,
                "fqn": v.fqn,
                "last_refreshed_at": v.last_refreshed_at,
                "actual_savings_usd": v.actual_savings_usd,
                "refresh_count": v.refresh_count,
                "is_active": v.is_active,
            }
            data[vid] = entry
        self.cfg.state_path.write_text(
            json.dumps(data, default=_serialise, indent=2)
        )

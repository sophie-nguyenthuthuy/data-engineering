"""
Cost model for materialized view benefit estimation.

The model tracks calibration ratios (actual / predicted) per view and uses an
exponential moving average to adjust future predictions.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .models import CandidateView, MaterializedView, Warehouse


# ---------------------------------------------------------------------------
# Warehouse pricing constants (USD, as of 2025 — update in config)
# ---------------------------------------------------------------------------

@dataclass
class PricingConfig:
    # BigQuery on-demand: $5 / TB scanned
    bq_scan_price_per_byte: float = 5.0 / 1e12
    # BigQuery active storage: $0.02 / GB / month
    bq_storage_price_per_byte_per_month: float = 0.02 / 1e9
    # BigQuery streaming / DML maintenance estimate per byte written
    bq_maintenance_price_per_byte: float = 0.01 / 1e9

    # Snowflake: $2 / credit, XS warehouse = 1 credit/h
    sf_credit_price: float = 2.0
    sf_credits_per_second_xs: float = 1.0 / 3600
    # Snowflake storage: $23 / TB / month
    sf_storage_price_per_byte_per_month: float = 23.0 / 1e12


DEFAULT_PRICING = PricingConfig()


# ---------------------------------------------------------------------------
# Calibration store
# ---------------------------------------------------------------------------

@dataclass
class _ViewCalibration:
    view_id: str
    alpha: float = 0.1          # EMA smoothing factor
    ratio: float = 1.0          # current calibration ratio (actual/predicted)
    observations: int = 0


class CalibrationStore:
    """
    Persists calibration ratios to a JSON file so they survive restarts.
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self._path = path or Path(".mv_calibration.json")
        self._data: dict[str, _ViewCalibration] = {}
        self._load()

    def update(self, view_id: str, predicted_usd: float, actual_usd: float) -> None:
        if predicted_usd <= 0:
            return
        ratio = actual_usd / predicted_usd
        cal = self._data.setdefault(view_id, _ViewCalibration(view_id=view_id))
        # EMA update
        cal.ratio = cal.alpha * ratio + (1 - cal.alpha) * cal.ratio
        cal.observations += 1
        self._save()

    def get_ratio(self, view_id: str) -> float:
        return self._data.get(view_id, _ViewCalibration(view_id=view_id)).ratio

    def all_ratios(self) -> dict[str, float]:
        return {vid: c.ratio for vid, c in self._data.items()}

    def summary(self) -> dict[str, object]:
        ratios = list(self.all_ratios().values())
        if not ratios:
            return {"count": 0}
        avg = sum(ratios) / len(ratios)
        return {
            "count": len(ratios),
            "avg_calibration_ratio": round(avg, 4),
            "min": round(min(ratios), 4),
            "max": round(max(ratios), 4),
        }

    # ------------------------------------------------------------------

    def _load(self) -> None:
        if self._path.exists():
            try:
                raw = json.loads(self._path.read_text())
                self._data = {
                    k: _ViewCalibration(**v) for k, v in raw.items()
                }
            except Exception:
                pass

    def _save(self) -> None:
        self._path.write_text(
            json.dumps(
                {k: v.__dict__ for k, v in self._data.items()}, indent=2
            )
        )


# ---------------------------------------------------------------------------
# CostModel
# ---------------------------------------------------------------------------

class CostModel:
    """
    Estimates and refines benefit / cost for candidate views.

    All monetary values are USD per 30-day month unless stated otherwise.
    """

    def __init__(
        self,
        pricing: Optional[PricingConfig] = None,
        calibration_store: Optional[CalibrationStore] = None,
        scan_reduction_factor: float = 0.70,
    ) -> None:
        self.pricing = pricing or DEFAULT_PRICING
        self.calibration = calibration_store or CalibrationStore()
        # Fraction of bytes scanned that a view eliminates
        self.scan_reduction_factor = scan_reduction_factor

    # ------------------------------------------------------------------
    # Estimation
    # ------------------------------------------------------------------

    def estimate_benefit(
        self,
        candidate: CandidateView,
        warehouse: Warehouse,
        queries_per_month: Optional[int] = None,
    ) -> float:
        """Monthly benefit in USD (raw, before calibration)."""
        q_count = queries_per_month or len(candidate.benefiting_query_ids)

        if warehouse == Warehouse.BIGQUERY:
            # Rough bytes saved per query hit: storage size × reduction factor
            bytes_saved_per_query = (
                candidate.estimated_storage_bytes * self.scan_reduction_factor
            )
            monthly_benefit = (
                q_count
                * bytes_saved_per_query
                * self.pricing.bq_scan_price_per_byte
            )
        else:  # Snowflake
            # Estimate seconds of compute saved: 1 s per 100 MB of storage
            seconds_per_query = (
                candidate.estimated_storage_bytes / (100 * 1024 * 1024)
            )
            compute_saved = (
                q_count
                * seconds_per_query
                * self.pricing.sf_credits_per_second_xs
                * self.pricing.sf_credit_price
            )
            monthly_benefit = compute_saved

        # Apply calibration ratio
        ratio = self.calibration.get_ratio(candidate.view_id)
        return monthly_benefit * ratio

    def estimate_storage_cost(
        self, candidate: CandidateView, warehouse: Warehouse
    ) -> float:
        """Monthly storage cost in USD."""
        if warehouse == Warehouse.BIGQUERY:
            return (
                candidate.estimated_storage_bytes
                * self.pricing.bq_storage_price_per_byte_per_month
            )
        return (
            candidate.estimated_storage_bytes
            * self.pricing.sf_storage_price_per_byte_per_month
        )

    def estimate_maintenance_cost(
        self,
        candidate: CandidateView,
        warehouse: Warehouse,
        refreshes_per_month: int = 30,
    ) -> float:
        """Monthly maintenance cost in USD (refresh DML)."""
        if warehouse == Warehouse.BIGQUERY:
            return (
                candidate.estimated_storage_bytes
                * self.pricing.bq_maintenance_price_per_byte
                * refreshes_per_month
            )
        # Snowflake: estimate 30 s per refresh on an XS warehouse
        return (
            30
            * self.pricing.sf_credits_per_second_xs
            * self.pricing.sf_credit_price
            * refreshes_per_month
        )

    # ------------------------------------------------------------------
    # Calibration feedback
    # ------------------------------------------------------------------

    def record_actual(
        self,
        view: MaterializedView,
        actual_savings_usd: float,
    ) -> None:
        """Call after each measurement period to update the calibration."""
        self.calibration.update(
            view.candidate.view_id,
            predicted_usd=view.candidate.estimated_benefit_usd,
            actual_usd=actual_savings_usd,
        )

    # ------------------------------------------------------------------
    # Refresh candidate estimates from current calibration
    # ------------------------------------------------------------------

    def refresh_estimates(
        self,
        candidates: list[CandidateView],
        warehouse: Warehouse,
    ) -> list[CandidateView]:
        """Return candidates with updated estimated_benefit_usd."""
        for c in candidates:
            c.estimated_benefit_usd = self.estimate_benefit(c, warehouse)
            c.estimated_maintenance_cost_usd = self.estimate_maintenance_cost(
                c, warehouse
            )
            c.estimated_storage_bytes = max(
                c.estimated_storage_bytes,
                int(self.estimate_storage_cost(c, warehouse) / (0.02 / 1e9)),
            )
        return candidates

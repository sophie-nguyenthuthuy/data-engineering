"""Tests for cost model and calibration."""

import pytest
from pathlib import Path
import tempfile

from mv_selector.cost_model import CalibrationStore, CostModel, PricingConfig
from mv_selector.models import CandidateView, MaterializedView, Warehouse
from datetime import datetime, timezone


def _candidate(view_id: str = "abc", storage_bytes: int = 10 * 1024**3) -> CandidateView:
    return CandidateView(
        sql="SELECT 1",
        name="mv_test",
        view_id=view_id,
        referenced_tables=["orders"],
        benefiting_query_ids=["q1", "q2"],
        estimated_storage_bytes=storage_bytes,
        estimated_maintenance_cost_usd=0.0,
        estimated_benefit_usd=0.0,
    )


class TestCalibrationStore:
    def test_default_ratio_is_one(self, tmp_path):
        store = CalibrationStore(tmp_path / "cal.json")
        assert store.get_ratio("nonexistent") == 1.0

    def test_update_moves_toward_actual(self, tmp_path):
        store = CalibrationStore(tmp_path / "cal.json")
        for _ in range(50):
            store.update("v1", predicted_usd=10.0, actual_usd=20.0)
        ratio = store.get_ratio("v1")
        assert ratio > 1.0  # actual > predicted → ratio > 1

    def test_persists_across_instances(self, tmp_path):
        p = tmp_path / "cal.json"
        s1 = CalibrationStore(p)
        s1.update("v1", 10.0, 5.0)
        r1 = s1.get_ratio("v1")
        s2 = CalibrationStore(p)
        assert s2.get_ratio("v1") == pytest.approx(r1)


class TestCostModel:
    def test_benefit_bigquery(self):
        model = CostModel()
        c = _candidate(storage_bytes=100 * 1024**3)
        benefit = model.estimate_benefit(c, Warehouse.BIGQUERY, queries_per_month=1000)
        assert benefit > 0

    def test_benefit_snowflake(self):
        model = CostModel()
        c = _candidate(storage_bytes=100 * 1024**3)
        benefit = model.estimate_benefit(c, Warehouse.SNOWFLAKE, queries_per_month=1000)
        assert benefit > 0

    def test_calibration_scales_benefit(self, tmp_path):
        store = CalibrationStore(tmp_path / "cal.json")
        # Pretend actual savings are 2× predicted
        for _ in range(100):
            store.update("abc", predicted_usd=10.0, actual_usd=20.0)

        model = CostModel(calibration_store=store)
        c = _candidate("abc")
        benefit_calibrated = model.estimate_benefit(c, Warehouse.BIGQUERY, queries_per_month=100)
        model2 = CostModel()  # no calibration
        benefit_raw = model2.estimate_benefit(c, Warehouse.BIGQUERY, queries_per_month=100)
        assert benefit_calibrated > benefit_raw

    def test_storage_cost_positive(self):
        model = CostModel()
        c = _candidate(storage_bytes=1024**3)
        assert model.estimate_storage_cost(c, Warehouse.BIGQUERY) > 0
        assert model.estimate_storage_cost(c, Warehouse.SNOWFLAKE) > 0

    def test_maintenance_cost_positive(self):
        model = CostModel()
        c = _candidate(storage_bytes=1024**3)
        assert model.estimate_maintenance_cost(c, Warehouse.BIGQUERY) > 0
        assert model.estimate_maintenance_cost(c, Warehouse.SNOWFLAKE) > 0

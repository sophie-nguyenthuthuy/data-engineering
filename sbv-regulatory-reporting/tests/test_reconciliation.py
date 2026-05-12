"""Tests for reconciliation checks."""
import pandas as pd
import pytest

from sbv_reporting.reconciliation.checks import ReconciliationEngine


@pytest.fixture
def base_df():
    return pd.DataFrame({
        "transaction_id": [f"TXN{i:04d}" for i in range(20)],
        "transaction_date": pd.to_datetime(["2025-03-15"] * 20),
        "currency": ["VND"] * 15 + ["USD"] * 5,
        "amount": [10_000_000.0] * 15 + [1_000.0] * 5,
        "vnd_equivalent": [10_000_000.0] * 15 + [23_500_000.0] * 5,
        "branch_code": ["HN001"] * 10 + ["HCM001"] * 10,
        "status": ["SUCCESS"] * 18 + ["FAILED", "PENDING"],
    })


class TestReconciliation:
    def test_row_count_pass(self, base_df):
        eng = ReconciliationEngine()
        r = eng.check_row_count(base_df, base_df.copy())
        assert r.passed
        assert r.delta == 0

    def test_row_count_fail(self, base_df):
        eng = ReconciliationEngine()
        r = eng.check_row_count(base_df, base_df.iloc[:15].copy())
        assert not r.passed
        assert r.delta == 5

    def test_vnd_total_pass(self, base_df):
        eng = ReconciliationEngine()
        r = eng.check_vnd_total(base_df, base_df.copy())
        assert r.passed

    def test_transaction_ids_pass(self, base_df):
        eng = ReconciliationEngine()
        r = eng.check_transaction_ids(base_df, base_df.copy())
        assert r.passed
        assert r.delta == 0

    def test_transaction_ids_fail_missing(self, base_df):
        eng = ReconciliationEngine()
        partial = base_df.iloc[:15].copy()
        r = eng.check_transaction_ids(base_df, partial)
        assert not r.passed
        assert r.delta == 5

    def test_large_value_coverage_pass(self, base_df):
        eng = ReconciliationEngine()
        # No transactions exceed 300M VND in base_df
        lv_report = pd.DataFrame({"transaction_id": []})
        r = eng.check_large_value_coverage(base_df, lv_report)
        assert r.passed

    def test_large_value_coverage_fail(self, base_df):
        eng = ReconciliationEngine()
        large_df = base_df.copy()
        large_df.loc[0, "vnd_equivalent"] = 500_000_000
        lv_report = pd.DataFrame({"transaction_id": []})
        r = eng.check_large_value_coverage(large_df, lv_report)
        assert not r.passed
        assert r.delta >= 1

    def test_run_all_returns_list(self, base_df):
        eng = ReconciliationEngine()
        results = eng.run_all(base_df, base_df.copy())
        assert len(results) >= 4
        for r in results:
            assert hasattr(r, "passed")
            assert hasattr(r, "check_name")

"""Integration test — full pipeline run against sample data."""
from pathlib import Path

import pytest

from sbv_reporting.pipeline import Pipeline


SAMPLE_CSV = Path(__file__).parents[1] / "data" / "sample" / "transactions.csv"


@pytest.mark.skipif(not SAMPLE_CSV.exists(), reason="sample data not generated")
class TestPipelineIntegration:
    def test_run_succeeds(self, tmp_path):
        pipeline = Pipeline()
        result = pipeline.run(
            input_path=SAMPLE_CSV,
            report_date="31/03/2025",
            operator="TEST_OP",
            write_excel=False,
            write_csv=True,
        )
        assert result.success
        assert len(result.errors) == 0

    def test_all_reports_generated(self, tmp_path):
        pipeline = Pipeline()
        result = pipeline.run(SAMPLE_CSV, write_excel=False, write_csv=False)
        assert "BCGD" in result.reports
        assert "B01-TCTD" in result.reports
        assert "BCGDLN" in result.reports
        assert "BCGDNS" in result.reports

    def test_bcgd_only_success(self, tmp_path):
        import pandas as pd
        pipeline = Pipeline()
        result = pipeline.run(SAMPLE_CSV, write_excel=False, write_csv=False)
        raw = pd.read_csv(SAMPLE_CSV)
        expected = (raw["status"] == "SUCCESS").sum()
        assert len(result.reports["BCGD"]) == expected

    def test_reconciliation_row_count_passes(self, tmp_path):
        pipeline = Pipeline()
        result = pipeline.run(SAMPLE_CSV, write_excel=False, write_csv=False)
        row_check = next(r for r in result.reconciliation if r.check_name == "row_count")
        assert row_check.passed

    def test_audit_chain_valid(self, tmp_path):
        from sbv_reporting.audit.trail import AuditTrail
        pipeline = Pipeline()
        result = pipeline.run(SAMPLE_CSV, write_excel=False, write_csv=False)
        run_id = result.run_id
        trail = AuditTrail(run_id)
        ok, errors = trail.verify()
        assert ok, f"Audit chain broken: {errors}"

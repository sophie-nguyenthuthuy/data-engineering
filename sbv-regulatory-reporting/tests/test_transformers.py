"""Tests for SBV report transformers."""
import pandas as pd
import pytest

from sbv_reporting.transformers.sbv_formats import SBVTransformer


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "transaction_id": [f"TXN{i:06d}" for i in range(10)],
        "transaction_date": pd.to_datetime(["2025-03-15"] * 10),
        "transaction_time": ["10:00:00"] * 10,
        "transaction_type": ["TRANSFER_IN"] * 5 + ["TRANSFER_OUT"] * 5,
        "debit_account": ["1000000001"] * 10,
        "credit_account": ["2000000001"] * 10,
        "account_type": ["CURRENT"] * 10,
        "currency": ["VND"] * 8 + ["USD", "EUR"],
        "amount": [50_000_000, 100_000_000, 300_000_000, 400_000_000,
                   500_000_000, 10_000, 5_000_000, 200_000_000,
                   5_000, 2_000],
        "vnd_equivalent": [50_000_000, 100_000_000, 300_000_000, 400_000_000,
                           500_000_000, 10_000, 5_000_000, 200_000_000,
                           117_500_000, 50_000_000],
        "branch_code": ["HN001"] * 10,
        "counterparty_bank": ["BIDV"] * 10,
        "counterparty_account": ["9000000001"] * 10,
        "counterparty_name": ["Test Customer"] * 10,
        "purpose_code": ["01"] * 10,
        "status": ["SUCCESS"] * 8 + ["FAILED", "PENDING"],
        "channel": ["COUNTER"] * 10,
        "operator_id": ["OP0001"] * 10,
        "reference_number": [f"REF{i}" for i in range(10)],
        "narrative": ["Test"] * 10,
    })


class TestTransactionReport:
    def test_only_success_rows(self, sample_df):
        t = SBVTransformer()
        report = t.build_transaction_report(sample_df)
        assert len(report) == 8
        assert "MA_GIAO_DICH" in report.columns

    def test_stt_sequential(self, sample_df):
        t = SBVTransformer()
        report = t.build_transaction_report(sample_df)
        assert list(report["STT"]) == list(range(1, len(report) + 1))

    def test_institution_code_present(self, sample_df):
        t = SBVTransformer()
        report = t.build_transaction_report(sample_df)
        assert report["MA_DINH_CHE"].nunique() == 1


class TestBalanceReport:
    def test_aggregation(self, sample_df):
        t = SBVTransformer()
        report = t.build_balance_report(sample_df)
        assert "TONG_SO_TIEN" in report.columns
        assert report["SO_LUONG_GIAO_DICH"].sum() == 8  # only SUCCESS

    def test_report_date_set(self, sample_df):
        t = SBVTransformer()
        report = t.build_balance_report(sample_df, report_date="31/03/2025")
        assert (report["NGAY_BAO_CAO"] == "31/03/2025").all()


class TestLargeValueReport:
    def test_threshold_filter(self, sample_df):
        t = SBVTransformer()
        report = t.build_large_value_report(sample_df)
        assert (report["SO_TIEN_VND"] >= 300_000_000).all()

    def test_excludes_failed(self, sample_df):
        t = SBVTransformer()
        report = t.build_large_value_report(sample_df)
        txn_ids = set(report["MA_GIAO_DICH"])
        # TXN000008 is FAILED — must not appear
        assert "TXN000008" not in txn_ids


class TestSTRReport:
    def test_r1_rule(self, sample_df):
        t = SBVTransformer()
        report = t.build_str_report(sample_df)
        r1 = report[report["QUITAC_NGHI_NGO"] == "R1"]
        assert len(r1) > 0

    def test_empty_when_clean(self):
        t = SBVTransformer()
        clean = pd.DataFrame({
            "transaction_id": ["TXN000001"],
            "transaction_date": pd.to_datetime(["2025-01-01"]),
            "transaction_time": ["09:00:00"],
            "transaction_type": ["TRANSFER_IN"],
            "debit_account": ["1000000001"],
            "credit_account": ["2000000001"],
            "currency": ["VND"],
            "amount": [1_000_000.0],
            "vnd_equivalent": [1_000_000.0],
            "branch_code": ["HN001"],
            "counterparty_bank": ["BIDV"],
            "counterparty_account": ["9000000001"],
            "counterparty_name": ["Customer"],
            "purpose_code": ["01"],
            "status": ["SUCCESS"],
            "channel": ["COUNTER"],
            "operator_id": ["OP0001"],
            "reference_number": ["REF001"],
            "narrative": ["Clean txn"],
        })
        report = t.build_str_report(clean)
        assert report.empty

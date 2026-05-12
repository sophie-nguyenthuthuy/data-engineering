"""Transform canonical DataFrame into SBV-specific report DataFrames.

Produces four report types:
  - BCGD   : Daily Transaction Report
  - B01-TCTD: Balance / Volume Summary Report (aggregated)
  - BCGDLN : Large-Value Transaction Report (≥ 300M VND)
  - BCGDNS : Suspicious Transaction Report (STR)
"""
from __future__ import annotations

import pandas as pd

from sbv_reporting.utils.config import get_config


class SBVTransformer:
    def __init__(self):
        self.cfg = get_config()
        self.inst = self.cfg["reporting"]
        self.thresholds = self.cfg["thresholds"]
        self.fmt = self.cfg["sbv_formats"]

    def _fmt_date(self, s: pd.Series) -> pd.Series:
        return s.dt.strftime(self.fmt["date_format"])

    def _fmt_datetime(self, s: pd.Series) -> pd.Series:
        return s.dt.strftime(self.fmt["datetime_format"])

    # ------------------------------------------------------------------ BCGD
    def build_transaction_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """BCGD — Daily Transaction Report."""
        ok = df[df["status"] == "SUCCESS"].copy()
        report = pd.DataFrame({
            "STT": range(1, len(ok) + 1),
            "MA_GIAO_DICH": ok["transaction_id"].values,
            "NGAY_GIAO_DICH": self._fmt_date(ok["transaction_date"]).values,
            "GIO_GIAO_DICH": ok["transaction_time"].values,
            "LOAI_GIAO_DICH": ok["transaction_type"].values,
            "SO_TK_NO": ok["debit_account"].values,
            "SO_TK_CO": ok["credit_account"].values,
            "LOAI_TIEN": ok["currency"].values,
            "SO_TIEN": ok["amount"].values,
            "SO_TIEN_VND": ok["vnd_equivalent"].values,
            "CHI_NHANH": ok["branch_code"].values,
            "NGAN_HANG_DOI_UNG": ok["counterparty_bank"].fillna("").values,
            "TK_DOI_UNG": ok["counterparty_account"].fillna("").values,
            "TEN_DOI_UNG": ok["counterparty_name"].fillna("").values,
            "MA_MUC_DICH": ok["purpose_code"].fillna("").values,
            "KENH_GIAO_DICH": ok["channel"].fillna("").values,
            "MA_NHAN_VIEN": ok["operator_id"].fillna("").values,
            "SO_THAM_CHIEU": ok["reference_number"].fillna("").values,
            "DIEN_GIAI": ok["narrative"].fillna("").values,
            "MA_DINH_CHE": self.inst["institution_code"],
        })
        return report

    # ------------------------------------------------------------------ B01-TCTD
    def build_balance_report(self, df: pd.DataFrame, report_date: str | None = None) -> pd.DataFrame:
        """B01-TCTD — Balance and Volume Summary by branch and currency."""
        ok = df[df["status"] == "SUCCESS"].copy()

        grp = ok.groupby(["branch_code", "currency", "transaction_type"]).agg(
            so_luong=("transaction_id", "count"),
            tong_so_tien=("amount", "sum"),
            tong_quy_doi_vnd=("vnd_equivalent", "sum"),
        ).reset_index()

        grp.insert(0, "STT", range(1, len(grp) + 1))
        grp.insert(1, "MA_DINH_CHE", self.inst["institution_code"])
        grp.insert(2, "TEN_DINH_CHE", self.inst["institution_name"])
        grp.insert(3, "NGAY_BAO_CAO", report_date or pd.Timestamp.today().strftime(self.fmt["date_format"]))

        grp.rename(columns={
            "branch_code": "MA_CHI_NHANH",
            "currency": "LOAI_TIEN",
            "transaction_type": "LOAI_GIAO_DICH",
            "so_luong": "SO_LUONG_GIAO_DICH",
            "tong_so_tien": "TONG_SO_TIEN",
            "tong_quy_doi_vnd": "TONG_QUY_DOI_VND",
        }, inplace=True)

        return grp

    # ------------------------------------------------------------------ BCGDLN
    def build_large_value_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """BCGDLN — Transactions at or above the large-value reporting threshold."""
        threshold = self.thresholds["large_value_vnd"]
        lv = df[
            (df["status"] == "SUCCESS") &
            (df["vnd_equivalent"] >= threshold)
        ].copy()

        report = pd.DataFrame({
            "STT": range(1, len(lv) + 1),
            "MA_DINH_CHE": self.inst["institution_code"],
            "MA_GIAO_DICH": lv["transaction_id"].values,
            "NGAY_GIAO_DICH": self._fmt_date(lv["transaction_date"]).values,
            "GIO_GIAO_DICH": lv["transaction_time"].values,
            "LOAI_GIAO_DICH": lv["transaction_type"].values,
            "SO_TK_NO": lv["debit_account"].values,
            "SO_TK_CO": lv["credit_account"].values,
            "LOAI_TIEN": lv["currency"].values,
            "SO_TIEN": lv["amount"].values,
            "SO_TIEN_VND": lv["vnd_equivalent"].values,
            "TEN_DOI_UNG": lv["counterparty_name"].fillna("").values,
            "NGAN_HANG_DOI_UNG": lv["counterparty_bank"].fillna("").values,
            "MA_MUC_DICH": lv["purpose_code"].fillna("").values,
            "CHI_NHANH": lv["branch_code"].values,
            "NGUONG_BAO_CAO_VND": threshold,
            "CO_VUOT_NGUONG": (lv["vnd_equivalent"] >= threshold).astype(int).values,
        })
        return report

    # ------------------------------------------------------------------ BCGDNS
    def build_str_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """BCGDNS — Suspicious Transaction Report (rule-based flagging).

        Flags:
          R1 — single transaction >= large_value threshold
          R2 — account exceeds suspicious_frequency transactions/day
          R3 — round-amount transactions (multiples of 100M VND)
          R4 — structuring: amounts just below threshold (> 90% of threshold)
        """
        threshold = self.thresholds["large_value_vnd"]
        freq_limit = self.thresholds["suspicious_frequency"]
        ok = df[df["status"] == "SUCCESS"].copy()

        # R1
        r1 = ok[ok["vnd_equivalent"] >= threshold].copy()
        r1["QUITAC"] = "R1"
        r1["MO_TA"] = "Giao dịch giá trị lớn đơn lẻ vượt ngưỡng báo cáo"

        # R2 — high-frequency per account per day
        freq = (
            ok.groupby(["debit_account", "transaction_date"])
            .size()
            .reset_index(name="count")
        )
        hf_accounts = set(
            freq.loc[freq["count"] >= freq_limit, "debit_account"]
        )
        r2 = ok[ok["debit_account"].isin(hf_accounts)].copy()
        r2["QUITAC"] = "R2"
        r2["MO_TA"] = f"Tần suất giao dịch cao (≥{freq_limit} GD/ngày/TK)"

        # R3 — round amounts >= 100M VND
        round_threshold = 100_000_000
        r3 = ok[
            (ok["currency"] == "VND") &
            (ok["amount"] >= round_threshold) &
            (ok["amount"] % round_threshold == 0)
        ].copy()
        r3["QUITAC"] = "R3"
        r3["MO_TA"] = "Giao dịch số tiền tròn giá trị lớn"

        # R4 — structuring (just below threshold)
        lower = threshold * 0.90
        r4 = ok[
            (ok["vnd_equivalent"] >= lower) &
            (ok["vnd_equivalent"] < threshold)
        ].copy()
        r4["QUITAC"] = "R4"
        r4["MO_TA"] = "Nghi vấn phân mảnh giao dịch (cấu trúc)"

        flagged = (
            pd.concat([r1, r2, r3, r4], ignore_index=True)
            .drop_duplicates(subset=["transaction_id", "QUITAC"])
            .sort_values(["transaction_date", "transaction_id"])
        )

        if flagged.empty:
            return pd.DataFrame()

        report = pd.DataFrame({
            "STT": range(1, len(flagged) + 1),
            "MA_DINH_CHE": self.inst["institution_code"],
            "MA_GIAO_DICH": flagged["transaction_id"].values,
            "NGAY_GIAO_DICH": self._fmt_date(flagged["transaction_date"]).values,
            "GIO_GIAO_DICH": flagged["transaction_time"].values,
            "LOAI_GIAO_DICH": flagged["transaction_type"].values,
            "SO_TK_NO": flagged["debit_account"].values,
            "SO_TK_CO": flagged["credit_account"].values,
            "LOAI_TIEN": flagged["currency"].values,
            "SO_TIEN": flagged["amount"].values,
            "SO_TIEN_VND": flagged["vnd_equivalent"].values,
            "TEN_DOI_UNG": flagged["counterparty_name"].fillna("").values,
            "QUITAC_NGHI_NGO": flagged["QUITAC"].values,
            "MO_TA_NGHI_NGO": flagged["MO_TA"].values,
            "CHI_NHANH": flagged["branch_code"].values,
        })
        return report

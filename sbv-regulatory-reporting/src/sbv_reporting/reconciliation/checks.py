"""Reconciliation checks between raw data, transformed data, and reports.

Each check returns a ReconciliationResult with:
  - check_name, passed, delta, details
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from sbv_reporting.utils.config import get_config


@dataclass
class ReconciliationResult:
    check_name: str
    passed: bool
    delta: float
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "delta": self.delta,
            "details": self.details,
        }


class ReconciliationEngine:
    def __init__(self):
        self.tolerance = get_config()["thresholds"]["reconciliation_tolerance"]

    # ------------------------------------------------------------------
    def check_row_count(self, raw: pd.DataFrame, transformed: pd.DataFrame) -> ReconciliationResult:
        raw_n = len(raw)
        tfm_n = len(transformed)
        delta = abs(raw_n - tfm_n)
        return ReconciliationResult(
            check_name="row_count",
            passed=(delta == 0),
            delta=float(delta),
            details={"raw_rows": raw_n, "transformed_rows": tfm_n},
        )

    def check_vnd_total(self, raw: pd.DataFrame, transformed: pd.DataFrame) -> ReconciliationResult:
        """Total VND equivalent must match between raw input and transformed output."""
        raw_total = raw.loc[raw["status"] == "SUCCESS", "vnd_equivalent"].sum()
        tfm_total = transformed.loc[transformed["status"] == "SUCCESS", "vnd_equivalent"].sum()
        delta = abs(raw_total - tfm_total)
        return ReconciliationResult(
            check_name="vnd_total",
            passed=(delta <= self.tolerance),
            delta=float(delta),
            details={"raw_total_vnd": float(raw_total), "transformed_total_vnd": float(tfm_total)},
        )

    def check_transaction_ids(self, raw: pd.DataFrame, transformed: pd.DataFrame) -> ReconciliationResult:
        raw_ids = set(raw["transaction_id"])
        tfm_ids = set(transformed["transaction_id"])
        missing = raw_ids - tfm_ids
        extra = tfm_ids - raw_ids
        passed = len(missing) == 0 and len(extra) == 0
        return ReconciliationResult(
            check_name="transaction_ids",
            passed=passed,
            delta=float(len(missing) + len(extra)),
            details={
                "missing_from_transformed": sorted(missing)[:20],
                "extra_in_transformed": sorted(extra)[:20],
            },
        )

    def check_currency_totals(self, raw: pd.DataFrame, transformed: pd.DataFrame) -> ReconciliationResult:
        """Per-currency sums must match (successful transactions only)."""
        raw_ok = raw[raw["status"] == "SUCCESS"]
        tfm_ok = transformed[transformed["status"] == "SUCCESS"]
        raw_sums = raw_ok.groupby("currency")["amount"].sum()
        tfm_sums = tfm_ok.groupby("currency")["amount"].sum()

        mismatches: dict[str, float] = {}
        for ccy in raw_sums.index:
            raw_v = raw_sums.get(ccy, 0.0)
            tfm_v = tfm_sums.get(ccy, 0.0)
            if abs(raw_v - tfm_v) > self.tolerance:
                mismatches[ccy] = float(abs(raw_v - tfm_v))

        return ReconciliationResult(
            check_name="currency_totals",
            passed=(len(mismatches) == 0),
            delta=sum(mismatches.values()),
            details={"mismatches": mismatches},
        )

    def check_branch_counts(self, raw: pd.DataFrame, transformed: pd.DataFrame) -> ReconciliationResult:
        raw_bc = raw.groupby("branch_code").size().to_dict()
        tfm_bc = transformed.groupby("branch_code").size().to_dict()
        mismatches = {
            b: {"raw": raw_bc.get(b, 0), "transformed": tfm_bc.get(b, 0)}
            for b in set(raw_bc) | set(tfm_bc)
            if raw_bc.get(b, 0) != tfm_bc.get(b, 0)
        }
        return ReconciliationResult(
            check_name="branch_counts",
            passed=(len(mismatches) == 0),
            delta=float(len(mismatches)),
            details={"mismatches": mismatches},
        )

    def check_large_value_coverage(
        self,
        transformed: pd.DataFrame,
        large_value_report: pd.DataFrame,
    ) -> ReconciliationResult:
        """Every SUCCESS transaction >= threshold must appear in LV report."""
        cfg = get_config()
        threshold = cfg["thresholds"]["large_value_vnd"]
        eligible = transformed[
            (transformed["status"] == "SUCCESS") &
            (transformed["vnd_equivalent"] >= threshold)
        ]["transaction_id"]
        # BCGDLN report uses SBV column name MA_GIAO_DICH
        id_col = "MA_GIAO_DICH" if "MA_GIAO_DICH" in large_value_report.columns else "transaction_id"
        reported = set(large_value_report[id_col]) if not large_value_report.empty else set()
        missing = set(eligible) - reported
        return ReconciliationResult(
            check_name="large_value_coverage",
            passed=(len(missing) == 0),
            delta=float(len(missing)),
            details={"eligible": len(eligible), "reported": len(reported), "missing": sorted(missing)[:20]},
        )

    # ------------------------------------------------------------------
    def run_all(
        self,
        raw: pd.DataFrame,
        transformed: pd.DataFrame,
        large_value_report: pd.DataFrame | None = None,
    ) -> list[ReconciliationResult]:
        results = [
            self.check_row_count(raw, transformed),
            self.check_vnd_total(raw, transformed),
            self.check_transaction_ids(raw, transformed),
            self.check_currency_totals(raw, transformed),
            self.check_branch_counts(raw, transformed),
        ]
        if large_value_report is not None:
            results.append(self.check_large_value_coverage(transformed, large_value_report))
        return results

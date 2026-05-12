"""Main pipeline orchestrator.

Usage:
    from sbv_reporting.pipeline import Pipeline
    result = Pipeline().run("data/sample/transactions.csv")
"""
from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from sbv_reporting.audit.trail import AuditTrail
from sbv_reporting.reconciliation.checks import ReconciliationEngine
from sbv_reporting.reports.writer import ReportWriter
from sbv_reporting.transformers.base import RawTransactionLoader
from sbv_reporting.transformers.sbv_formats import SBVTransformer
from sbv_reporting.utils.config import get_config
from sbv_reporting.utils.validators import ValidationError


class PipelineResult:
    def __init__(
        self,
        run_id: str,
        success: bool,
        reports: dict[str, pd.DataFrame],
        output_files: dict[str, Any],
        reconciliation: list,
        audit_summary: dict,
        warnings: list[str],
        errors: list[str],
    ):
        self.run_id = run_id
        self.success = success
        self.reports = reports
        self.output_files = output_files
        self.reconciliation = reconciliation
        self.audit_summary = audit_summary
        self.warnings = warnings
        self.errors = errors

    def print_summary(self) -> None:
        status = "SUCCESS" if self.success else "FAILED"
        print(f"\n{'='*60}")
        print(f"  SBV PIPELINE RUN: {self.run_id}")
        print(f"  Status : {status}")
        print(f"{'='*60}")

        if self.warnings:
            print(f"\n[WARNINGS] ({len(self.warnings)})")
            for w in self.warnings:
                print(f"  ⚠  {w}")

        if self.errors:
            print(f"\n[ERRORS] ({len(self.errors)})")
            for e in self.errors:
                print(f"  ✗  {e}")

        print(f"\n[REPORTS GENERATED]")
        for code, df in self.reports.items():
            print(f"  {code:12s}  {len(df):>6,} rows")

        print(f"\n[RECONCILIATION]")
        all_pass = True
        for r in self.reconciliation:
            mark = "✓" if r.passed else "✗"
            print(f"  {mark}  {r.check_name:30s}  delta={r.delta:.2f}")
            if not r.passed:
                all_pass = False
        if all_pass:
            print("  All reconciliation checks PASSED")

        print(f"\n[OUTPUT FILES]")
        if "excel" in self.output_files:
            print(f"  Excel : {self.output_files['excel']}")
        if "csv" in self.output_files:
            for code, p in self.output_files["csv"].items():
                print(f"  CSV   : {p}")

        print(f"\n[AUDIT TRAIL]")
        print(f"  Log   : {self.audit_summary.get('log_path')}")
        print(f"  Entries: {self.audit_summary.get('total_entries')}")
        print(f"  Chain : {self.audit_summary.get('chain_hash', '')[:16]}...")
        print(f"{'='*60}\n")


class Pipeline:
    def __init__(self, config_path: str | Path | None = None):
        self.cfg = get_config()
        self.loader = RawTransactionLoader()
        self.transformer = SBVTransformer()
        self.reconciler = ReconciliationEngine()
        self.writer = ReportWriter()

    # ------------------------------------------------------------------
    def run(
        self,
        input_path: str | Path,
        report_date: str | None = None,
        run_id: str | None = None,
        operator: str = "SYSTEM",
        write_excel: bool = True,
        write_csv: bool = True,
    ) -> PipelineResult:
        run_id = run_id or datetime.now().strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:6].upper()
        audit = AuditTrail(run_id)
        warnings: list[str] = []
        errors: list[str] = []

        # 1. START
        audit.log("PIPELINE_START", {
            "input": str(input_path),
            "report_date": report_date,
            "operator": operator,
        }, operator=operator)

        # 2. LOAD & VALIDATE
        try:
            raw_df, load_warnings = self.loader.load(input_path)
            warnings.extend(load_warnings)
            audit.log("DATA_LOADED", {
                "rows": len(raw_df),
                "columns": list(raw_df.columns),
                "warnings": load_warnings,
            }, operator=operator)
        except ValidationError as exc:
            errors.append(str(exc))
            audit.log("VALIDATION_FAILED", {"error": str(exc)}, operator=operator, level="ERROR")
            return PipelineResult(run_id, False, {}, {}, [], audit.summary(), warnings, errors)

        # 3. TRANSFORM
        reports: dict[str, pd.DataFrame] = {}
        try:
            reports["BCGD"] = self.transformer.build_transaction_report(raw_df)
            reports["B01-TCTD"] = self.transformer.build_balance_report(raw_df, report_date)
            reports["BCGDLN"] = self.transformer.build_large_value_report(raw_df)
            reports["BCGDNS"] = self.transformer.build_str_report(raw_df)

            audit.log("REPORTS_BUILT", {
                code: len(df) for code, df in reports.items()
            }, operator=operator)
        except Exception as exc:
            errors.append(f"Transform error: {exc}")
            audit.log("TRANSFORM_FAILED", {"error": str(exc)}, operator=operator, level="ERROR")
            return PipelineResult(run_id, False, reports, {}, [], audit.summary(), warnings, errors)

        # 4. RECONCILIATION
        recon_results = self.reconciler.run_all(
            raw_df,
            raw_df,  # transformed == raw (column-compatible); real-world: use enriched df
            large_value_report=reports["BCGDLN"],
        )
        failed_checks = [r for r in recon_results if not r.passed]
        audit.log("RECONCILIATION_COMPLETE", {
            "total": len(recon_results),
            "passed": len(recon_results) - len(failed_checks),
            "failed": len(failed_checks),
            "results": [r.to_dict() for r in recon_results],
        }, operator=operator, level="WARNING" if failed_checks else "INFO")

        if failed_checks:
            for r in failed_checks:
                warnings.append(f"Reconciliation FAILED: {r.check_name} (delta={r.delta})")

        # 5. WRITE OUTPUTS
        output_files: dict[str, Any] = {}
        if write_excel:
            try:
                xlsx_path = self.writer.write_excel(reports, run_id, report_date)
                output_files["excel"] = xlsx_path
                audit.log("EXCEL_WRITTEN", {"path": str(xlsx_path)}, operator=operator)
            except Exception as exc:
                warnings.append(f"Excel write failed: {exc}")
                audit.log("EXCEL_WRITE_FAILED", {"error": str(exc)}, operator=operator, level="WARNING")

        if write_csv:
            csv_paths = self.writer.write_csv(reports, run_id, report_date)
            output_files["csv"] = csv_paths
            audit.log("CSV_WRITTEN", {k: str(v) for k, v in csv_paths.items()}, operator=operator)

        # 6. AUDIT CHAIN VERIFICATION
        chain_ok, chain_errors = audit.verify()
        if not chain_ok:
            errors.extend(chain_errors)
            audit.log("AUDIT_CHAIN_BROKEN", {"errors": chain_errors}, operator=operator, level="ERROR")

        audit.log("PIPELINE_COMPLETE", {
            "success": len(errors) == 0,
            "warnings": len(warnings),
            "errors": len(errors),
        }, operator=operator)

        return PipelineResult(
            run_id=run_id,
            success=len(errors) == 0,
            reports=reports,
            output_files=output_files,
            reconciliation=recon_results,
            audit_summary=audit.summary(),
            warnings=warnings,
            errors=errors,
        )

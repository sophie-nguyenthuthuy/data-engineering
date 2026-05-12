from __future__ import annotations
import io
import time
import uuid
import structlog
import pandas as pd

from ..config import settings
from ..models import (
    MicroBatch,
    ValidationResult,
    CheckResult,
    ValidationStatus,
    ValidatorBackend,
)
from .base import BaseValidator

log = structlog.get_logger(__name__)


class SodaValidator(BaseValidator):
    """Runs Soda Core checks against an in-memory pandas DataFrame."""

    def __init__(self, checks_dir: str | None = None) -> None:
        self._checks_dir = checks_dir or "config/soda"

    async def validate(self, batch: MicroBatch) -> ValidationResult:
        # Deferred import — keeps startup fast when Soda is not the active backend
        try:
            from soda.scan import Scan  # type: ignore
        except ImportError as exc:
            raise RuntimeError("soda-core is not installed") from exc

        start = time.perf_counter()
        result_id = str(uuid.uuid4())
        table = batch.metadata.table_name
        checks_file = f"{self._checks_dir}/{table}_checks.yml"

        try:
            df = batch.to_dataframe()
            scan = Scan()
            scan.set_scan_definition_name(f"dq_monitor_{table}")
            scan.set_data_source_name("in_memory")
            scan.add_pandas_dataframe(data_source_name="in_memory", dataset_name=table, pandas_df=df)
            scan.add_sodacl_yaml_file(checks_file)
            scan.execute()

            check_results = _parse_soda_results(scan)
            passed = sum(1 for c in check_results if c.status == ValidationStatus.PASSED)
            failed = sum(1 for c in check_results if c.status == ValidationStatus.FAILED)
            warnings = sum(1 for c in check_results if c.status == ValidationStatus.WARNING)
            total = len(check_results)
            pass_rate = passed / total if total else 0.0
            status = ValidationStatus.PASSED if pass_rate >= settings.failure_threshold else ValidationStatus.FAILED

            return ValidationResult(
                result_id=result_id,
                batch_id=batch.batch_id,
                table_name=table,
                backend=ValidatorBackend.SODA,
                suite_name=checks_file,
                status=status,
                pass_rate=pass_rate,
                total_checks=total,
                passed_checks=passed,
                failed_checks=failed,
                warning_checks=warnings,
                check_results=check_results,
                row_count=batch.row_count,
                duration_ms=(time.perf_counter() - start) * 1000,
            )

        except Exception as exc:
            log.error("soda_validation_error", batch_id=batch.batch_id, error=str(exc))
            return ValidationResult(
                result_id=result_id,
                batch_id=batch.batch_id,
                table_name=table,
                backend=ValidatorBackend.SODA,
                suite_name=checks_file,
                status=ValidationStatus.ERROR,
                pass_rate=0.0,
                total_checks=0,
                passed_checks=0,
                failed_checks=0,
                warning_checks=0,
                row_count=batch.row_count,
                duration_ms=(time.perf_counter() - start) * 1000,
                error_message=str(exc),
            )

    async def health_check(self) -> bool:
        try:
            from soda.scan import Scan  # type: ignore  # noqa: F401
            return True
        except ImportError:
            return False


def _parse_soda_results(scan) -> list[CheckResult]:
    results = []
    for check in scan.get_checks_dataframe().to_dict(orient="records") if hasattr(scan, "get_checks_dataframe") else []:
        outcome = str(check.get("outcome", "")).lower()
        if outcome == "pass":
            status = ValidationStatus.PASSED
        elif outcome == "warn":
            status = ValidationStatus.WARNING
        else:
            status = ValidationStatus.FAILED

        results.append(CheckResult(
            check_name=str(check.get("name", "unknown")),
            expectation_type=str(check.get("type", "unknown")),
            status=status,
            observed_value=check.get("value"),
            details={k: v for k, v in check.items()},
        ))

    # Fallback: use scan logs when structured check data is unavailable
    if not results:
        for check_result in getattr(scan, "_checks", []):
            outcome = getattr(check_result, "outcome", None)
            if outcome is None:
                continue
            status_map = {"pass": ValidationStatus.PASSED, "warn": ValidationStatus.WARNING}
            status = status_map.get(str(outcome).lower(), ValidationStatus.FAILED)
            results.append(CheckResult(
                check_name=str(getattr(check_result, "name", "unknown")),
                expectation_type=str(getattr(check_result, "check_cfg", {.__class__.__name__ if hasattr(check_result, "check_cfg") else "unknown"})),
                status=status,
                details={},
            ))
    return results

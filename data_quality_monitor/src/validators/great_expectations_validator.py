from __future__ import annotations
import time
import uuid
import structlog
from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.exceptions import GreatExpectationsError

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


class GreatExpectationsValidator(BaseValidator):
    def __init__(self, suite_name: str | None = None) -> None:
        self._suite_name = suite_name or "default_suite"
        self._context: FileDataContext | None = None

    def _get_context(self) -> FileDataContext:
        if self._context is None:
            self._context = FileDataContext(context_root_dir=settings.ge_data_context_root)
        return self._context

    async def validate(self, batch: MicroBatch) -> ValidationResult:
        start = time.perf_counter()
        result_id = str(uuid.uuid4())
        table = batch.metadata.table_name
        suite_name = f"{table}_{self._suite_name}"

        try:
            context = self._get_context()
            df = batch.to_dataframe()

            batch_request = RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name=table,
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": batch.batch_id},
            )

            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name,
            )
            ge_result = validator.validate()

            check_results = _parse_ge_results(ge_result)
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
                backend=ValidatorBackend.GREAT_EXPECTATIONS,
                suite_name=suite_name,
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

        except GreatExpectationsError as exc:
            log.error("ge_validation_error", batch_id=batch.batch_id, error=str(exc))
            return ValidationResult(
                result_id=result_id,
                batch_id=batch.batch_id,
                table_name=table,
                backend=ValidatorBackend.GREAT_EXPECTATIONS,
                suite_name=suite_name,
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
            self._get_context()
            return True
        except Exception:
            return False


def _parse_ge_results(ge_result) -> list[CheckResult]:
    results = []
    for r in ge_result.results:
        exp_type = r.expectation_config.expectation_type
        success = r.success

        observed = r.result.get("observed_value")
        unexpected_count = r.result.get("unexpected_count", 0)
        unexpected_pct = r.result.get("unexpected_percent", 0.0) or 0.0
        element_count = r.result.get("element_count", 0)

        results.append(CheckResult(
            check_name=exp_type,
            expectation_type=exp_type,
            status=ValidationStatus.PASSED if success else ValidationStatus.FAILED,
            observed_value=observed,
            expected_value=r.expectation_config.kwargs,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_percent=unexpected_pct,
            details=dict(r.result),
        ))
    return results

from __future__ import annotations
import asyncio

import structlog

from ..config import settings
from ..models import MicroBatch, ValidationResult, ValidationStatus
from ..validators import BaseValidator, GreatExpectationsValidator, SodaValidator
from ..stream.producer import KafkaResultProducer
from ..storage.repository import ValidationRepository
from ..metrics.collector import MetricsCollector
from ..metrics.publisher import MetricsPublisher
from ..blocking.job_controller import JobController

log = structlog.get_logger(__name__)


class MicroBatchProcessor:
    """
    Orchestrates the full per-batch quality-check lifecycle:

    1. Run GE and/or Soda validators in parallel
    2. Merge results (worst status wins)
    3. Persist to PostgreSQL
    4. Publish result event to Kafka
    5. Cache result + push metric snapshot via Redis
    6. Block downstream jobs on failure
    """

    def __init__(
        self,
        repository: ValidationRepository,
        producer: KafkaResultProducer,
        collector: MetricsCollector,
        publisher: MetricsPublisher,
        job_controller: JobController,
    ) -> None:
        self._repo = repository
        self._producer = producer
        self._collector = collector
        self._publisher = publisher
        self._job_controller = job_controller
        self._validators = self._build_validators()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def process(self, batch: MicroBatch) -> ValidationResult:
        log.info(
            "processing_batch",
            batch_id=batch.batch_id,
            table=batch.metadata.table_name,
            rows=batch.row_count,
        )

        # 1. Validate (run all configured backends concurrently)
        results = await asyncio.gather(
            *[v.validate(batch) for v in self._validators],
            return_exceptions=True,
        )

        merged = self._merge_results(batch, results)

        # 2. Persist
        await self._repo.save_result(merged)

        # 3. Publish to Kafka results topic
        await self._producer.publish_result(merged)

        # 4. Push to Redis (cache + metrics channel)
        await self._publisher.publish_result(merged)

        # 5. Update in-memory metrics
        self._collector.record(merged)

        # 6. Gate downstream jobs
        blocked = await self._job_controller.apply_result(merged)
        if blocked:
            log.warning(
                "downstream_jobs_blocked",
                jobs=blocked,
                batch_id=batch.batch_id,
                table=batch.metadata.table_name,
                pass_rate=merged.pass_rate,
            )

        return merged

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_validators(self) -> list[BaseValidator]:
        backend = settings.validator_backend
        validators: list[BaseValidator] = []
        if backend in ("great_expectations", "both"):
            validators.append(GreatExpectationsValidator())
        if backend in ("soda", "both"):
            validators.append(SodaValidator())
        if not validators:
            raise ValueError(f"Unknown validator_backend: {backend!r}")
        return validators

    @staticmethod
    def _merge_results(
        batch: MicroBatch,
        results: list[ValidationResult | BaseException],
    ) -> ValidationResult:
        """
        Combine results from multiple validator backends.
        The worst status across all backends wins.
        Pass rate is averaged.
        """
        valid = [r for r in results if isinstance(r, ValidationResult)]
        errors = [r for r in results if isinstance(r, BaseException)]

        if errors:
            for exc in errors:
                log.error("validator_exception", error=str(exc))

        if not valid:
            # All backends failed — treat as ERROR
            from ..models import ValidatorBackend
            import uuid
            return ValidationResult(
                result_id=str(uuid.uuid4()),
                batch_id=batch.batch_id,
                table_name=batch.metadata.table_name,
                backend=ValidatorBackend.GREAT_EXPECTATIONS,
                suite_name="unknown",
                status=ValidationStatus.ERROR,
                pass_rate=0.0,
                total_checks=0,
                passed_checks=0,
                failed_checks=0,
                warning_checks=0,
                row_count=batch.row_count,
                error_message="; ".join(str(e) for e in errors),
            )

        if len(valid) == 1:
            return valid[0]

        # Merge: aggregate check lists, worst status, average pass rate
        _STATUS_RANK = {
            ValidationStatus.PASSED: 0,
            ValidationStatus.WARNING: 1,
            ValidationStatus.FAILED: 2,
            ValidationStatus.ERROR: 3,
        }
        worst = max(valid, key=lambda r: _STATUS_RANK[r.status])
        merged_checks = []
        for r in valid:
            merged_checks.extend(r.check_results)

        import uuid
        return ValidationResult(
            result_id=str(uuid.uuid4()),
            batch_id=batch.batch_id,
            table_name=batch.metadata.table_name,
            backend=worst.backend,
            suite_name="+".join(r.suite_name for r in valid),
            status=worst.status,
            pass_rate=sum(r.pass_rate for r in valid) / len(valid),
            total_checks=sum(r.total_checks for r in valid),
            passed_checks=sum(r.passed_checks for r in valid),
            failed_checks=sum(r.failed_checks for r in valid),
            warning_checks=sum(r.warning_checks for r in valid),
            check_results=merged_checks,
            row_count=batch.row_count,
            duration_ms=max(r.duration_ms for r in valid),
        )

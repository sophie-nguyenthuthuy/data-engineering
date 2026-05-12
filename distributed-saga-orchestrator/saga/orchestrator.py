from __future__ import annotations

import logging
import time
import uuid
from dataclasses import asdict
from typing import Any

from .exceptions import (
    SagaAlreadyRunningError,
    SagaNotFoundError,
    SagaNotRecoverableError,
    StepCompensationError,
    StepExecutionError,
)
from .persistence import SagaRecord, SagaStatus, SagaStore
from .step import SagaStep, StepRecord, StepStatus

logger = logging.getLogger(__name__)


class SagaResult:
    def __init__(self, record: SagaRecord) -> None:
        self._record = record

    @property
    def saga_id(self) -> str:
        return self._record.saga_id

    @property
    def succeeded(self) -> bool:
        return self._record.status == SagaStatus.COMPLETED

    @property
    def status(self) -> SagaStatus:
        return self._record.status

    @property
    def context(self) -> dict[str, Any]:
        return dict(self._record.context)

    @property
    def failure_step(self) -> str | None:
        return self._record.failure_step

    @property
    def failure_reason(self) -> str | None:
        return self._record.failure_reason

    @property
    def compensation_errors(self) -> list[dict]:
        return list(self._record.compensation_errors)

    @property
    def step_records(self) -> list[StepRecord]:
        return [
            StepRecord(
                name=r["name"],
                status=StepStatus(r["status"]),
                output=r.get("output", {}),
                error_message=r.get("error_message"),
                attempts=r.get("attempts", 0),
                started_at=r.get("started_at"),
                completed_at=r.get("completed_at"),
                compensated_at=r.get("compensated_at"),
            )
            for r in self._record.step_records
        ]

    def __repr__(self) -> str:
        return (
            f"SagaResult(saga_id={self.saga_id!r}, status={self.status.value!r}, "
            f"steps={len(self.step_records)})"
        )


class SagaOrchestrator:
    """
    Executes a sequence of SagaSteps.  If any step fails, compensations run in
    reverse order for all previously-completed steps.  State is persisted to
    SagaStore after every step so the run survives process restarts.

    Usage::

        store = SagaStore("sagas.db")
        orchestrator = SagaOrchestrator(store)
        result = await orchestrator.run(
            steps=[Step1(), Step2(), ...],
            initial_context={"key": "value"},
            saga_type="my_pipeline",
        )
    """

    def __init__(self, store: SagaStore | None = None) -> None:
        self._store = store or SagaStore()

    @property
    def store(self) -> SagaStore:
        return self._store

    async def run(
        self,
        steps: list[SagaStep],
        initial_context: dict[str, Any] | None = None,
        saga_type: str = "saga",
        saga_id: str | None = None,
    ) -> SagaResult:
        """Execute all steps. Returns SagaResult regardless of outcome."""
        saga_id = saga_id or str(uuid.uuid4())
        context: dict[str, Any] = dict(initial_context or {})

        record = SagaRecord(
            saga_id=saga_id,
            saga_type=saga_type,
            status=SagaStatus.RUNNING,
            context=context,
            step_records=[
                asdict(StepRecord(name=s.name)) for s in steps
            ],
        )
        self._store.save(record)
        logger.info("Saga %s (%s) started with %d steps", saga_id, saga_type, len(steps))

        completed: list[int] = []

        for idx, step in enumerate(steps):
            rec = self._get_step_rec(record, idx)
            rec["status"] = StepStatus.RUNNING.value
            rec["started_at"] = time.time()
            rec["attempts"] = 0
            self._store.save(record)

            logger.debug("  → Executing step [%d/%d] %s", idx + 1, len(steps), step.name)
            result = await step._execute_with_retry(context)

            rec["attempts"] = result.duration_ms  # reuse field for timing? No — store properly
            rec["attempts"] = getattr(step.retry_policy, "max_attempts", 1)
            rec["completed_at"] = time.time()

            if result.success:
                rec["status"] = StepStatus.COMPLETED.value
                rec["output"] = result.output
                context.update(result.output)
                record.context = context
                completed.append(idx)
                self._store.save(record)
                logger.debug("  ✓ Step %s completed in %.1f ms", step.name, result.duration_ms)
            else:
                rec["status"] = StepStatus.FAILED.value
                rec["error_message"] = str(result.error)
                record.failure_step = step.name
                record.failure_reason = str(result.error)
                record.status = SagaStatus.COMPENSATING
                self._store.save(record)

                logger.warning(
                    "  ✗ Step %s failed: %s — starting compensation for %d steps",
                    step.name, result.error, len(completed),
                )
                await self._compensate(steps, completed, record, context)
                return SagaResult(record)

        record.status = SagaStatus.COMPLETED
        record.completed_at = time.time()
        self._store.save(record)
        logger.info("Saga %s completed successfully", saga_id)
        return SagaResult(record)

    async def recover(self, saga_id: str, steps: list[SagaStep]) -> SagaResult:
        """
        Re-attach to an interrupted saga and continue compensation.
        Only valid for sagas stuck in RUNNING or COMPENSATING state.
        """
        record = self._store.load(saga_id)
        if record is None:
            raise SagaNotFoundError(saga_id)

        if record.status not in (SagaStatus.RUNNING, SagaStatus.COMPENSATING):
            raise SagaNotRecoverableError(saga_id, record.status.value)

        logger.info("Recovering saga %s from status %s", saga_id, record.status.value)
        context = dict(record.context)

        completed = [
            idx
            for idx, r in enumerate(record.step_records)
            if r["status"] == StepStatus.COMPLETED.value
        ]

        record.status = SagaStatus.COMPENSATING
        record.failure_reason = record.failure_reason or "recovery"
        self._store.save(record)

        await self._compensate(steps, completed, record, context)
        return SagaResult(record)

    async def _compensate(
        self,
        steps: list[SagaStep],
        completed: list[int],
        record: SagaRecord,
        context: dict[str, Any],
    ) -> None:
        compensation_errors: list[dict] = []

        for idx in reversed(completed):
            step = steps[idx]
            rec = self._get_step_rec(record, idx)
            rec["status"] = StepStatus.COMPENSATING.value
            self._store.save(record)

            logger.debug("  ↩ Compensating step [%d] %s", idx + 1, step.name)
            exc = await step._compensate_safe(context)

            if exc is None:
                rec["status"] = StepStatus.COMPENSATED.value
                rec["compensated_at"] = time.time()
                logger.debug("  ✓ Compensation of %s succeeded", step.name)
            else:
                rec["status"] = StepStatus.COMPENSATION_FAILED.value
                err_entry = {"step": step.name, "error": str(exc)}
                compensation_errors.append(err_entry)
                logger.error(
                    "  ✗ Compensation of %s failed: %s (continuing)", step.name, exc
                )

            self._store.save(record)

        record.compensation_errors = compensation_errors
        record.status = SagaStatus.COMPENSATED if not compensation_errors else SagaStatus.FAILED
        record.completed_at = time.time()
        self._store.save(record)

        if compensation_errors:
            logger.error(
                "Saga %s compensation finished with %d error(s)",
                record.saga_id, len(compensation_errors),
            )
        else:
            logger.info("Saga %s fully compensated", record.saga_id)

    @staticmethod
    def _get_step_rec(record: SagaRecord, idx: int) -> dict:
        return record.step_records[idx]

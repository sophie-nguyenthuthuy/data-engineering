"""
Tests for SagaOrchestrator — happy path, context propagation, retry, recovery.
"""

from __future__ import annotations

import asyncio
import pytest
from typing import Any

from saga import (
    RetryPolicy,
    SagaOrchestrator,
    SagaStatus,
    SagaStep,
    SagaStore,
    StepStatus,
)
from saga.exceptions import SagaNotFoundError, SagaNotRecoverableError


# ---------------------------------------------------------------------------
# Minimal step helpers
# ---------------------------------------------------------------------------

class PassStep(SagaStep):
    """Always succeeds; writes its name into context."""
    def __init__(self, label: str = "pass") -> None:
        self._label = label

    @property
    def name(self) -> str:
        return f"PassStep_{self._label}"

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        return {self._label: True}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        ctx[f"compensated_{self._label}"] = True


class FailStep(SagaStep):
    """Always raises on execute."""
    @property
    def name(self) -> str:
        return "FailStep"

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("intentional failure")

    async def compensate(self, ctx: dict[str, Any]) -> None:
        ctx["compensated_fail"] = True


class CountingStep(SagaStep):
    """Tracks how many times execute() is called; fails on first N-1 attempts."""
    def __init__(self, fail_times: int = 0) -> None:
        self.call_count = 0
        self._fail_times = fail_times
        self.retry_policy = RetryPolicy(
            max_attempts=fail_times + 1,
            backoff_base_seconds=0.0,
        )

    @property
    def name(self) -> str:
        return "CountingStep"

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        self.call_count += 1
        if self.call_count <= self._fail_times:
            raise ValueError(f"attempt {self.call_count} failed")
        return {"counted": self.call_count}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store() -> SagaStore:
    return SagaStore()  # in-memory SQLite


@pytest.fixture
def orchestrator(store: SagaStore) -> SagaOrchestrator:
    return SagaOrchestrator(store)


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_steps_succeed(orchestrator: SagaOrchestrator) -> None:
    steps = [PassStep("a"), PassStep("b"), PassStep("c")]
    result = await orchestrator.run(steps, saga_type="test")

    assert result.succeeded
    assert result.status == SagaStatus.COMPLETED
    assert result.failure_step is None
    assert result.context["a"] is True
    assert result.context["b"] is True
    assert result.context["c"] is True


@pytest.mark.asyncio
async def test_step_outputs_propagate_to_next_steps(orchestrator: SagaOrchestrator) -> None:
    class ProducerStep(SagaStep):
        async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
            return {"produced_value": 42}

        async def compensate(self, ctx: dict[str, Any]) -> None:
            pass

    class ConsumerStep(SagaStep):
        async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
            assert ctx["produced_value"] == 42, "Producer output not in context"
            return {"consumed": True}

        async def compensate(self, ctx: dict[str, Any]) -> None:
            pass

    result = await orchestrator.run([ProducerStep(), ConsumerStep()])
    assert result.succeeded
    assert result.context["consumed"] is True


@pytest.mark.asyncio
async def test_initial_context_available_in_first_step(orchestrator: SagaOrchestrator) -> None:
    class CheckCtxStep(SagaStep):
        async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
            assert ctx.get("seed") == "hello"
            return {}

        async def compensate(self, ctx: dict[str, Any]) -> None:
            pass

    result = await orchestrator.run(
        [CheckCtxStep()],
        initial_context={"seed": "hello"},
    )
    assert result.succeeded


@pytest.mark.asyncio
async def test_empty_steps_completes_immediately(orchestrator: SagaOrchestrator) -> None:
    result = await orchestrator.run([], saga_type="empty")
    assert result.succeeded
    assert result.step_records == []


@pytest.mark.asyncio
async def test_explicit_saga_id_is_preserved(orchestrator: SagaOrchestrator, store: SagaStore) -> None:
    result = await orchestrator.run([PassStep()], saga_id="my-fixed-id")
    assert result.saga_id == "my-fixed-id"
    record = store.load("my-fixed-id")
    assert record is not None
    assert record.status == SagaStatus.COMPLETED


# ---------------------------------------------------------------------------
# Step-status tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_step_statuses_after_success(orchestrator: SagaOrchestrator) -> None:
    result = await orchestrator.run([PassStep("x"), PassStep("y")])
    records = result.step_records
    assert records[0].status == StepStatus.COMPLETED
    assert records[1].status == StepStatus.COMPLETED


@pytest.mark.asyncio
async def test_step_statuses_after_failure(orchestrator: SagaOrchestrator) -> None:
    result = await orchestrator.run([PassStep("a"), FailStep(), PassStep("b")])
    records = result.step_records
    assert records[0].status == StepStatus.COMPENSATED   # rolled back
    assert records[1].status == StepStatus.FAILED        # the failing step
    assert records[2].status == StepStatus.PENDING       # never ran


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_step_retried_before_succeeding(orchestrator: SagaOrchestrator) -> None:
    step = CountingStep(fail_times=2)  # fails twice, succeeds on 3rd
    result = await orchestrator.run([step])
    assert result.succeeded
    assert step.call_count == 3


@pytest.mark.asyncio
async def test_step_exhausts_retries_and_triggers_rollback(orchestrator: SagaOrchestrator) -> None:
    preceding = PassStep("pre")
    step = CountingStep(fail_times=99)  # will always fail (max_attempts=100 would be slow)
    step.retry_policy = RetryPolicy(max_attempts=2, backoff_base_seconds=0.0)
    result = await orchestrator.run([preceding, step])

    assert not result.succeeded
    assert result.failure_step == "CountingStep"
    # preceding step should have been compensated
    assert result.step_records[0].status == StepStatus.COMPENSATED


# ---------------------------------------------------------------------------
# Persistence round-trip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_completed_saga_persisted(store: SagaStore, orchestrator: SagaOrchestrator) -> None:
    result = await orchestrator.run([PassStep()], saga_id="persist-ok")
    record = store.load("persist-ok")
    assert record is not None
    assert record.status == SagaStatus.COMPLETED


@pytest.mark.asyncio
async def test_failed_saga_persisted(store: SagaStore, orchestrator: SagaOrchestrator) -> None:
    result = await orchestrator.run([PassStep(), FailStep()], saga_id="persist-fail")
    record = store.load("persist-fail")
    assert record is not None
    assert record.status in (SagaStatus.COMPENSATED, SagaStatus.FAILED)
    assert record.failure_step == "FailStep"


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recover_raises_for_unknown_saga(orchestrator: SagaOrchestrator) -> None:
    with pytest.raises(SagaNotFoundError):
        await orchestrator.recover("nonexistent-id", [PassStep()])


@pytest.mark.asyncio
async def test_recover_raises_for_completed_saga(
    store: SagaStore, orchestrator: SagaOrchestrator
) -> None:
    await orchestrator.run([PassStep()], saga_id="done-saga")
    with pytest.raises(SagaNotRecoverableError):
        await orchestrator.recover("done-saga", [PassStep()])


@pytest.mark.asyncio
async def test_recover_compensates_stuck_running_saga(store: SagaStore) -> None:
    """Simulate a saga that crashed mid-flight (status=RUNNING, one step completed)."""
    from dataclasses import asdict
    from saga.step import StepRecord
    from saga.persistence import SagaRecord, SagaStatus

    # Manually plant a "stuck" saga record
    step_a = PassStep("alpha")
    step_b = PassStep("beta")

    stuck_record = SagaRecord(
        saga_id="stuck-saga",
        saga_type="test",
        status=SagaStatus.RUNNING,
        context={"alpha": True},
        step_records=[
            {**asdict(StepRecord(name="PassStep_alpha")), "status": StepStatus.COMPLETED.value},
            {**asdict(StepRecord(name="PassStep_beta")), "status": StepStatus.RUNNING.value},
        ],
    )
    store.save(stuck_record)

    orchestrator = SagaOrchestrator(store)
    result = await orchestrator.recover("stuck-saga", [step_a, step_b])

    assert result.status in (SagaStatus.COMPENSATED, SagaStatus.FAILED)
    # Step alpha was completed, so it should be compensated
    assert result.step_records[0].status == StepStatus.COMPENSATED

"""
Tests for rollback (compensation) behaviour.

Covers:
- Compensation order is strictly reverse of execution order
- Compensation runs for exactly the completed steps (not the failing one)
- A compensation failure does NOT stop remaining compensations
- Compensation receives the correct accumulated context
- Compensation errors are recorded in the result
- First-step failure means zero compensations
"""

from __future__ import annotations

import pytest
from typing import Any

from saga import SagaOrchestrator, SagaStatus, SagaStep, SagaStore, StepStatus


# ---------------------------------------------------------------------------
# Instrumented step — records call order globally
# ---------------------------------------------------------------------------

class OrderedStep(SagaStep):
    """
    Records execution and compensation events into a shared list so tests can
    assert exact ordering.
    """
    def __init__(self, label: str, events: list[str], fail_execute: bool = False,
                 fail_compensate: bool = False) -> None:
        self._label = label
        self._events = events
        self._fail_execute = fail_execute
        self._fail_compensate = fail_compensate

    @property
    def name(self) -> str:
        return f"Step_{self._label}"

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        self._events.append(f"exec:{self._label}")
        if self._fail_execute:
            raise RuntimeError(f"Step {self._label} intentionally failed")
        return {f"out_{self._label}": self._label}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        self._events.append(f"comp:{self._label}")
        if self._fail_compensate:
            raise RuntimeError(f"Compensation for {self._label} intentionally failed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_orchestrator() -> SagaOrchestrator:
    return SagaOrchestrator(SagaStore())


# ---------------------------------------------------------------------------
# Core rollback ordering tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compensation_runs_in_reverse_order() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("1", events),
        OrderedStep("2", events),
        OrderedStep("3", events),
        OrderedStep("4", events, fail_execute=True),  # fails here
        OrderedStep("5", events),
    ]
    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    # Steps 1-3 executed, step 4 failed, step 5 never ran
    assert events[:4] == ["exec:1", "exec:2", "exec:3", "exec:4"]
    # Compensations run in reverse: 3, 2, 1
    assert events[4:] == ["comp:3", "comp:2", "comp:1"]


@pytest.mark.asyncio
async def test_failing_step_itself_is_not_compensated() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("a", events),
        OrderedStep("b", events, fail_execute=True),
    ]
    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    comp_events = [e for e in events if e.startswith("comp:")]
    # Only step "a" should be compensated, not "b"
    assert comp_events == ["comp:a"]


@pytest.mark.asyncio
async def test_first_step_failure_triggers_no_compensation() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("x", events, fail_execute=True),
        OrderedStep("y", events),
    ]
    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    comp_events = [e for e in events if e.startswith("comp:")]
    assert comp_events == [], "No compensation should run when step 1 fails"


@pytest.mark.asyncio
async def test_last_step_failure_compensates_all_prior_steps() -> None:
    events: list[str] = []
    n = 5
    steps = [OrderedStep(str(i), events) for i in range(1, n + 1)]
    steps.append(OrderedStep("boom", events, fail_execute=True))

    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    comp_events = [e for e in events if e.startswith("comp:")]
    assert comp_events == [f"comp:{i}" for i in range(n, 0, -1)]


# ---------------------------------------------------------------------------
# Context availability during compensation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compensation_receives_accumulated_context() -> None:
    captured: dict = {}

    class WriterStep(SagaStep):
        async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
            return {"written_key": "written_value"}

        async def compensate(self, ctx: dict[str, Any]) -> None:
            pass

    class ReaderCompStep(SagaStep):
        """Captures context during its compensate() call."""
        async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
            return {}

        async def compensate(self, ctx: dict[str, Any]) -> None:
            captured.update(ctx)

    class BoomStep(SagaStep):
        async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("boom")

        async def compensate(self, ctx: dict[str, Any]) -> None:
            pass

    result = await make_orchestrator().run(
        [WriterStep(), ReaderCompStep(), BoomStep()],
        initial_context={"seed": 99},
    )

    assert not result.succeeded
    # The context passed to ReaderCompStep.compensate should contain the seed
    # and the output from WriterStep
    assert captured.get("seed") == 99
    assert captured.get("written_key") == "written_value"


# ---------------------------------------------------------------------------
# Compensation failure handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compensation_failure_does_not_stop_other_compensations() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("1", events),
        OrderedStep("2", events, fail_compensate=True),  # comp for "2" will fail
        OrderedStep("3", events, fail_execute=True),      # triggers rollback
    ]
    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    # Compensation for step "1" must still run even though "2" failed
    comp_events = [e for e in events if e.startswith("comp:")]
    assert "comp:1" in comp_events
    assert "comp:2" in comp_events


@pytest.mark.asyncio
async def test_compensation_errors_recorded_in_result() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("p", events, fail_compensate=True),
        OrderedStep("q", events, fail_execute=True),
    ]
    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    assert len(result.compensation_errors) == 1
    assert result.compensation_errors[0]["step"] == "Step_p"


@pytest.mark.asyncio
async def test_clean_compensation_leaves_no_errors() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("ok1", events),
        OrderedStep("ok2", events),
        OrderedStep("fail", events, fail_execute=True),
    ]
    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    assert result.compensation_errors == []
    assert result.status == SagaStatus.COMPENSATED


@pytest.mark.asyncio
async def test_partial_compensation_failure_sets_failed_status() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("bad_comp", events, fail_compensate=True),
        OrderedStep("trigger", events, fail_execute=True),
    ]
    result = await make_orchestrator().run(steps)

    # At least one compensation failed → overall status should be FAILED
    assert result.status == SagaStatus.FAILED
    assert len(result.compensation_errors) >= 1


# ---------------------------------------------------------------------------
# Step status assertions after rollback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_step_statuses_after_rollback() -> None:
    events: list[str] = []
    steps = [
        OrderedStep("1", events),
        OrderedStep("2", events),
        OrderedStep("3", events, fail_execute=True),
        OrderedStep("4", events),
    ]
    result = await make_orchestrator().run(steps)
    recs = result.step_records

    assert recs[0].status == StepStatus.COMPENSATED   # executed then rolled back
    assert recs[1].status == StepStatus.COMPENSATED   # executed then rolled back
    assert recs[2].status == StepStatus.FAILED         # the trigger
    assert recs[3].status == StepStatus.PENDING        # never ran


@pytest.mark.asyncio
async def test_ten_step_rollback_from_step_6() -> None:
    """The headline use-case: step 6 of 10 fails → steps 1-5 compensated in reverse."""
    events: list[str] = []
    steps = [OrderedStep(str(i), events) for i in range(1, 11)]
    steps[5]._fail_execute = True  # step 6 (index 5) fails

    result = await make_orchestrator().run(steps)

    assert not result.succeeded
    assert result.failure_step == "Step_6"

    exec_events = [e for e in events if e.startswith("exec:")]
    comp_events = [e for e in events if e.startswith("comp:")]

    # Steps 1–6 executed (6 fails)
    assert exec_events == [f"exec:{i}" for i in range(1, 7)]
    # Steps 1–5 compensated in reverse order
    assert comp_events == [f"comp:{i}" for i in range(5, 0, -1)]

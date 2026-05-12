from __future__ import annotations

import asyncio
import inspect
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"
    SKIPPED = "skipped"


@dataclass
class RetryPolicy:
    max_attempts: int = 1
    backoff_base_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 60.0
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,)

    def delay_for(self, attempt: int) -> float:
        delay = self.backoff_base_seconds * (self.backoff_multiplier ** (attempt - 1))
        return min(delay, self.max_backoff_seconds)


@dataclass
class StepResult:
    success: bool
    output: dict[str, Any] = field(default_factory=dict)
    error: Exception | None = None
    duration_ms: float = 0.0


@dataclass
class StepRecord:
    name: str
    status: StepStatus = StepStatus.PENDING
    output: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None
    attempts: int = 0
    started_at: float | None = None
    completed_at: float | None = None
    compensated_at: float | None = None


class SagaStep(ABC):
    """Base class for all saga steps. Subclass and implement execute() and compensate()."""

    retry_policy: RetryPolicy = RetryPolicy()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Run the step. Return a dict of outputs to merge into context.
        Raise any exception to trigger rollback.
        """
        ...

    @abstractmethod
    async def compensate(self, context: dict[str, Any]) -> None:
        """
        Undo the effects of execute(). Called during rollback.
        Should be idempotent — may be called more than once.
        """
        ...

    async def _execute_with_retry(self, context: dict[str, Any]) -> StepResult:
        policy = self.retry_policy
        last_error: Exception | None = None

        for attempt in range(1, policy.max_attempts + 1):
            t0 = time.monotonic()
            try:
                result = self.execute(context)
                if inspect.isawaitable(result):
                    result = await result
                duration_ms = (time.monotonic() - t0) * 1000
                return StepResult(success=True, output=result or {}, duration_ms=duration_ms)
            except policy.retryable_exceptions as exc:
                last_error = exc
                duration_ms = (time.monotonic() - t0) * 1000
                if attempt < policy.max_attempts:
                    delay = policy.delay_for(attempt)
                    await asyncio.sleep(delay)

        return StepResult(success=False, error=last_error, duration_ms=0.0)

    async def _compensate_safe(self, context: dict[str, Any]) -> Exception | None:
        try:
            result = self.compensate(context)
            if inspect.isawaitable(result):
                await result
            return None
        except Exception as exc:
            return exc

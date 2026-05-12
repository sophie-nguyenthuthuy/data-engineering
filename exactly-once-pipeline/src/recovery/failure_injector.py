"""
Failure Injector — deterministically or randomly raises exceptions at
named pipeline steps to demonstrate recovery behaviour.

Usage:
    injector = FailureInjector(step="kafka", rate=1.0)   # always fail
    injector = FailureInjector(step="warehouse", rate=0.5) # 50 % fail
    injector.maybe_raise("kafka", idempotency_key)
"""
from __future__ import annotations

import random
from typing import Any

import structlog

from src.config import settings

log = structlog.get_logger(__name__)


class TransientFailure(Exception):
    """Simulated transient error — should be retried."""


class PermanentFailure(Exception):
    """Simulated permanent error — triggers compensation."""


class FailureInjector:
    def __init__(
        self,
        step: str | None = None,
        rate: float | None = None,
        *,
        permanent: bool = False,
    ) -> None:
        self._step = step or settings.inject_failure_step
        self._rate = rate if rate is not None else settings.inject_failure_rate
        self._permanent = permanent
        self._call_count: dict[str, int] = {}

    def maybe_raise(self, current_step: str, key: Any = None) -> None:
        if not self._step or current_step != self._step:
            return
        if random.random() > self._rate:
            return

        count = self._call_count.get(str(key), 0) + 1
        self._call_count[str(key)] = count

        log.warning(
            "failure_injector.triggering",
            step=current_step,
            key=str(key),
            attempt=count,
            permanent=self._permanent,
        )

        if self._permanent:
            raise PermanentFailure(
                f"[INJECTED] Permanent failure at step={current_step} key={key}"
            )
        raise TransientFailure(
            f"[INJECTED] Transient failure at step={current_step} key={key} attempt={count}"
        )

    def reset(self) -> None:
        self._call_count.clear()

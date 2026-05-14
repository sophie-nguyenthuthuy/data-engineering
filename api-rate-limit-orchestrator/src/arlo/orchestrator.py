"""Orchestrator — a thin "wait until you can acquire" loop.

Worker code that wants to ingest from a quota-limited upstream should
call :meth:`Orchestrator.wait_and_acquire`. The orchestrator keeps
calling the bucket's ``acquire`` until it succeeds, sleeping for the
bucket's suggested wait between attempts, with a configurable
``max_wait`` ceiling and ``max_attempts`` budget.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from arlo.bucket import AcquireResult, TokenBucket


class AcquireTimeout(TimeoutError):
    """Raised when the orchestrator exhausts ``max_wait`` or ``max_attempts``."""


@dataclass
class Orchestrator:
    """Polling acquirer with bounded wait."""

    bucket: TokenBucket
    max_wait: float = 30.0
    max_attempts: int = 1_000
    min_sleep: float = 0.001
    sleep: Callable[[float], None] = field(default=time.sleep)

    def __post_init__(self) -> None:
        if self.max_wait <= 0:
            raise ValueError("max_wait must be > 0")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be ≥ 1")
        if self.min_sleep < 0:
            raise ValueError("min_sleep must be ≥ 0")

    def wait_and_acquire(self, tokens: float = 1.0) -> AcquireResult:
        """Block until ``tokens`` are taken or budgets are exhausted."""
        start = time.monotonic()
        for attempt in range(self.max_attempts):
            result = self.bucket.acquire(tokens)
            if result.took:
                return result
            elapsed = time.monotonic() - start
            remaining = self.max_wait - elapsed
            if remaining <= 0:
                raise AcquireTimeout(
                    f"could not acquire after {attempt + 1} attempts in {elapsed:.3f}s"
                )
            wait = max(self.min_sleep, min(result.suggested_wait, remaining))
            self.sleep(wait)
        raise AcquireTimeout(f"max_attempts={self.max_attempts} reached without acquire")


__all__ = ["AcquireTimeout", "Orchestrator"]

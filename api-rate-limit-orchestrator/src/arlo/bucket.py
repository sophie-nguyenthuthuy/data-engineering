"""TokenBucket — the small object every caller talks to.

A :class:`TokenBucket` wraps a :class:`StorageBackend` + :class:`Quota`
+ a logical bucket key. The single mutation entry point is
:meth:`acquire`, which returns an :class:`AcquireResult` saying whether
tokens were taken and how long the caller should wait if not.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from arlo.quota import Quota
    from arlo.storage.base import StorageBackend


@dataclass(frozen=True, slots=True)
class AcquireResult:
    """Outcome of one :meth:`TokenBucket.acquire` attempt."""

    took: bool
    tokens_remaining: float
    suggested_wait: float  # seconds; 0.0 when ``took`` is True

    def __post_init__(self) -> None:
        if self.tokens_remaining < 0:
            raise ValueError("tokens_remaining must be ≥ 0")
        if self.suggested_wait < 0:
            raise ValueError("suggested_wait must be ≥ 0")


@dataclass
class TokenBucket:
    """Single-key token-bucket consumer."""

    key: str
    quota: Quota
    storage: StorageBackend
    clock: Callable[[], float] = field(default=time.monotonic)

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("key must be non-empty")

    def acquire(self, tokens: float = 1.0) -> AcquireResult:
        """Try to take ``tokens``. Returns ``(took, remaining, suggested_wait)``."""
        if tokens <= 0:
            raise ValueError("tokens must be > 0")
        if tokens > self.quota.capacity:
            raise ValueError(
                f"requested {tokens} > capacity {self.quota.capacity}; impossible to acquire"
            )
        now = self.clock()
        took, state = self.storage.atomic_take(
            self.key,
            capacity=self.quota.capacity,
            refill_per_second=self.quota.refill_per_second,
            requested=tokens,
            now=now,
        )
        if took:
            return AcquireResult(took=True, tokens_remaining=state.tokens, suggested_wait=0.0)
        # `state.tokens` is the post-refill, no-deduct value; tell the caller
        # the soonest they could succeed.
        deficit = tokens - state.tokens
        wait = deficit / self.quota.refill_per_second
        return AcquireResult(took=False, tokens_remaining=state.tokens, suggested_wait=wait)


__all__ = ["AcquireResult", "TokenBucket"]

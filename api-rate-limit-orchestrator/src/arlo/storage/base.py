"""Storage-backend protocol.

The bucket needs exactly one atomic primitive: "given the previous
state for key ``k``, atomically compute the new state and tell me how
many tokens I got." Every backend that supports compare-and-swap (or a
Lua-script-style atomic block, in Redis's case) can implement this.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BucketState:
    """Persisted state for one bucket key."""

    tokens: float
    last_refill_ts: float

    def __post_init__(self) -> None:
        if self.tokens < 0:
            raise ValueError("tokens must be ≥ 0")
        if self.last_refill_ts < 0:
            raise ValueError("last_refill_ts must be ≥ 0")


class StorageBackend(ABC):
    """Backend interface implemented by every storage adapter."""

    @abstractmethod
    def atomic_take(
        self,
        key: str,
        *,
        capacity: float,
        refill_per_second: float,
        requested: float,
        now: float,
    ) -> tuple[bool, BucketState]:
        """Atomically refill + (maybe) take tokens.

        Returns ``(took, state_after)``. ``took`` is ``True`` iff
        ``requested`` ≤ ``state_after.tokens`` *before* the deduction;
        in that case the deduction is applied. If ``took`` is
        ``False`` the state still gets refilled — i.e. concurrent
        readers see consistent timestamps.
        """


__all__ = ["BucketState", "StorageBackend"]

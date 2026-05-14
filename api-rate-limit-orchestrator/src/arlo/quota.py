"""Quota declaration.

A :class:`Quota` says "at most ``capacity`` tokens, refilled at
``refill_per_second``". The bucket starts full; each successful
``acquire`` deducts tokens.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Quota:
    """Token-bucket quota parameters."""

    capacity: float
    refill_per_second: float

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("capacity must be > 0")
        if self.refill_per_second <= 0:
            raise ValueError("refill_per_second must be > 0")

    @classmethod
    def per_second(cls, n: int) -> Quota:
        """Helper: ``n`` requests per second, burst = ``n``."""
        return cls(capacity=float(n), refill_per_second=float(n))

    @classmethod
    def per_minute(cls, n: int) -> Quota:
        return cls(capacity=float(n), refill_per_second=n / 60.0)

    @classmethod
    def per_hour(cls, n: int) -> Quota:
        return cls(capacity=float(n), refill_per_second=n / 3600.0)


__all__ = ["Quota"]

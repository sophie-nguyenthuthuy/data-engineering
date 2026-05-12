"""
Pattern DSL for declaring multi-step event sequences.

Example
-------
from cep.pattern import Pattern

fraud = (
    Pattern("fraud_sequence")
    .begin(EventTypes.LOGIN_FAILURE, count=3, within_ns=10_000_000_000)
    .then(EventTypes.PASSWORD_RESET, max_gap_ns=5_000_000_000)
    .then(EventTypes.LARGE_WITHDRAWAL, value_gte=1000.0, max_gap_ns=30_000_000_000)
    .total_window(60_000_000_000)
)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StepPredicate:
    type_id: int
    value_gte: Optional[float] = None
    value_lte: Optional[float] = None
    flags_mask: int = 0         # ANDed with event.flags
    flags_value: int = 0        # expected result after mask
    max_gap_ns: Optional[int] = None  # max nanoseconds since previous step
    # count > 1 means we need *count* matching events before advancing
    count: int = 1


@dataclass
class Pattern:
    name: str
    steps: list[StepPredicate] = field(default_factory=list)
    _total_window_ns: int = 60_000_000_000  # default 60 s

    # ------------------------------------------------------------------
    # Fluent builder

    def begin(
        self,
        type_id: int,
        *,
        value_gte: Optional[float] = None,
        value_lte: Optional[float] = None,
        flags_mask: int = 0,
        flags_value: int = 0,
        count: int = 1,
        within_ns: Optional[int] = None,
    ) -> "Pattern":
        """First step — resets the pattern; sets the anchor timestamp."""
        self.steps = []
        self.steps.append(
            StepPredicate(
                type_id=type_id,
                value_gte=value_gte,
                value_lte=value_lte,
                flags_mask=flags_mask,
                flags_value=flags_value,
                max_gap_ns=within_ns,
                count=count,
            )
        )
        return self

    def then(
        self,
        type_id: int,
        *,
        value_gte: Optional[float] = None,
        value_lte: Optional[float] = None,
        flags_mask: int = 0,
        flags_value: int = 0,
        max_gap_ns: Optional[int] = None,
        count: int = 1,
    ) -> "Pattern":
        """Append a subsequent step."""
        if not self.steps:
            raise ValueError("Call .begin() before .then()")
        self.steps.append(
            StepPredicate(
                type_id=type_id,
                value_gte=value_gte,
                value_lte=value_lte,
                flags_mask=flags_mask,
                flags_value=flags_value,
                max_gap_ns=max_gap_ns,
                count=count,
            )
        )
        return self

    def total_window(self, ns: int) -> "Pattern":
        self._total_window_ns = ns
        return self

    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self.steps)

    def __repr__(self) -> str:
        return f"Pattern({self.name!r}, steps={len(self.steps)}, window={self._total_window_ns}ns)"

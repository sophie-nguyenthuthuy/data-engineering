from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class HLCTimestamp:
    """Hybrid Logical Clock timestamp — totally ordered pair (wall_ms, logical)."""

    wall_ms: int
    logical: int

    def __str__(self) -> str:
        return f"({self.wall_ms}ms,+{self.logical})"

    def to_dict(self) -> dict:
        return {"wall_ms": self.wall_ms, "logical": self.logical}

    @classmethod
    def from_dict(cls, d: dict) -> HLCTimestamp:
        return cls(d["wall_ms"], d["logical"])


ZERO = HLCTimestamp(0, 0)

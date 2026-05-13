"""Versioned-row record shared by hot and cold tiers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Version:
    """A single (value, write-clock, wall-time) record."""

    value: Any
    clock: dict[str, int] = field(default_factory=dict)
    wall: float = 0.0

    def __post_init__(self) -> None:
        if self.wall < 0:
            raise ValueError("wall must be ≥ 0")


__all__ = ["Version"]

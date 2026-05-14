"""Log-sequence-number types used by both MySQL + Postgres parsers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, order=True)
class LSN:
    """Postgres-style Log Sequence Number (64-bit unsigned)."""

    value: int

    def __post_init__(self) -> None:
        if self.value < 0:
            raise ValueError("LSN must be ≥ 0")
        if self.value > (1 << 64) - 1:
            raise ValueError("LSN does not fit in 64 bits")

    def __str__(self) -> str:
        # Postgres prints LSNs as ``HEXHI/HEXLO`` (32-bit / 32-bit halves).
        hi = (self.value >> 32) & 0xFFFFFFFF
        lo = self.value & 0xFFFFFFFF
        return f"{hi:X}/{lo:X}"

    @classmethod
    def parse(cls, s: str) -> LSN:
        """Parse a Postgres-formatted ``HEXHI/HEXLO`` string."""
        if "/" not in s:
            raise ValueError(f"LSN string {s!r} must contain '/'")
        hi_s, lo_s = s.split("/", 1)
        return cls(value=(int(hi_s, 16) << 32) | int(lo_s, 16))


@dataclass(frozen=True, slots=True, order=True)
class BinlogPosition:
    """MySQL binlog cursor — ``(file, position)``."""

    file: str
    position: int

    def __post_init__(self) -> None:
        if not self.file:
            raise ValueError("file must be non-empty")
        if self.position < 0:
            raise ValueError("position must be ≥ 0")

    def __str__(self) -> str:
        return f"{self.file}:{self.position}"


__all__ = ["LSN", "BinlogPosition"]

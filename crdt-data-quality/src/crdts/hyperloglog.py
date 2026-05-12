"""
HyperLogLog CRDT for approximate distinct counts.

Each node maintains a register array of size 2^b (b = precision bits).
The register at index j holds the maximum number of leading zeros seen
for any hash whose first b bits equal j.

CRDT merge = element-wise maximum of register arrays.
This is a G-Counter in register space: registers only increase.

Standard error: 1.04 / sqrt(2^b)
  b=10 → ~1024 registers, ~1.6% error
  b=14 → ~16384 registers, ~0.81% error
"""
from __future__ import annotations
import hashlib
import math
import struct
from dataclasses import dataclass, field
from typing import Any, List


_ALPHA = {
    4: 0.673,
    5: 0.697,
    6: 0.709,
}


def _alpha(m: int) -> float:
    if m <= 16:
        return _ALPHA.get(int(math.log2(m)), 0.7213 / (1 + 1.079 / m))
    return 0.7213 / (1 + 1.079 / m)


def _hash64(value: Any) -> int:
    raw = str(value).encode()
    digest = hashlib.sha256(raw).digest()
    return struct.unpack(">Q", digest[:8])[0]


def _rho(bits: int, max_bits: int) -> int:
    """Position of leftmost 1-bit (1-indexed), capped at max_bits+1."""
    for i in range(max_bits, -1, -1):
        if bits & (1 << i):
            return max_bits - i
    return max_bits + 1


@dataclass
class HyperLogLogCRDT:
    node_id: str
    precision: int = 10  # b: number of index bits; m = 2^b registers
    registers: List[int] = field(default_factory=list)

    def __post_init__(self):
        if not self.registers:
            self.registers = [0] * (2 ** self.precision)

    @property
    def m(self) -> int:
        return len(self.registers)

    def add(self, value: Any) -> None:
        h = _hash64(value)
        # top `precision` bits select the register
        idx = h >> (64 - self.precision)
        # remaining bits used to count leading zeros
        w = h & ((1 << (64 - self.precision)) - 1)
        rho = _rho(w, 64 - self.precision)
        if rho > self.registers[idx]:
            self.registers[idx] = rho

    def count(self) -> int:
        """Estimate of distinct elements with bias correction."""
        m = self.m
        alpha = _alpha(m)
        raw = alpha * m * m / sum(2 ** -r for r in self.registers)

        # small range correction
        if raw <= 2.5 * m:
            zeros = self.registers.count(0)
            if zeros:
                return int(round(m * math.log(m / zeros)))

        # large range correction
        if raw > (1 / 30) * (2 ** 32):
            return int(round(-(2 ** 32) * math.log(1 - raw / (2 ** 32))))

        return int(round(raw))

    def merge(self, other: "HyperLogLogCRDT") -> "HyperLogLogCRDT":
        if self.precision != other.precision:
            raise ValueError("Cannot merge HyperLogLogs with different precision")
        merged_regs = [max(a, b) for a, b in zip(self.registers, other.registers)]
        return HyperLogLogCRDT(
            node_id=self.node_id,
            precision=self.precision,
            registers=merged_regs,
        )

    def merge_into(self, other: "HyperLogLogCRDT") -> None:
        if self.precision != other.precision:
            raise ValueError("Cannot merge HyperLogLogs with different precision")
        for i, val in enumerate(other.registers):
            if val > self.registers[i]:
                self.registers[i] = val

    def clone(self) -> "HyperLogLogCRDT":
        return HyperLogLogCRDT(
            node_id=self.node_id,
            precision=self.precision,
            registers=list(self.registers),
        )

    def error_rate(self) -> float:
        return 1.04 / math.sqrt(self.m)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "precision": self.precision,
            "registers": list(self.registers),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HyperLogLogCRDT":
        return cls(
            node_id=data["node_id"],
            precision=data["precision"],
            registers=list(data["registers"]),
        )

    def __repr__(self) -> str:
        return (
            f"HyperLogLog(node={self.node_id}, "
            f"count≈{self.count()}, precision={self.precision}, "
            f"error≤{self.error_rate():.2%})"
        )

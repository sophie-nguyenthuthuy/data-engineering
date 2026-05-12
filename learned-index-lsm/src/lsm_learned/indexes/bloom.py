"""
Space-efficient Bloom filter using double-hashing (Kirsch & Mitzenmacher, 2008).

Two independent 64-bit MurmurHash3 seeds simulate k independent hash functions
without the cost of k separate hash calls.
"""

from __future__ import annotations

import math
import struct
from array import array

import mmh3


class BloomFilter:
    """
    Probabilistic set-membership test.

    Parameters
    ----------
    capacity:
        Expected number of elements to insert.
    fpr:
        Desired false-positive rate (0 < fpr < 1).
    """

    def __init__(self, capacity: int, fpr: float = 0.01) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if not 0 < fpr < 1:
            raise ValueError("fpr must be in (0, 1)")
        self._capacity = capacity
        self._fpr = fpr
        self._num_bits = self._optimal_bits(capacity, fpr)
        self._num_hashes = self._optimal_hashes(self._num_bits, capacity)
        # Use array of unsigned longs (64-bit words)
        words = (self._num_bits + 63) // 64
        self._bits: array[int] = array("Q", [0] * words)
        self._count = 0

    # ------------------------------------------------------------------
    # Core
    # ------------------------------------------------------------------

    def add(self, key: int) -> None:
        h1, h2 = self._hashes(key)
        for i in range(self._num_hashes):
            bit = (h1 + i * h2) % self._num_bits
            self._bits[bit >> 6] |= 1 << (bit & 63)
        self._count += 1

    def __contains__(self, key: int) -> bool:
        h1, h2 = self._hashes(key)
        for i in range(self._num_hashes):
            bit = (h1 + i * h2) % self._num_bits
            if not (self._bits[bit >> 6] >> (bit & 63)) & 1:
                return False
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _hashes(self, key: int) -> tuple[int, int]:
        raw = struct.pack("<q", key & 0x7FFFFFFFFFFFFFFF)
        h1 = mmh3.hash(raw, seed=0, signed=False)
        h2 = mmh3.hash(raw, seed=1, signed=False)
        return h1, h2 or 1  # h2 must be non-zero for double-hashing

    @staticmethod
    def _optimal_bits(n: int, p: float) -> int:
        return max(1, int(-n * math.log(p) / (math.log(2) ** 2)))

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        return max(1, round((m / n) * math.log(2)))

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def count(self) -> int:
        return self._count

    @property
    def bit_count(self) -> int:
        return self._num_bits

    @property
    def num_hashes(self) -> int:
        return self._num_hashes

    def estimated_fpr(self) -> float:
        k = self._num_hashes
        m = self._num_bits
        n = self._count
        if n == 0:
            return 0.0
        return (1 - math.exp(-k * n / m)) ** k

    def memory_bytes(self) -> int:
        return len(self._bits) * 8

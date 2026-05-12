"""Bloom filter using double hashing (Kirsch-Mitzenmacher optimization).

Uses two independent hash functions to simulate k hash functions:
  h_i(x) = h1(x) + i * h2(x)  mod m

False-positive rate: ~(1 - e^(-kn/m))^k
For k=7, n/m=0.01 → ~0.8% FPR.
"""
from __future__ import annotations

import hashlib
import math
from array import array


def _hash_pair(data: bytes) -> tuple[int, int]:
    """Two independent 64-bit hashes via SHA-256 halves."""
    digest = hashlib.sha256(data).digest()
    h1 = int.from_bytes(digest[:8], "little")
    h2 = int.from_bytes(digest[8:16], "little")
    return h1, h2


class BloomFilter:
    def __init__(self, capacity: int, fpr: float = 0.01):
        """
        capacity: expected number of elements
        fpr: desired false-positive rate
        """
        self.capacity = capacity
        self.fpr = fpr
        # Optimal bit count and hash count
        self.num_bits = max(1, math.ceil(-capacity * math.log(fpr) / math.log(2) ** 2))
        self.num_hashes = max(1, round(self.num_bits / capacity * math.log(2)))
        self._bits = array("B", [0]) * math.ceil(self.num_bits / 8)

    def _positions(self, data: bytes) -> list[int]:
        h1, h2 = _hash_pair(data)
        return [(h1 + i * h2) % self.num_bits for i in range(self.num_hashes)]

    def add(self, data: bytes) -> None:
        for pos in self._positions(data):
            self._bits[pos >> 3] |= 1 << (pos & 7)

    def may_contain(self, data: bytes) -> bool:
        return all(
            self._bits[pos >> 3] & (1 << (pos & 7))
            for pos in self._positions(data)
        )

    def to_bytes(self) -> bytes:
        import struct
        header = struct.pack(">IIH", self.capacity, self.num_bits, self.num_hashes)
        return header + bytes(self._bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> BloomFilter:
        import struct
        capacity, num_bits, num_hashes = struct.unpack_from(">IIH", data)
        bf = cls.__new__(cls)
        bf.capacity = capacity
        bf.fpr = 0.01
        bf.num_bits = num_bits
        bf.num_hashes = num_hashes
        header_size = struct.calcsize(">IIH")
        bf._bits = array("B", data[header_size:])
        return bf

    @classmethod
    def header_size(cls) -> int:
        import struct
        return struct.calcsize(">IIH")

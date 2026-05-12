"""Bit-level I/O used by Gorilla encoding."""
from __future__ import annotations


class BitWriter:
    def __init__(self) -> None:
        self._buf = bytearray()
        self._current = 0
        self._bits = 0

    def write_bit(self, bit: int) -> None:
        self._current = (self._current << 1) | (bit & 1)
        self._bits += 1
        if self._bits == 8:
            self._buf.append(self._current)
            self._current = 0
            self._bits = 0

    def write_bits(self, value: int, n: int) -> None:
        for i in range(n - 1, -1, -1):
            self.write_bit((value >> i) & 1)

    def finish(self) -> bytes:
        if self._bits:
            self._buf.append(self._current << (8 - self._bits))
        return bytes(self._buf)


class BitReader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._byte_pos = 0
        self._bit_pos = 7

    def read_bit(self) -> int:
        if self._byte_pos >= len(self._data):
            raise EOFError("BitReader exhausted")
        bit = (self._data[self._byte_pos] >> self._bit_pos) & 1
        self._bit_pos -= 1
        if self._bit_pos < 0:
            self._bit_pos = 7
            self._byte_pos += 1
        return bit

    def read_bits(self, n: int) -> int:
        result = 0
        for _ in range(n):
            result = (result << 1) | self.read_bit()
        return result

    def read_signed_bits(self, n: int) -> int:
        raw = self.read_bits(n)
        if raw >= (1 << (n - 1)):
            raw -= 1 << n
        return raw

"""Delta encoding for monotonically-increasing integer columns.

Stores ``values[0]`` followed by ``values[i] − values[i-1]`` for the
rest. NULLs are unsupported in this minimal version — the schema layer
swaps to plain encoding when null_count > 0.
"""

from __future__ import annotations

import struct


def delta_encode(values: list[int]) -> bytes:
    if not values:
        return b""
    if any(v is None for v in values):
        raise ValueError("delta_encode does not support NULL — use plain encoding instead")
    out = bytearray()
    out += struct.pack(">q", values[0])
    prev = values[0]
    for v in values[1:]:
        out += struct.pack(">q", v - prev)
        prev = v
    return bytes(out)


def delta_decode(buf: bytes) -> list[int]:
    if not buf:
        return []
    if len(buf) % 8 != 0:
        raise ValueError("delta buffer length not a multiple of 8")
    n = len(buf) // 8
    out: list[int] = []
    (first,) = struct.unpack_from(">q", buf, 0)
    out.append(first)
    cur = first
    for i in range(1, n):
        (delta,) = struct.unpack_from(">q", buf, i * 8)
        cur += delta
        out.append(cur)
    return out


__all__ = ["delta_decode", "delta_encode"]

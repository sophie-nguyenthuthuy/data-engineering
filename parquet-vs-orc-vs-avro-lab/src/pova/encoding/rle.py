"""Run-length encoding for repeating values.

Each run is encoded as a 4-byte length prefix + the plain-encoded
single value. Decoder repeats the value ``length`` times. NULL is
represented as a -1-length plain payload (matching the plain encoder).
"""

from __future__ import annotations

import struct
from typing import Any

from pova.encoding.plain import plain_decode, plain_encode

if False:  # pragma: no cover — forward refs only
    pass


def rle_encode(values: list[Any], ctype) -> bytes:  # type: ignore[no-untyped-def]
    out = bytearray()
    if not values:
        return bytes(out)
    cursor = 0
    while cursor < len(values):
        run_value = values[cursor]
        run_length = 1
        while (
            cursor + run_length < len(values)
            and values[cursor + run_length] == run_value
            and (run_value is None) == (values[cursor + run_length] is None)
        ):
            run_length += 1
        out += struct.pack(">I", run_length)
        out += plain_encode([run_value], ctype)
        cursor += run_length
    return bytes(out)


def rle_decode(buf: bytes, ctype) -> list[Any]:  # type: ignore[no-untyped-def]
    out: list[Any] = []
    cursor = 0
    while cursor < len(buf):
        if cursor + 4 > len(buf):
            raise ValueError("rle decoder truncated at run length")
        (run,) = struct.unpack_from(">I", buf, cursor)
        cursor += 4
        # Each run carries exactly one plain-encoded value. plain_decode
        # walks the buffer until exhausted, so we have to slice exactly
        # one value's worth of bytes for it.
        if cursor + 4 > len(buf):
            raise ValueError("rle decoder truncated at value length")
        (vlen,) = struct.unpack_from(">i", buf, cursor)
        slice_end = cursor + 4 + (vlen if vlen >= 0 else 0)
        if slice_end > len(buf):
            raise ValueError("rle decoder truncated at value payload")
        (value,) = plain_decode(buf[cursor:slice_end], ctype)
        cursor = slice_end
        out.extend([value] * run)
    return out


__all__ = ["rle_decode", "rle_encode"]

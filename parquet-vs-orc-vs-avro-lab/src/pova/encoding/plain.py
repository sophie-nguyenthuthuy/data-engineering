"""Plain (length-prefixed) encoding — the columnar-format baseline.

We serialise each value as a 4-byte big-endian length (so ``-1`` can
encode NULL) followed by the UTF-8 / struct-packed payload. Plain
encoding is bandwidth-heavy but gives the other encodings something
unambiguous to beat.
"""

from __future__ import annotations

import struct
from typing import Any

from pova.columnar.column import ColumnType


def plain_encode(values: list[Any], ctype: ColumnType) -> bytes:
    out = bytearray()
    for v in values:
        if v is None:
            out += struct.pack(">i", -1)
            continue
        body = _value_bytes(v, ctype)
        out += struct.pack(">i", len(body))
        out += body
    return bytes(out)


def plain_decode(buf: bytes, ctype: ColumnType) -> list[Any]:
    out: list[Any] = []
    cursor = 0
    while cursor < len(buf):
        if cursor + 4 > len(buf):
            raise ValueError("plain decoder truncated at length prefix")
        (length,) = struct.unpack_from(">i", buf, cursor)
        cursor += 4
        if length == -1:
            out.append(None)
            continue
        if length < 0:
            raise ValueError(f"plain decoder bad length {length}")
        if cursor + length > len(buf):
            raise ValueError("plain decoder truncated at payload")
        out.append(_bytes_value(buf[cursor : cursor + length], ctype))
        cursor += length
    return out


def _value_bytes(v: Any, ctype: ColumnType) -> bytes:
    if ctype is ColumnType.INT64:
        return struct.pack(">q", int(v))
    if ctype is ColumnType.FLOAT64:
        return struct.pack(">d", float(v))
    if ctype is ColumnType.BOOL:
        return b"\x01" if bool(v) else b"\x00"
    return str(v).encode("utf-8")


def _bytes_value(b: bytes, ctype: ColumnType) -> Any:
    if ctype is ColumnType.INT64:
        return struct.unpack(">q", b)[0]
    if ctype is ColumnType.FLOAT64:
        return struct.unpack(">d", b)[0]
    if ctype is ColumnType.BOOL:
        return b == b"\x01"
    return b.decode("utf-8")


__all__ = ["plain_decode", "plain_encode"]

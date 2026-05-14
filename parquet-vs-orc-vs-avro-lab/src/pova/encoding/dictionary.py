"""Dictionary encoding for low-cardinality columns.

Output layout::

    +-----------------+----------------+--------------+
    | dict-count (4B) | dict (plain)   | indices (4B*) |
    +-----------------+----------------+--------------+

Indices are 4-byte big-endian. NULL maps to index -1.
"""

from __future__ import annotations

import struct
from typing import Any

from pova.encoding.plain import plain_decode, plain_encode

if False:  # pragma: no cover — forward refs only
    pass


def dictionary_encode(values: list[Any], ctype) -> bytes:  # type: ignore[no-untyped-def]
    seen: dict[Any, int] = {}
    indices: list[int] = []
    dict_values: list[Any] = []
    for v in values:
        if v is None:
            indices.append(-1)
            continue
        key = (type(v).__name__, v)
        if key not in seen:
            seen[key] = len(dict_values)
            dict_values.append(v)
        indices.append(seen[key])
    dict_bytes = plain_encode(dict_values, ctype)
    out = bytearray()
    out += struct.pack(">I", len(dict_values))
    out += struct.pack(">I", len(dict_bytes))
    out += dict_bytes
    for idx in indices:
        out += struct.pack(">i", idx)
    return bytes(out)


def dictionary_decode(buf: bytes, ctype) -> list[Any]:  # type: ignore[no-untyped-def]
    if len(buf) < 8:
        raise ValueError("dictionary buffer too short for header")
    n_dict, dict_len = struct.unpack_from(">II", buf, 0)
    cursor = 8
    if cursor + dict_len > len(buf):
        raise ValueError("dictionary buffer truncated at dict body")
    dict_values = plain_decode(buf[cursor : cursor + dict_len], ctype)
    if len(dict_values) != n_dict:
        raise ValueError(
            f"dictionary header claims {n_dict} entries but decoded {len(dict_values)}"
        )
    cursor += dict_len
    out: list[Any] = []
    while cursor < len(buf):
        if cursor + 4 > len(buf):
            raise ValueError("dictionary indices truncated")
        (idx,) = struct.unpack_from(">i", buf, cursor)
        cursor += 4
        if idx == -1:
            out.append(None)
        elif 0 <= idx < n_dict:
            out.append(dict_values[idx])
        else:
            raise ValueError(f"dictionary index {idx} out of range [0, {n_dict})")
    return out


__all__ = ["dictionary_decode", "dictionary_encode"]

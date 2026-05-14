"""ORC-shaped layout.

ORC uses *stripes* (Parquet calls them row groups) but pushes column
statistics into an **index stream** that lives at the *start* of each
stripe (Parquet writes them at the end). Practically that means an
ORC reader can decide to skip a stripe after reading only its index
stream — handy for predicate pushdown over network connections where
seeking to the end is expensive.

This implementation mirrors that layout: each stripe begins with a
JSON-encoded per-column stats index, followed by the gzipped plain-
encoded column streams. The choice of plain encoding (vs Parquet-
style dictionary) is deliberate — ORC's primary win over Parquet is
the stats placement, not the encoding heuristics.
"""

from __future__ import annotations

import gzip
import json
import struct
from typing import Any

from pova.columnar.column import Column, ColumnType
from pova.columnar.schema import Schema
from pova.encoding.plain import plain_decode, plain_encode
from pova.stats.column import ColumnStats

MAGIC = b"ORCL"  # ORC-Lite


def orc_write(
    schema: Schema,
    columns: list[Column],
    *,
    stripe_size: int = 1024,
    gzip_level: int = 6,
) -> bytes:
    if stripe_size < 1:
        raise ValueError("stripe_size must be ≥ 1")
    schema.validate(columns)
    n_rows = len(columns[0])
    out = bytearray(MAGIC)
    # File-level postscript at the end carries schema + per-stripe offsets so
    # a smart reader can skip straight to the stripe whose index matters.
    stripe_offsets: list[int] = []
    for start in range(0, n_rows, stripe_size):
        end = min(n_rows, start + stripe_size)
        stripe_offsets.append(len(out))
        # 1) Build the column-level stats index for this stripe.
        index = []
        encoded_streams: list[bytes] = []
        for col in columns:
            values = list(col.values[start:end])
            stats = ColumnStats.from_values(values)
            index.append(
                {
                    "name": col.name,
                    "type": col.type.value,
                    "n_rows": stats.n_rows,
                    "null_count": stats.null_count,
                    "min": stats.min,
                    "max": stats.max,
                }
            )
            compressed = gzip.compress(plain_encode(values, col.type), compresslevel=gzip_level)
            encoded_streams.append(compressed)
        index_bytes = json.dumps(index, sort_keys=True).encode("utf-8")
        out += struct.pack(">I", len(index_bytes))
        out += index_bytes
        # 2) Column streams in schema order, length-prefixed.
        for stream in encoded_streams:
            out += struct.pack(">I", len(stream))
            out += stream
    postscript = {
        "schema": [(n, t.value) for n, t in schema.fields],
        "stripes": stripe_offsets,
    }
    ps_bytes = json.dumps(postscript, sort_keys=True).encode("utf-8")
    out += ps_bytes
    out += struct.pack(">I", len(ps_bytes))
    return bytes(out)


def orc_read(buf: bytes) -> tuple[Schema, list[Column]]:
    if not buf.startswith(MAGIC):
        raise ValueError("not an orc-like file (magic mismatch)")
    if len(buf) < 4:
        raise ValueError("file too short for postscript length")
    (ps_len,) = struct.unpack_from(">I", buf, len(buf) - 4)
    ps = json.loads(buf[len(buf) - 4 - ps_len : len(buf) - 4].decode("utf-8"))
    schema = Schema(fields=tuple((n, ColumnType(t)) for n, t in ps["schema"]))
    accum: dict[str, list[Any]] = {name: [] for name, _ in ps["schema"]}
    for offset in ps["stripes"]:
        cursor = offset
        (idx_len,) = struct.unpack_from(">I", buf, cursor)
        cursor += 4
        index = json.loads(buf[cursor : cursor + idx_len].decode("utf-8"))
        cursor += idx_len
        for col_meta in index:
            (clen,) = struct.unpack_from(">I", buf, cursor)
            cursor += 4
            compressed = buf[cursor : cursor + clen]
            cursor += clen
            raw = gzip.decompress(compressed)
            values = plain_decode(raw, ColumnType(col_meta["type"]))
            accum[col_meta["name"]].extend(values)
    columns = [Column(name=n, type=ColumnType(t), values=tuple(accum[n])) for n, t in ps["schema"]]
    return schema, columns


__all__ = ["orc_read", "orc_write"]

"""Parquet-shaped layout.

A tiny, didactic Parquet-like encoder: file is a sequence of *row
groups*; each row group is a sequence of *column chunks*; each column
chunk carries the encoded bytes for one column plus a stats footer
(min, max, null_count). gzip is applied to the encoded bytes — the
real Parquet uses snappy/zstd/gzip; we always pick gzip because it
ships with stdlib.

The point of this lab isn't byte compatibility with Apache Parquet —
it's exposing the three trade-offs:

  1. row groups → per-chunk stats → predicate pushdown.
  2. per-column encoding → dictionary works wonders on low-cardinality.
  3. file header carries the schema once.

A real Parquet file is several thousand additional lines of Thrift
metadata; this implementation surfaces the *shape* in 150 lines.
"""

from __future__ import annotations

import gzip
import json
import struct
from dataclasses import dataclass
from typing import Any

from pova.columnar.column import Column, ColumnType
from pova.columnar.schema import Schema
from pova.encoding.dictionary import dictionary_decode, dictionary_encode
from pova.encoding.plain import plain_decode, plain_encode
from pova.encoding.rle import rle_decode, rle_encode
from pova.stats.column import ColumnStats

MAGIC = b"PRQT"  # "PRQT" — parquet-like magic; not Apache Parquet's "PAR1".


@dataclass(frozen=True, slots=True)
class ColumnChunkMeta:
    """Footer metadata for one column chunk."""

    name: str
    type: ColumnType
    encoding: str
    n_rows: int
    null_count: int
    min: Any | None
    max: Any | None
    compressed_size: int
    uncompressed_size: int


def _pick_encoding(values: list[Any], ctype: ColumnType) -> str:
    """Pick the encoding heuristically — same heuristic Parquet uses."""
    if not values:
        return "plain"
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "plain"
    distinct = len(set(non_null))
    if distinct <= max(1, len(non_null) // 8) and ctype in (ColumnType.STRING, ColumnType.INT64):
        return "dictionary"
    # Heavy run-length wins when most adjacent values agree.
    from itertools import pairwise

    same = sum(1 for a, b in pairwise(non_null) if a == b)
    if same * 2 > len(non_null):
        return "rle"
    return "plain"


def _encode(values: list[Any], ctype: ColumnType, encoding: str) -> bytes:
    if encoding == "plain":
        return plain_encode(values, ctype)
    if encoding == "rle":
        return rle_encode(values, ctype)
    if encoding == "dictionary":
        return dictionary_encode(values, ctype)
    raise ValueError(f"unknown encoding {encoding!r}")


def _decode(buf: bytes, ctype: ColumnType, encoding: str) -> list[Any]:
    if encoding == "plain":
        return plain_decode(buf, ctype)
    if encoding == "rle":
        return rle_decode(buf, ctype)
    if encoding == "dictionary":
        return dictionary_decode(buf, ctype)
    raise ValueError(f"unknown encoding {encoding!r}")


def parquet_write(
    schema: Schema,
    columns: list[Column],
    *,
    row_group_size: int = 1024,
    gzip_level: int = 6,
) -> bytes:
    if row_group_size < 1:
        raise ValueError("row_group_size must be ≥ 1")
    schema.validate(columns)
    n_rows = len(columns[0])
    out = bytearray(MAGIC)
    row_groups_meta: list[list[ColumnChunkMeta]] = []
    for start in range(0, n_rows, row_group_size):
        end = min(n_rows, start + row_group_size)
        chunks_meta: list[ColumnChunkMeta] = []
        for col in columns:
            values = list(col.values[start:end])
            stats = ColumnStats.from_values(values)
            enc = _pick_encoding(values, col.type)
            raw = _encode(values, col.type, enc)
            compressed = gzip.compress(raw, compresslevel=gzip_level)
            out += struct.pack(">I", len(compressed))
            out += compressed
            chunks_meta.append(
                ColumnChunkMeta(
                    name=col.name,
                    type=col.type,
                    encoding=enc,
                    n_rows=stats.n_rows,
                    null_count=stats.null_count,
                    min=stats.min,
                    max=stats.max,
                    compressed_size=len(compressed),
                    uncompressed_size=len(raw),
                )
            )
        row_groups_meta.append(chunks_meta)
    footer = {
        "schema": [(n, t.value) for n, t in schema.fields],
        "row_groups": [
            [
                {
                    "name": m.name,
                    "type": m.type.value,
                    "encoding": m.encoding,
                    "n_rows": m.n_rows,
                    "null_count": m.null_count,
                    "min": m.min,
                    "max": m.max,
                    "compressed_size": m.compressed_size,
                    "uncompressed_size": m.uncompressed_size,
                }
                for m in rg
            ]
            for rg in row_groups_meta
        ],
    }
    footer_bytes = json.dumps(footer, sort_keys=True).encode("utf-8")
    out += footer_bytes
    out += struct.pack(">I", len(footer_bytes))
    return bytes(out)


def parquet_read(buf: bytes) -> tuple[Schema, list[Column]]:
    if not buf.startswith(MAGIC):
        raise ValueError("not a parquet-like file (magic mismatch)")
    if len(buf) < 4:
        raise ValueError("file too short for footer length")
    (footer_len,) = struct.unpack_from(">I", buf, len(buf) - 4)
    if footer_len <= 0 or footer_len > len(buf) - len(MAGIC) - 4:
        raise ValueError("invalid footer length")
    footer = json.loads(buf[len(buf) - 4 - footer_len : len(buf) - 4].decode("utf-8"))
    schema = Schema(fields=tuple((n, ColumnType(t)) for n, t in footer["schema"]))
    # Decode column chunks back into per-column accumulators.
    cursor = len(MAGIC)
    accum: dict[str, list[Any]] = {name: [] for name, _ in footer["schema"]}
    for rg in footer["row_groups"]:
        for meta in rg:
            (clen,) = struct.unpack_from(">I", buf, cursor)
            cursor += 4
            compressed = buf[cursor : cursor + clen]
            cursor += clen
            raw = gzip.decompress(compressed)
            values = _decode(raw, ColumnType(meta["type"]), meta["encoding"])
            accum[meta["name"]].extend(values)
    columns = [
        Column(name=n, type=ColumnType(t), values=tuple(accum[n])) for n, t in footer["schema"]
    ]
    return schema, columns


__all__ = ["ColumnChunkMeta", "parquet_read", "parquet_write"]

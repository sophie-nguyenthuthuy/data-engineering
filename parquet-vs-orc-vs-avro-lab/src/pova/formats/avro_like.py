"""Avro-shaped layout.

Avro is *row-oriented* — every record is serialised in full before the
next one begins, with the schema written once at the top of the file.
This implementation mirrors that shape: a JSON header carries the
schema, then each record is plain-encoded row-by-row. Compression is
applied to the entire data block.

The trade-off: Avro reads cheaply when you want every column of a few
records (OLTP-ish), but loses to Parquet/ORC when only a subset of
columns is projected (because you have to scan past every byte of
unwanted columns).
"""

from __future__ import annotations

import gzip
import json
import struct
from typing import Any

from pova.columnar.column import Column, ColumnType
from pova.columnar.schema import Schema
from pova.encoding.plain import plain_encode

MAGIC = b"AVRL"  # Avro-Lite


def avro_write(
    schema: Schema,
    columns: list[Column],
    *,
    gzip_level: int = 6,
) -> bytes:
    schema.validate(columns)
    n_rows = len(columns[0])
    out = bytearray(MAGIC)
    header = {
        "schema": [(n, t.value) for n, t in schema.fields],
        "n_rows": n_rows,
    }
    header_bytes = json.dumps(header, sort_keys=True).encode("utf-8")
    out += struct.pack(">I", len(header_bytes))
    out += header_bytes
    # Pack rows: for each row, emit each column's value plain-encoded.
    body = bytearray()
    for i in range(n_rows):
        for col in columns:
            body += plain_encode([col.values[i]], col.type)
    compressed = gzip.compress(bytes(body), compresslevel=gzip_level)
    out += struct.pack(">I", len(compressed))
    out += compressed
    return bytes(out)


def avro_read(buf: bytes) -> tuple[Schema, list[Column]]:
    if not buf.startswith(MAGIC):
        raise ValueError("not an avro-like file (magic mismatch)")
    cursor = len(MAGIC)
    if cursor + 4 > len(buf):
        raise ValueError("avro-like file too short for header length")
    (header_len,) = struct.unpack_from(">I", buf, cursor)
    cursor += 4
    header = json.loads(buf[cursor : cursor + header_len].decode("utf-8"))
    cursor += header_len
    schema = Schema(fields=tuple((n, ColumnType(t)) for n, t in header["schema"]))
    n_rows = int(header["n_rows"])
    if cursor + 4 > len(buf):
        raise ValueError("avro-like file truncated at body length")
    (body_len,) = struct.unpack_from(">I", buf, cursor)
    cursor += 4
    body = gzip.decompress(buf[cursor : cursor + body_len])
    # Decode rows back into per-column accumulators.
    from pova.encoding.plain import plain_decode

    accum: dict[str, list[Any]] = {name: [] for name, _ in header["schema"]}
    b_cursor = 0
    for _ in range(n_rows):
        for col_name, col_type in header["schema"]:
            # plain_encode wrote (length, payload) for the single value.
            (vlen,) = struct.unpack_from(">i", body, b_cursor)
            chunk = body[b_cursor : b_cursor + 4 + (vlen if vlen >= 0 else 0)]
            (value,) = plain_decode(chunk, ColumnType(col_type))
            accum[col_name].append(value)
            b_cursor += len(chunk)
    columns = [
        Column(name=n, type=ColumnType(t), values=tuple(accum[n])) for n, t in header["schema"]
    ]
    return schema, columns


__all__ = ["avro_read", "avro_write"]

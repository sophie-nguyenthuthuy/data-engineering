"""pgoutput message types.

The Postgres logical-replication output plugin ``pgoutput`` ships a
small set of message types. Each message starts with a single byte
identifying the kind, followed by big-endian binary fields. We
implement the slice every CDC consumer needs:

  * ``B`` — :class:`BeginMessage` (transaction start).
  * ``C`` — :class:`CommitMessage` (transaction commit).
  * ``R`` — :class:`RelationMessage` (table schema).
  * ``I`` — :class:`InsertMessage`.
  * ``U`` — :class:`UpdateMessage`.
  * ``D`` — :class:`DeleteMessage`.
  * ``T`` — :class:`TruncateMessage`.

See https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
for the full grammar.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum


class TupleColumnKind(IntEnum):
    """Column-data byte that prefixes each value in a pgoutput tuple."""

    NULL = ord("n")  # 'n' — NULL
    UNCHANGED = ord("u")  # 'u' — TOAST unchanged, value omitted
    TEXT = ord("t")  # 't' — text-formatted value
    BINARY = ord("b")  # 'b' — binary-formatted value


@dataclass(frozen=True, slots=True)
class TupleColumn:
    """One column slot inside a :class:`TupleData`."""

    kind: TupleColumnKind
    value: bytes | None = None  # ``None`` for ``NULL`` / ``UNCHANGED``


@dataclass(frozen=True, slots=True)
class TupleData:
    """A wire-format tuple (column count + per-column slots)."""

    columns: tuple[TupleColumn, ...]


def _decode_tuple(buf: bytes, offset: int) -> tuple[TupleData, int]:
    if offset + 2 > len(buf):
        raise ValueError("truncated tuple column count")
    (ncols,) = struct.unpack_from(">H", buf, offset)
    offset += 2
    cols: list[TupleColumn] = []
    for _ in range(ncols):
        if offset + 1 > len(buf):
            raise ValueError("truncated tuple column kind byte")
        kind_byte = buf[offset]
        offset += 1
        try:
            kind = TupleColumnKind(kind_byte)
        except ValueError as exc:
            raise ValueError(f"unknown tuple column kind 0x{kind_byte:02x}") from exc
        if kind in (TupleColumnKind.NULL, TupleColumnKind.UNCHANGED):
            cols.append(TupleColumn(kind=kind))
            continue
        if offset + 4 > len(buf):
            raise ValueError("truncated tuple column length")
        (length,) = struct.unpack_from(">i", buf, offset)
        offset += 4
        if length < 0:
            raise ValueError(f"negative column length {length}")
        if offset + length > len(buf):
            raise ValueError("truncated tuple column value")
        value = buf[offset : offset + length]
        offset += length
        cols.append(TupleColumn(kind=kind, value=value))
    return TupleData(columns=tuple(cols)), offset


def _read_cstring(buf: bytes, offset: int) -> tuple[str, int]:
    end = buf.find(b"\x00", offset)
    if end == -1:
        raise ValueError("unterminated C string")
    return buf[offset:end].decode("utf-8", errors="replace"), end + 1


# --------------------------------------------------------------- Begin


@dataclass(frozen=True, slots=True)
class BeginMessage:
    """``B`` — transaction begin."""

    final_lsn: int  # commit LSN of this transaction
    timestamp_us: int  # microseconds since 2000-01-01 (PG epoch)
    xid: int

    @classmethod
    def decode(cls, buf: bytes) -> BeginMessage:
        if len(buf) < 20:
            raise ValueError("BEGIN message too short")
        lsn, ts, xid = struct.unpack_from(">QqI", buf, 0)
        return cls(final_lsn=lsn, timestamp_us=ts, xid=xid)


# --------------------------------------------------------------- Commit


@dataclass(frozen=True, slots=True)
class CommitMessage:
    """``C`` — transaction commit."""

    flags: int
    commit_lsn: int
    end_lsn: int
    timestamp_us: int

    @classmethod
    def decode(cls, buf: bytes) -> CommitMessage:
        if len(buf) < 1 + 8 + 8 + 8:
            raise ValueError("COMMIT message too short")
        flags = buf[0]
        commit_lsn, end_lsn, ts = struct.unpack_from(">QQq", buf, 1)
        return cls(flags=flags, commit_lsn=commit_lsn, end_lsn=end_lsn, timestamp_us=ts)


# ------------------------------------------------------------- Relation


@dataclass(frozen=True, slots=True)
class RelationColumn:
    """One column of a :class:`RelationMessage`."""

    flags: int
    name: str
    type_oid: int
    type_modifier: int

    @property
    def is_key(self) -> bool:
        return bool(self.flags & 0x01)


@dataclass(frozen=True, slots=True)
class RelationMessage:
    """``R`` — table schema reference."""

    relation_id: int
    namespace: str
    name: str
    replica_identity: str
    columns: tuple[RelationColumn, ...]

    @classmethod
    def decode(cls, buf: bytes) -> RelationMessage:
        if len(buf) < 4:
            raise ValueError("RELATION message too short")
        (rel_id,) = struct.unpack_from(">I", buf, 0)
        ns, off = _read_cstring(buf, 4)
        name, off = _read_cstring(buf, off)
        if off + 1 > len(buf):
            raise ValueError("RELATION truncated at replica_identity")
        replica_identity = chr(buf[off])
        off += 1
        if off + 2 > len(buf):
            raise ValueError("RELATION truncated at column count")
        (ncols,) = struct.unpack_from(">H", buf, off)
        off += 2
        cols: list[RelationColumn] = []
        for _ in range(ncols):
            if off + 1 > len(buf):
                raise ValueError("RELATION column truncated at flags")
            flags = buf[off]
            off += 1
            col_name, off = _read_cstring(buf, off)
            if off + 8 > len(buf):
                raise ValueError("RELATION column truncated at oid/typmod")
            type_oid, type_mod = struct.unpack_from(">Ii", buf, off)
            off += 8
            cols.append(
                RelationColumn(
                    flags=flags,
                    name=col_name,
                    type_oid=type_oid,
                    type_modifier=type_mod,
                )
            )
        return cls(
            relation_id=rel_id,
            namespace=ns,
            name=name,
            replica_identity=replica_identity,
            columns=tuple(cols),
        )


# ---------------------------------------------------------- Insert


@dataclass(frozen=True, slots=True)
class InsertMessage:
    """``I`` — insert into ``relation_id``."""

    relation_id: int
    new_tuple: TupleData

    @classmethod
    def decode(cls, buf: bytes) -> InsertMessage:
        if len(buf) < 4 + 1:
            raise ValueError("INSERT message too short")
        (rel_id,) = struct.unpack_from(">I", buf, 0)
        marker = buf[4]
        if marker != ord("N"):
            raise ValueError(f"INSERT expected 'N' marker, got 0x{marker:02x}")
        new_tuple, _ = _decode_tuple(buf, 5)
        return cls(relation_id=rel_id, new_tuple=new_tuple)


# ---------------------------------------------------------- Update


@dataclass(frozen=True, slots=True)
class UpdateMessage:
    """``U`` — update with old + new tuples (old may be absent)."""

    relation_id: int
    old_tuple: TupleData | None
    new_tuple: TupleData

    @classmethod
    def decode(cls, buf: bytes) -> UpdateMessage:
        if len(buf) < 5:
            raise ValueError("UPDATE message too short")
        (rel_id,) = struct.unpack_from(">I", buf, 0)
        cursor = 4
        old_tuple: TupleData | None = None
        # 'K' = key-only old tuple; 'O' = full old tuple; absent = none.
        marker = buf[cursor]
        if marker in (ord("K"), ord("O")):
            cursor += 1
            old_tuple, cursor = _decode_tuple(buf, cursor)
            if cursor >= len(buf):
                raise ValueError("UPDATE missing new-tuple marker")
            marker = buf[cursor]
        if marker != ord("N"):
            raise ValueError(f"UPDATE expected 'N' marker, got 0x{marker:02x}")
        cursor += 1
        new_tuple, _ = _decode_tuple(buf, cursor)
        return cls(relation_id=rel_id, old_tuple=old_tuple, new_tuple=new_tuple)


# ---------------------------------------------------------- Delete


@dataclass(frozen=True, slots=True)
class DeleteMessage:
    """``D`` — delete with old tuple (key-only or full)."""

    relation_id: int
    old_tuple: TupleData
    is_key_only: bool

    @classmethod
    def decode(cls, buf: bytes) -> DeleteMessage:
        if len(buf) < 5:
            raise ValueError("DELETE message too short")
        (rel_id,) = struct.unpack_from(">I", buf, 0)
        marker = buf[4]
        if marker not in (ord("K"), ord("O")):
            raise ValueError(f"DELETE expected 'K'|'O' marker, got 0x{marker:02x}")
        old_tuple, _ = _decode_tuple(buf, 5)
        return cls(relation_id=rel_id, old_tuple=old_tuple, is_key_only=(marker == ord("K")))


# ---------------------------------------------------------- Truncate


@dataclass(frozen=True, slots=True)
class TruncateMessage:
    """``T`` — truncate one or more relations."""

    flags: int
    relation_ids: tuple[int, ...]

    @classmethod
    def decode(cls, buf: bytes) -> TruncateMessage:
        if len(buf) < 5:
            raise ValueError("TRUNCATE message too short")
        (nrel,) = struct.unpack_from(">I", buf, 0)
        flags = buf[4]
        cursor = 5
        if cursor + 4 * nrel > len(buf):
            raise ValueError("TRUNCATE truncated at relation list")
        ids = tuple(struct.unpack_from(">I", buf, cursor + 4 * i)[0] for i in range(nrel))
        return cls(flags=flags, relation_ids=ids)


__all__ = [
    "BeginMessage",
    "CommitMessage",
    "DeleteMessage",
    "InsertMessage",
    "RelationColumn",
    "RelationMessage",
    "TruncateMessage",
    "TupleColumn",
    "TupleColumnKind",
    "TupleData",
    "UpdateMessage",
]

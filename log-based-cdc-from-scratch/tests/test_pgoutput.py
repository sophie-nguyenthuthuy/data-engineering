"""pgoutput decoder tests."""

from __future__ import annotations

import struct

import pytest

from lcdc.postgres.messages import (
    BeginMessage,
    CommitMessage,
    DeleteMessage,
    InsertMessage,
    RelationMessage,
    TruncateMessage,
    TupleColumnKind,
    UpdateMessage,
)
from lcdc.postgres.reader import PgOutputDecodeError, PgOutputReader

# ---------------------------------------------------------------- helpers


def _tuple_bytes(columns: list[tuple[str, bytes | None]]) -> bytes:
    """Build a pgoutput tuple from ``(kind, value_or_None)`` pairs."""
    out = struct.pack(">H", len(columns))
    for kind, val in columns:
        out += kind.encode("ascii")
        if kind in ("n", "u"):
            continue
        assert val is not None
        out += struct.pack(">i", len(val)) + val
    return out


# --------------------------------------------------------------- Begin


def test_begin_decodes():
    body = struct.pack(">QqI", 0x1234, 1000, 42)
    msg = PgOutputReader.decode(b"B" + body)
    assert isinstance(msg, BeginMessage)
    assert msg.final_lsn == 0x1234
    assert msg.xid == 42


def test_begin_rejects_short_body():
    with pytest.raises(PgOutputDecodeError):
        PgOutputReader.decode(b"B" + b"\x00" * 5)


# --------------------------------------------------------------- Commit


def test_commit_decodes():
    body = bytes([0]) + struct.pack(">QQq", 100, 200, 300)
    msg = PgOutputReader.decode(b"C" + body)
    assert isinstance(msg, CommitMessage)
    assert msg.commit_lsn == 100
    assert msg.end_lsn == 200


# ----------------------------------------------------------- Relation


def test_relation_decodes():
    body = (
        struct.pack(">I", 16384)  # rel id
        + b"public\x00"
        + b"users\x00"
        + b"d"  # replica identity 'd'
        + struct.pack(">H", 2)  # 2 columns
        + b"\x01"  # flags = is-key
        + b"id\x00"
        + struct.pack(">Ii", 23, -1)  # int4 oid, no typmod
        + b"\x00"  # flags
        + b"name\x00"
        + struct.pack(">Ii", 25, -1)
    )
    msg = PgOutputReader.decode(b"R" + body)
    assert isinstance(msg, RelationMessage)
    assert msg.namespace == "public"
    assert msg.name == "users"
    assert msg.replica_identity == "d"
    assert msg.columns[0].name == "id"
    assert msg.columns[0].is_key
    assert msg.columns[1].name == "name"
    assert not msg.columns[1].is_key


def test_relation_unterminated_cstring_raises():
    body = struct.pack(">I", 1) + b"missing-nul"
    with pytest.raises(PgOutputDecodeError):
        PgOutputReader.decode(b"R" + body)


# -------------------------------------------------------------- Insert


def test_insert_decodes_text_and_null_columns():
    tup = _tuple_bytes([("t", b"42"), ("n", None), ("t", b"alice")])
    body = struct.pack(">I", 16384) + b"N" + tup
    msg = PgOutputReader.decode(b"I" + body)
    assert isinstance(msg, InsertMessage)
    assert len(msg.new_tuple.columns) == 3
    assert msg.new_tuple.columns[0].value == b"42"
    assert msg.new_tuple.columns[1].kind == TupleColumnKind.NULL
    assert msg.new_tuple.columns[2].value == b"alice"


def test_insert_rejects_missing_new_marker():
    body = struct.pack(">I", 1) + b"X" + _tuple_bytes([])
    with pytest.raises(PgOutputDecodeError):
        PgOutputReader.decode(b"I" + body)


# -------------------------------------------------------------- Update


def test_update_with_key_only_old_tuple():
    old = _tuple_bytes([("t", b"7")])
    new = _tuple_bytes([("t", b"7"), ("t", b"bob")])
    body = struct.pack(">I", 16384) + b"K" + old + b"N" + new
    msg = PgOutputReader.decode(b"U" + body)
    assert isinstance(msg, UpdateMessage)
    assert msg.old_tuple is not None
    assert msg.old_tuple.columns[0].value == b"7"
    assert msg.new_tuple.columns[1].value == b"bob"


def test_update_without_old_tuple_when_replica_identity_default():
    """For REPLICA IDENTITY DEFAULT updates the old tuple may be omitted."""
    new = _tuple_bytes([("t", b"42")])
    body = struct.pack(">I", 16384) + b"N" + new
    msg = PgOutputReader.decode(b"U" + body)
    assert msg.old_tuple is None


# -------------------------------------------------------------- Delete


def test_delete_with_key_old_tuple():
    old = _tuple_bytes([("t", b"99")])
    body = struct.pack(">I", 16384) + b"K" + old
    msg = PgOutputReader.decode(b"D" + body)
    assert isinstance(msg, DeleteMessage)
    assert msg.is_key_only
    assert msg.old_tuple.columns[0].value == b"99"


def test_delete_rejects_unknown_marker():
    body = struct.pack(">I", 1) + b"X" + _tuple_bytes([])
    with pytest.raises(PgOutputDecodeError):
        PgOutputReader.decode(b"D" + body)


# ----------------------------------------------------------- Truncate


def test_truncate_lists_relation_ids():
    body = struct.pack(">I", 3) + b"\x00" + struct.pack(">III", 16384, 16385, 16386)
    msg = PgOutputReader.decode(b"T" + body)
    assert isinstance(msg, TruncateMessage)
    assert msg.relation_ids == (16384, 16385, 16386)


def test_truncate_rejects_truncated_payload():
    # Claims 5 relations but supplies bytes for fewer.
    body = struct.pack(">I", 5) + b"\x00" + b"\x00" * 4
    with pytest.raises(PgOutputDecodeError):
        PgOutputReader.decode(b"T" + body)


# ------------------------------------------------------- dispatcher


def test_decode_rejects_empty_payload():
    with pytest.raises(PgOutputDecodeError):
        PgOutputReader.decode(b"")


def test_decode_rejects_unknown_tag():
    with pytest.raises(PgOutputDecodeError):
        PgOutputReader.decode(b"Z")


def test_iter_messages_streams_multiple_payloads():
    begin = b"B" + struct.pack(">QqI", 1, 0, 1)
    commit = b"C" + bytes([0]) + struct.pack(">QQq", 1, 1, 0)
    out = list(PgOutputReader().iter_messages([begin, commit]))
    assert isinstance(out[0], BeginMessage)
    assert isinstance(out[1], CommitMessage)

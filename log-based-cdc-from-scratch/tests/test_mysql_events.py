"""MySQL event-payload decoder tests."""

from __future__ import annotations

import struct

import pytest

from lcdc.mysql.events import (
    EventType,
    QueryEvent,
    RotateEvent,
    RowsEvent,
    RowsEventKind,
    TableMapEvent,
    XidEvent,
)

# ---------------------------------------------------------------- Rotate


def test_rotate_decodes_position_and_file():
    payload = struct.pack("<Q", 0xDEADBEEF) + b"binlog.000002"
    ev = RotateEvent.decode(payload)
    assert ev.next_position == 0xDEADBEEF
    assert ev.next_file == "binlog.000002"


def test_rotate_rejects_short_payload():
    with pytest.raises(ValueError):
        RotateEvent.decode(b"\x00" * 4)


def test_rotate_rejects_empty_filename():
    with pytest.raises(ValueError):
        RotateEvent.decode(struct.pack("<Q", 1) + b"")


# ---------------------------------------------------------------- Query


def _build_query(schema: str, query: str, status: bytes = b"") -> bytes:
    schema_b = schema.encode()
    query_b = query.encode()
    head = struct.pack("<IIBHH", 1, 0, len(schema_b), 0, len(status))
    return head + status + schema_b + b"\x00" + query_b


def test_query_event_basic():
    payload = _build_query("mydb", "BEGIN")
    ev = QueryEvent.decode(payload)
    assert ev.schema == "mydb"
    assert ev.query == "BEGIN"
    assert ev.error_code == 0


def test_query_event_ddl():
    payload = _build_query("mydb", "CREATE TABLE t (id int)")
    ev = QueryEvent.decode(payload)
    assert ev.query.startswith("CREATE TABLE")


def test_query_event_rejects_short_payload():
    with pytest.raises(ValueError):
        QueryEvent.decode(b"\x00" * 5)


# ---------------------------------------------------------------- XID


def test_xid_decode():
    ev = XidEvent.decode(struct.pack("<Q", 12345))
    assert ev.xid == 12345


def test_xid_rejects_short():
    with pytest.raises(ValueError):
        XidEvent.decode(b"\x00" * 4)


# ---------------------------------------------------------- TableMap


def _build_table_map(schema: str, table: str, col_types: list[int]) -> bytes:
    table_id = (0x010203 << 8) | 0x04  # arbitrary 6-byte id
    out = bytearray()
    out += table_id.to_bytes(6, "little")
    out += (0x0001).to_bytes(2, "little")  # flags
    out += bytes([len(schema)]) + schema.encode() + b"\x00"
    out += bytes([len(table)]) + table.encode() + b"\x00"
    # length-encoded column count + column types
    n = len(col_types)
    if n < 0xFB:
        out += bytes([n])
    else:
        out += b"\xfc" + n.to_bytes(2, "little")
    out += bytes(col_types)
    return bytes(out)


def test_table_map_decodes_schema_and_columns():
    payload = _build_table_map("mydb", "orders", [3, 15, 12])  # INT, VARCHAR, DATETIME
    ev = TableMapEvent.decode(payload)
    assert ev.schema == "mydb"
    assert ev.table == "orders"
    assert ev.column_types == (3, 15, 12)
    assert ev.flags == 1


def test_table_map_rejects_short():
    with pytest.raises(ValueError):
        TableMapEvent.decode(b"\x00" * 5)


# ---------------------------------------------------------- Rows


def _build_rows_v2(ncols: int, body: bytes, *, update: bool = False) -> bytes:
    out = bytearray()
    out += (0x10).to_bytes(6, "little")  # table_id
    out += (0).to_bytes(2, "little")  # flags
    out += (2).to_bytes(2, "little")  # extra length = 2 (just the length bytes)
    out += bytes([ncols])  # length-enc ncols
    bitmap_bytes = (ncols + 7) // 8
    out += b"\xff" * bitmap_bytes  # all columns included
    if update:
        out += b"\xff" * bitmap_bytes  # second bitmap for UPDATE
    out += body
    return bytes(out)


def test_rows_insert_decodes():
    payload = _build_rows_v2(2, b"\x01\x02\x03")
    ev = RowsEvent.decode(payload, RowsEventKind.INSERT)
    assert ev.kind == RowsEventKind.INSERT
    assert ev.table_id == 0x10
    assert ev.column_count == 2
    assert ev.image == b"\x01\x02\x03"
    assert ev.after_image is None


def test_rows_update_splits_image():
    payload = _build_rows_v2(2, b"AAAABBBB", update=True)
    ev = RowsEvent.decode(payload, RowsEventKind.UPDATE)
    assert ev.kind == RowsEventKind.UPDATE
    assert ev.image == b"AAAA"
    assert ev.after_image == b"BBBB"


def test_rows_delete_no_after_image():
    payload = _build_rows_v2(2, b"\xaa\xbb")
    ev = RowsEvent.decode(payload, RowsEventKind.DELETE)
    assert ev.kind == RowsEventKind.DELETE
    assert ev.image == b"\xaa\xbb"
    assert ev.after_image is None


def test_rows_rejects_truncated_payload():
    with pytest.raises(ValueError):
        RowsEvent.decode(b"\x00" * 5, RowsEventKind.INSERT)


def test_eventtype_enum_values():
    assert EventType.WRITE_ROWS_V2.value == 0x1E
    assert EventType.UPDATE_ROWS_V2.value == 0x1F
    assert EventType.DELETE_ROWS_V2.value == 0x20

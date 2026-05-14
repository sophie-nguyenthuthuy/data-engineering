"""log-based-cdc-from-scratch — protocol-level CDC readers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from lcdc.lsn import LSN, BinlogPosition
    from lcdc.mysql.events import (
        EventType,
        QueryEvent,
        RotateEvent,
        RowsEvent,
        RowsEventKind,
        TableMapEvent,
        XidEvent,
    )
    from lcdc.mysql.header import HEADER_LEN, EventHeader
    from lcdc.mysql.reader import BinlogReader, MagicHeaderError
    from lcdc.postgres.messages import (
        BeginMessage,
        CommitMessage,
        DeleteMessage,
        InsertMessage,
        RelationMessage,
        TruncateMessage,
        TupleData,
        UpdateMessage,
    )
    from lcdc.postgres.reader import PgOutputDecodeError, PgOutputReader


_LAZY: dict[str, tuple[str, str]] = {
    "LSN": ("lcdc.lsn", "LSN"),
    "BinlogPosition": ("lcdc.lsn", "BinlogPosition"),
    "EventHeader": ("lcdc.mysql.header", "EventHeader"),
    "HEADER_LEN": ("lcdc.mysql.header", "HEADER_LEN"),
    "EventType": ("lcdc.mysql.events", "EventType"),
    "QueryEvent": ("lcdc.mysql.events", "QueryEvent"),
    "RotateEvent": ("lcdc.mysql.events", "RotateEvent"),
    "TableMapEvent": ("lcdc.mysql.events", "TableMapEvent"),
    "XidEvent": ("lcdc.mysql.events", "XidEvent"),
    "RowsEvent": ("lcdc.mysql.events", "RowsEvent"),
    "RowsEventKind": ("lcdc.mysql.events", "RowsEventKind"),
    "BinlogReader": ("lcdc.mysql.reader", "BinlogReader"),
    "MagicHeaderError": ("lcdc.mysql.reader", "MagicHeaderError"),
    "BeginMessage": ("lcdc.postgres.messages", "BeginMessage"),
    "CommitMessage": ("lcdc.postgres.messages", "CommitMessage"),
    "RelationMessage": ("lcdc.postgres.messages", "RelationMessage"),
    "InsertMessage": ("lcdc.postgres.messages", "InsertMessage"),
    "UpdateMessage": ("lcdc.postgres.messages", "UpdateMessage"),
    "DeleteMessage": ("lcdc.postgres.messages", "DeleteMessage"),
    "TruncateMessage": ("lcdc.postgres.messages", "TruncateMessage"),
    "TupleData": ("lcdc.postgres.messages", "TupleData"),
    "PgOutputReader": ("lcdc.postgres.reader", "PgOutputReader"),
    "PgOutputDecodeError": ("lcdc.postgres.reader", "PgOutputDecodeError"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "HEADER_LEN",
    "LSN",
    "BeginMessage",
    "BinlogPosition",
    "BinlogReader",
    "CommitMessage",
    "DeleteMessage",
    "EventHeader",
    "EventType",
    "InsertMessage",
    "MagicHeaderError",
    "PgOutputDecodeError",
    "PgOutputReader",
    "QueryEvent",
    "RelationMessage",
    "RotateEvent",
    "RowsEvent",
    "RowsEventKind",
    "TableMapEvent",
    "TruncateMessage",
    "TupleData",
    "UpdateMessage",
    "XidEvent",
    "__version__",
]

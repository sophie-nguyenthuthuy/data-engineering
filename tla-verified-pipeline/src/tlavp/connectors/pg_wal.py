"""Simulated PG WAL reader.

A production deployment would read from `pg_logical_emit_message` or
parse `pg_recvlogical` output. We model it as a callable that yields
`{"action": "pg_insert", "record": (id, group, value)}` events.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from tlavp.state.machine import Record


def pg_insert_events(records: Iterable[Record]) -> Iterator[dict[str, Any]]:
    for r in records:
        yield {"action": "pg_insert", "record": r}

"""Warehouse + reverse-ETL event streams.

Production deployment: read warehouse `INFORMATION_SCHEMA.AUDIT_LOG`
(Snowflake) or Stitch / Fivetran's audit table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from tlavp.state.machine import Record


def dw_load_events(records: Iterable[Record]) -> Iterator[dict[str, Any]]:
    for r in records:
        yield {"action": "warehouse_load", "record": r}


def reverse_etl_events(records: Iterable[Record]) -> Iterator[dict[str, Any]]:
    for r in records:
        yield {"action": "reverse_etl", "record": r}

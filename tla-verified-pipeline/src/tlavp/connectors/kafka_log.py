"""Simulated Kafka producer event stream.

Yields publish-from-WAL events that downstream consumers (Flink) pick up.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from tlavp.state.machine import Record


def kafka_publish_events(records: Iterable[Record]) -> Iterator[dict[str, Any]]:
    for r in records:
        yield {"action": "debezium_publish", "record": r}


def flink_consume_events(n: int) -> Iterator[dict[str, Any]]:
    """A Flink consume event has no payload — it pops head of kafka.

    Yields `n` consume events.
    """
    for _ in range(n):
        yield {"action": "flink_consume"}

"""Collecting in-memory sink — primarily for tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sire.sinks.base import Sink

if TYPE_CHECKING:
    from sire.log.record import Record


@dataclass
class CollectingSink(Sink):
    """Stash every record into ``records`` for later inspection."""

    records: list[Record] = field(default_factory=list)

    def write(self, record: Record) -> None:
        self.records.append(record)


__all__ = ["CollectingSink"]

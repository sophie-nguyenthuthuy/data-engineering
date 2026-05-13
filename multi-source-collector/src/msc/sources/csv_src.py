"""CSV source adapter.

Reads a CSV file from a local path (or any file-like opened by the
caller) with the stdlib :mod:`csv` reader. The first row is treated
as the header; subsequent rows are zipped into ``Record.fields``.

The ``id_column`` parameter selects which column becomes
``Record.source_id``; if omitted, the 1-based row number is used.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

from msc.sources.base import Record, Source, SourceError


@dataclass
class CSVSource(Source):
    """Local-file CSV source."""

    path: Path
    dataset: str = ""
    id_column: str | None = None
    encoding: str = "utf-8"
    delimiter: str = ","
    kind: str = "csv"

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            self.path = Path(self.path)
        if not self.dataset:
            # Default dataset = file stem ("orders.csv" -> "orders").
            self.dataset = self.path.stem
        super().__post_init__()

    def fetch(self) -> Iterator[Record]:
        if not self.path.exists():
            raise SourceError(f"CSV file not found: {self.path}")
        with self.path.open("r", encoding=self.encoding, newline="") as fh:
            yield from self._read(fh)

    def _read(self, fh: IO[str]) -> Iterator[Record]:
        reader = csv.DictReader(fh, delimiter=self.delimiter)
        if reader.fieldnames is None:
            return
        if self.id_column and self.id_column not in reader.fieldnames:
            raise SourceError(f"id_column {self.id_column!r} not in CSV header {reader.fieldnames}")
        for i, row in enumerate(reader, start=1):
            source_id = str(row[self.id_column]) if self.id_column is not None else f"row-{i}"
            yield Record(source_id=source_id, fields=dict(row))


__all__ = ["CSVSource"]

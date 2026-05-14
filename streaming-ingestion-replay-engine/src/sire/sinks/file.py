"""JSONL file sink."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, TYPE_CHECKING

from sire.sinks.base import Sink

if TYPE_CHECKING:
    from sire.log.record import Record


@dataclass
class JsonlFileSink(Sink):
    """Append one JSON object per record to ``path``.

    Bytes-valued ``key`` / ``value`` columns are base64-encoded so the
    output file is plain ASCII no matter what the producer sent.
    """

    path: Path
    _fh: IO[str] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def write(self, record: Record) -> None:
        if self._fh is None:
            raise RuntimeError("sink is closed")
        obj = {
            "offset": record.offset,
            "timestamp": record.timestamp,
            "key": base64.b64encode(bytes(record.key)).decode("ascii"),
            "value": base64.b64encode(bytes(record.value)).decode("ascii"),
        }
        self._fh.write(json.dumps(obj, sort_keys=True))
        self._fh.write("\n")

    def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


__all__ = ["JsonlFileSink"]

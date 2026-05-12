"""Stdout target — pretty-prints events to the terminal. Useful for dry-runs and debugging."""

from __future__ import annotations

import json
import sys

from replay.models import Event
from replay.targets.base import BaseTarget


class StdoutTarget(BaseTarget):
    """Prints each event as a JSON line to stdout (or a custom stream)."""

    def __init__(self, pretty: bool = False, stream=None) -> None:
        self._pretty = pretty
        self._stream = stream or sys.stdout
        self._count = 0

    async def open(self) -> None:
        pass  # nothing to initialise

    async def send(self, event: Event) -> None:
        doc = {
            "topic": event.topic,
            "partition": event.partition,
            "offset": event.offset,
            "timestamp": event.timestamp.isoformat(),
            "key": event.key.decode("utf-8", errors="replace") if event.key else None,
            "value": _decode_value(event.value),
            "headers": {k: v.decode("utf-8", errors="replace") for k, v in event.headers.items()},
        }
        if self._pretty:
            print(json.dumps(doc, indent=2), file=self._stream)
        else:
            print(json.dumps(doc), file=self._stream)
        self._count += 1

    async def close(self) -> None:
        self._stream.flush()


def _decode_value(raw: bytes) -> object:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return raw.decode("utf-8", errors="replace")

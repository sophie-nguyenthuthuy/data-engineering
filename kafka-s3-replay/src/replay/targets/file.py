"""File target — writes replayed events to a local JSONL or Avro file."""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path

import fastavro

from replay.models import ArchiveFormat, Event, FileTargetConfig
from replay.targets.base import BaseTarget

logger = logging.getLogger(__name__)

_AVRO_SCHEMA = {
    "type": "record",
    "name": "ReplayedEvent",
    "fields": [
        {"name": "topic", "type": "string"},
        {"name": "partition", "type": "int"},
        {"name": "offset", "type": "long"},
        {"name": "timestamp", "type": "string"},
        {"name": "key", "type": ["null", "string"], "default": None},
        {"name": "value", "type": "string"},
        {"name": "source_path", "type": "string"},
    ],
}


class FileTarget(BaseTarget):
    """
    Writes events to a local file.
    Supports JSONL (default) and Avro output formats.
    """

    def __init__(self, config: FileTargetConfig) -> None:
        self.config = config
        self._fh = None
        self._avro_records: list[dict] = []
        self._count = 0

    async def open(self) -> None:
        mode = "a" if self.config.append else "w"
        path = Path(self.config.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.config.format == ArchiveFormat.AVRO:
            # Avro is written on close in one shot
            logger.info("File target (Avro) → %s", path)
        else:
            self._fh = open(path, mode, encoding="utf-8")  # noqa: SIM115
            logger.info("File target (JSONL) opened → %s (mode=%s)", path, mode)

    async def send(self, event: Event) -> None:
        if self.config.format == ArchiveFormat.AVRO:
            self._avro_records.append(_event_to_avro_record(event))
        else:
            assert self._fh is not None
            self._fh.write(json.dumps(_event_to_dict(event)) + "\n")
        self._count += 1

    async def close(self) -> None:
        if self.config.format == ArchiveFormat.AVRO and self._avro_records:
            parsed_schema = fastavro.parse_schema(_AVRO_SCHEMA)
            with open(self.config.path, "wb") as f:
                fastavro.writer(f, parsed_schema, self._avro_records)
        if self._fh:
            self._fh.flush()
            self._fh.close()
        logger.info("File target closed. Wrote %d events.", self._count)


def _event_to_dict(event: Event) -> dict:
    try:
        value = json.loads(event.value)
    except Exception:
        value = event.value.decode("utf-8", errors="replace")
    return {
        "topic": event.topic,
        "partition": event.partition,
        "offset": event.offset,
        "timestamp": event.timestamp.isoformat(),
        "key": event.key.decode("utf-8", errors="replace") if event.key else None,
        "value": value,
        "headers": {k: v.decode("utf-8", errors="replace") for k, v in event.headers.items()},
        "source_path": event.source_path,
    }


def _event_to_avro_record(event: Event) -> dict:
    try:
        value_str = json.dumps(json.loads(event.value))
    except Exception:
        value_str = event.value.decode("utf-8", errors="replace")
    return {
        "topic": event.topic,
        "partition": event.partition,
        "offset": event.offset,
        "timestamp": event.timestamp.isoformat(),
        "key": event.key.decode("utf-8", errors="replace") if event.key else None,
        "value": value_str,
        "source_path": event.source_path,
    }

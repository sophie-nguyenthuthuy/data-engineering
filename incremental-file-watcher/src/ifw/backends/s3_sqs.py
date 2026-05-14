"""S3 → SQS event-notification backend.

Real S3 notifications arrive as JSON messages with the shape::

    {
      "Records": [
        {
          "eventName": "ObjectCreated:Put",
          "s3": {
            "bucket": {"name": "..."},
            "object": {"key": "...", "size": 12345,
                        "eTag": "...", "sequencer": "..."}
          },
          "eventTime": "2024-01-01T12:00:00.000Z"
        }
      ]
    }

:func:`parse_s3_event` decodes one such payload (already-deserialised
dict or raw JSON bytes) into a list of :class:`FileEvent`. The
:class:`S3SqsBackend` itself is a thin shim that drains messages from
an injectable SQS-like client, parses them, and deletes them on
success.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from ifw.backends.base import Backend
from ifw.events import EventKind, FileEvent

#: SQS-like client: ``receive()`` returns ``[(receipt, body_bytes)]``;
#: ``delete(receipt)`` acknowledges one message.
SqsClient = tuple[
    Callable[[], list[tuple[str, bytes]]],
    Callable[[str], None],
]


def _to_ms(iso8601: str) -> int:
    """Parse an S3 ``eventTime`` ISO-8601 string into ms-since-epoch."""
    # S3 emits "...Z"; datetime.fromisoformat accepts "+00:00" suffix.
    cleaned = iso8601.replace("Z", "+00:00")
    when = dt.datetime.fromisoformat(cleaned)
    if when.tzinfo is None:
        when = when.replace(tzinfo=dt.timezone.utc)
    return int(when.timestamp() * 1000)


def parse_s3_event(payload: bytes | str | dict[str, Any]) -> list[FileEvent]:
    """Translate one SQS message body into ``FileEvent`` instances."""
    if isinstance(payload, bytes | str):
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid SQS body JSON: {exc}") from exc
    else:
        obj = payload
    records = obj.get("Records", []) if isinstance(obj, dict) else []
    events: list[FileEvent] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        name = str(rec.get("eventName", ""))
        s3 = rec.get("s3", {})
        if not isinstance(s3, dict):
            continue
        bucket = s3.get("bucket", {}).get("name", "") if isinstance(s3.get("bucket"), dict) else ""
        obj_meta = s3.get("object", {}) if isinstance(s3.get("object"), dict) else {}
        key = obj_meta.get("key", "")
        size = int(obj_meta.get("size", 0))
        etag = obj_meta.get("eTag") or obj_meta.get("etag") or ""
        ts = rec.get("eventTime") or ""
        if not bucket or not key or not etag:
            raise ValueError(f"S3 record missing bucket/key/eTag: {rec}")
        kind = EventKind.DELETED if name.startswith("ObjectRemoved") else EventKind.CREATED
        events.append(
            FileEvent(
                bucket=bucket,
                key=key,
                size=size,
                last_modified_ms=_to_ms(ts) if ts else 0,
                etag=etag,
                kind=kind,
            )
        )
    return events


@dataclass
class S3SqsBackend(Backend):
    """SQS-driven S3 event backend."""

    client: SqsClient
    kind: str = "s3_sqs"

    def poll(self) -> Iterator[FileEvent]:
        receive, delete = self.client
        for receipt, body in receive():
            yield from parse_s3_event(body)
            delete(receipt)


__all__ = ["S3SqsBackend", "SqsClient", "parse_s3_event"]

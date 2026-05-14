"""streaming-ingestion-replay-engine — Kafka-style replay with on-the-fly transforms."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from sire.log.cursor import Cursor, EndOfLog
    from sire.log.record import Record, RecordHeader
    from sire.log.segment import Segment, SegmentError
    from sire.log.topic import Topic
    from sire.offsets import OffsetStore
    from sire.replay import ReplayEngine, ReplayPosition
    from sire.sinks.base import Sink
    from sire.sinks.collect import CollectingSink
    from sire.sinks.file import JsonlFileSink
    from sire.transforms.base import Transform
    from sire.transforms.composed import ComposedTransform
    from sire.transforms.filter import Filter
    from sire.transforms.mapper import Mapper


_LAZY: dict[str, tuple[str, str]] = {
    "Record": ("sire.log.record", "Record"),
    "RecordHeader": ("sire.log.record", "RecordHeader"),
    "Segment": ("sire.log.segment", "Segment"),
    "SegmentError": ("sire.log.segment", "SegmentError"),
    "Topic": ("sire.log.topic", "Topic"),
    "Cursor": ("sire.log.cursor", "Cursor"),
    "EndOfLog": ("sire.log.cursor", "EndOfLog"),
    "OffsetStore": ("sire.offsets", "OffsetStore"),
    "ReplayEngine": ("sire.replay", "ReplayEngine"),
    "ReplayPosition": ("sire.replay", "ReplayPosition"),
    "Transform": ("sire.transforms.base", "Transform"),
    "Mapper": ("sire.transforms.mapper", "Mapper"),
    "Filter": ("sire.transforms.filter", "Filter"),
    "ComposedTransform": ("sire.transforms.composed", "ComposedTransform"),
    "Sink": ("sire.sinks.base", "Sink"),
    "CollectingSink": ("sire.sinks.collect", "CollectingSink"),
    "JsonlFileSink": ("sire.sinks.file", "JsonlFileSink"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CollectingSink",
    "ComposedTransform",
    "Cursor",
    "EndOfLog",
    "Filter",
    "JsonlFileSink",
    "Mapper",
    "OffsetStore",
    "Record",
    "RecordHeader",
    "ReplayEngine",
    "ReplayPosition",
    "Segment",
    "SegmentError",
    "Sink",
    "Topic",
    "Transform",
    "__version__",
]

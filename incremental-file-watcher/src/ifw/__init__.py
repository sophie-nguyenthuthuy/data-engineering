"""incremental-file-watcher — event-driven S3/MinIO file watcher."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from ifw.backends.base import Backend
    from ifw.backends.inmemory import InMemoryBackend
    from ifw.backends.polling import PollingBackend
    from ifw.backends.s3_sqs import S3SqsBackend, parse_s3_event
    from ifw.dedupe import Deduplicator
    from ifw.events import EventKind, FileEvent
    from ifw.late import LateArrivalDetector
    from ifw.manifest import Manifest, ManifestEntry
    from ifw.runner import Runner, RunReport

_LAZY: dict[str, tuple[str, str]] = {
    "EventKind": ("ifw.events", "EventKind"),
    "FileEvent": ("ifw.events", "FileEvent"),
    "Manifest": ("ifw.manifest", "Manifest"),
    "ManifestEntry": ("ifw.manifest", "ManifestEntry"),
    "Deduplicator": ("ifw.dedupe", "Deduplicator"),
    "LateArrivalDetector": ("ifw.late", "LateArrivalDetector"),
    "Backend": ("ifw.backends.base", "Backend"),
    "InMemoryBackend": ("ifw.backends.inmemory", "InMemoryBackend"),
    "PollingBackend": ("ifw.backends.polling", "PollingBackend"),
    "S3SqsBackend": ("ifw.backends.s3_sqs", "S3SqsBackend"),
    "parse_s3_event": ("ifw.backends.s3_sqs", "parse_s3_event"),
    "Runner": ("ifw.runner", "Runner"),
    "RunReport": ("ifw.runner", "RunReport"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Backend",
    "Deduplicator",
    "EventKind",
    "FileEvent",
    "InMemoryBackend",
    "LateArrivalDetector",
    "Manifest",
    "ManifestEntry",
    "PollingBackend",
    "RunReport",
    "Runner",
    "S3SqsBackend",
    "__version__",
    "parse_s3_event",
]

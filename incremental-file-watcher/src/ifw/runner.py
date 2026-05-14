"""Watcher runner — ties backend → dedupe → late-detect → processor.

``Runner.run_once`` drains the backend's current poll, applies the
dedupe + late-arrival rules, calls the processor for each surviving
event, and records the result in the manifest.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ifw.dedupe import Deduplicator
from ifw.late import LateArrivalDetector
from ifw.manifest import ManifestEntry

if TYPE_CHECKING:
    from collections.abc import Callable

    from ifw.backends.base import Backend
    from ifw.events import FileEvent
    from ifw.manifest import Manifest


@dataclass(frozen=True, slots=True)
class RunReport:
    """Summary of one :meth:`Runner.run_once` invocation."""

    processed: int
    duplicates: int
    late: int
    failures: int


@dataclass
class Runner:
    """Compose a backend with a manifest + dedupe + late detector."""

    backend: Backend
    manifest: Manifest
    processor: Callable[[FileEvent], None]
    dedupe: Deduplicator = field(default_factory=Deduplicator)
    late: LateArrivalDetector = field(default_factory=LateArrivalDetector)
    clock_ms: Callable[[], int] = field(default=lambda: int(time.time() * 1000))

    def __post_init__(self) -> None:
        # Rehydrate dedupe + watermark from any pre-existing manifest entries.
        keys = self.manifest.keys()
        if keys:
            self.dedupe.seen.update(keys)
        wm = self.manifest.watermark_ms()
        if wm > self.late.watermark_ms:
            self.late.watermark_ms = wm

    def run_once(self) -> RunReport:
        processed = duplicates = late = failures = 0
        for event in self.backend.poll():
            if not self.dedupe.is_new(event):
                duplicates += 1
                continue
            is_late = self.late.is_late(event)
            try:
                self.processor(event)
            except Exception:
                failures += 1
                continue
            self.dedupe.remember(event)
            self.late.update(event)
            self.manifest.record(
                ManifestEntry(
                    dedupe_key=event.dedupe_key(),
                    bucket=event.bucket,
                    key=event.key,
                    etag=event.etag,
                    last_modified_ms=event.last_modified_ms,
                    processed_at_ms=self.clock_ms(),
                )
            )
            if is_late:
                late += 1
            else:
                processed += 1
        return RunReport(
            processed=processed,
            duplicates=duplicates,
            late=late,
            failures=failures,
        )


__all__ = ["RunReport", "Runner"]

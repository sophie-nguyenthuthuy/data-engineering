"""Late-arrival detector.

An event is *late* when its ``last_modified_ms`` lies before the
manifest's high-water mark minus a configurable ``grace_ms``. Late
events are still safe to process (dedupe handles them) — the detector
just labels them so the caller can route them to a slower pipeline or
emit a Prometheus counter.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ifw.events import FileEvent


@dataclass
class LateArrivalDetector:
    """Compares event mtimes to a running watermark."""

    watermark_ms: int = 0
    grace_ms: int = 60_000  # 1 minute

    def __post_init__(self) -> None:
        if self.watermark_ms < 0:
            raise ValueError("watermark_ms must be ≥ 0")
        if self.grace_ms < 0:
            raise ValueError("grace_ms must be ≥ 0")

    def is_late(self, event: FileEvent) -> bool:
        return event.last_modified_ms + self.grace_ms < self.watermark_ms

    def update(self, event: FileEvent) -> None:
        if event.last_modified_ms > self.watermark_ms:
            self.watermark_ms = event.last_modified_ms


__all__ = ["LateArrivalDetector"]

"""Multi-source ingestion runner.

Couples a :class:`NamingConvention`, :class:`StagingZone`, and
:class:`Manifest` to drive one or more :class:`Source` instances to
completion. Idempotency is provided by the manifest: if a record with
the same ``(source, dataset, run_id)`` already exists, the runner
skips the source and reports ``skipped=True``.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from msc.naming import NamingConvention

if TYPE_CHECKING:
    from msc.manifest import Manifest
    from msc.sources.base import Source
    from msc.staging.zone import StagingZone


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Per-source outcome reported by the runner."""

    source: str
    dataset: str
    run_id: str
    staged_path: str
    row_count: int
    bytes_written: int
    sha256: str
    skipped: bool


@dataclass
class Runner:
    """Orchestrate :class:`Source` instances into the staging zone."""

    zone: StagingZone
    manifest: Manifest
    naming: NamingConvention = field(default_factory=NamingConvention)
    now: dt.datetime | None = None

    def ingest(self, source: Source, run_id: str | None = None) -> IngestionResult:
        """Ingest one source. Honours the manifest for idempotency."""
        when = self.now or dt.datetime.now(tz=dt.timezone.utc)
        key = self.naming.make(
            source=source.kind,
            dataset=source.dataset,
            when=when,
            run_id=run_id,
        )

        if self.manifest.has(source=key.source, dataset=key.dataset, run_id=key.run_id):
            entry = self.manifest.latest(source=key.source, dataset=key.dataset)
            assert entry is not None  # has(...) returned True
            return IngestionResult(
                source=key.source,
                dataset=key.dataset,
                run_id=key.run_id,
                staged_path=entry.staged_path,
                row_count=entry.row_count,
                bytes_written=0,
                sha256=entry.sha256,
                skipped=True,
            )

        report = self.zone.write(key, source.fetch())
        self.manifest.record(
            staged_path=report.staged_path,
            source=key.source,
            dataset=key.dataset,
            run_id=key.run_id,
            row_count=report.row_count,
            sha256=report.sha256,
            completed_at=when,
        )
        return IngestionResult(
            source=key.source,
            dataset=key.dataset,
            run_id=key.run_id,
            staged_path=report.staged_path,
            row_count=report.row_count,
            bytes_written=report.bytes_written,
            sha256=report.sha256,
            skipped=False,
        )

    def ingest_many(self, sources: list[Source]) -> list[IngestionResult]:
        return [self.ingest(s) for s in sources]


__all__ = ["IngestionResult", "Runner"]

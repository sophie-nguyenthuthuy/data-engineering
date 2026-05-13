"""Idempotency manifest for ingestion runs.

A :class:`Manifest` is a JSONL-on-disk log of every successful
ingestion. Each entry records ``(staged_path, source, dataset, run_id,
row_count, sha256, completed_at)``. The runner consults the manifest
before re-running a job so the second invocation of the same
``(source, dataset, run_id)`` triple is a no-op.

Storing only the relative staged path (not the full filesystem path)
lets the manifest survive a move of the staging root.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """Single completed ingestion."""

    staged_path: str
    source: str
    dataset: str
    run_id: str
    row_count: int
    sha256: str
    completed_at: str  # ISO-8601 UTC

    def key(self) -> tuple[str, str, str]:
        return (self.source, self.dataset, self.run_id)

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)


@dataclass
class Manifest:
    """Append-only JSONL manifest with file-locking for parallel writers."""

    path: Path
    _lock: threading.RLock = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch()
        self._lock = threading.RLock()

    # ------------------------------------------------------------ writes

    def record(
        self,
        *,
        staged_path: str,
        source: str,
        dataset: str,
        run_id: str,
        row_count: int,
        sha256: str,
        completed_at: dt.datetime | None = None,
    ) -> ManifestEntry:
        ts = completed_at or dt.datetime.now(tz=dt.timezone.utc)
        if ts.tzinfo is None:
            raise ValueError("completed_at must be timezone-aware")
        entry = ManifestEntry(
            staged_path=staged_path,
            source=source,
            dataset=dataset,
            run_id=run_id,
            row_count=row_count,
            sha256=sha256,
            completed_at=ts.isoformat(),
        )
        with self._lock, self.path.open("a", encoding="utf-8") as fh:
            fh.write(entry.to_json())
            fh.write("\n")
        return entry

    # ------------------------------------------------------------- reads

    def entries(self) -> list[ManifestEntry]:
        with self._lock:
            return list(self._iter_entries())

    def _iter_entries(self) -> Iterator[ManifestEntry]:
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                yield ManifestEntry(**obj)

    def latest(self, source: str, dataset: str) -> ManifestEntry | None:
        latest: ManifestEntry | None = None
        for e in self._iter_entries():
            if (
                e.source == source
                and e.dataset == dataset
                and (latest is None or e.completed_at > latest.completed_at)
            ):
                latest = e
        return latest

    def has(self, source: str, dataset: str, run_id: str) -> bool:
        key = (source, dataset, run_id)
        return any(e.key() == key for e in self._iter_entries())


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


__all__ = ["Manifest", "ManifestEntry", "sha256_bytes"]

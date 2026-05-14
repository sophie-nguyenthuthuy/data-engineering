"""Processed-file manifest.

Append-only JSONL on disk; one entry per successfully processed
:class:`FileEvent`. Records include the dedupe key + last-modified
timestamp so the late-arrival detector can compute the high-water
mark.
"""

from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """One processed file."""

    dedupe_key: str
    bucket: str
    key: str
    etag: str
    last_modified_ms: int
    processed_at_ms: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)


@dataclass
class Manifest:
    """JSONL manifest with thread-safe append + iterator-style reads."""

    path: Path
    _lock: threading.RLock = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch()
        self._lock = threading.RLock()

    def record(self, entry: ManifestEntry) -> None:
        with self._lock, self.path.open("a", encoding="utf-8") as fh:
            fh.write(entry.to_json())
            fh.write("\n")

    def entries(self) -> list[ManifestEntry]:
        with self._lock:
            return list(self._iter())

    def keys(self) -> set[str]:
        with self._lock:
            return {e.dedupe_key for e in self._iter()}

    def watermark_ms(self) -> int:
        """Highest ``last_modified_ms`` seen so far (0 when empty)."""
        with self._lock:
            return max((e.last_modified_ms for e in self._iter()), default=0)

    def _iter(self) -> Iterator[ManifestEntry]:
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield ManifestEntry(**json.loads(line))


__all__ = ["Manifest", "ManifestEntry"]

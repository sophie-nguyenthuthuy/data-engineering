"""Local-filesystem staging zone.

A :class:`StagingZone` is rooted at a directory; records are serialised
as JSONL (one record per line, each line ``{"source_id": ..., "fields":
{...}}``). The on-disk path is determined by the :class:`StagedKey`
naming convention so two writers that produce the same staged key
*must* land at the same location.

Writes are atomic: bytes are dumped to a sibling ``.tmp`` file then
``os.replace``-d into place, so a reader on the same filesystem
either sees the previous version or the new one — never a torn write.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from msc.naming import StagedKey
    from msc.sources.base import Record


@dataclass(frozen=True, slots=True)
class WriteReport:
    """Per-write summary returned by :meth:`StagingZone.write`."""

    staged_path: str
    bytes_written: int
    row_count: int
    sha256: str


@dataclass
class StagingZone:
    """File-system-backed staging zone."""

    root: Path

    def __post_init__(self) -> None:
        if not isinstance(self.root, Path):
            self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ writes

    def write(self, key: StagedKey, records: Iterable[Record]) -> WriteReport:
        """Serialise ``records`` as JSONL under ``key``'s canonical path."""
        target = self.root / key.path()
        target.parent.mkdir(parents=True, exist_ok=True)

        tmp = target.with_suffix(target.suffix + ".tmp")
        digest = hashlib.sha256()
        row_count = 0
        bytes_written = 0
        try:
            with tmp.open("wb") as fh:
                for rec in records:
                    payload = json.dumps(
                        {"source_id": rec.source_id, "fields": rec.fields},
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                    line = (payload + "\n").encode("utf-8")
                    fh.write(line)
                    digest.update(line)
                    bytes_written += len(line)
                    row_count += 1
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        finally:
            if tmp.exists():
                with contextlib.suppress(OSError):
                    tmp.unlink()

        return WriteReport(
            staged_path=key.path(),
            bytes_written=bytes_written,
            row_count=row_count,
            sha256=digest.hexdigest(),
        )

    # ------------------------------------------------------------- reads

    def exists(self, key: StagedKey) -> bool:
        return (self.root / key.path()).exists()

    def list_paths(self) -> list[str]:
        out: list[str] = []
        for path in self.root.rglob("*"):
            if path.is_file():
                rel = path.relative_to(self.root).as_posix()
                if rel.endswith(".tmp"):
                    continue
                out.append(rel)
        out.sort()
        return out


__all__ = ["StagingZone", "WriteReport"]

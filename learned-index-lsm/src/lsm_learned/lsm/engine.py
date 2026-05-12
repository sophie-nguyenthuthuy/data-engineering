"""
LSM-tree storage engine.

Write path:  put → MemTable → (flush when full) → L0 SSTable
Read path:   MemTable → L0 SSTables (newest-first) → L1 SSTables
Compaction:  merge N L0 SSTables into one L1 SSTable when L0 count > threshold

The engine supports two index strategies for SSTables:
  - ``"rmi"``    – Recursive Model Index (learned)
  - ``"btree"``  – Classic sorted-list B-tree
  - ``"adaptive"`` – starts with RMI; falls back to B-tree on detected drift

This class is deliberately simple — no WAL, no crash recovery — to keep the
benchmark surface clean and focused on index comparison.
"""

from __future__ import annotations

import itertools
import os
import tempfile
from pathlib import Path
from typing import Iterator, Literal

from .memtable import MemTable
from .sstable import SSTable, SSTableBuilder

IndexStrategy = Literal["rmi", "btree", "adaptive"]
_L0_COMPACTION_THRESHOLD = 8


class LSMEngine:
    """
    Minimal LSM-tree with swappable index strategies.

    Parameters
    ----------
    data_dir:
        Directory for SSTable files.  Created if absent.
    memtable_capacity:
        Maximum number of entries before a MemTable flush.
    index_strategy:
        ``"rmi"``, ``"btree"``, or ``"adaptive"``.
    """

    def __init__(
        self,
        data_dir: str | Path | None = None,
        *,
        memtable_capacity: int = 100_000,
        index_strategy: IndexStrategy = "rmi",
    ) -> None:
        if data_dir is None:
            self._tmpdir = tempfile.TemporaryDirectory()
            self._dir = Path(self._tmpdir.name)
        else:
            self._tmpdir = None
            self._dir = Path(data_dir)
            self._dir.mkdir(parents=True, exist_ok=True)

        self._strategy = index_strategy
        self._memtable = MemTable(memtable_capacity)
        self._l0: list[SSTable] = []
        self._l1: list[SSTable] = []
        self._seq = itertools.count()
        self._writes = 0
        self._reads = 0

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def put(self, key: int, value: int) -> None:
        self._memtable.put(key, value)
        self._writes += 1
        if self._memtable.is_full():
            self._flush()

    def delete(self, key: int) -> None:
        self._memtable.delete(key)
        if self._memtable.is_full():
            self._flush()

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get(self, key: int) -> int | None:
        self._reads += 1
        v = self._memtable.get(key)
        if v is not None:
            return v
        for tbl in reversed(self._l0):
            v = tbl.get(key)
            if v is not None:
                return v
        for tbl in reversed(self._l1):
            v = tbl.get(key)
            if v is not None:
                return v
        return None

    def scan(self, lo: int, hi: int) -> list[tuple[int, int]]:
        """Return sorted (key, value) pairs for lo <= key <= hi."""
        seen: dict[int, int] = {}
        # MemTable has freshest data
        for k, v in self._memtable.items():
            if lo <= k <= hi and v is not None:
                seen[k] = v
        for tbl in reversed(self._l0 + self._l1):
            for k, v in tbl.scan(lo, hi):
                if k not in seen:
                    seen[k] = v
        return sorted(seen.items())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _flush(self) -> None:
        items = self._memtable.items()
        if not items:
            return
        seq = next(self._seq)
        path = self._dir / f"l0_{seq:06d}.sstable"
        use_rmi = self._strategy in ("rmi", "adaptive")
        builder = SSTableBuilder(path)
        tbl = builder.build(items)
        # Reopen with correct strategy
        tbl.close()
        tbl = SSTable.open(path, use_rmi=use_rmi)
        self._l0.append(tbl)
        self._memtable.clear()
        if len(self._l0) >= _L0_COMPACTION_THRESHOLD:
            self._compact_l0()

    def _compact_l0(self) -> None:
        """Merge all L0 SSTables into one L1 SSTable."""
        # Collect all entries, latest write wins
        merged: dict[int, int | None] = {}
        for tbl in self._l0:
            for i in range(tbl.num_entries):
                k, v = tbl._read_record(i)  # noqa: SLF001
                if k not in merged:
                    from .sstable import _TOMBSTONE_VALUE
                    merged[k] = None if v == _TOMBSTONE_VALUE else v

        seq = next(self._seq)
        path = self._dir / f"l1_{seq:06d}.sstable"
        items = sorted(merged.items())
        use_rmi = self._strategy in ("rmi", "adaptive")
        builder = SSTableBuilder(path)
        tbl = builder.build(items)  # type: ignore[arg-type]
        tbl.close()
        tbl = SSTable.open(path, use_rmi=use_rmi)

        for old in self._l0:
            old.close()
            try:
                old.path.unlink()
            except FileNotFoundError:
                pass
        self._l0.clear()
        self._l1.append(tbl)

    def flush(self) -> None:
        """Force-flush the current MemTable even if not full."""
        if len(self._memtable) > 0:
            self._flush()

    # ------------------------------------------------------------------
    # Properties / diagnostics
    # ------------------------------------------------------------------

    @property
    def writes(self) -> int:
        return self._writes

    @property
    def reads(self) -> int:
        return self._reads

    @property
    def l0_count(self) -> int:
        return len(self._l0)

    @property
    def l1_count(self) -> int:
        return len(self._l1)

    def stats(self) -> dict:
        total_entries = sum(t.num_entries for t in self._l0 + self._l1)
        return {
            "memtable_entries": len(self._memtable),
            "l0_tables": len(self._l0),
            "l1_tables": len(self._l1),
            "total_sstable_entries": total_entries,
            "total_writes": self._writes,
            "total_reads": self._reads,
            "index_strategy": self._strategy,
        }

    def close(self) -> None:
        for tbl in self._l0 + self._l1:
            tbl.close()
        if self._tmpdir:
            self._tmpdir.cleanup()

    def __enter__(self) -> "LSMEngine":
        return self

    def __exit__(self, *_) -> None:
        self.close()

"""Leveled compaction strategy.

Level 0: accepts fresh flushes from memtable; files may overlap in key range.
Level 1+: non-overlapping; each level is ~10× larger than the previous.

Compaction algorithm:
  1. When L0 reaches L0_COMPACTION_TRIGGER files, compact all L0 into L1.
  2. When Ln exceeds its size quota, pick the oldest file and merge it
     with all overlapping files in L(n+1).

This is the same strategy used by LevelDB / RocksDB.
"""
from __future__ import annotations

import heapq
import itertools
from pathlib import Path
from typing import Iterator

from .sstable import SSTableReader, SSTableWriter

L0_COMPACTION_TRIGGER = 4
LEVEL_SIZE_MULTIPLIER = 10
L1_MAX_BYTES = 10 * 1024 * 1024   # 10 MB


def _level_max_bytes(level: int) -> int:
    if level == 0:
        return float("inf")
    return L1_MAX_BYTES * (LEVEL_SIZE_MULTIPLIER ** (level - 1))


def _overlaps(a: SSTableReader, b: SSTableReader) -> bool:
    """True if two SSTables have overlapping key ranges."""
    a_min, a_max = a.min_key, a.max_key
    b_min, b_max = b.min_key, b.max_key
    if a_min is None or b_min is None:
        return False
    return not (a_max < b_min or b_max < a_min)


def merge_sorted_iterators(
    iterators: list[Iterator[tuple[bytes, bytes | None]]],
) -> Iterator[tuple[bytes, bytes | None]]:
    """
    k-way merge of sorted iterators.  When multiple iterators yield the same
    key, the *first* (i.e., most-recent) wins — caller must order iterators
    newest-first.
    """
    # Heap entries: (key, seq_no, value, iterator)
    heap: list[tuple[bytes, int, bytes | None, Iterator]] = []
    for seq, it in enumerate(iterators):
        try:
            k, v = next(it)
            heapq.heappush(heap, (k, seq, v, it))
        except StopIteration:
            pass

    last_key: bytes | None = None
    while heap:
        key, seq, value, it = heapq.heappop(heap)
        if key != last_key:
            yield key, value
            last_key = key
        # Advance this iterator
        try:
            k, v = next(it)
            heapq.heappush(heap, (k, seq, v, it))
        except StopIteration:
            pass


class CompactionController:
    def __init__(self, data_dir: Path, compress: bool = True):
        self.data_dir = data_dir
        self.compress = compress
        # levels[0] = list of SSTableReaders at L0, etc.
        self.levels: list[list[SSTableReader]] = [[] for _ in range(7)]
        self._next_seq = 0

    def add_l0(self, sst: SSTableReader) -> None:
        self.levels[0].append(sst)
        self._maybe_compact()

    def _maybe_compact(self) -> list[Path]:
        removed: list[Path] = []
        # L0 threshold
        if len(self.levels[0]) >= L0_COMPACTION_TRIGGER:
            removed += self._compact_level(0)
        # Level-size threshold for L1+
        for level in range(1, len(self.levels) - 1):
            total = sum(r.path.stat().st_size for r in self.levels[level])
            if total > _level_max_bytes(level):
                removed += self._compact_level(level)
        return removed

    def _compact_level(self, level: int) -> list[Path]:
        """Compact level → level+1 and return paths of obsolete files."""
        sources = self.levels[level]
        if not sources:
            return []

        # Find overlapping files in level+1
        if level == 0:
            # All L0 files overlap; compact all into L1
            inputs_upper = [
                r for r in self.levels[level + 1]
                if any(_overlaps(r, s) for s in sources)
            ]
        else:
            # Pick the oldest file and its overlaps
            sources = [sources[0]]
            inputs_upper = [
                r for r in self.levels[level + 1]
                if _overlaps(r, sources[0])
            ]

        all_inputs = list(sources) + inputs_upper
        out_path = self.data_dir / f"L{level+1}_{self._next_seq:06d}.sst"
        self._next_seq += 1

        # Newest iterator first so merge_sorted_iterators keeps freshest value
        iters = [sst.scan() for sst in reversed(all_inputs)]
        writer = SSTableWriter(out_path, compress=self.compress)
        for key, value in merge_sorted_iterators(iters):
            if value is not None:  # drop tombstones at the deepest level
                writer.add(key, value)
            else:
                if level + 1 < len(self.levels) - 1:
                    writer.add(key, value)  # keep tombstone if not final level
        new_sst = writer.finish()

        # Update level lists
        removed = []
        self.levels[level] = [r for r in self.levels[level] if r not in sources]
        self.levels[level + 1] = [
            r for r in self.levels[level + 1] if r not in inputs_upper
        ]
        self.levels[level + 1].append(new_sst)

        for old in all_inputs:
            removed.append(old.path)
            try:
                old.path.unlink()
            except FileNotFoundError:
                pass
        return removed

    def get(self, key: bytes) -> bytes | None:
        """Search all levels newest-first."""
        for level_sstables in self.levels:
            for sst in reversed(level_sstables):
                result = sst.get(key)
                if result is not None:
                    return result
                if result is None and not sst.may_contain(key):
                    continue
        return None

    def scan(
        self, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, bytes | None]]:
        """Merge-scan all levels, newest-first wins on duplicate keys."""
        iters = []
        for level_sstables in self.levels:
            for sst in reversed(level_sstables):
                iters.append(sst.scan(start, end))
        return merge_sorted_iterators(iters)

    def stats(self) -> dict:
        stats = {}
        for i, level in enumerate(self.levels):
            if level:
                total = sum(r.path.stat().st_size for r in level)
                stats[f"L{i}"] = {"files": len(level), "bytes": total}
        return stats

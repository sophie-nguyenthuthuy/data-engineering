"""
Immutable Sorted String Table (SSTable) stored as a binary file.

File layout
-----------
Header  (24 bytes):  magic(8) | num_entries(8) | index_interval(4) | reserved(4)
Data    (16 * n):    [key(8) | value(8)] * num_entries   (value=INT64_MIN = tombstone)
Index   (16 * m):    [key(8) | byte_offset(8)] * m       (sparse index, every index_interval entries)

The index is kept in memory; data records are read on demand via mmap.
"""

from __future__ import annotations

import mmap
import os
import struct
from pathlib import Path
from typing import Iterator, Optional

import numpy as np

from ..indexes.btree import BTreeIndex
from ..indexes.bloom import BloomFilter
from ..indexes.rmi import RMI

_MAGIC = b"LSMLRND1"
_HEADER_FMT = ">8sQII"   # magic, num_entries, index_interval, reserved
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)
_RECORD_FMT = ">qq"      # key (int64), value (int64)
_RECORD_SIZE = struct.calcsize(_RECORD_FMT)
_TOMBSTONE_VALUE = -(2**63)
_INDEX_INTERVAL = 128    # one sparse-index entry per N records


class SSTableBuilder:
    """Write a new SSTable from a sorted sequence of (key, value) pairs."""

    def __init__(self, path: str | Path, index_interval: int = _INDEX_INTERVAL) -> None:
        self._path = Path(path)
        self._index_interval = index_interval

    def build(self, items: list[tuple[int, int | None]]) -> "SSTable":
        """
        ``items`` must be sorted by key.  ``value=None`` writes a tombstone.
        Returns the opened SSTable.
        """
        n = len(items)
        data_offset = _HEADER_SIZE

        sparse_index: list[tuple[int, int]] = []  # (key, byte_offset)

        with self._path.open("wb") as f:
            # Placeholder header
            f.write(struct.pack(_HEADER_FMT, _MAGIC, n, self._index_interval, 0))

            for i, (k, v) in enumerate(items):
                offset = data_offset + i * _RECORD_SIZE
                if i % self._index_interval == 0:
                    sparse_index.append((k, offset))
                raw_v = _TOMBSTONE_VALUE if v is None else v
                f.write(struct.pack(_RECORD_FMT, k, raw_v))

            # Write sparse index after data
            index_start = f.tell()
            for (ik, io_) in sparse_index:
                f.write(struct.pack(">qq", ik, io_))

            # Rewrite header with correct values
            f.seek(0)
            f.write(struct.pack(_HEADER_FMT, _MAGIC, n, self._index_interval, 0))

        return SSTable.open(self._path)


class SSTable:
    """Read-only view over an on-disk SSTable with an in-memory learned/classic index."""

    def __init__(
        self,
        path: str | Path,
        *,
        use_rmi: bool = True,
        bloom_fpr: float = 0.01,
    ) -> None:
        self._path = Path(path)
        self._use_rmi = use_rmi
        self._mmap: mmap.mmap | None = None
        self._file = None
        self._num_entries = 0
        self._index_interval = _INDEX_INTERVAL
        self._sparse_keys: np.ndarray = np.array([], dtype=np.int64)
        self._sparse_offsets: np.ndarray = np.array([], dtype=np.int64)
        self._bloom: BloomFilter | None = None
        self._rmi: RMI | None = None
        self._btree: BTreeIndex | None = None

    @classmethod
    def open(
        cls,
        path: str | Path,
        *,
        use_rmi: bool = True,
        bloom_fpr: float = 0.01,
    ) -> "SSTable":
        tbl = cls(path, use_rmi=use_rmi, bloom_fpr=bloom_fpr)
        tbl._load(bloom_fpr)
        return tbl

    # ------------------------------------------------------------------
    # Internal loading
    # ------------------------------------------------------------------

    def _load(self, bloom_fpr: float) -> None:
        self._file = open(self._path, "rb")
        raw_header = self._file.read(_HEADER_SIZE)
        magic, n, idx_iv, _ = struct.unpack(_HEADER_FMT, raw_header)
        if magic != _MAGIC:
            raise ValueError(f"Not a valid SSTable: {self._path}")
        self._num_entries = n
        self._index_interval = idx_iv
        self._file.seek(0)
        self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)

        # Read all keys for bloom filter and index construction
        all_keys: list[int] = []
        data_offset = _HEADER_SIZE
        for i in range(n):
            off = data_offset + i * _RECORD_SIZE
            k, _ = struct.unpack_from(">qq", self._mmap, off)
            all_keys.append(k)

        # Bloom filter
        if n > 0:
            self._bloom = BloomFilter(n, bloom_fpr)
            for k in all_keys:
                self._bloom.add(k)

        # Chosen index structure
        key_arr = np.array(all_keys, dtype=np.float64)
        if self._use_rmi and n >= 4:
            self._rmi = RMI(num_stage2=max(1, min(512, n // 10)))
            self._rmi.train(key_arr)
        else:
            self._btree = BTreeIndex()
            self._btree.build(all_keys)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get(self, key: int) -> Optional[int]:
        if self._bloom is not None and key not in self._bloom:
            return None

        if self._rmi and self._rmi.trained:
            lo, hi = self._rmi.search_range(float(key))
        elif self._btree:
            idx = self._btree.lookup(key)
            if idx is None:
                return None
            return self._read_value_at(idx)
        else:
            lo, hi = 0, self._num_entries - 1

        # Binary search over [lo, hi]
        return self._binary_search(key, lo, hi)

    def _binary_search(self, key: int, lo: int, hi: int) -> Optional[int]:
        while lo <= hi:
            mid = (lo + hi) >> 1
            k, v = self._read_record(mid)
            if k == key:
                return None if v == _TOMBSTONE_VALUE else v
            elif k < key:
                lo = mid + 1
            else:
                hi = mid - 1
        return None

    def _read_record(self, idx: int) -> tuple[int, int]:
        off = _HEADER_SIZE + idx * _RECORD_SIZE
        k, v = struct.unpack_from(">qq", self._mmap, off)  # type: ignore[arg-type]
        return k, v

    def _read_value_at(self, idx: int) -> Optional[int]:
        _, v = self._read_record(idx)
        return None if v == _TOMBSTONE_VALUE else v

    def contains(self, key: int) -> bool:
        return self.get(key) is not None

    def scan(self, lo: int, hi: int) -> Iterator[tuple[int, int]]:
        """Yield (key, value) pairs where lo <= key <= hi (tombstones skipped)."""
        # Use bloom filter to skip if single-key negative
        # For ranges we scan linearly from first candidate position
        if self._rmi and self._rmi.trained:
            start_lo, _ = self._rmi.search_range(float(lo))
            # Widen left by a safety margin
            start = max(0, start_lo - 64)
        else:
            start = 0

        for i in range(start, self._num_entries):
            k, v = self._read_record(i)
            if k > hi:
                break
            if k >= lo and v != _TOMBSTONE_VALUE:
                yield k, v

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def num_entries(self) -> int:
        return self._num_entries

    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        try:
            if self._mmap:
                self._mmap.close()
        except (ValueError, OSError):
            pass
        try:
            if self._file:
                self._file.close()
        except OSError:
            pass

    def __del__(self) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"SSTable(path={self._path}, n={self._num_entries}, rmi={self._use_rmi})"

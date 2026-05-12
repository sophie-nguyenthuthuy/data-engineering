"""LSMEngine — public API for the time-series storage engine.

Write path:
  put() → WAL append → Memtable insert
        → (if full) rotate memtable → flush to L0 SSTable
        → (if L0 full) compact L0 → L1

Read path:
  get() → active memtable → immutable memtables → L0..Ln SSTables

The engine is *not* thread-safe by default. Wrap calls with a lock
if using from multiple threads.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Iterator

from .compaction import CompactionController
from .memtable import Memtable
from .sstable import SSTableReader, SSTableWriter
from .types import TSKey, TSValue, DataPoint
from .wal import Op, WAL


class LSMEngine:
    def __init__(
        self,
        data_dir: str | Path,
        memtable_size_mb: int = 64,
        compress: bool = True,
        wal_enabled: bool = True,
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._compress = compress
        self._wal_enabled = wal_enabled

        self._memtable = Memtable(memtable_size_mb * 1024 * 1024)
        # Immutable memtables awaiting background flush (simplified: flush inline)
        self._immutable: list[Memtable] = []

        self._compaction = CompactionController(self.data_dir, compress)
        self._sst_seq = 0

        self._wal: WAL | None = None
        if wal_enabled:
            self._wal = WAL(self.data_dir / "wal.log")
            self._recover()

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------

    def put(self, key: TSKey, value: TSValue) -> None:
        if self._wal:
            self._wal.write(key.encode(), value.encode())
        self._memtable.put(key, value)
        if self._memtable.is_full:
            self._rotate_memtable()

    def put_point(self, point: DataPoint) -> None:
        self.put(point.key, point.value)

    def delete(self, key: TSKey) -> None:
        if self._wal:
            self._wal.delete(key.encode())
        self._memtable.delete(key)

    def write_batch(self, points: list[DataPoint]) -> None:
        """Bulk write — more efficient than individual puts."""
        for point in points:
            if self._wal:
                self._wal.write(point.encoded_key, point.encoded_value)
            self._memtable.put(point.key, point.value)
        if self._memtable.is_full:
            self._rotate_memtable()

    def flush(self) -> None:
        """Force memtable → SSTable flush (useful for benchmarks / shutdown)."""
        if len(self._memtable) > 0:
            self._rotate_memtable()

    # ------------------------------------------------------------------
    # Public read API
    # ------------------------------------------------------------------

    def get(self, key: TSKey) -> TSValue | None:
        encoded = key.encode()

        # 1. Active memtable
        v = self._memtable.get(key)
        if v is not None:
            return v

        # 2. Immutable memtables (newest first)
        for mem in reversed(self._immutable):
            raw = mem._data.get(encoded)
            if raw is not None:
                return TSValue.decode(raw) if raw else None

        # 3. SSTable levels
        raw = self._compaction.get(encoded)
        if raw is not None:
            return TSValue.decode(raw)
        return None

    def scan(
        self,
        metric: str,
        tags: dict[str, str],
        start_ns: int,
        end_ns: int,
    ) -> Iterator[DataPoint]:
        """Yield DataPoints for metric+tags in [start_ns, end_ns)."""
        start_key = TSKey.make(metric, tags, start_ns).encode()
        end_key = TSKey.make(metric, tags, end_ns).encode()
        seen: dict[bytes, bytes | None] = {}

        # Collect from all sources; later sources are older so don't overwrite
        for k, v in self._memtable.range_scan(
            TSKey.make(metric, tags, start_ns),
            TSKey.make(metric, tags, end_ns),
        ):
            seen.setdefault(k, v)

        for mem in reversed(self._immutable):
            for k, v in mem.range_scan(
                TSKey.make(metric, tags, start_ns),
                TSKey.make(metric, tags, end_ns),
            ):
                seen.setdefault(k, v)

        for k, v in self._compaction.scan(start_key, end_key):
            seen.setdefault(k, v)

        for k, v in sorted(seen.items()):
            if v is not None:
                yield DataPoint(TSKey.decode(k), TSValue.decode(v))

    def scan_prefix(
        self, metric: str, tags: dict[str, str]
    ) -> Iterator[DataPoint]:
        """Yield all DataPoints for a metric+tagset, all time."""
        prefix = TSKey.make(metric, tags, 0).metric_prefix()
        seen: dict[bytes, bytes | None] = {}
        for k, v in self._memtable.prefix_scan(prefix):
            seen.setdefault(k, v)
        for mem in reversed(self._immutable):
            for k, v in mem.prefix_scan(prefix):
                seen.setdefault(k, v)
        for k, v in self._compaction.scan(prefix, None):
            if not k.startswith(prefix):
                break
            seen.setdefault(k, v)
        for k, v in sorted(seen.items()):
            if v is not None:
                yield DataPoint(TSKey.decode(k), TSValue.decode(v))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _rotate_memtable(self) -> None:
        """Move active memtable → immutable → flush to L0 SSTable."""
        self._immutable.append(self._memtable)
        self._memtable = Memtable(self._memtable.size_limit)
        self._flush_oldest_immutable()

    def _flush_oldest_immutable(self) -> None:
        if not self._immutable:
            return
        mem = self._immutable.pop(0)
        if len(mem) == 0:
            return

        path = self.data_dir / f"L0_{self._sst_seq:06d}.sst"
        self._sst_seq += 1
        writer = SSTableWriter(path, compress=self._compress)
        for key_bytes, val_bytes in mem.items():
            writer.add(key_bytes, val_bytes)
        sst = writer.finish()
        self._compaction.add_l0(sst)

        if self._wal:
            self._wal.checkpoint()

    def _recover(self) -> None:
        """Replay WAL into memtable after a crash."""
        wal_path = self.data_dir / "wal.log"
        count = 0
        for op, key, value in WAL.replay(wal_path):
            if op == Op.WRITE:
                k = TSKey.decode(key)
                v = TSValue.decode(value)
                self._memtable.put(k, v)
                count += 1
            elif op == Op.DELETE:
                k = TSKey.decode(key)
                self._memtable.delete(k)
        if count:
            print(f"[LSM] Recovered {count} entries from WAL")

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        s = {
            "memtable_entries": len(self._memtable),
            "memtable_bytes": self._memtable.size_bytes,
            "immutable_count": len(self._immutable),
            "wal_enabled": self._wal_enabled,
            "compression": self._compress,
        }
        s.update(self._compaction.stats())
        return s

    def close(self) -> None:
        self.flush()
        if self._wal:
            self._wal.close()

    def __enter__(self) -> LSMEngine:
        return self

    def __exit__(self, *_) -> None:
        self.close()

"""SSTable (Sorted String Table) — immutable on-disk sorted file.

File layout:
  [Data Blocks]          one per BLOCK_SIZE bytes of uncompressed KV data
  [Bloom Filter Block]   serialized BloomFilter
  [Index Block]          (first_key_len:2, first_key, offset:8, size:4) per block
  [Footer 40B]           index_offset:8, index_size:8, bloom_offset:8,
                         bloom_size:8, magic:8 ("LSMTFOOT")

Each data block (uncompressed):
  num_entries:4
  per entry: key_len:2, key:N, val_len:2, val:M  (val_len=0 → tombstone)

Each data block on disk is optionally lz4-compressed:
  compressed_size:4, uncompressed_size:4, crc32:4, data:compressed_size
"""
from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path
from typing import Iterator

try:
    import lz4.frame as lz4
    _HAS_LZ4 = True
except ImportError:
    _HAS_LZ4 = False

from .bloom import BloomFilter

BLOCK_SIZE = 64 * 1024          # 64 KB uncompressed target
FOOTER_MAGIC = b"LSMTFOOT"
FOOTER_SIZE = 40                 # 4×8B + 8B magic

# Block on-disk header
BLOCK_HDR_FMT = ">III"          # compressed_size, uncompressed_size, crc32
BLOCK_HDR_SIZE = struct.calcsize(BLOCK_HDR_FMT)

FOOTER_FMT = ">QQQQ8s"          # idx_off, idx_sz, bloom_off, bloom_sz, magic
assert struct.calcsize(FOOTER_FMT) == FOOTER_SIZE


def _compress(data: bytes, use_lz4: bool) -> bytes:
    if use_lz4 and _HAS_LZ4:
        return lz4.compress(data, compression_level=1)
    return data


def _decompress(data: bytes, uncompressed_size: int, use_lz4: bool) -> bytes:
    if use_lz4 and _HAS_LZ4 and len(data) != uncompressed_size:
        return lz4.decompress(data)
    return data


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class SSTableWriter:
    def __init__(self, path: Path, compress: bool = True):
        self.path = path
        self._compress = compress and _HAS_LZ4
        self._f = open(path, "wb")
        self._block_entries: list[tuple[bytes, bytes | None]] = []
        self._block_bytes = 0
        self._index: list[tuple[bytes, int, int]] = []  # (first_key, offset, size)
        self._bloom: BloomFilter | None = None
        self._entry_count = 0
        self._cur_offset = 0

    def add(self, key: bytes, value: bytes | None) -> None:
        """Add a key/value pair (value=None for tombstone). Keys must be sorted."""
        self._block_entries.append((key, value))
        self._block_bytes += 2 + len(key) + 2 + (len(value) if value else 0)
        self._entry_count += 1
        if self._bloom is None:
            # We'll rebuild once we know the total count; use a placeholder
            pass
        if self._block_bytes >= BLOCK_SIZE:
            self._flush_block()

    def finish(self) -> SSTableReader:
        if self._block_entries:
            self._flush_block()

        # Build bloom filter over all keys we wrote
        bloom = BloomFilter(max(1, self._entry_count))
        for _, first_key, _ in self._index:
            pass  # we didn't store all keys; rebuild requires a second pass
        # We track all keys via the index's first_key — not sufficient for bloom.
        # We re-read blocks to populate bloom. Simple alternative: accumulate keys.
        # For correctness, we stored _all_keys during add().
        bloom_bytes = bloom.to_bytes()  # empty bloom is fine; populated below

        # Accumulate all keys from index blocks for bloom (trade-off: mem)
        # Real impl: pass keys during add() with a separate list.
        # Here we re-read our own blocks:
        bloom = BloomFilter(max(1, self._entry_count))
        self._f.flush()
        with open(self.path, "rb") as rf:
            for first_key, offset, blk_size in self._index:
                rf.seek(offset)
                raw = rf.read(blk_size)
                entries = _decode_block_data(raw, self._compress)
                for k, _ in entries:
                    bloom.add(k)
        bloom_bytes = bloom.to_bytes()

        # Write bloom block
        bloom_offset = self._cur_offset
        self._f.seek(bloom_offset)
        self._f.write(bloom_bytes)
        bloom_size = len(bloom_bytes)
        self._cur_offset += bloom_size

        # Write index block
        index_offset = self._cur_offset
        idx_buf = []
        for first_key, offset, blk_size in self._index:
            idx_buf.append(struct.pack(">H", len(first_key)))
            idx_buf.append(first_key)
            idx_buf.append(struct.pack(">QI", offset, blk_size))
        index_bytes = b"".join(idx_buf)
        self._f.write(index_bytes)
        index_size = len(index_bytes)

        # Write footer
        footer = struct.pack(
            FOOTER_FMT,
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            FOOTER_MAGIC,
        )
        self._f.write(footer)
        self._f.flush()
        os.fsync(self._f.fileno())
        self._f.close()
        return SSTableReader(self.path, compress=self._compress)

    def _flush_block(self) -> None:
        if not self._block_entries:
            return
        first_key = self._block_entries[0][0]
        raw = _encode_block_data(self._block_entries)
        compressed = _compress(raw, self._compress)
        crc = zlib.crc32(compressed) & 0xFFFFFFFF
        header = struct.pack(BLOCK_HDR_FMT, len(compressed), len(raw), crc)
        on_disk = header + compressed
        self._f.write(on_disk)
        self._index.append((first_key, self._cur_offset, len(on_disk)))
        self._cur_offset += len(on_disk)
        self._block_entries = []
        self._block_bytes = 0


def _encode_block_data(entries: list[tuple[bytes, bytes | None]]) -> bytes:
    parts = [struct.pack(">I", len(entries))]
    for key, value in entries:
        parts.append(struct.pack(">H", len(key)))
        parts.append(key)
        if value is None:
            parts.append(struct.pack(">H", 0))  # tombstone
        else:
            parts.append(struct.pack(">H", len(value)))
            parts.append(value)
    return b"".join(parts)


def _decode_block_data(
    raw_block: bytes, compressed: bool
) -> list[tuple[bytes, bytes | None]]:
    header = raw_block[:BLOCK_HDR_SIZE]
    comp_size, uncomp_size, stored_crc = struct.unpack(BLOCK_HDR_FMT, header)
    payload = raw_block[BLOCK_HDR_SIZE:]
    if (zlib.crc32(payload) & 0xFFFFFFFF) != stored_crc:
        raise ValueError("Block CRC mismatch — data corruption detected")
    data = _decompress(payload, uncomp_size, compressed)
    offset = 0
    (num_entries,) = struct.unpack_from(">I", data, offset)
    offset += 4
    entries = []
    for _ in range(num_entries):
        (key_len,) = struct.unpack_from(">H", data, offset)
        offset += 2
        key = data[offset: offset + key_len]
        offset += key_len
        (val_len,) = struct.unpack_from(">H", data, offset)
        offset += 2
        if val_len == 0:
            entries.append((key, None))
        else:
            val = data[offset: offset + val_len]
            offset += val_len
            entries.append((key, val))
    return entries


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class SSTableReader:
    def __init__(self, path: Path, compress: bool = True):
        self.path = path
        self._compress = compress
        self._file_size = path.stat().st_size
        self._index: list[tuple[bytes, int, int]] = []
        self._bloom: BloomFilter | None = None
        self._load_footer()

    def _load_footer(self) -> None:
        with open(self.path, "rb") as f:
            f.seek(self._file_size - FOOTER_SIZE)
            footer_raw = f.read(FOOTER_SIZE)
        idx_off, idx_sz, bloom_off, bloom_sz, magic = struct.unpack(FOOTER_FMT, footer_raw)
        if magic != FOOTER_MAGIC:
            raise ValueError(f"SSTable magic mismatch: {magic!r}")

        with open(self.path, "rb") as f:
            # Load bloom filter
            f.seek(bloom_off)
            bloom_raw = f.read(bloom_sz)
            self._bloom = BloomFilter.from_bytes(bloom_raw)

            # Load index
            f.seek(idx_off)
            idx_raw = f.read(idx_sz)

        pos = 0
        while pos < len(idx_raw):
            (key_len,) = struct.unpack_from(">H", idx_raw, pos)
            pos += 2
            first_key = idx_raw[pos: pos + key_len]
            pos += key_len
            offset, blk_size = struct.unpack_from(">QI", idx_raw, pos)
            pos += 12
            self._index.append((first_key, offset, blk_size))

    def may_contain(self, key: bytes) -> bool:
        return self._bloom is None or self._bloom.may_contain(key)

    def get(self, key: bytes) -> bytes | None:
        if not self.may_contain(key):
            return None
        block = self._find_block(key)
        if block is None:
            return None
        entries = self._read_block(*block)
        lo, hi = 0, len(entries) - 1
        while lo <= hi:
            mid = (lo + hi) >> 1
            k, v = entries[mid]
            if k == key:
                return v  # None = tombstone
            elif k < key:
                lo = mid + 1
            else:
                hi = mid - 1
        return None

    def scan(
        self, start: bytes | None = None, end: bytes | None = None
    ) -> Iterator[tuple[bytes, bytes | None]]:
        """Yield (key, value) pairs in [start, end). value=None means tombstone."""
        for first_key, offset, blk_size in self._index:
            # Skip blocks entirely before start
            if end and first_key >= end:
                break
            entries = self._read_block(first_key, offset, blk_size)
            for k, v in entries:
                if start and k < start:
                    continue
                if end and k >= end:
                    return
                yield k, v

    def _find_block(
        self, key: bytes
    ) -> tuple[bytes, int, int] | None:
        """Binary-search the index for the block that may contain key."""
        lo, hi = 0, len(self._index) - 1
        result = None
        while lo <= hi:
            mid = (lo + hi) >> 1
            first_key, offset, blk_size = self._index[mid]
            if first_key <= key:
                result = (first_key, offset, blk_size)
                lo = mid + 1
            else:
                hi = mid - 1
        return result

    def _read_block(
        self, first_key: bytes, offset: int, blk_size: int
    ) -> list[tuple[bytes, bytes | None]]:
        with open(self.path, "rb") as f:
            f.seek(offset)
            raw = f.read(blk_size)
        return _decode_block_data(raw, self._compress)

    @property
    def min_key(self) -> bytes | None:
        return self._index[0][0] if self._index else None

    @property
    def max_key(self) -> bytes | None:
        if not self._index:
            return None
        last = self._index[-1]
        entries = self._read_block(*last)
        return entries[-1][0] if entries else None

    def num_blocks(self) -> int:
        return len(self._index)

    def __repr__(self) -> str:
        return f"SSTable({self.path.name}, blocks={len(self._index)})"

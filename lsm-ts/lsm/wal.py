"""Write-Ahead Log (WAL) for crash recovery.

Binary record format (each record):
  magic    : 4B  = 0xCAFEBABE
  op       : 1B  (0x01=WRITE, 0x02=DELETE, 0xFF=CHECKPOINT)
  key_len  : 2B
  val_len  : 2B  (0 for DELETE)
  crc32    : 4B  (covers op+key_len+val_len+key+value)
  key      : key_len bytes
  value    : val_len bytes

On replay, any record with a bad CRC is treated as the log tail
(partial write after a crash) and discarded.
"""
from __future__ import annotations

import os
import struct
import zlib
from enum import IntEnum
from pathlib import Path
from typing import Iterator


MAGIC = 0xCAFEBABE
HEADER_FMT = ">IBHHIH"  # magic(4) pad(1 for alignment), op(1), key_len(2), val_len(2), crc(4)
# Actually let's keep it simple:
RECORD_FMT = ">IBHHI"  # magic(4), op(1), key_len(2), val_len(2), crc32(4)
RECORD_HEADER_SIZE = struct.calcsize(RECORD_FMT)


class Op(IntEnum):
    WRITE = 0x01
    DELETE = 0x02
    CHECKPOINT = 0xFF


class WAL:
    def __init__(self, path: Path):
        self.path = path
        self._file = open(path, "ab")
        self._write_count = 0

    def write(self, key: bytes, value: bytes) -> None:
        self._append(Op.WRITE, key, value)

    def delete(self, key: bytes) -> None:
        self._append(Op.DELETE, key, b"")

    def checkpoint(self) -> None:
        """Mark that memtable was flushed; records before this can be dropped."""
        self._append(Op.CHECKPOINT, b"", b"")
        self._file.flush()
        os.fsync(self._file.fileno())

    def flush(self) -> None:
        self._file.flush()

    def close(self) -> None:
        self._file.close()

    def _append(self, op: Op, key: bytes, value: bytes) -> None:
        payload = bytes([op]) + struct.pack(">HH", len(key), len(value)) + key + value
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        header = struct.pack(">IBI", MAGIC, op, crc)
        kv_len = struct.pack(">HH", len(key), len(value))
        self._file.write(header + kv_len + key + value)
        self._write_count += 1
        if self._write_count % 1000 == 0:
            self._file.flush()

    @staticmethod
    def replay(path: Path) -> Iterator[tuple[Op, bytes, bytes]]:
        """Yield (op, key, value) records. Stops at first corrupted record."""
        if not path.exists():
            return
        with open(path, "rb") as f:
            while True:
                header_raw = f.read(9)  # magic(4) + op(1) + crc(4)
                if len(header_raw) < 9:
                    break
                magic, op_byte, crc_stored = struct.unpack(">IBI", header_raw)
                if magic != MAGIC:
                    break  # corrupt tail
                kv_len_raw = f.read(4)
                if len(kv_len_raw) < 4:
                    break
                key_len, val_len = struct.unpack(">HH", kv_len_raw)
                payload_rest = f.read(key_len + val_len)
                if len(payload_rest) < key_len + val_len:
                    break
                key = payload_rest[:key_len]
                value = payload_rest[key_len:]
                # Verify CRC over (op + key_len + val_len + key + value)
                check_data = bytes([op_byte]) + kv_len_raw + payload_rest
                if (zlib.crc32(check_data) & 0xFFFFFFFF) != crc_stored:
                    break  # partial write at crash boundary
                yield Op(op_byte), key, value

    @staticmethod
    def truncate_after_checkpoint(wal_path: Path, new_path: Path) -> None:
        """Rewrite WAL keeping only records after the last CHECKPOINT."""
        records: list[tuple[Op, bytes, bytes]] = []
        last_checkpoint_idx = -1
        for i, record in enumerate(WAL.replay(wal_path)):
            records.append(record)
            if record[0] == Op.CHECKPOINT:
                last_checkpoint_idx = i
        with WAL(new_path) as wal:
            for op, key, value in records[last_checkpoint_idx + 1:]:
                if op == Op.WRITE:
                    wal.write(key, value)
                elif op == Op.DELETE:
                    wal.delete(key)

    def __enter__(self) -> WAL:
        return self

    def __exit__(self, *_) -> None:
        self.close()

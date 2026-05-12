import tempfile
from pathlib import Path

import pytest
from lsm.wal import Op, WAL


def test_write_and_replay():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "wal.log"
        with WAL(path) as wal:
            wal.write(b"key1", b"val1")
            wal.write(b"key2", b"val2")
            wal.delete(b"key1")

        records = list(WAL.replay(path))
        assert len(records) == 3
        assert records[0] == (Op.WRITE, b"key1", b"val1")
        assert records[1] == (Op.WRITE, b"key2", b"val2")
        assert records[2] == (Op.DELETE, b"key1", b"")


def test_checkpoint_recorded():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "wal.log"
        with WAL(path) as wal:
            wal.write(b"k", b"v")
            wal.checkpoint()

        records = list(WAL.replay(path))
        assert any(op == Op.CHECKPOINT for op, _, _ in records)


def test_corrupt_tail_truncated():
    """Partial write at end of file should not cause crash."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "wal.log"
        with WAL(path) as wal:
            wal.write(b"good", b"data")

        # Append garbage bytes simulating a partial write
        with open(path, "ab") as f:
            f.write(b"\xCA\xFE\xBA\xBE\x01\xFF\xFF")  # truncated record

        records = list(WAL.replay(path))
        # Only the good record should be present
        assert len(records) == 1
        assert records[0][1] == b"good"


def test_empty_wal():
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "nonexistent.log"
        assert list(WAL.replay(path)) == []

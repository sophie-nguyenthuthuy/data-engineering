"""Tests for the ReplayEngine."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from replay.engine.checkpoint import CheckpointStore
from replay.engine.engine import ReplayEngine, _TokenBucket
from replay.models import (
    ArchiveFormat,
    Event,
    ReplayConfig,
    ReplayStatus,
    S3ArchiveConfig,
    TargetType,
    TimeWindow,
)
from replay.targets.base import BaseTarget


class _CollectingTarget(BaseTarget):
    """Test target that collects all received events."""

    def __init__(self):
        self.received: list[Event] = []
        self.opened = False
        self.closed = False

    async def open(self):
        self.opened = True

    async def send(self, event: Event):
        self.received.append(event)

    async def close(self):
        self.closed = True


class _FailingTarget(BaseTarget):
    async def open(self): pass
    async def send(self, event: Event):
        raise RuntimeError("target failure")
    async def close(self): pass


def _make_config(tmp_path, window, dry_run=False) -> ReplayConfig:
    return ReplayConfig(
        job_id="test-job",
        topics=["orders"],
        window=window,
        archive=S3ArchiveConfig(
            bucket="test-archive-bucket",
            prefix="topics",
            region="us-east-1",
            format=ArchiveFormat.JSONL,
        ),
        target_type=TargetType.STDOUT,
        dry_run=dry_run,
        checkpoint_dir=str(tmp_path / "checkpoints"),
    )


@pytest.mark.asyncio
class TestReplayEngine:
    async def test_full_replay(self, mock_s3, window, tmp_path):
        cfg = _make_config(tmp_path, window)
        target = _CollectingTarget()
        engine = ReplayEngine(cfg, target)

        result = await engine.run()

        assert result.status == ReplayStatus.COMPLETED
        assert result.replayed_events == 4  # 4 events within window
        assert result.failed_events == 0
        assert target.opened
        assert target.closed
        assert len(target.received) == 4

    async def test_dry_run_does_not_call_target(self, mock_s3, window, tmp_path):
        cfg = _make_config(tmp_path, window, dry_run=True)
        target = _CollectingTarget()
        engine = ReplayEngine(cfg, target)

        result = await engine.run()

        assert result.status == ReplayStatus.COMPLETED
        assert result.replayed_events == 4
        assert len(target.received) == 0  # dry_run: no actual sends

    async def test_target_failures_counted(self, mock_s3, window, tmp_path):
        cfg = _make_config(tmp_path, window)
        target = _FailingTarget()
        engine = ReplayEngine(cfg, target)

        result = await engine.run()

        assert result.failed_events == 4
        assert result.replayed_events == 0

    async def test_checkpoint_skips_completed_keys(self, mock_s3, window, tmp_path):
        cfg = _make_config(tmp_path, window)

        # Pre-seed checkpoint with the key already done
        ckpt = CheckpointStore(cfg.checkpoint_dir, cfg.job_id)
        ckpt.mark_key_done("topics/orders/0000/orders+0000+00000000000000000001.json")
        ckpt.record_progress(4, 0)

        target = _CollectingTarget()
        engine = ReplayEngine(cfg, target)
        result = await engine.run()

        # All keys are already checkpointed → skipped
        assert result.skipped_events >= 1
        assert len(target.received) == 0

    async def test_progress_queue_emits_updates(self, mock_s3, window, tmp_path):
        cfg = _make_config(tmp_path, window)
        target = _CollectingTarget()
        engine = ReplayEngine(cfg, target)

        updates = []
        async def drain():
            while True:
                try:
                    updates.append(engine.progress_queue.get_nowait())
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.05)
                if updates and updates[-1].status in (
                    ReplayStatus.COMPLETED, ReplayStatus.FAILED
                ):
                    return

        await asyncio.gather(engine.run(), drain())
        assert len(updates) >= 1
        assert updates[-1].status == ReplayStatus.COMPLETED


class TestTokenBucket:
    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        import time
        bucket = _TokenBucket(rate=100)  # 100 events/sec
        start = time.monotonic()
        for _ in range(50):
            await bucket.acquire()
        elapsed = time.monotonic() - start
        # 50 events at 100/s should take ~0.5s — give generous tolerance
        assert elapsed < 2.0

    @pytest.mark.asyncio
    async def test_does_not_exceed_rate(self):
        import time
        bucket = _TokenBucket(rate=10)  # slow: 10/s
        start = time.monotonic()
        for _ in range(5):
            await bucket.acquire()
        elapsed = time.monotonic() - start
        # 5 events at 10/s ≥ 0.4s
        assert elapsed >= 0.3


class TestCheckpointStore:
    def test_roundtrip(self, tmp_path):
        ckpt = CheckpointStore(str(tmp_path), "job-1")
        assert not ckpt.is_key_done("some/key")
        ckpt.mark_key_done("some/key")
        assert ckpt.is_key_done("some/key")

    def test_persists_across_instances(self, tmp_path):
        ckpt1 = CheckpointStore(str(tmp_path), "job-2")
        ckpt1.mark_key_done("key/a")
        ckpt1.record_progress(100, 2)

        ckpt2 = CheckpointStore(str(tmp_path), "job-2")
        assert ckpt2.is_key_done("key/a")
        assert ckpt2.get_replayed_count() == 100

    def test_reset_clears_state(self, tmp_path):
        ckpt = CheckpointStore(str(tmp_path), "job-3")
        ckpt.mark_key_done("key/x")
        ckpt.reset()
        assert not ckpt.is_key_done("key/x")

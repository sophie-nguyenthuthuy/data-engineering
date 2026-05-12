"""Tests for the BackfillJob — reads files and produces correct event counts."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from src.lambda_arch.models import Event
from src.migration.backfill import BackfillJob


def _write_events(tmp_path: Path, events: list[Event], filename: str = "2024-01-01.json") -> Path:
    data = [json.loads(e.model_dump_json()) for e in events]
    out = tmp_path / filename
    out.write_text(json.dumps(data))
    return tmp_path


def _make_events(n: int) -> list[Event]:
    return [
        Event(
            event_id=str(uuid.uuid4()),
            user_id=f"user_{i % 10}",
            event_type="view",
            amount=0.0,
            timestamp=datetime(2024, 1, 1, 10, 0, 0),
        )
        for i in range(n)
    ]


class TestBackfillJob:
    def test_dry_run_counts_events(self, tmp_path: Path) -> None:
        """dry_run() must report the correct event count without writing anything."""
        events = _make_events(50)
        hist_dir = _write_events(tmp_path, events)
        job = BackfillJob(historical_dir=hist_dir, local_mode=True, rate=0)
        stats = job.dry_run()
        assert stats["events_found"] == 50

    def test_run_writes_to_local_file(self, tmp_path: Path) -> None:
        """run() in local mode must write events to the local JSONL file."""
        from src.config import LOCAL_KAFKA_FILE

        events = _make_events(20)
        hist_dir = _write_events(tmp_path, events)

        # Use a temp local kafka file to avoid polluting the real one
        import src.config as cfg_module
        original = cfg_module.LOCAL_KAFKA_FILE
        test_kafka_file = tmp_path / "test_kafka.jsonl"
        cfg_module.LOCAL_KAFKA_FILE = test_kafka_file

        try:
            import src.kappa_arch.replay_manager as rm_module
            rm_module.LOCAL_KAFKA_FILE = test_kafka_file

            job = BackfillJob(historical_dir=hist_dir, local_mode=True, rate=0)
            stats = job.run()
            assert stats["events_published"] == 20

            # Verify lines in the JSONL file
            lines = [l for l in test_kafka_file.read_text().splitlines() if l.strip()]
            assert len(lines) == 20
        finally:
            cfg_module.LOCAL_KAFKA_FILE = original
            rm_module.LOCAL_KAFKA_FILE = original

    def test_run_preserves_event_ids(self, tmp_path: Path) -> None:
        """Backfill must preserve original event_ids in the output."""
        from src.config import LOCAL_KAFKA_FILE
        import src.config as cfg_module
        import src.kappa_arch.replay_manager as rm_module

        events = _make_events(5)
        original_ids = {e.event_id for e in events}
        hist_dir = _write_events(tmp_path, events)

        test_kafka_file = tmp_path / "test_kafka2.jsonl"
        original_kafka = cfg_module.LOCAL_KAFKA_FILE
        cfg_module.LOCAL_KAFKA_FILE = test_kafka_file
        rm_module.LOCAL_KAFKA_FILE = test_kafka_file

        try:
            job = BackfillJob(historical_dir=hist_dir, local_mode=True, rate=0)
            job.run()
            published_ids = set()
            for line in test_kafka_file.read_text().splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                published_ids.add(record["payload"]["event_id"])
            assert published_ids == original_ids
        finally:
            cfg_module.LOCAL_KAFKA_FILE = original_kafka
            rm_module.LOCAL_KAFKA_FILE = original_kafka

    def test_multiple_files_aggregated(self, tmp_path: Path) -> None:
        """dry_run should count events across multiple daily files."""
        hist_dir = tmp_path / "historical"
        hist_dir.mkdir()
        for day in range(1, 4):
            _write_events(hist_dir, _make_events(10), filename=f"2024-01-0{day}.json")
        job = BackfillJob(historical_dir=hist_dir, local_mode=True, rate=0)
        stats = job.dry_run()
        assert stats["events_found"] == 30

    def test_empty_directory_yields_zero(self, tmp_path: Path) -> None:
        """Backfill on empty directory must report 0 events."""
        hist_dir = tmp_path / "empty"
        hist_dir.mkdir()
        job = BackfillJob(historical_dir=hist_dir, local_mode=True, rate=0)
        stats = job.dry_run()
        assert stats["events_found"] == 0

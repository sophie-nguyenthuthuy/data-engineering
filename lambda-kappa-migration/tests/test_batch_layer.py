"""Tests for the Lambda batch layer aggregate computations."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from src.lambda_arch.batch_layer import BatchProcessor
from src.lambda_arch.models import Event


class TestBatchProcessorAggregates:
    """Unit tests for BatchProcessor using synthetic in-memory event data."""

    def _write_events(self, tmp_path: Path, events: list[Event]) -> Path:
        """Helper: serialise events to a JSON file in tmp_path."""
        data = [json.loads(e.model_dump_json()) for e in events]
        out = tmp_path / "2024-01-01.json"
        out.write_text(json.dumps(data))
        return tmp_path

    def test_aggregate_counts_match(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Total event count in batch view must match input."""
        hist_dir = self._write_events(tmp_path, sample_events)
        processor = BatchProcessor(historical_dir=hist_dir)
        view = processor.run()

        # Sum all hourly counts
        total_from_hourly = sum(
            c for hour in view.hourly_event_counts.data.values() for c in hour.values()
        )
        assert total_from_hourly == len(sample_events)

    def test_user_totals_event_count(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Sum of per-user event counts must equal total event count."""
        hist_dir = self._write_events(tmp_path, sample_events)
        processor = BatchProcessor(historical_dir=hist_dir)
        view = processor.run()

        total_from_users = sum(
            int(v["event_count"]) for v in view.user_totals.data.values()
        )
        assert total_from_users == len(sample_events)

    def test_event_type_summary_counts(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Sum of event-type counts must equal total event count."""
        hist_dir = self._write_events(tmp_path, sample_events)
        processor = BatchProcessor(historical_dir=hist_dir)
        view = processor.run()

        total = sum(int(v["count"]) for v in view.event_type_summary.data.values())
        assert total == len(sample_events)

    def test_avg_amount_computed_correctly(self, tmp_path: Path) -> None:
        """avg_amount should equal total_amount / count for each event type."""
        events = [
            Event(event_id=str(uuid.uuid4()), user_id="u1", event_type="purchase",
                  amount=100.0, timestamp=datetime(2024, 1, 1, 9, 0)),
            Event(event_id=str(uuid.uuid4()), user_id="u1", event_type="purchase",
                  amount=200.0, timestamp=datetime(2024, 1, 1, 10, 0)),
        ]
        hist_dir = self._write_events(tmp_path, events)
        processor = BatchProcessor(historical_dir=hist_dir)
        view = processor.run()

        summary = view.event_type_summary.data["purchase"]
        assert summary["count"] == 2
        assert abs(float(summary["total_amount"]) - 300.0) < 1e-6
        assert abs(float(summary["avg_amount"]) - 150.0) < 1e-6

    def test_hourly_bucket_grouping(self, tmp_path: Path) -> None:
        """Events in different hours must land in different buckets."""
        events = [
            Event(event_id=str(uuid.uuid4()), user_id="u1", event_type="view",
                  amount=0.0, timestamp=datetime(2024, 1, 1, 8, 30)),
            Event(event_id=str(uuid.uuid4()), user_id="u2", event_type="view",
                  amount=0.0, timestamp=datetime(2024, 1, 1, 9, 15)),
        ]
        hist_dir = self._write_events(tmp_path, events)
        processor = BatchProcessor(historical_dir=hist_dir)
        view = processor.run()

        assert "2024-01-01T08" in view.hourly_event_counts.data
        assert "2024-01-01T09" in view.hourly_event_counts.data

    def test_empty_directory_returns_empty_view(self, tmp_path: Path) -> None:
        """Processing an empty directory should return an empty BatchView."""
        (tmp_path / "historical").mkdir()
        processor = BatchProcessor(historical_dir=tmp_path / "historical")
        view = processor.run()
        assert view.hourly_event_counts.data == {}
        assert view.user_totals.data == {}
        assert view.event_type_summary.data == {}

    def test_user_total_amount_correct(self, tmp_path: Path) -> None:
        """Per-user total_amount must be the sum of their event amounts."""
        events = [
            Event(event_id=str(uuid.uuid4()), user_id="alice", event_type="purchase",
                  amount=50.0, timestamp=datetime(2024, 1, 1, 10, 0)),
            Event(event_id=str(uuid.uuid4()), user_id="alice", event_type="purchase",
                  amount=75.0, timestamp=datetime(2024, 1, 1, 11, 0)),
            Event(event_id=str(uuid.uuid4()), user_id="bob", event_type="view",
                  amount=0.0, timestamp=datetime(2024, 1, 1, 10, 0)),
        ]
        hist_dir = self._write_events(tmp_path, events)
        view = BatchProcessor(historical_dir=hist_dir).run()

        alice = view.user_totals.data["alice"]
        assert abs(float(alice["total_amount"]) - 125.0) < 1e-6
        assert int(alice["event_count"]) == 2

        bob = view.user_totals.data["bob"]
        assert int(bob["event_count"]) == 1

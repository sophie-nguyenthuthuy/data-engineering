"""Tests verifying that Kappa stream processor produces aggregations identical to the batch layer."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from src.kappa_arch.stream_processor import KappaProcessor
from src.lambda_arch.batch_layer import BatchProcessor
from src.lambda_arch.models import Event


def _write_events(tmp_path: Path, events: list[Event]) -> Path:
    data = [json.loads(e.model_dump_json()) for e in events]
    out = tmp_path / "2024-01-01.json"
    out.write_text(json.dumps(data))
    return tmp_path


class TestKappaMatchesBatch:
    def test_event_type_counts_match(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Kappa event-type counts must exactly match batch layer counts."""
        hist_dir = _write_events(tmp_path, sample_events)
        batch_view = BatchProcessor(historical_dir=hist_dir).run()
        kappa = KappaProcessor(local_mode=True)
        kappa.run_replay(historical_dir=hist_dir)
        kappa_results = kappa.get_results()

        for et, batch_vals in batch_view.event_type_summary.data.items():
            kappa_vals = kappa_results["event_type_summary"][et]
            assert int(batch_vals["count"]) == int(kappa_vals["count"]), (
                f"Count mismatch for event_type={et}: batch={batch_vals['count']}, kappa={kappa_vals['count']}"
            )

    def test_user_event_counts_match(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Per-user event counts must be identical between batch and Kappa."""
        hist_dir = _write_events(tmp_path, sample_events)
        batch_view = BatchProcessor(historical_dir=hist_dir).run()
        kappa = KappaProcessor(local_mode=True)
        kappa.run_replay(historical_dir=hist_dir)
        kappa_results = kappa.get_results()

        for uid, batch_vals in batch_view.user_totals.data.items():
            kappa_vals = kappa_results["user_totals"][uid]
            assert int(batch_vals["event_count"]) == int(kappa_vals["event_count"]), (
                f"Event count mismatch for {uid}"
            )

    def test_user_amounts_match_within_tolerance(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Per-user total amounts must match within float precision tolerance."""
        hist_dir = _write_events(tmp_path, sample_events)
        batch_view = BatchProcessor(historical_dir=hist_dir).run()
        kappa = KappaProcessor(local_mode=True)
        kappa.run_replay(historical_dir=hist_dir)
        kappa_results = kappa.get_results()

        for uid, batch_vals in batch_view.user_totals.data.items():
            kappa_vals = kappa_results["user_totals"][uid]
            b_amt = float(batch_vals["total_amount"])
            k_amt = float(kappa_vals["total_amount"])
            assert abs(b_amt - k_amt) < 1e-6, f"Amount mismatch for {uid}: {b_amt} vs {k_amt}"

    def test_hourly_counts_match(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Hourly event counts must exactly match between batch and Kappa."""
        hist_dir = _write_events(tmp_path, sample_events)
        batch_view = BatchProcessor(historical_dir=hist_dir).run()
        kappa = KappaProcessor(local_mode=True)
        kappa.run_replay(historical_dir=hist_dir)
        kappa_results = kappa.get_results()

        for hour, batch_counts in batch_view.hourly_event_counts.data.items():
            kappa_counts = kappa_results["hourly_event_counts"].get(hour, {})
            for et, cnt in batch_counts.items():
                assert cnt == kappa_counts.get(et, 0), (
                    f"Hourly count mismatch at {hour}/{et}: batch={cnt}, kappa={kappa_counts.get(et, 0)}"
                )

    def test_total_event_count_preserved(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Total processed event count in Kappa must match input size."""
        hist_dir = _write_events(tmp_path, sample_events)
        kappa = KappaProcessor(local_mode=True)
        replayed = kappa.run_replay(historical_dir=hist_dir)
        assert replayed == len(sample_events)
        assert kappa.state.event_count == len(sample_events)

    def test_replay_resets_state(self, tmp_path: Path, sample_events: list[Event]) -> None:
        """Running replay twice should produce the same final state (not doubled)."""
        hist_dir = _write_events(tmp_path, sample_events)
        kappa = KappaProcessor(local_mode=True)
        kappa.run_replay(historical_dir=hist_dir)
        count_after_first = kappa.state.event_count
        kappa.run_replay(historical_dir=hist_dir)
        count_after_second = kappa.state.event_count
        assert count_after_first == count_after_second

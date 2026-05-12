"""Tests for the what-if simulator."""
import pytest
from src import Event, FixedLagWatermark, DynamicPerKeyWatermark, TumblingWindow
from src.policies import DropPolicy, RestatePolicy, SideOutputPolicy
from src.simulator import StreamReplay, ReplayConfig, WhatIfComparator


def make_stream(n=50, base_event_time=1000.0, lag=5.0, late_fraction=0.1):
    """Generate a synthetic event stream with some late arrivals."""
    events = []
    for i in range(n):
        et = base_event_time + i
        pt = et + lag
        events.append(Event(event_time=et, key=f"k{i % 3}", value=i,
                             processing_time=pt, sequence_id=i))

    # Add late events
    late_count = int(n * late_fraction)
    for j in range(late_count):
        et = base_event_time + j
        pt = base_event_time + n + 200 + j  # arrive very late
        events.append(Event(event_time=et, key="late_key", value=-j,
                             processing_time=pt, sequence_id=n + j))
    return events


class TestStreamReplay:
    def test_run_returns_metrics(self):
        events = make_stream(30)
        replay = StreamReplay(events)
        cfg = ReplayConfig(
            name="test",
            watermark=FixedLagWatermark(lag_seconds=10),
            window=TumblingWindow(size_seconds=30),
            late_policy=DropPolicy(),
        )
        metrics = replay.run(cfg)
        assert metrics.total_events == len(events)
        assert metrics.windows_emitted >= 0
        assert 0.0 <= metrics.completeness <= 1.0

    def test_higher_lag_higher_completeness(self):
        events = make_stream(100, late_fraction=0.2)
        replay = StreamReplay(events)

        tight_cfg = ReplayConfig(
            "tight",
            FixedLagWatermark(lag_seconds=1),
            TumblingWindow(30),
            DropPolicy(),
        )
        loose_cfg = ReplayConfig(
            "loose",
            FixedLagWatermark(lag_seconds=500),
            TumblingWindow(30),
            DropPolicy(),
        )
        m_tight = replay.run(tight_cfg)
        m_loose = replay.run(loose_cfg)
        # Loose watermark should capture more events
        assert m_loose.completeness >= m_tight.completeness


class TestWhatIfComparator:
    def test_comparator_runs_all_configs(self):
        events = make_stream(60)
        comparator = WhatIfComparator(events)
        for lag in [5, 30, 120]:
            comparator.add(ReplayConfig(
                f"fixed_{lag}s",
                FixedLagWatermark(lag_seconds=lag),
                TumblingWindow(60),
                DropPolicy(),
            ))
        report = comparator.run()
        assert len(report.results) == 3

    def test_summary_table_contains_all_names(self):
        events = make_stream(40)
        comparator = WhatIfComparator(events)
        comparator.add(ReplayConfig("A", FixedLagWatermark(5), TumblingWindow(10), DropPolicy()))
        comparator.add(ReplayConfig("B", FixedLagWatermark(60), TumblingWindow(10), DropPolicy()))
        report = comparator.run()
        table = report.summary_table()
        assert "A" in table and "B" in table

    def test_best_by_completeness(self):
        events = make_stream(60)
        comparator = WhatIfComparator(events)
        comparator.add(ReplayConfig("tight", FixedLagWatermark(1), TumblingWindow(30), DropPolicy()))
        comparator.add(ReplayConfig("loose", FixedLagWatermark(300), TumblingWindow(30), DropPolicy()))
        report = comparator.run()
        best = report.best_by("completeness")
        assert best.config.name == "loose"

    def test_pareto_frontier_non_empty(self):
        events = make_stream(60)
        comparator = WhatIfComparator(events)
        for lag in [1, 10, 60, 300]:
            comparator.add(ReplayConfig(
                f"lag_{lag}",
                FixedLagWatermark(lag_seconds=lag),
                TumblingWindow(30),
                DropPolicy(),
            ))
        report = comparator.run()
        frontier = report.pareto_frontier()
        assert len(frontier) >= 1

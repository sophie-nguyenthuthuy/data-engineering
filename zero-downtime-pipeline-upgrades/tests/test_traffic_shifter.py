"""Tests for TrafficShifter state transitions and auto-promotion logic."""

import time
import pytest
from typing import Any, Dict

from pipeline_deployer import (
    BasePipeline, DeploymentConfig, ShadowRunner,
    TrafficShifter, ShiftState,
)
from pipeline_deployer.comparator import DivergenceTracker


# ---------------------------------------------------------------------------
# Stub pipeline
# ---------------------------------------------------------------------------

class EchoPipeline(BasePipeline):
    def __init__(self, tag: str):
        self._tag = tag

    @property
    def version(self) -> str:
        return self._tag

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return dict(record)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_shifter(
    divergence_rate: float = 0.0,
    threshold: float = 0.05,
    rollback_threshold: float = 0.50,
    step: float = 0.25,
    interval: float = 0.05,
    min_samples: int = 1,
    auto_promote: bool = True,
    auto_rollback: bool = True,
):
    config = DeploymentConfig(
        divergence_threshold=threshold,
        rollback_threshold=rollback_threshold,
        traffic_shift_step=step,
        traffic_shift_interval_sec=interval,
        min_samples_for_promotion=min_samples,
        enable_auto_promotion=auto_promote,
        enable_auto_rollback=auto_rollback,
    )
    tracker = DivergenceTracker(window_size=50)

    # Pre-fill tracker with synthetic scores
    for _ in range(20):
        v1_out = {"val": 1}
        v2_out = {"val": 1} if divergence_rate == 0.0 else {"val": 2}
        tracker.record(v1_out, v2_out)

    v1 = EchoPipeline("v1")
    v2 = EchoPipeline("v2")
    runner = ShadowRunner(v1=v1, v2=v2, config=config, tracker=tracker)

    promoted_calls = []
    rolled_back_calls = []

    shifter = TrafficShifter(
        runner=runner,
        tracker=tracker,
        config=config,
        on_promoted=lambda: promoted_calls.append(1),
        on_rolled_back=lambda: rolled_back_calls.append(1),
    )

    return shifter, runner, tracker, promoted_calls, rolled_back_calls


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTrafficShifterStates:
    def test_initial_state_is_idle(self):
        shifter, *_ = build_shifter()
        assert shifter.state == ShiftState.IDLE

    def test_start_transitions_to_shadow_only(self):
        shifter, *_ = build_shifter()
        shifter.start()
        assert shifter.state == ShiftState.SHADOW_ONLY
        shifter.stop()

    def test_auto_promotion_on_zero_divergence(self):
        shifter, runner, _, promoted, _ = build_shifter(
            divergence_rate=0.0, step=0.5, interval=0.05, min_samples=5
        )
        shifter.start()
        time.sleep(0.6)  # allow several ticks
        shifter.stop()

        assert runner.v2_percentage == pytest.approx(1.0, abs=0.01)
        assert shifter.state == ShiftState.PROMOTED
        assert len(promoted) >= 1

    def test_no_promotion_when_divergence_too_high(self):
        shifter, runner, _, promoted, _ = build_shifter(
            divergence_rate=1.0,  # 100 % divergence
            threshold=0.05,
            step=0.25,
            interval=0.05,
            min_samples=5,
        )
        shifter.start()
        time.sleep(0.4)
        shifter.stop()

        assert runner.v2_percentage == pytest.approx(0.0, abs=0.01)
        assert len(promoted) == 0

    def test_auto_rollback_on_high_divergence_with_live_traffic(self):
        shifter, runner, tracker, _, rolled_back = build_shifter(
            divergence_rate=0.0,  # start clean
            rollback_threshold=0.30,
            step=0.5,
            interval=0.05,
            min_samples=5,
        )
        shifter.start()
        time.sleep(0.3)  # let it promote partway

        # Inject divergent records to spike divergence
        for _ in range(40):
            tracker.record({"val": 1}, {"val": 99})

        time.sleep(0.3)
        shifter.stop()

        assert runner.v2_percentage == pytest.approx(0.0, abs=0.01)
        assert shifter.state == ShiftState.ROLLED_BACK
        assert len(rolled_back) >= 1

    def test_force_shift_sets_percentage(self):
        shifter, runner, *_ = build_shifter()
        shifter.start()
        shifter.force_shift(0.75)
        assert runner.v2_percentage == pytest.approx(0.75)
        shifter.stop()

    def test_force_rollback(self):
        shifter, runner, *_ = build_shifter()
        shifter.start()
        shifter.force_shift(0.60)
        shifter.force_rollback()
        assert runner.v2_percentage == pytest.approx(0.0)
        assert shifter.state == ShiftState.ROLLED_BACK
        shifter.stop()

    def test_pause_halts_shifting(self):
        shifter, runner, _, promoted, _ = build_shifter(
            divergence_rate=0.0, step=0.5, interval=0.05, min_samples=5
        )
        shifter.start()
        shifter.pause()
        time.sleep(0.4)
        shifter.stop()

        # Paused → should not have promoted
        assert shifter.state == ShiftState.PAUSED
        assert len(promoted) == 0

    def test_history_records_events(self):
        shifter, runner, *_ = build_shifter()
        shifter.start()
        shifter.force_shift(0.5)
        shifter.force_rollback()
        shifter.stop()

        history = shifter.history()
        events = [e["event"] for e in history]
        assert "force_shift" in events
        assert "rollback" in events

"""Integration tests for DeploymentOrchestrator."""

import time
import pytest
from typing import Any, Dict

from pipeline_deployer import BasePipeline, DeploymentConfig, DeploymentOrchestrator
from pipeline_deployer.traffic_shifter import ShiftState


# ---------------------------------------------------------------------------
# Stub pipelines
# ---------------------------------------------------------------------------

class EchoPipeline(BasePipeline):
    def __init__(self, tag: str):
        self._tag = tag
        self.setup_called = False
        self.teardown_called = False

    @property
    def version(self) -> str:
        return self._tag

    def setup(self) -> None:
        self.setup_called = True

    def teardown(self) -> None:
        self.teardown_called = True

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return dict(record)


class OffByOnePipeline(BasePipeline):
    """Returns val+1 — always diverges from EchoPipeline on numeric records."""

    def __init__(self, tag: str):
        self._tag = tag

    @property
    def version(self) -> str:
        return self._tag

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(record)
        if "val" in out:
            out["val"] = out["val"] + 1
        return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAST_CONFIG = DeploymentConfig(
    divergence_threshold=0.05,
    rollback_threshold=0.50,
    traffic_shift_step=0.5,
    traffic_shift_interval_sec=0.05,
    min_samples_for_promotion=10,
    comparison_window_size=50,
    enable_auto_promotion=True,
    enable_auto_rollback=True,
)

HIGH_TOLERANCE_CONFIG = DeploymentConfig(
    divergence_threshold=0.99,
    rollback_threshold=1.0,
    traffic_shift_step=0.5,
    traffic_shift_interval_sec=0.05,
    min_samples_for_promotion=5,
    comparison_window_size=50,
)

LOW_ROLLBACK_CONFIG = DeploymentConfig(
    divergence_threshold=0.05,
    rollback_threshold=0.10,
    traffic_shift_step=0.5,
    traffic_shift_interval_sec=0.05,
    min_samples_for_promotion=5,
    comparison_window_size=50,
)


def stream(n=50):
    return [{"val": i, "doc_id": f"d{i}"} for i in range(n)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestOrchestratorLifecycle:
    def test_setup_and_teardown_called(self):
        v1 = EchoPipeline("v1")
        v2 = EchoPipeline("v2")
        orch = DeploymentOrchestrator(v1, v2, FAST_CONFIG)
        orch.start()
        orch.complete()
        assert v1.setup_called
        assert v2.setup_called
        assert v1.teardown_called
        assert v2.teardown_called

    def test_process_before_start_raises(self):
        orch = DeploymentOrchestrator(EchoPipeline("v1"), EchoPipeline("v2"), FAST_CONFIG)
        with pytest.raises(RuntimeError):
            orch.process({"val": 1})

    def test_process_after_complete_raises(self):
        orch = DeploymentOrchestrator(EchoPipeline("v1"), EchoPipeline("v2"), FAST_CONFIG)
        orch.start()
        orch.complete()
        with pytest.raises(RuntimeError):
            orch.process({"val": 1})

    def test_returns_output_for_every_record(self):
        orch = DeploymentOrchestrator(EchoPipeline("v1"), EchoPipeline("v2"), FAST_CONFIG)
        orch.start()
        results = list(orch.process_stream(stream(30)))
        orch.complete()
        assert len(results) == 30

    def test_complete_returns_summary_keys(self):
        orch = DeploymentOrchestrator(EchoPipeline("v1"), EchoPipeline("v2"), FAST_CONFIG)
        orch.start()
        for r in stream(20):
            orch.process(r)
        summary = orch.complete()
        for key in ("state", "promoted", "rolled_back", "runner_stats", "shift_history"):
            assert key in summary


class TestOrchestratorPromotion:
    def test_identical_pipelines_promote(self):
        orch = DeploymentOrchestrator(
            EchoPipeline("v1"), EchoPipeline("v2"), FAST_CONFIG
        )
        orch.start()
        for r in stream(50):
            orch.process(r)
            time.sleep(0.005)
        time.sleep(0.3)
        summary = orch.complete()
        assert summary["promoted"] is True
        assert summary["state"] == ShiftState.PROMOTED.name

    def test_divergent_pipelines_do_not_promote(self):
        orch = DeploymentOrchestrator(
            EchoPipeline("v1"), OffByOnePipeline("v2"), FAST_CONFIG
        )
        orch.start()
        for r in stream(50):
            orch.process(r)
            time.sleep(0.005)
        time.sleep(0.3)
        summary = orch.complete()
        assert summary["promoted"] is False


class TestOrchestratorRollback:
    def test_rollback_on_high_divergence(self):
        orch = DeploymentOrchestrator(
            EchoPipeline("v1"), OffByOnePipeline("v2"), LOW_ROLLBACK_CONFIG
        )
        orch.start()
        # Force live v2 traffic so rollback can trigger
        orch.force_shift(0.5)
        for r in stream(60):
            orch.process(r)
            time.sleep(0.005)
        time.sleep(0.3)
        summary = orch.complete()
        assert summary["rolled_back"] is True

    def test_manual_rollback(self):
        orch = DeploymentOrchestrator(
            EchoPipeline("v1"), EchoPipeline("v2"), FAST_CONFIG
        )
        orch.start()
        orch.force_shift(0.8)
        orch.rollback()
        assert orch.runner.v2_percentage == pytest.approx(0.0)
        orch.complete()


class TestOrchestratorStatus:
    def test_status_reflects_current_state(self):
        orch = DeploymentOrchestrator(EchoPipeline("v1"), EchoPipeline("v2"), FAST_CONFIG)
        orch.start()
        s = orch.status()
        assert s["v1_version"] == "v1"
        assert s["v2_version"] == "v2"
        assert "state" in s
        orch.complete()

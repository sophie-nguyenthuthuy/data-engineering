"""Tests for ShadowRunner routing and divergence recording."""

import time
import pytest
from typing import Any, Dict

from pipeline_deployer import BasePipeline, DeploymentConfig, ShadowRunner
from pipeline_deployer.comparator import DivergenceTracker


# ---------------------------------------------------------------------------
# Stub pipelines
# ---------------------------------------------------------------------------

class ConstantPipeline(BasePipeline):
    """Always returns a fixed output regardless of input."""

    def __init__(self, version_tag: str, output: Dict[str, Any]):
        self._version = version_tag
        self._output = output

    @property
    def version(self) -> str:
        return self._version

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return dict(self._output)


class EchoPipeline(BasePipeline):
    """Echoes the input record back as output."""

    def __init__(self, version_tag: str):
        self._version = version_tag

    @property
    def version(self) -> str:
        return self._version

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return dict(record)


class BrokenPipeline(BasePipeline):
    """Always raises an exception."""

    @property
    def version(self) -> str:
        return "broken"

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("Pipeline exploded")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_runner(v1, v2, v2_pct=0.0, window=100):
    config = DeploymentConfig(initial_v2_percentage=v2_pct)
    tracker = DivergenceTracker(window_size=window)
    runner = ShadowRunner(v1=v1, v2=v2, config=config, tracker=tracker)
    return runner, tracker


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestShadowRunnerRouting:
    def test_v1_is_primary_at_zero_percent(self):
        v1 = ConstantPipeline("v1", {"result": "from-v1"})
        v2 = ConstantPipeline("v2", {"result": "from-v2"})
        runner, _ = make_runner(v1, v2, v2_pct=0.0)

        # With 0 % v2 traffic all responses must come from v1
        outputs = [runner.process({"id": i}) for i in range(50)]
        assert all(o["result"] == "from-v1" for o in outputs)

    def test_v2_is_primary_at_hundred_percent(self):
        v1 = ConstantPipeline("v1", {"result": "from-v1"})
        v2 = ConstantPipeline("v2", {"result": "from-v2"})
        runner, _ = make_runner(v1, v2, v2_pct=1.0)

        outputs = [runner.process({"id": i}) for i in range(50)]
        assert all(o["result"] == "from-v2" for o in outputs)

    def test_mixed_split_both_versions_serve(self):
        v1 = ConstantPipeline("v1", {"result": "from-v1"})
        v2 = ConstantPipeline("v2", {"result": "from-v2"})
        runner, _ = make_runner(v1, v2, v2_pct=0.5)

        results = set()
        for i in range(200):
            out = runner.process({"id": i, "noise": str(i)})
            results.add(out["result"])

        # With 200 varied records and 50 % split both versions should appear
        assert "from-v1" in results
        assert "from-v2" in results

    def test_v2_percentage_setter_clamps(self):
        v1 = EchoPipeline("v1")
        v2 = EchoPipeline("v2")
        runner, _ = make_runner(v1, v2)
        runner.v2_percentage = 1.5
        assert runner.v2_percentage == 1.0
        runner.v2_percentage = -0.5
        assert runner.v2_percentage == 0.0


class TestShadowRunnerDivergence:
    def test_identical_outputs_zero_divergence(self):
        v1 = EchoPipeline("v1")
        v2 = EchoPipeline("v2")
        runner, tracker = make_runner(v1, v2, v2_pct=0.0)

        for i in range(20):
            runner.process({"val": i})

        time.sleep(0.1)  # let shadow threads finish
        assert tracker.window_divergence_rate == 0.0

    def test_different_outputs_divergence_detected(self):
        v1 = ConstantPipeline("v1", {"x": 1})
        v2 = ConstantPipeline("v2", {"x": 2})
        runner, tracker = make_runner(v1, v2, v2_pct=0.0)

        for i in range(30):
            runner.process({"id": i})

        time.sleep(0.1)
        assert tracker.window_divergence_rate == 1.0

    def test_broken_shadow_does_not_crash_primary(self):
        """A crash in the shadow pipeline must not propagate to the caller."""
        v1 = ConstantPipeline("v1", {"ok": True})
        v2 = BrokenPipeline()
        runner, _ = make_runner(v1, v2, v2_pct=0.0)

        out = runner.process({"x": 1})
        time.sleep(0.1)
        assert out == {"ok": True}
        assert runner._v2_errors == 1

    def test_stats_keys_present(self):
        v1 = EchoPipeline("v1")
        v2 = EchoPipeline("v2")
        runner, _ = make_runner(v1, v2)
        runner.process({"a": 1})
        s = runner.stats()
        for key in ("records_processed", "v2_percentage", "v1_errors", "v2_errors",
                    "window_divergence_rate", "mean_divergence_score"):
            assert key in s, f"Missing key: {key}"

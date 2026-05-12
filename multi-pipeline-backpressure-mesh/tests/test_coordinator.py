"""Tests for BackpressureCoordinator signal propagation."""
import asyncio

import pytest

from mesh.bus import InMemoryBus
from mesh.coordinator import BackpressureCoordinator
from mesh.metrics import BackpressureLevel, BackpressureSignal, ThrottleCommand
from mesh.topology import PipelineTopology


@pytest.fixture
def linear_topo():
    return PipelineTopology.linear("A", "B", "C")


@pytest.mark.asyncio
async def test_signal_triggers_upstream_throttle(linear_topo):
    """When C signals backpressure, both A and B should receive throttle commands."""
    bus = InMemoryBus()
    coordinator = BackpressureCoordinator(bus, linear_topo)
    await coordinator.start()

    throttled = {}

    async def capture(cmd: ThrottleCommand):
        throttled[cmd.target_job_id] = cmd.throttle_factor

    await bus.subscribe_throttle("A", capture)
    await bus.subscribe_throttle("B", capture)

    sig = BackpressureSignal("C", BackpressureLevel.HIGH, score=0.80)
    await bus.publish_signal(sig)
    await asyncio.sleep(0.05)  # let async callbacks settle

    assert "A" in throttled, "A should be throttled when C has backpressure"
    assert "B" in throttled, "B should be throttled when C has backpressure"
    # A is further upstream → less throttle than B
    assert throttled["A"] > throttled["B"], "Further upstream = less restriction"

    await coordinator.stop()
    await bus.close()


@pytest.mark.asyncio
async def test_downstream_not_throttled(linear_topo):
    """Throttle commands should not be sent to C's downstream (there are none in linear)."""
    bus = InMemoryBus()
    coordinator = BackpressureCoordinator(bus, linear_topo)
    await coordinator.start()

    wrong_targets = []

    async def capture_c(cmd):
        wrong_targets.append(cmd)

    await bus.subscribe_throttle("C", capture_c)

    sig = BackpressureSignal("B", BackpressureLevel.MEDIUM, score=0.50)
    await bus.publish_signal(sig)
    await asyncio.sleep(0.05)

    assert wrong_targets == [], "Source of backpressure should not receive a throttle command"

    await coordinator.stop()
    await bus.close()


@pytest.mark.asyncio
async def test_unknown_job_signal_ignored():
    topo = PipelineTopology.linear("X", "Y")
    bus = InMemoryBus()
    coordinator = BackpressureCoordinator(bus, topo)
    await coordinator.start()

    sig = BackpressureSignal("Z", BackpressureLevel.CRITICAL, score=1.0)
    await bus.publish_signal(sig)
    await asyncio.sleep(0.05)

    assert coordinator.active_pressure == {}

    await coordinator.stop()
    await bus.close()


@pytest.mark.asyncio
async def test_active_pressure_tracked(linear_topo):
    bus = InMemoryBus()
    coordinator = BackpressureCoordinator(bus, linear_topo)
    await coordinator.start()

    sig = BackpressureSignal("C", BackpressureLevel.MEDIUM, score=0.55)
    await bus.publish_signal(sig)
    await asyncio.sleep(0.05)

    assert "C" in coordinator.active_pressure
    assert abs(coordinator.active_pressure["C"] - 0.55) < 0.01

    await coordinator.stop()
    await bus.close()

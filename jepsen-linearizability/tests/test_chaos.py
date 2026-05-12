"""Tests for chaos components: partition table, clock registry, process registry."""

import time
import pytest
from jepsen.chaos.network import PartitionTable
from jepsen.chaos.clock import ClockRegistry
from jepsen.chaos.process import ProcessRegistry


class TestPartitionTable:
    def test_initially_no_partitions(self):
        t = PartitionTable()
        assert not t.is_partitioned(0, 1)

    def test_partition_and_heal(self):
        t = PartitionTable()
        t.partition(0, 1)
        assert t.is_partitioned(0, 1)
        assert t.is_partitioned(1, 0)  # symmetric
        t.heal(0, 1)
        assert not t.is_partitioned(0, 1)

    def test_heal_all(self):
        t = PartitionTable()
        t.partition(0, 1)
        t.partition(1, 2)
        t.heal()
        assert not t.is_partitioned(0, 1)
        assert not t.is_partitioned(1, 2)

    def test_latency(self):
        t = PartitionTable()
        t.set_latency(0, 1, 150.0)
        assert t.extra_latency(0, 1) == 150.0
        assert t.extra_latency(1, 0) == 150.0  # symmetric
        t.heal(0, 1)
        assert t.extra_latency(0, 1) == 0.0

    def test_active_partitions_list(self):
        t = PartitionTable()
        t.partition(0, 2)
        t.partition(1, 3)
        active = t.active_partitions()
        assert (0, 2) in active
        assert (1, 3) in active


class TestClockRegistry:
    def test_no_skew_returns_monotonic(self):
        r = ClockRegistry()
        t1 = time.monotonic()
        t2 = r.now(0)
        t3 = time.monotonic()
        assert t1 <= t2 <= t3 + 0.01

    def test_positive_skew(self):
        r = ClockRegistry()
        r.skew(0, 10.0)
        t = r.now(0)
        assert t > time.monotonic() + 9.0

    def test_negative_skew(self):
        r = ClockRegistry()
        r.skew(0, -5.0)
        t = r.now(0)
        assert t < time.monotonic() - 4.9

    def test_skew_accumulates(self):
        r = ClockRegistry()
        r.skew(0, 1.0)
        r.skew(0, 2.0)
        offsets = r.offsets()
        assert abs(offsets[0] - 3.0) < 1e-9

    def test_reset_single_node(self):
        r = ClockRegistry()
        r.skew(0, 5.0)
        r.skew(1, 3.0)
        r.reset(0)
        offsets = r.offsets()
        assert 0 not in offsets
        assert offsets[1] == 3.0

    def test_reset_all(self):
        r = ClockRegistry()
        r.skew(0, 1.0)
        r.skew(1, 2.0)
        r.reset()
        assert r.offsets() == {}


class TestProcessRegistry:
    def test_register_and_alive(self):
        r = ProcessRegistry()
        r.register(0, 12345)
        assert r.is_alive(0)

    def test_mark_dead(self):
        r = ProcessRegistry()
        r.register(0, 12345)
        r.mark_dead(0)
        assert not r.is_alive(0)
        assert 0 in r.dead_nodes()

    def test_mark_alive(self):
        r = ProcessRegistry()
        r.register(0, 12345)
        r.mark_dead(0)
        r.mark_alive(0)
        assert r.is_alive(0)

    def test_unknown_node_is_alive(self):
        r = ProcessRegistry()
        # Unregistered nodes are considered alive (not in dead set)
        assert r.is_alive(99)

"""Unit tests for HybridLogicalClock correctness."""
from __future__ import annotations

import pytest

from hlc_store.clock import HybridLogicalClock, WallClock
from hlc_store.timestamp import HLCTimestamp


def fixed_wall(ms: int):
    """Return a wall-clock function that always returns `ms`."""
    return lambda: ms


class TestHLCTick:
    def test_tick_advances_to_wall(self):
        clk = HybridLogicalClock("n1", wall_fn=fixed_wall(1000))
        ts = clk.tick()
        assert ts.wall_ms == 1000
        assert ts.logical == 0

    def test_tick_increments_logical_when_wall_stalls(self):
        clk = HybridLogicalClock("n1", wall_fn=fixed_wall(1000))
        t1 = clk.tick()
        t2 = clk.tick()
        assert t2.wall_ms == t1.wall_ms == 1000
        assert t2.logical == t1.logical + 1

    def test_tick_resets_logical_on_wall_advance(self):
        counter = [1000]

        def advancing_wall():
            counter[0] += 1
            return counter[0]

        clk = HybridLogicalClock("n1", wall_fn=advancing_wall)
        _ = clk.tick()
        _ = clk.tick()
        t3 = clk.tick()
        assert t3.logical == 0  # wall advanced → logical reset

    def test_tick_monotone(self):
        clk = HybridLogicalClock("n1", wall_fn=fixed_wall(500))
        timestamps = [clk.tick() for _ in range(100)]
        for a, b in zip(timestamps, timestamps[1:]):
            assert b > a


class TestHLCUpdate:
    def test_update_advances_past_remote(self):
        clk = HybridLogicalClock("n1", wall_fn=fixed_wall(100))
        remote = HLCTimestamp(wall_ms=500, logical=3)
        ts = clk.update(remote)
        assert ts.wall_ms == 500
        assert ts.logical == 4

    def test_update_breaks_tie_with_max_logical(self):
        clk = HybridLogicalClock("n1", wall_fn=fixed_wall(500))
        clk.tick()  # local = (500, 0)
        clk.tick()  # local = (500, 1)
        remote = HLCTimestamp(wall_ms=500, logical=5)
        ts = clk.update(remote)
        assert ts.wall_ms == 500
        assert ts.logical == 6  # max(1, 5) + 1

    def test_update_uses_wall_when_ahead(self):
        clk = HybridLogicalClock("n1", wall_fn=fixed_wall(1000))
        remote = HLCTimestamp(wall_ms=200, logical=99)
        ts = clk.update(remote)
        assert ts.wall_ms == 1000
        assert ts.logical == 0

    def test_causal_order_preserved_across_nodes(self):
        """
        e1 on n1 → message → e2 on n2.
        Even if n2's wall clock is behind, ts(e2) > ts(e1).
        """
        n1 = HybridLogicalClock("n1", wall_fn=fixed_wall(1000))
        n2 = HybridLogicalClock("n2", wall_fn=fixed_wall(800))  # 200ms behind

        e1_ts = n1.tick()
        e2_ts = n2.update(e1_ts)  # receive e1, then local event

        assert e2_ts > e1_ts, f"Expected {e2_ts} > {e1_ts} — causal order violated"

    def test_causal_order_chain(self):
        """e1 → e2 → e3 across three nodes, all timestamps monotone."""
        n1 = HybridLogicalClock("n1", wall_fn=fixed_wall(1000))
        n2 = HybridLogicalClock("n2", wall_fn=fixed_wall(700))   # 300ms behind
        n3 = HybridLogicalClock("n3", wall_fn=fixed_wall(500))   # 500ms behind

        ts1 = n1.tick()
        ts2 = n2.update(ts1)
        ts3 = n3.update(ts2)

        assert ts3 > ts2 > ts1


class TestWallClockBaseline:
    def test_wall_clock_does_not_preserve_causal_order(self):
        """
        Demonstrates the core problem: a drifted wall clock causes causal inversion.
        n1's clock reads 1000ms (fast); n2's clock reads 700ms (slow).
        e1 on n1 sends to n2; n2 ignores the remote ts and stamps with its own time.
        """
        n1 = HybridLogicalClock("n1", wall_fn=fixed_wall(1000))
        # WallClock for n2 with a controlled wall_fn that returns 700ms
        n2 = WallClock("n2", wall_fn=fixed_wall(700))

        e1_ts = n1.tick()          # (1000, 0)
        e2_ts = n2.update(e1_ts)   # ignores remote — stamps with own wall: (700, 0)

        # n2's clock is behind → e2_ts < e1_ts despite e2 causally following e1
        assert e2_ts < e1_ts, (
            "Expected wall clock to produce causal inversion "
            f"(e1={e1_ts}, e2={e2_ts})"
        )

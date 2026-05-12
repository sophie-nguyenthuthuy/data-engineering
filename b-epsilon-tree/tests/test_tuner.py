"""Tuner + workload observer."""

from __future__ import annotations

from beps.tuner.epsilon import EpsilonTuner
from beps.tuner.observer import Op, WorkloadObserver


class TestObserver:
    def test_initial_50_50(self):
        o = WorkloadObserver()
        assert o.read_fraction == 0.5
        assert o.write_fraction == 0.5

    def test_after_writes(self):
        o = WorkloadObserver()
        for _ in range(10):
            o.observe(Op.WRITE)
        assert o.write_fraction == 1.0

    def test_sliding_window(self):
        o = WorkloadObserver(window=100)
        for _ in range(100):
            o.observe(Op.WRITE)
        assert o.write_fraction == 1.0
        # New reads start pushing out writes
        for _ in range(50):
            o.observe(Op.READ)
        # Now 50 reads + 50 writes (oldest 50 writes evicted)
        assert 0.4 <= o.read_fraction <= 0.6


class TestTuner:
    def test_recommend_write_heavy_increases_epsilon(self):
        t = EpsilonTuner(initial_epsilon=0.5, hysteresis=0.01)
        for _ in range(200):
            t.observe(Op.WRITE)
        rec = t.recommend()
        assert rec > 0.5

    def test_recommend_read_heavy_decreases_epsilon(self):
        t = EpsilonTuner(initial_epsilon=0.5, hysteresis=0.01)
        for _ in range(200):
            t.observe(Op.READ)
        rec = t.recommend()
        assert rec < 0.5

    def test_bounded(self):
        t = EpsilonTuner(eps_min=0.1, eps_max=0.9)
        for _ in range(200):
            t.observe(Op.WRITE)
        rec = t.recommend()
        assert 0.1 <= rec <= 0.9

    def test_hysteresis_prevents_thrashing(self):
        """A tiny change in workload mix should NOT trigger a switch."""
        t = EpsilonTuner(initial_epsilon=0.5, hysteresis=0.2)
        t.observe(Op.WRITE)
        t.observe(Op.READ)   # almost 50/50
        before = t.recommend()
        for _ in range(3):
            t.observe(Op.WRITE)
            t.observe(Op.READ)
        after = t.recommend()
        assert abs(after - before) < 1e-9 or t.n_switches <= 1

"""Strategy controller."""

from __future__ import annotations

from ivm.strategy.controller import StrategyController
from ivm.strategy.cost_model import LinearCostModel


def test_small_delta_stays_delta():
    sc = StrategyController(alpha=0.5, beta=0.3)
    s = sc.decide(delta_size=10, state_size=10_000)
    assert s == "delta"


def test_huge_delta_switches_to_full():
    sc = StrategyController(alpha=0.5, beta=0.3)
    s = sc.decide(delta_size=10_000, state_size=1_000)
    assert s == "full"


def test_hysteresis_prevents_thrashing():
    sc = StrategyController(alpha=0.5, beta=0.3)
    # Switch to full
    sc.decide(delta_size=10_000, state_size=1_000)
    assert sc.strategy == "full"
    # Mid-zone delta size → between alpha and beta → stays full
    sc.decide(delta_size=600, state_size=1_000)   # ratio 1.2 > beta=0.3
    assert sc.strategy == "full"
    # Well below beta → switch back
    sc.decide(delta_size=100, state_size=1_000)   # ratio 0.2 < beta
    assert sc.strategy == "delta"


def test_cost_model_ratio():
    cm = LinearCostModel(per_tuple_delta=2.0, per_tuple_full=1.0)
    # delta_cost = 100 * 2 = 200; full_cost = 1000 * 1 = 1000; ratio 0.2
    assert cm.cost_ratio(100, 1000) == 0.2


def test_history_bounded():
    sc = StrategyController(history_size=5)
    for delta in (100, 200, 300, 400, 500, 600):
        sc.decide(delta_size=delta, state_size=1000)
    assert len(sc.recent_ratios) == 5

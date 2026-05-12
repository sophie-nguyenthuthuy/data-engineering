import random

from src import RowNumberIVM, CorrelatedSubqueryIVM, MaxOfSum, StrategyController


def test_row_number_inserts_at_end():
    ivm = RowNumberIVM()
    ivm.insert("p1", 10, "r1")
    ivm.insert("p1", 20, "r2")
    ivm.insert("p1", 30, "r3")
    assert ivm.rank("p1", 10, "r1") == 1
    assert ivm.rank("p1", 20, "r2") == 2
    assert ivm.rank("p1", 30, "r3") == 3


def test_row_number_insert_in_middle_shifts_suffix():
    ivm = RowNumberIVM()
    ivm.insert("p", 10, "r1")
    ivm.insert("p", 30, "r3")
    # Insert r2 at t=20 → between r1 and r3 → r3's rank should now be 3
    deltas = ivm.insert("p", 20, "r2")
    # Deltas should include r2 (rank 2) and r3 (rank 3)
    delta_dict = dict(deltas)
    assert delta_dict["r2"] == 2
    assert delta_dict["r3"] == 3


def test_correlated_subquery_picks_up_qualifying():
    """orders that exceed per-customer average."""
    ivm = CorrelatedSubqueryIVM()
    ivm.insert("c1", 100)        # avg=100; nothing exceeds
    ivm.insert("c1", 200)        # avg=150; 200 > 150
    qualifying = ivm.qualifying()
    assert ("c1", 200) in qualifying
    assert ("c1", 100) not in qualifying


def test_correlated_subquery_drop_when_avg_grows_past_value():
    ivm = CorrelatedSubqueryIVM()
    ivm.insert("c1", 100)
    ivm.insert("c1", 200)        # avg=150; 200 > 150 ✓
    # Insert a huge value → avg goes up, 200 may fall below
    ivm.insert("c1", 1000)       # avg ≈ 433; only 1000 qualifies now
    qualifying = ivm.qualifying()
    assert ("c1", 1000) in qualifying
    assert ("c1", 200) not in qualifying


def test_max_of_sum_basic():
    ivm = MaxOfSum()
    ivm.insert("2024-01-01", 100)
    ivm.insert("2024-01-02", 50)
    ivm.insert("2024-01-01", 30)    # day1: 130; day2: 50; max = (130, day1)
    val, date = ivm.max
    assert val == 130
    assert date == "2024-01-01"
    # Push day2 above day1
    ivm.insert("2024-01-02", 200)   # day2: 250
    val, date = ivm.max
    assert val == 250
    assert date == "2024-01-02"


def test_max_of_sum_delete_recomputes_when_max_decreases():
    ivm = MaxOfSum()
    ivm.insert("d1", 100)
    ivm.insert("d2", 80)
    val, _ = ivm.max
    assert val == 100
    ivm.delete("d1", 50)           # d1 sum drops to 50; new max should be d2 at 80
    val, date = ivm.max
    assert val == 80 and date == "d2"


def test_strategy_switches_under_heavy_delta():
    sc = StrategyController(alpha=0.5, beta=0.3)
    # Small delta vs big state → cheap delta
    s = sc.decide(delta_size=10, state_size=1_000)
    assert s == "delta"
    # Huge delta → switch to full
    s = sc.decide(delta_size=10_000, state_size=1_000)
    assert s == "full"
    # Back to small delta → hysteresis: stay full until well below beta
    s = sc.decide(delta_size=500, state_size=1_000)
    assert s == "full"   # 500*1 / (1000*0.5) = 1.0 > beta=0.3
    s = sc.decide(delta_size=100, state_size=1_000)
    assert s == "delta"  # 100/500 = 0.2 < beta=0.3 → switch back


def test_correctness_matches_ground_truth_random():
    """For row_number IVM, compare against full recompute on random workloads."""
    rng = random.Random(0)
    ivm = RowNumberIVM()
    rows = []
    for _ in range(200):
        t = rng.uniform(0, 100)
        rid = f"r{len(rows)}"
        ivm.insert("p", t, rid)
        rows.append((t, rid))
    # Ground truth: sort by t
    rows.sort()
    for i, (t, rid) in enumerate(rows):
        assert ivm.rank("p", t, rid) == i + 1

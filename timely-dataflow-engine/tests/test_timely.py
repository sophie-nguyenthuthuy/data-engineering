from src import Timestamp, ProgressTracker, Graph


def test_timestamp_partial_order():
    t00 = Timestamp(0, 0)
    t10 = Timestamp(1, 0)
    t01 = Timestamp(0, 1)
    t11 = Timestamp(1, 1)
    assert t00 < t10
    assert t00 < t11
    # (1,0) and (0,1) are incomparable
    assert not (t10 <= t01)
    assert not (t01 <= t10)


def test_timestamp_lattice_join():
    a = Timestamp(2, 1)
    b = Timestamp(1, 3)
    j = a.join(b)
    assert j == Timestamp(2, 3)


def test_progress_tracker_basic():
    pt = ProgressTracker()
    pt.update("opA", Timestamp(0, 0), +1)
    pt.update("opA", Timestamp(0, 1), +1)
    assert "opA" in pt.active_locations()
    # Frontier = minimal active
    assert pt.frontier("opA") == {Timestamp(0, 0)}
    pt.update("opA", Timestamp(0, 0), -1)
    assert pt.frontier("opA") == {Timestamp(0, 1)}


def test_progress_completion():
    pt = ProgressTracker()
    pt.update("opA", Timestamp(0, 0), +1)
    assert not pt.is_complete_at("opA", Timestamp(0, 0))
    pt.update("opA", Timestamp(0, 0), -1)
    assert pt.is_complete_at("opA", Timestamp(0, 0))


def test_simple_pipeline_executes():
    g = Graph()
    # source emits (0,0) and (1,0); doubler doubles; sink collects
    g.add_sink("sink")
    g.add("doubler", lambda ts, v, emit: emit("sink", ts, v * 2))
    g.send("doubler", Timestamp(0, 0), 5)
    g.send("doubler", Timestamp(1, 0), 7)
    g.run()
    sink_values = [v for _, v in g.sinks["sink"]]
    assert sorted(sink_values) == [10, 14]


def test_iterative_pagerank_like_convergence():
    """Mini PageRank-style fixpoint: each iteration multiplies by 0.5.

    Stop when value < 0.01.
    """
    g = Graph()
    g.add_sink("done")

    def step(ts, v, emit):
        if v < 0.01:
            emit("done", ts, v)
        else:
            emit("loop", ts, v * 0.5)

    g.add("loop", step, feedback=True)
    g.send("loop", Timestamp(0, 0), 1.0)
    g.run()
    # All values should land in done at epoch 0
    final = [(ts, v) for ts, v in g.sinks["done"]]
    assert len(final) == 1
    final_ts, final_v = final[0]
    assert final_v < 0.01
    # Iteration counter increased
    assert final_ts.iteration >= 7    # 0.5^7 ≈ 0.0078125
    assert final_ts.epoch == 0


def test_progress_invariant_holds():
    """Sum of counts ≥ 0 invariant: never produce negative pointstamps."""
    pt = ProgressTracker()
    pt.update("a", Timestamp(0, 0), +3)
    pt.update("a", Timestamp(0, 0), -2)
    assert pt.counts[("a", Timestamp(0, 0))] == 1
    pt.update("a", Timestamp(0, 0), -1)
    # Now zero → entry removed
    assert ("a", Timestamp(0, 0)) not in pt.counts

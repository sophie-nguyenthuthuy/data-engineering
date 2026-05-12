"""Single-worker runtime."""

from __future__ import annotations

import pytest

from timely.graph.builder import GraphBuilder
from timely.graph.runtime import Runtime
from timely.timestamp.ts import Timestamp


def test_source_to_sink():
    g = GraphBuilder()
    g.source("src", [(Timestamp(0, 0), 1), (Timestamp(0, 0), 2)])
    g.map("double", lambda x: x * 2, input="src", downstream="snk")
    g.sink("snk", input="double")
    # Wire source -> double manually via initial emission
    # Hack: re-route source's initial inputs to feed into the map
    # (Our source places records into "src"'s queue; we want them in "double".)
    g.initial_inputs["double"] = g.initial_inputs.pop("src")
    rt = Runtime(g)
    rt.run()
    vals = [v for _, v in g.sinks["snk"]]
    assert sorted(vals) == [2, 4]


def test_filter_drops():
    g = GraphBuilder()
    g.source("src", [(Timestamp(0, 0), 1), (Timestamp(0, 0), 5), (Timestamp(0, 0), 10)])
    g.filter("big", lambda x: x > 3, input="src", downstream="snk")
    g.sink("snk", input="big")
    g.initial_inputs["big"] = g.initial_inputs.pop("src")
    rt = Runtime(g)
    rt.run()
    vals = sorted(v for _, v in g.sinks["snk"])
    assert vals == [5, 10]


def test_reduce_accumulates_per_ts():
    g = GraphBuilder()
    g.source("src", [
        (Timestamp(0, 0), 1), (Timestamp(0, 0), 2), (Timestamp(0, 0), 3),
        (Timestamp(1, 0), 10), (Timestamp(1, 0), 20),
    ])
    g.reduce("sum", lambda acc, v: acc + v, input="src", downstream="snk")
    g.sink("snk", input="sum")
    g.initial_inputs["sum"] = g.initial_inputs.pop("src")
    rt = Runtime(g)
    rt.run()
    # Each emit is the running total; the LAST one per ts is the final value.
    final: dict[Timestamp, int] = {}
    for ts, v in g.sinks["snk"]:
        final[ts] = v
    assert final[Timestamp(0, 0)] == 6
    assert final[Timestamp(1, 0)] == 30


def test_iterate_converges():
    g = GraphBuilder()

    def step(ts: Timestamp, v: float, emit) -> None:
        if v < 0.01:
            emit("done", ts, v)
        else:
            emit("loop", ts, v * 0.5)

    g.iterate("loop", step, input="seed")
    g.source("seed", [(Timestamp(0, 0), 1.0)])
    g.sink("done", input="loop")
    g.initial_inputs["loop"] = g.initial_inputs.pop("seed")
    rt = Runtime(g)
    rt.run()
    final = g.sinks["done"]
    assert len(final) == 1
    ts, v = final[0]
    assert v < 0.01
    assert ts.iteration > 5     # at least 6 halvings needed


def test_max_steps_enforced():
    g = GraphBuilder()

    def infinite(ts: Timestamp, v: int, emit) -> None:
        emit("loop", ts, v + 1)

    g.iterate("loop", infinite, input="seed")
    g.source("seed", [(Timestamp(0, 0), 0)])
    g.initial_inputs["loop"] = g.initial_inputs.pop("seed")
    rt = Runtime(g)
    with pytest.raises(RuntimeError):
        rt.run(max_steps=100)


def test_validate_missing_input():
    g = GraphBuilder()
    g.map("double", lambda x: x, input="missing", downstream="snk")
    g.sink("snk", input="double")
    rt_ctor = Runtime
    import pytest
    with pytest.raises(ValueError):
        rt_ctor(g)

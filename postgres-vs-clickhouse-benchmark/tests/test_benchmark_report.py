"""Benchmark runner + report tests."""

from __future__ import annotations

from itertools import count

import pytest

from pvc.benchmark import BenchmarkRunner
from pvc.engines.injectable import InjectableEngine
from pvc.report import build_comparison
from pvc.workloads.base import Query, Workload


def _engine(name: str, delays: list[float]) -> InjectableEngine:
    """Engine that returns one row and lets the test clock advance per query."""
    eng = InjectableEngine(execute_fn=lambda _sql: [("ok",)])
    eng.name = name
    # Stash delays on the engine for the test clock helper to consume.
    eng.delays = delays  # type: ignore[attr-defined]
    return eng


def _workload() -> Workload:
    return Workload(
        name="w",
        queries=(
            Query(id="q1", description="q1", sql="SELECT 1"),
            Query(id="q2", description="q2", sql="SELECT 2"),
        ),
    )


def test_runner_rejects_no_engines():
    with pytest.raises(ValueError):
        BenchmarkRunner(engines=[], workload=_workload())


def test_runner_rejects_zero_repeat():
    with pytest.raises(ValueError):
        BenchmarkRunner(engines=[_engine("a", [])], workload=_workload(), repeat=0)


def test_runner_rejects_negative_warmup():
    with pytest.raises(ValueError):
        BenchmarkRunner(engines=[_engine("a", [])], workload=_workload(), warmup=-1)


def test_runner_rejects_excessive_trim():
    with pytest.raises(ValueError):
        BenchmarkRunner(engines=[_engine("a", [])], workload=_workload(), repeat=4, trim=2)


def test_runner_collects_samples_with_deterministic_clock():
    eng = _engine("a", [])
    eng.setup(ddl=[], inserts=[])
    # Fake clock advances by 1.0 each call → repeat=3 produces samples of 1.0
    # (clock called once before, once after each iteration).
    clock_iter = (float(x) for x in count(0))
    runner = BenchmarkRunner(
        engines=[eng],
        workload=_workload(),
        warmup=0,
        repeat=3,
        clock=lambda: next(clock_iter),
    )
    results = runner.run()
    assert len(results) == 2  # one entry per query
    # 3 samples per query.
    for r in results:
        assert r.by_engine["a"].stats.n == 3


def test_runner_drops_warmup_iterations():
    calls: list[str] = []

    def runner_fn(sql: str):
        calls.append(sql)
        return [("ok",)]

    eng = InjectableEngine(execute_fn=runner_fn)
    eng.name = "a"
    eng.setup(ddl=[], inserts=[])
    calls.clear()  # ignore setup calls
    BenchmarkRunner(engines=[eng], workload=_workload(), warmup=2, repeat=3).run()
    # For each of 2 queries: 2 warmup + 3 timed = 5 executions.
    assert len(calls) == 10


def test_runner_trim_drops_outliers():
    # Use a fake clock so we can predict the trimmed sample set.
    # 5 samples per query: deltas 1, 2, 3, 4, 5 → trim=1 → {2, 3, 4}
    state = {"t": 0.0}

    def tick() -> float:
        t = state["t"]
        state["t"] += 1.0
        return t

    eng = InjectableEngine(execute_fn=lambda _sql: [("ok",)])
    eng.name = "a"
    eng.setup(ddl=[], inserts=[])
    runner = BenchmarkRunner(
        engines=[eng], workload=_workload(), warmup=0, repeat=5, trim=1, clock=tick
    )
    results = runner.run()
    # samples are sorted-then-trimmed → all deltas are 1.0 each, so stats stay 1.0.
    for r in results:
        s = r.by_engine["a"].stats
        assert s.n == 3
        assert s.min == s.max == 1.0


# --------------------------------------------------------- comparison


def test_build_comparison_baseline_speedup():
    eng_a = _engine("a", [])
    eng_b = _engine("b", [])
    eng_a.setup(ddl=[], inserts=[])
    eng_b.setup(ddl=[], inserts=[])

    state = {"t": 0.0}

    def tick() -> float:
        t = state["t"]
        # Each query iteration takes 1.0 on "a" and 0.5 on "b".
        # We're stubbing only the elapsed difference; the runner reads twice
        # per timed sample (t0, t1).
        state["t"] = t + 0.0
        return t

    # Replace the bench runner with deterministic samples.
    from pvc.benchmark import IterationResult, QueryResult
    from pvc.stats import summarise

    qr = QueryResult(
        query_id="q1",
        by_engine={
            "a": IterationResult(
                engine="a",
                query_id="q1",
                samples=(1.0, 1.0, 1.0),
                stats=summarise([1.0, 1.0, 1.0]),
            ),
            "b": IterationResult(
                engine="b",
                query_id="q1",
                samples=(0.5, 0.5, 0.5),
                stats=summarise([0.5, 0.5, 0.5]),
            ),
        },
    )
    report = build_comparison([qr], baseline="a")
    by_q = report.by_query()
    speedups = {r.engine: r.speedup_vs_baseline for r in by_q["q1"]}
    assert speedups["a"] == 1.0
    assert speedups["b"] == 2.0  # baseline 1.0 / engine 0.5
    assert report.winners() == {"q1": "b"}


def test_build_comparison_rejects_empty_and_unknown_baseline():
    from pvc.benchmark import IterationResult, QueryResult
    from pvc.stats import summarise

    with pytest.raises(ValueError):
        build_comparison([], baseline="a")
    qr = QueryResult(
        query_id="q1",
        by_engine={
            "a": IterationResult(
                engine="a",
                query_id="q1",
                samples=(1.0,),
                stats=summarise([1.0]),
            ),
        },
    )
    with pytest.raises(ValueError):
        build_comparison([qr], baseline="zzz")


def test_query_result_winner_picks_lowest_p50():
    from pvc.benchmark import IterationResult, QueryResult
    from pvc.stats import summarise

    qr = QueryResult(
        query_id="q",
        by_engine={
            "a": IterationResult("a", "q", (5.0,), summarise([5.0])),
            "b": IterationResult("b", "q", (1.0,), summarise([1.0])),
        },
    )
    assert qr.winner() == "b"

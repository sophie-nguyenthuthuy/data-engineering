"""Pattern DSL and compiler unit tests."""

import pytest
from cep.pattern import Pattern, StepPredicate
from cep.compiler import PatternCompiler


class E:
    A, B, C = 1, 2, 3


def test_builder_fluent():
    p = Pattern("p1").begin(E.A).then(E.B).then(E.C).total_window(5_000_000_000)
    assert len(p) == 3
    assert p.steps[0].type_id == E.A
    assert p.steps[2].type_id == E.C
    assert p._total_window_ns == 5_000_000_000


def test_builder_requires_begin():
    with pytest.raises(ValueError):
        Pattern("bad").then(E.B)


def test_predicate_conditions():
    p = (
        Pattern("pred_test")
        .begin(E.A, value_gte=10.0, value_lte=100.0, flags_mask=0b11, flags_value=0b10)
        .then(E.B)
        .total_window(10_000_000_000)
    )
    pred = p.steps[0]
    assert pred.value_gte == 10.0
    assert pred.value_lte == 100.0
    assert pred.flags_mask == 0b11
    assert pred.flags_value == 0b10


def test_compiler_generates_valid_source():
    compiler = PatternCompiler()
    p = Pattern("gen").begin(E.A).then(E.B).then(E.C).total_window(30_000_000_000)
    src = compiler.source(p)
    assert "_cep_match_gen" in src
    assert "njit" in src
    assert "step_arr" in src
    assert "matched" in src


def test_compiler_produces_callable():
    compiler = PatternCompiler()
    p = Pattern("call_test").begin(E.A).then(E.B).total_window(10_000_000_000)
    cp = compiler.compile(p)
    assert callable(cp.match_fn)
    assert cp.name == "call_test"


def test_python_fallback():
    from cep.compiler import _make_python_fallback, MAX_ENTITIES
    import numpy as np

    p = Pattern("fb").begin(E.A).then(E.B).total_window(10_000_000_000)
    fn = _make_python_fallback(p)

    step_arr = np.zeros(MAX_ENTITIES, np.int8)
    count_arr = np.zeros(MAX_ENTITIES, np.int32)
    start_ts = np.zeros(MAX_ENTITIES, np.int64)
    last_ts = np.zeros(MAX_ENTITIES, np.int64)

    eid = np.int64(1)
    assert not fn(np.int32(E.A), eid, np.int64(1000), np.float64(0), np.uint32(0),
                  step_arr, count_arr, start_ts, last_ts)
    assert not fn(np.int32(E.C), eid, np.int64(2000), np.float64(0), np.uint32(0),
                  step_arr, count_arr, start_ts, last_ts)
    assert fn(np.int32(E.B), eid, np.int64(3000), np.float64(0), np.uint32(0),
              step_arr, count_arr, start_ts, last_ts)

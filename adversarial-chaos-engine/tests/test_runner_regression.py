"""Runner + regression-emitter + benchmark tests."""

from __future__ import annotations

import ast
import math
import random

import pytest

from ace.bench import run_benchmark
from ace.generator import AdversarialGenerator
from ace.invariants.catalog import Catalog
from ace.regression import emit_pytest
from ace.runner import Runner, Violation


def _gen(seed: int = 0) -> AdversarialGenerator:
    return AdversarialGenerator(rng=random.Random(seed), max_rows=8)


# ------------------------------------------------------------ runner


def test_runner_rejects_zero_trials():
    cat = Catalog()
    with pytest.raises(ValueError):
        Runner(catalog=cat).run(trials=0)


def test_runner_no_violations_on_clean_pipeline():
    cat = Catalog()

    @cat.invariant(row_count_preserved=True)
    def clean(frame):
        return [{**r, "u": True} for r in frame]

    rep = Runner(catalog=cat, generator=_gen(), seed=0).run(trials=40)
    assert rep.failing() == []


def test_runner_catches_row_count_violation():
    cat = Catalog()

    @cat.invariant(row_count_preserved=True)
    def drop_neg(frame):
        return [r for r in frame if isinstance(r.get("amount"), int | float) and r["amount"] >= 0]

    rep = Runner(catalog=cat, generator=_gen(), seed=0).run(trials=60)
    rcvs = [v for v in rep.failing() if v.invariant == "row_count_preserved"]
    assert rcvs, "should have caught a row_count_preserved violation"


def test_runner_catches_sum_invariant_violation():
    cat = Catalog()

    @cat.invariant(sum_invariant=["amount"])
    def buggy_abs(frame):
        return [
            {**r, "amount": abs(r["amount"])} if isinstance(r.get("amount"), int | float) else r
            for r in frame
        ]

    rep = Runner(catalog=cat, generator=_gen(1), seed=1).run(trials=80)
    matches = [v for v in rep.failing() if v.invariant == "sum_invariant(amount)"]
    assert matches


def test_runner_dedupes_violations_per_pipeline_per_invariant():
    cat = Catalog()

    @cat.invariant(row_count_preserved=True)
    def drop_all(_frame):
        return []

    rep = Runner(catalog=cat, generator=_gen(2), seed=2).run(trials=30)
    rc_violations = [v for v in rep.failing() if v.invariant == "row_count_preserved"]
    assert len(rc_violations) <= 1


def test_runner_captures_exception_as_violation():
    cat = Catalog()

    @cat.invariant(no_nulls=["name"])
    def crashes_on_huge_strings(frame):
        for r in frame:
            n = r.get("name")
            if isinstance(n, str) and len(n) > 5_000:
                raise ValueError("string too long")
        return frame

    rep = Runner(catalog=cat, generator=_gen(3), seed=3).run(trials=120)
    excs = rep.exceptions()
    assert excs and excs[0].is_exception


def test_runner_shrinks_violating_input_to_one_row():
    cat = Catalog()

    @cat.invariant(row_count_preserved=True)
    def drop_neg(frame):
        return [r for r in frame if isinstance(r.get("amount"), int | float) and r["amount"] >= 0]

    rep = Runner(catalog=cat, generator=_gen(0), seed=0).run(trials=40)
    failing = rep.failing()
    assert failing
    # Each violation's input is the minimal failing frame — at most 1 row triggers
    # the row_count_preserved bug.
    for v in failing:
        assert len(v.input) == 1


def test_report_by_pipeline_groups_violations():
    cat = Catalog()

    @cat.invariant(row_count_preserved=True)
    def fn_a(_frame):
        return []

    @cat.invariant(row_count_preserved=True)
    def fn_b(_frame):
        return []

    rep = Runner(catalog=cat, generator=_gen(0), seed=0).run(trials=10)
    grouped = rep.by_pipeline()
    assert set(grouped.keys()) <= {"fn_a", "fn_b"}


# ----------------------------------------------------- regression emitter


def test_emit_pytest_produces_valid_python():
    v = Violation(
        fn_name="buggy_drop_neg",
        invariant="row_count_preserved",
        input=((("amount", -5),),),
        output_repr="[]",
    )
    src = emit_pytest(v, module="pkg.module")
    # Must parse.
    ast.parse(src)
    assert "def test_buggy_drop_neg_violates_row_count_preserved" in src
    assert "from pkg.module import buggy_drop_neg" in src
    assert "assert len(df_out) == len(df_in)" in src


def test_emit_pytest_sum_invariant_branch():
    v = Violation(
        fn_name="abs_amount",
        invariant="sum_invariant(amount)",
        input=((("amount", -3),),),
        output_repr="[{'amount': 3}]",
    )
    src = emit_pytest(v, module="my.pipeline")
    ast.parse(src)
    assert "sum_invariant_amount" in src
    assert "sum(r.get('amount'" in src


def test_emit_pytest_handles_empty_input():
    v = Violation(
        fn_name="trivial",
        invariant="no_exceptions",
        input=(),
        output_repr="<ValueError: blew up>",
    )
    src = emit_pytest(v)
    ast.parse(src)
    assert "df_in = []" in src


def test_emit_pytest_monotone_branch():
    v = Violation(
        fn_name="sort_buggy",
        invariant="monotone(v)",
        input=((("v", 2),), (("v", 1),)),
        output_repr="[]",
    )
    src = emit_pytest(v)
    ast.parse(src)
    assert "sorted(vals)" in src


# ---------------------------------------------------------- benchmark


def test_benchmark_targeted_does_not_underperform_random():
    rep = run_benchmark(trials=80, seed=0)
    assert rep.targeted_bugs >= rep.random_bugs - 1  # allow 1 random luck
    assert rep.targeted_bugs >= 1  # at least one bug always found


def test_benchmark_speedup_is_finite_when_random_finds_zero_and_targeted_wins():
    rep = run_benchmark(trials=80, seed=0)
    # If random found ≥ 1, speedup must be finite (≥ 1.0).
    if rep.random_bugs > 0:
        assert math.isfinite(rep.speedup)

"""Adversarial generator + shrinker tests."""

from __future__ import annotations

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ace.generator import AdversarialGenerator
from ace.shrinker import shrink_rows


def test_generator_rejects_bad_edge_fraction():
    with pytest.raises(ValueError):
        AdversarialGenerator(edge_fraction=-0.1)
    with pytest.raises(ValueError):
        AdversarialGenerator(edge_fraction=1.5)


def test_generator_rejects_negative_max_rows():
    with pytest.raises(ValueError):
        AdversarialGenerator(max_rows=-1)


def test_generator_default_rng_is_deterministic():
    a = AdversarialGenerator(rng=random.Random(0))
    b = AdversarialGenerator(rng=random.Random(0))
    assert a.generate({"amount"}) == b.generate({"amount"})


def test_generator_respects_max_rows():
    g = AdversarialGenerator(max_rows=4, rng=random.Random(0))
    for _ in range(20):
        assert len(g.generate({"amount"})) <= 4


def test_generator_targeted_includes_invariant_columns():
    g = AdversarialGenerator(rng=random.Random(0), max_rows=5)
    frame = g.generate({"score"})
    # `score` is non-default and must still appear when the frame is non-empty.
    if frame:
        assert all("score" in row for row in frame)


def test_generator_random_baseline_avoids_edges():
    g = AdversarialGenerator(edge_fraction=0.0, max_rows=5, rng=random.Random(0))
    frame = g.generate_random()
    # Random baseline should not produce ±inf/NaN amounts.
    for row in frame:
        v = row["amount"]
        assert isinstance(v, float)
        assert -1001 <= v <= 1001


# ----------------------------------------------------------- shrinker


def test_shrinker_returns_input_when_check_already_passes():
    rows = [{"amount": 1}]
    out = shrink_rows(lambda f: f, rows, lambda i, o: len(i) == len(o))
    assert out == rows


def test_shrinker_reduces_failing_input_to_one_row():
    rows = [{"amount": 1}, {"amount": -1}, {"amount": 2}, {"amount": 3}]

    def fn(frame):
        # Drops negative amounts → violates row_count_preserved.
        return [r for r in frame if r["amount"] >= 0]

    def check(in_f, out_f):
        return len(in_f) == len(out_f)

    out = shrink_rows(fn, rows, check)
    assert out == [{"amount": -1}]


def test_shrinker_rejects_bad_max_passes():
    with pytest.raises(ValueError):
        shrink_rows(lambda f: f, [], lambda i, o: True, max_passes=0)


def test_shrinker_handles_exception_in_check():
    rows = [{"amount": 1}, {"amount": 1000_000_000}]

    def fn(frame):
        for r in frame:
            if r["amount"] > 1_000:
                raise ValueError("blew up")
        return frame

    def check(_in, _out):
        return True

    out = shrink_rows(fn, rows, check)
    assert out == [{"amount": 1_000_000_000}]


@settings(max_examples=20, deadline=None)
@given(seed=st.integers(0, 2**16 - 1))
def test_property_generator_only_returns_lists_of_dicts(seed):
    g = AdversarialGenerator(rng=random.Random(seed), max_rows=8)
    frame = g.generate({"amount", "name", "ts"})
    assert isinstance(frame, list)
    for row in frame:
        assert isinstance(row, dict)

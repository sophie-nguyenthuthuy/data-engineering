"""Hypothesis property tests on the timestamp lattice."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from timely.timestamp.antichain import Antichain
from timely.timestamp.ts import Timestamp

_ts_strategy = st.builds(
    Timestamp,
    epoch=st.integers(min_value=0, max_value=10),
    iteration=st.integers(min_value=0, max_value=10),
)


@given(a=_ts_strategy, b=_ts_strategy)
@settings(max_examples=100, deadline=None)
def test_partial_order_reflexive(a: Timestamp, b: Timestamp):
    assert a <= a


@given(a=_ts_strategy, b=_ts_strategy, c=_ts_strategy)
@settings(max_examples=100, deadline=None)
def test_partial_order_transitive(a, b, c):
    if a <= b and b <= c:
        assert a <= c


@given(a=_ts_strategy, b=_ts_strategy)
@settings(max_examples=100, deadline=None)
def test_join_is_upper_bound(a, b):
    j = a.join(b)
    assert a <= j and b <= j


@given(a=_ts_strategy, b=_ts_strategy)
@settings(max_examples=100, deadline=None)
def test_meet_is_lower_bound(a, b):
    m = a.meet(b)
    assert m <= a and m <= b


@given(elements=st.lists(_ts_strategy, min_size=1, max_size=10))
@settings(max_examples=100, deadline=None)
def test_antichain_pairwise_incomparable(elements):
    ac = Antichain()
    for t in elements:
        ac.insert(t)
    survivors = list(ac)
    for i, a in enumerate(survivors):
        for j, b in enumerate(survivors):
            if i == j:
                continue
            assert not (a < b), f"antichain has {a} < {b}"

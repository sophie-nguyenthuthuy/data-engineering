"""Hypothesis: IVM result matches full recompute under arbitrary ops."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from ivm.correlated.subquery import CorrelatedSubqueryIVM
from ivm.nested.max_of_sum import MaxOfSum
from ivm.window.row_number import RowNumberIVM


@st.composite
def _rn_ops(draw):
    n = draw(st.integers(min_value=1, max_value=40))
    return [
        (
            draw(st.sampled_from(["insert", "insert", "insert", "delete"])),
            draw(st.floats(min_value=0, max_value=100, allow_nan=False)),
        )
        for _ in range(n)
    ]


@given(operations=_rn_ops())
@settings(max_examples=60, deadline=None)
def test_row_number_matches_ground_truth(operations):
    rn = RowNumberIVM()
    rows: list[tuple[float, int]] = []
    for i, (op, t) in enumerate(operations):
        rid = i
        if op == "insert":
            rn.insert("p", t, rid)
            rows.append((t, rid))
        else:
            # Delete a random existing row if any (use the latest)
            if rows:
                tt, rrid = rows.pop()
                rn.delete("p", tt, rrid)
    # Ground truth: sort rows by t, rank = position+1
    rows.sort()
    for rank, (t, rid) in enumerate(rows, start=1):
        assert rn.rank("p", t, rid) == rank


@st.composite
def _mos_ops(draw):
    n = draw(st.integers(min_value=1, max_value=30))
    return [
        (
            draw(st.text(min_size=1, max_size=3, alphabet="abc")),
            draw(st.integers(min_value=1, max_value=100)),
        )
        for _ in range(n)
    ]


@given(operations=_mos_ops())
@settings(max_examples=60, deadline=None)
def test_max_of_sum_matches_ground_truth(operations):
    mos = MaxOfSum()
    sums: dict[str, int] = {}
    for k, v in operations:
        mos.insert(k, v)
        sums[k] = sums.get(k, 0) + v
    expected_max = max(sums.values())
    actual_max, _ = mos.max
    assert actual_max == expected_max


@st.composite
def _cq_ops(draw):
    n = draw(st.integers(min_value=1, max_value=20))
    return [
        (
            draw(st.sampled_from(["c1", "c2", "c3"])),
            draw(st.floats(min_value=1, max_value=1000, allow_nan=False)),
        )
        for _ in range(n)
    ]


@given(operations=_cq_ops())
@settings(max_examples=60, deadline=None)
def test_correlated_subquery_matches_full_scan(operations):
    cq = CorrelatedSubqueryIVM()
    all_rows: list[tuple[str, float]] = []
    for c, a in operations:
        cq.insert(c, a)
        all_rows.append((c, a))
    # Ground truth: each row qualifies iff its amount > per-customer AVG
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    for c, a in all_rows:
        sums[c] = sums.get(c, 0.0) + a
        counts[c] = counts.get(c, 0) + 1
    expected = {
        (c, a) for c, a in all_rows
        if a > sums[c] / counts[c]
    }
    actual = set(cq.qualifying())
    assert actual == expected

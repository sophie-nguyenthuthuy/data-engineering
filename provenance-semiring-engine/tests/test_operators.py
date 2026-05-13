"""Annotated relational operators."""

from __future__ import annotations

from prov.lineage import lineage, multiplicity, witnesses
from prov.operators import aggregate, annotate, join, project, select, union
from prov.semiring.bag import BagSemiring
from prov.semiring.how import HowProvenance
from prov.semiring.why import WhyProvenance


def _build_how(rows, prefix: str):
    H = HowProvenance()
    return annotate(rows, lambda i, _t: H.singleton(f"{prefix}{i+1}"), H)


def test_annotate_dedupes_via_plus():
    K = BagSemiring()
    rel = annotate([(1,), (1,), (2,)], lambda i, _t: 1, K)
    assert rel[(1,)] == 2
    assert rel[(2,)] == 1


def test_select_preserves_annotations():
    H = HowProvenance()
    rel = _build_how([(1,), (2,), (3,)], "t")
    s = select(rel, lambda t: t[0] > 1, H)
    assert set(s.keys()) == {(2,), (3,)}


def test_project_collapses_annotations():
    H = HowProvenance()
    rel = _build_how([(1, 10), (1, 20), (2, 5)], "t")
    p = project(rel, (0,), H)
    # (1,) should aggregate t1 + t2
    assert lineage(p[(1,)]) == {"t1", "t2"}
    assert lineage(p[(2,)]) == {"t3"}


def test_union_combines_matching_tuples():
    K = BagSemiring()
    a = annotate([(1,), (2,)], lambda i, _t: 1, K)
    b = annotate([(2,), (3,)], lambda i, _t: 1, K)
    u = union(a, b, K)
    assert u[(1,)] == 1
    assert u[(2,)] == 2     # appeared in both
    assert u[(3,)] == 1


def test_join_multiplies_annotations():
    H = HowProvenance()
    R = _build_how([(1, "x"), (2, "y")], "r")
    S = _build_how([(1, "a"), (2, "b")], "s")
    J = join(R, S, key_a=(0,), key_b=(0,), K=H)
    # (1, "x", 1, "a") should have annotation r1 * s1
    assert lineage(J[(1, "x", 1, "a")]) == {"r1", "s1"}


def test_join_multiple_matches_sum_via_plus():
    H = HowProvenance()
    R = _build_how([(1,), (1,)], "r")   # two rows with key=1
    S = _build_how([(1,)], "s")
    J = join(R, S, key_a=(0,), key_b=(0,), K=H)
    # Combined tuple (1, 1) has annotation r1*s1 + r2*s1
    poly = J[(1, 1)]
    assert lineage(poly) == {"r1", "r2", "s1"}
    assert witnesses(poly) == 2


def test_aggregate_groups():
    H = HowProvenance()
    rel = _build_how([(1, 10), (1, 20), (2, 5)], "t")
    a = aggregate(rel, (0,), H)
    assert (1,) in a
    assert (2,) in a


def test_bag_join_counts_combinations():
    K = BagSemiring()
    R = {(1,): 3}     # 3 copies of (1,)
    S = {(1, "a"): 2}  # 2 copies of (1, "a")
    J = join(R, S, key_a=(0,), key_b=(0,), K=K)
    # (1, 1, "a") should appear 3 * 2 = 6 times
    assert J[(1, 1, "a")] == 6


def test_why_join_combines_witnesses():
    W = WhyProvenance()
    R = annotate([(1, "x")], lambda i, _t: W.singleton(f"r{i+1}"), W)
    S = annotate([(1, "a")], lambda i, _t: W.singleton(f"s{i+1}"), W)
    J = join(R, S, key_a=(0,), key_b=(0,), K=W)
    # Single witness with both r1 and s1
    ann = J[(1, "x", 1, "a")]
    assert ann == frozenset({frozenset({"r1", "s1"})})


def test_multiplicity():
    H = HowProvenance()
    rel = _build_how([(1,), (1,), (1,)], "t")    # 3 rows
    # Project to () would aggregate all three
    p = project(rel, (), H)
    # After project, only one tuple key () with annotation t1+t2+t3
    assert multiplicity(p[()]) == 3

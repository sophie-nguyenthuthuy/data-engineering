"""End-to-end: small queries across multiple semirings."""

from __future__ import annotations

from prov.lineage import lineage
from prov.operators import annotate, join, project, select
from prov.semiring.bag import BagSemiring
from prov.semiring.how import HowProvenance
from prov.semiring.trics import TriCS
from prov.semiring.why import WhyProvenance


def test_filter_join_project_chain():
    """Hanoi customers' order amounts."""
    customers = [(1, "Hanoi"), (2, "HCMC"), (3, "Hanoi")]
    orders = [(10, 1, 50), (11, 1, 70), (12, 2, 30), (13, 3, 25)]

    H = HowProvenance()
    cust = annotate(customers, lambda i, _t: H.singleton(f"c{i+1}"), H)
    ord_ = annotate(orders, lambda i, _t: H.singleton(f"o{i+1}"), H)

    hn = select(cust, lambda t: t[1] == "Hanoi", H)
    j = join(hn, ord_, key_a=(0,), key_b=(1,), K=H)
    amounts = project(j, (4,), H)

    # 3 amounts: 50, 70 (from c1) and 25 (from c3)
    assert 50 in {t[0] for t in amounts}
    assert 70 in {t[0] for t in amounts}
    assert 25 in {t[0] for t in amounts}
    # Each amount's lineage should reference the right customer + order
    assert lineage(amounts[(50,)]) == {"c1", "o1"}
    assert lineage(amounts[(70,)]) == {"c1", "o2"}
    assert lineage(amounts[(25,)]) == {"c3", "o4"}


def test_bag_counting_pipeline():
    """Counting semantics: how many derivations of each result?"""
    K = BagSemiring()
    R = annotate([(1,), (1,)], lambda i, _t: 1, K)   # two 1s
    S = annotate([(1,)], lambda i, _t: 1, K)
    J = join(R, S, key_a=(0,), key_b=(0,), K=K)
    # (1, 1) appears 2*1 = 2 times
    assert J[(1, 1)] == 2


def test_why_witness_assembly():
    """Why-provenance: each output's witnesses = sets of contributing inputs."""
    W = WhyProvenance()
    R = annotate([(1, "x"), (1, "y")], lambda i, _t: W.singleton(f"r{i+1}"), W)
    S = annotate([(1, "a")], lambda i, _t: W.singleton(f"s{i+1}"), W)
    J = join(R, S, key_a=(0,), key_b=(0,), K=W)
    # Two output tuples: (1, "x", 1, "a") and (1, "y", 1, "a")
    assert len(J) == 2
    for ann in J.values():
        # Each is a single witness with two tokens
        wits = W.witnesses(ann)
        assert len(wits) == 1
        assert len(wits[0]) == 2


def test_trics_independence():
    """TriCS treats events as independent; check basic correctness."""
    T = TriCS()
    R = {(1,): 0.5, (2,): 0.3}
    S = {(1,): 0.7}
    J = join(R, S, key_a=(0,), key_b=(0,), K=T)
    # (1, 1) annotation = 0.5 * 0.7 = 0.35
    assert abs(J[(1, 1)] - 0.35) < 1e-9

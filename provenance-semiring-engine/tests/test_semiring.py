"""Semiring axiom tests + operator tests."""
from src import BagSemiring, WhyProvenance, HowProvenance, TriCS
from src import Polynomial, Monomial
from src import annotate, project, select, union, join
from src import lineage, witness_count


# ---------------------------------------------------------------------------
# Semiring axioms (spot-check on each instance)
# ---------------------------------------------------------------------------

def _check_axioms(K, samples):
    """plus/times associative + commutative, distributivity, identity."""
    a, b, c = samples
    # Identity
    assert K.plus(a, K.zero()) == a
    assert K.times(a, K.one()) == a
    # Commutativity
    assert K.plus(a, b) == K.plus(b, a)
    assert K.times(a, b) == K.times(b, a)
    # Associativity
    assert K.plus(K.plus(a, b), c) == K.plus(a, K.plus(b, c))
    assert K.times(K.times(a, b), c) == K.times(a, K.times(b, c))
    # Distributivity
    assert K.times(a, K.plus(b, c)) == K.plus(K.times(a, b), K.times(a, c))


def test_bag_axioms():
    _check_axioms(BagSemiring(), [2, 3, 5])


def test_why_axioms():
    W = WhyProvenance()
    a = W.singleton("t1")
    b = W.singleton("t2")
    c = W.singleton("t3")
    _check_axioms(W, [a, b, c])


def test_how_axioms_distributivity():
    H = HowProvenance()
    a = H.singleton("t1")
    b = H.singleton("t2")
    c = H.singleton("t3")
    # Distributivity is the heart of provenance reasoning
    left = H.times(a, H.plus(b, c))
    right = H.plus(H.times(a, b), H.times(a, c))
    assert left.coeffs == right.coeffs


def test_trics_independent_or():
    T = TriCS()
    # p ⊕ q = 1 - (1-p)(1-q)
    assert abs(T.plus(0.3, 0.5) - (1 - 0.7 * 0.5)) < 1e-9
    # p ⊗ q = p*q
    assert abs(T.times(0.3, 0.5) - 0.15) < 1e-9


# ---------------------------------------------------------------------------
# Operator tests
# ---------------------------------------------------------------------------

def test_join_under_how():
    H = HowProvenance()
    # R(a, x): t1, t2
    R = annotate([(1, 10), (2, 20)], lambda i, t: H.singleton(f"r{i+1}"), H)
    # S(a, y): s1, s2
    S = annotate([(1, "A"), (2, "B")], lambda i, t: H.singleton(f"s{i+1}"), H)
    J = join(R, S, key_a=(0,), key_b=(0,), K=H)
    # (1,10,1,"A") should have annotation r1*s1
    assert (1, 10, 1, "A") in J
    poly = J[(1, 10, 1, "A")]
    assert lineage(poly) == {"r1", "s1"}


def test_union_collapses_under_bag():
    B = BagSemiring()
    R = annotate([(1,), (1,), (2,)], lambda i, t: 1, B)
    S = annotate([(1,), (3,)], lambda i, t: 1, B)
    U = union(R, S, B)
    assert U[(1,)] == 3            # 2 from R + 1 from S
    assert U[(2,)] == 1
    assert U[(3,)] == 1


def test_lineage_drill_through():
    H = HowProvenance()
    # R(a): r1, r2 ; S(a): s1
    R = annotate([(1,), (1,)], lambda i, t: H.singleton(f"r{i+1}"), H)
    S = annotate([(1,)], lambda i, t: H.singleton("s1"), H)
    J = join(R, S, key_a=(0,), key_b=(0,), K=H)
    # (1, 1) tuple has provenance r1*s1 + r2*s1
    poly = J[(1, 1)]
    assert lineage(poly) == {"r1", "r2", "s1"}
    assert witness_count(poly) == 2


def test_projection_aggregates_annotations():
    H = HowProvenance()
    rel = annotate([(1, 10), (1, 20), (2, 5)],
                   lambda i, t: H.singleton(f"t{i+1}"), H)
    P = project(rel, (0,), H)
    # tuple (1,) should aggregate t1 + t2
    assert lineage(P[(1,)]) == {"t1", "t2"}
    assert lineage(P[(2,)]) == {"t3"}


def test_select_preserves_annotations():
    H = HowProvenance()
    rel = annotate([(1,), (2,), (3,)],
                   lambda i, t: H.singleton(f"t{i+1}"), H)
    S = select(rel, lambda t: t[0] > 1, H)
    assert set(S.keys()) == {(2,), (3,)}
    assert lineage(S[(2,)]) == {"t2"}

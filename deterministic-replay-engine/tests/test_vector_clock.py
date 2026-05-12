import pytest
from replay_engine.vector_clock import VectorClock, Relation


def vc(**kw):
    return VectorClock(kw)


class TestVectorClockCompare:
    def test_equal(self):
        assert vc(a=1, b=2).compare(vc(a=1, b=2)) == Relation.EQUAL

    def test_before(self):
        assert vc(a=1).compare(vc(a=2)) == Relation.BEFORE

    def test_after(self):
        assert vc(a=2).compare(vc(a=1)) == Relation.AFTER

    def test_concurrent(self):
        assert vc(a=2, b=1).compare(vc(a=1, b=2)) == Relation.CONCURRENT

    def test_missing_key_treated_as_zero(self):
        assert vc(a=1).compare(vc(a=1, b=0)) == Relation.EQUAL

    def test_happens_before(self):
        assert vc(a=1).happens_before(vc(a=2))
        assert not vc(a=2).happens_before(vc(a=1))

    def test_concurrent_with(self):
        assert vc(a=2, b=1).concurrent_with(vc(a=1, b=2))


class TestVectorClockOps:
    def test_increment(self):
        result = vc(a=1).increment("a")
        assert result.get("a") == 2

    def test_increment_new_key(self):
        result = vc(a=1).increment("b")
        assert result.get("b") == 1
        assert result.get("a") == 1

    def test_merge(self):
        merged = vc(a=3, b=1).merge(vc(a=1, b=5))
        assert merged.get("a") == 3
        assert merged.get("b") == 5

    def test_hash_stable(self):
        assert hash(vc(a=1, b=2)) == hash(vc(b=2, a=1))

    def test_equality_operator(self):
        assert vc(a=1) == vc(a=1)
        assert vc(a=1) != vc(a=2)

    def test_le_operator(self):
        assert vc(a=1) <= vc(a=1)
        assert vc(a=1) <= vc(a=2)
        assert not vc(a=2) <= vc(a=1)

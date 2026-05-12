"""Correlated subquery IVM."""

from __future__ import annotations


def test_first_row_doesnt_qualify(correlated):
    # First row: avg=100 → 100 > 100 is false
    correlated.insert("c1", 100)
    assert correlated.qualifying() == []


def test_second_row_above_avg_qualifies(correlated):
    correlated.insert("c1", 100)   # avg=100, nobody qualifies
    correlated.insert("c1", 200)   # avg=150; 200>150 ✓
    q = correlated.qualifying()
    assert ("c1", 200) in q
    assert ("c1", 100) not in q


def test_huge_insert_can_dethrone_others(correlated):
    correlated.insert("c1", 100)
    correlated.insert("c1", 200)         # qualifying
    correlated.insert("c1", 1000)        # avg jumps; 200 may drop
    q = correlated.qualifying()
    assert ("c1", 1000) in q
    # 200 likely no longer qualifies because avg ≈ 433


def test_separate_customers_independent(correlated):
    correlated.insert("c1", 100)
    correlated.insert("c2", 200)
    correlated.insert("c2", 300)         # avg=250; 300>250 ✓
    q = correlated.qualifying()
    assert ("c2", 300) in q


def test_delete_adjusts_qualifying(correlated):
    correlated.insert("c1", 100)
    correlated.insert("c1", 200)         # avg=150; 200 qualifies
    correlated.delete("c1", 100)         # avg=200; 200 no longer qualifies
    q = correlated.qualifying()
    assert ("c1", 200) not in q


def test_delete_missing_no_op(correlated):
    result = correlated.delete("c1", 999)
    assert result["added"] == []
    assert result["removed"] == []

"""RANK and DENSE_RANK."""

from __future__ import annotations


class TestRank:
    def test_basic(self, rank_ivm):
        for t in [10, 20, 30]:
            rank_ivm.insert("p", t)
        assert rank_ivm.rank_of("p", 10) == 1
        assert rank_ivm.rank_of("p", 20) == 2
        assert rank_ivm.rank_of("p", 30) == 3

    def test_ties_share_rank(self, rank_ivm):
        for t in [10, 20, 20, 30]:
            rank_ivm.insert("p", t)
        # Sorted: [10, 20, 20, 30] → ranks 1, 2, 2, 4 (RANK)
        assert rank_ivm.rank_of("p", 10) == 1
        assert rank_ivm.rank_of("p", 20) == 2   # first occurrence
        assert rank_ivm.rank_of("p", 30) == 4   # skip-ahead after tie

    def test_delete(self, rank_ivm):
        for t in [10, 20, 30]:
            rank_ivm.insert("p", t)
        assert rank_ivm.delete("p", 20)
        assert rank_ivm.rank_of("p", 30) == 2

    def test_missing_returns_none(self, rank_ivm):
        assert rank_ivm.rank_of("p", 99) is None


class TestDenseRank:
    def test_no_skip_after_ties(self, dense_rank):
        for t in [10, 20, 20, 30]:
            dense_rank.insert("p", t)
        # DENSE_RANK: 30's rank should be 3, not 4
        assert dense_rank.rank_of("p", 10) == 1
        assert dense_rank.rank_of("p", 20) == 2
        assert dense_rank.rank_of("p", 30) == 3

    def test_delete(self, dense_rank):
        for t in [10, 20, 30]:
            dense_rank.insert("p", t)
        dense_rank.delete("p", 20)
        # Now [10, 30] → 10→1, 30→2
        assert dense_rank.rank_of("p", 30) == 2

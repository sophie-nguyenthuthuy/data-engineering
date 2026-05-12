"""Nested aggregates."""

from __future__ import annotations


class TestMaxOfSum:
    def test_first_insert_sets_max(self, max_of_sum):
        v, k = max_of_sum.insert("d1", 100)
        assert v == 100 and k == "d1"

    def test_max_tracks_largest_sum(self, max_of_sum):
        max_of_sum.insert("d1", 100)
        max_of_sum.insert("d2", 50)
        v, k = max_of_sum.insert("d1", 30)    # d1 now 130
        assert v == 130 and k == "d1"
        # Push d2 above d1
        v, k = max_of_sum.insert("d2", 200)   # d2 now 250
        assert v == 250 and k == "d2"

    def test_delete_from_max_recomputes(self, max_of_sum):
        max_of_sum.insert("d1", 100)
        max_of_sum.insert("d2", 80)
        v, k = max_of_sum.max
        assert v == 100
        # Subtract enough from d1 that d2 takes over
        max_of_sum.delete("d1", 50)        # d1: 50, d2: 80
        v, k = max_of_sum.max
        assert v == 80 and k == "d2"

    def test_delete_to_zero_drops_key(self, max_of_sum):
        max_of_sum.insert("d1", 100)
        max_of_sum.delete("d1", 100)
        assert max_of_sum.sum_of("d1") == 0


class TestSumOfMax:
    def test_total_after_inserts(self, sum_of_max):
        sum_of_max.insert("d1", 100)
        sum_of_max.insert("d1", 50)    # d1.max stays 100
        sum_of_max.insert("d2", 30)    # d2.max = 30
        # Outer sum = 100 + 30
        assert sum_of_max.total == 130

    def test_new_max_increments_total(self, sum_of_max):
        sum_of_max.insert("d1", 100)
        sum_of_max.insert("d1", 200)   # d1.max bumps to 200
        # Outer sum should go from 100 → 200
        assert sum_of_max.total == 200

    def test_delete_decrements(self, sum_of_max):
        sum_of_max.insert("d1", 100)
        sum_of_max.insert("d2", 200)
        # outer = 300
        sum_of_max.delete("d2", 200)
        # outer = 100
        assert sum_of_max.total == 100

    def test_delete_when_not_max_no_change(self, sum_of_max):
        sum_of_max.insert("d1", 100)
        sum_of_max.insert("d1", 50)
        # d1.max = 100, outer = 100
        sum_of_max.delete("d1", 50)
        # d1.max still 100, outer still 100
        assert sum_of_max.total == 100

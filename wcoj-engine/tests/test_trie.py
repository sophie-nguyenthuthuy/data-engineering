"""Unit tests for TrieIterator."""
import numpy as np
import pytest

from wcoj.trie import TrieIterator


def make_iter(tuples):
    data = np.array(tuples, dtype=np.int64)
    return TrieIterator(data)


class TestOpen:
    def test_full_range_after_first_open(self):
        it = make_iter([(1, 2), (1, 3), (2, 1)])
        it.open()
        assert not it.at_end()
        assert it.key() == 1

    def test_subtrie_after_second_open(self):
        it = make_iter([(1, 2), (1, 3), (2, 1)])
        it.open()          # depth 0: key=1
        it.open()          # depth 1: narrowed to rows where col0==1
        assert it.key() == 2

    def test_up_restores_depth(self):
        it = make_iter([(1, 2), (1, 3), (2, 1)])
        it.open()
        it.open()
        it.up()
        assert it.key() == 1


class TestNext:
    def test_advance_single_column(self):
        it = make_iter([(1,), (2,), (3,)])
        it.open()
        assert it.key() == 1
        it.next()
        assert it.key() == 2
        it.next()
        assert it.key() == 3
        it.next()
        assert it.at_end()

    def test_advance_skips_duplicates(self):
        # At depth 0, distinct values of col 0 are 1 and 2.
        it = make_iter([(1, 10), (1, 20), (2, 10)])
        it.open()
        assert it.key() == 1
        it.next()
        assert it.key() == 2
        it.next()
        assert it.at_end()


class TestSeek:
    def test_seek_exact(self):
        it = make_iter([(1,), (3,), (5,)])
        it.open()
        it.seek(3)
        assert it.key() == 3

    def test_seek_past_existing(self):
        it = make_iter([(1,), (3,), (5,)])
        it.open()
        it.seek(4)
        assert it.key() == 5

    def test_seek_beyond_end(self):
        it = make_iter([(1,), (3,)])
        it.open()
        it.seek(10)
        assert it.at_end()

    def test_seek_idempotent(self):
        it = make_iter([(2,), (4,), (6,)])
        it.open()
        it.seek(4)
        it.seek(4)
        assert it.key() == 4


class TestNestedOpenClose:
    def test_second_column_values(self):
        data = [(1, 10), (1, 20), (1, 30), (2, 10), (2, 40)]
        it = make_iter(data)
        it.open()   # depth 0
        assert it.key() == 1

        it.open()   # depth 1, restricted to x==1
        keys = []
        while not it.at_end():
            keys.append(it.key())
            it.next()
        assert keys == [10, 20, 30]

        it.up()
        it.next()
        assert it.key() == 2

        it.open()   # depth 1, restricted to x==2
        keys = []
        while not it.at_end():
            keys.append(it.key())
            it.next()
        assert keys == [10, 40]

        it.up()
        it.next()
        assert it.at_end()

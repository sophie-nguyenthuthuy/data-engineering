"""Tests for the Bloom filter."""
import pytest
from lsm_learned.indexes.bloom import BloomFilter


def test_no_false_negatives():
    bf = BloomFilter(1000, fpr=0.01)
    keys = list(range(0, 1000, 2))
    for k in keys:
        bf.add(k)
    for k in keys:
        assert k in bf


def test_false_positive_rate():
    n = 10_000
    bf = BloomFilter(n, fpr=0.01)
    for k in range(n):
        bf.add(k)
    # Test keys known to be absent
    fp = sum(1 for k in range(n, 2 * n) if k in bf)
    measured = fp / n
    # Allow 3× the target FPR as tolerance
    assert measured < 0.03, f"FPR {measured:.4f} exceeds 3% tolerance"


def test_empty_filter():
    bf = BloomFilter(100, fpr=0.05)
    assert 42 not in bf


def test_count():
    bf = BloomFilter(50)
    for k in range(10):
        bf.add(k)
    assert bf.count == 10


def test_memory_reasonable():
    bf = BloomFilter(1_000_000, fpr=0.01)
    # ~1.44 MB for 1M items at 1% FPR
    assert bf.memory_bytes() < 3 * 1024 * 1024

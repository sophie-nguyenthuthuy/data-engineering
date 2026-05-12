import pytest
from lsm.bloom import BloomFilter


def test_no_false_negatives():
    bf = BloomFilter(capacity=1000)
    keys = [f"key_{i}".encode() for i in range(500)]
    for k in keys:
        bf.add(k)
    for k in keys:
        assert bf.may_contain(k), f"False negative for {k}"


def test_false_positive_rate():
    bf = BloomFilter(capacity=1000, fpr=0.01)
    for i in range(1000):
        bf.add(f"present_{i}".encode())
    fps = sum(
        1 for i in range(10_000)
        if bf.may_contain(f"absent_{i}".encode())
    )
    # Allow 3× the target FPR due to randomness
    assert fps / 10_000 < 0.03, f"FPR too high: {fps/10_000:.3%}"


def test_serialization_roundtrip():
    bf = BloomFilter(capacity=100)
    keys = [f"serde_{i}".encode() for i in range(50)]
    for k in keys:
        bf.add(k)
    restored = BloomFilter.from_bytes(bf.to_bytes())
    for k in keys:
        assert restored.may_contain(k)


def test_empty_filter():
    bf = BloomFilter(capacity=10)
    assert not bf.may_contain(b"anything")

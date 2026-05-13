"""Cryptographic shuffler."""

from __future__ import annotations

import pytest

from sdp.shuffler.mix import MixNode, encrypt, shuffle


def test_shuffle_preserves_multiset():
    nodes = [MixNode.fresh() for _ in range(3)]
    records = [f"vote-{i:02d}".encode() for i in range(20)]
    out = shuffle(records, nodes)
    assert sorted(records) == sorted(out)


def test_shuffle_permutes_order():
    nodes = [MixNode.fresh() for _ in range(3)]
    records = [f"vote-{i:02d}".encode() for i in range(30)]
    out = shuffle(records, nodes)
    # P(identity order with 30! permutations) ≈ 0
    assert out != records


def test_encrypt_requires_at_least_one_node():
    with pytest.raises(ValueError):
        encrypt(b"x", [])


def test_single_mix_node():
    nodes = [MixNode.fresh()]
    records = [b"alpha", b"beta", b"gamma"]
    out = shuffle(records, nodes)
    assert sorted(out) == sorted(records)


def test_messages_too_long_are_truncated():
    from sdp.shuffler.mix import PAYLOAD_SIZE

    nodes = [MixNode.fresh()]
    long_record = b"x" * (PAYLOAD_SIZE + 100)
    out = shuffle([long_record], nodes)
    # Output is at most PAYLOAD_SIZE bytes
    assert len(out[0]) <= PAYLOAD_SIZE

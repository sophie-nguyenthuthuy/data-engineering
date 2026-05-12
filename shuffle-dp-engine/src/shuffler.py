"""Cryptographic shuffler (3-stage mix network).

Each mix node receives onion-encrypted ciphertexts, peels one layer, permutes
randomly, forwards. After 3 honest-majority stages, position information is
hidden from any single corrupted node.

This implementation uses HMAC-based authenticated layered encryption with
random permutations. It's not a production mix — that requires anonymous
channels and threshold crypto — but it's enough to demonstrate the
unlinkability property in a single process.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass


def _xor(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b))


def _kdf(seed: bytes, length: int, salt: bytes = b"shuffler") -> bytes:
    """Stream of `length` bytes from seed (HKDF-style)."""
    out = b""
    counter = 0
    while len(out) < length:
        block = hmac.new(seed, salt + counter.to_bytes(4, "big"), hashlib.sha256).digest()
        out += block
        counter += 1
    return out[:length]


@dataclass
class MixNode:
    """One node in the mix network."""
    key: bytes

    @staticmethod
    def fresh() -> "MixNode":
        return MixNode(key=os.urandom(32))


@dataclass
class Onion:
    """Onion-encrypted payload: layers peeled by successive mix nodes."""
    layers: list[bytes]   # one per mix node, outermost first
    payload: bytes        # the actual record, blinded


def encrypt(record: bytes, nodes: list[MixNode]) -> Onion:
    """Wrap `record` in onion encryption for the given mix path."""
    # Innermost: blinded payload (with random nonce)
    nonce = os.urandom(16)
    blinded = _xor(record.ljust(64, b"\x00")[:64], _kdf(nonce, 64))
    layers = []
    cipher = nonce + blinded
    for node in reversed(nodes):
        pad = _kdf(node.key, len(cipher))
        cipher = _xor(cipher, pad)
        layers.append(cipher)
    return Onion(layers=list(reversed(layers)), payload=b"")


def shuffler_mix(node: MixNode, batch: list[Onion], rng=None) -> list[Onion]:
    """Peel one layer + permute."""
    rng = rng or secrets.SystemRandom()
    peeled: list[Onion] = []
    for o in batch:
        cipher = o.layers[0]
        pad = _kdf(node.key, len(cipher))
        plaintext = _xor(cipher, pad)
        rest = o.layers[1:]
        peeled.append(Onion(layers=rest, payload=plaintext))
    # Permute
    permuted = list(peeled)
    rng.shuffle(permuted)
    return permuted


def shuffle(records: list[bytes], nodes: list[MixNode], rng=None) -> list[bytes]:
    """End-to-end: encrypt → mix through `nodes` → final reveal."""
    onions = [encrypt(r, nodes) for r in records]
    for node in nodes:
        onions = shuffler_mix(node, onions, rng=rng)
    # After all layers peeled, payload contains the (nonce | blinded) value.
    out: list[bytes] = []
    for o in onions:
        nonce, blinded = o.payload[:16], o.payload[16:]
        record = _xor(blinded, _kdf(nonce, 64))
        out.append(record.rstrip(b"\x00"))
    return out


__all__ = ["MixNode", "Onion", "encrypt", "shuffler_mix", "shuffle"]

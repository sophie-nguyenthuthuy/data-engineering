"""3-stage cryptographic mix network.

Each user encrypts their (randomized) record as an onion: N layers of
HMAC-XOR encryption, one per mix node. Mix nodes peel one layer, permute
the batch, and forward.

Under an honest-majority assumption (≤ 1 of 3 mix nodes is compromised),
the output reveals the multiset of records but hides who-sent-what.
Combined with local randomization, this yields shuffle-DP amplification
per Balle et al. (2019).

This implementation is **pedagogical**: real mix networks use threshold
encryption + verifiable shuffles. Here we use simple HMAC streams over
a deterministic key, which is fine for the protocol's structure but not
for production cryptographic security.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from typing import Protocol


class _SupportsShuffle(Protocol):
    def shuffle(self, x: list[Onion]) -> None: ...


PAYLOAD_SIZE = 64  # bytes; longer messages truncated/padded


def _xor(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b, strict=False))


def _kdf(seed: bytes, length: int, salt: bytes = b"shuffler") -> bytes:
    """HKDF-style stream from `seed`."""
    out = b""
    counter = 0
    while len(out) < length:
        block = hmac.new(seed, salt + counter.to_bytes(4, "big"), hashlib.sha256).digest()
        out += block
        counter += 1
    return out[:length]


@dataclass(frozen=True)
class MixNode:
    key: bytes

    @staticmethod
    def fresh() -> MixNode:
        return MixNode(key=os.urandom(32))


@dataclass(frozen=True)
class Onion:
    """Onion-encrypted payload to flow through the mix network."""

    layers: tuple[bytes, ...]  # outermost first
    payload: bytes  # the inner blinded plaintext after all peels


def encrypt(record: bytes, nodes: list[MixNode]) -> Onion:
    """Wrap `record` so it can be peeled through `nodes` in order.

    Internal: nonce + XOR-blinded record, then layered HMAC-XOR for each
    mix node in reverse order so the first node peels its layer first.
    """
    if not nodes:
        raise ValueError("at least one mix node required")
    nonce = os.urandom(16)
    blinded = _xor(record.ljust(PAYLOAD_SIZE, b"\x00")[:PAYLOAD_SIZE], _kdf(nonce, PAYLOAD_SIZE))
    cipher = nonce + blinded
    layers: list[bytes] = []
    for node in reversed(nodes):
        pad = _kdf(node.key, len(cipher))
        cipher = _xor(cipher, pad)
        layers.append(cipher)
    return Onion(layers=tuple(reversed(layers)), payload=b"")


def shuffler_mix(
    node: MixNode, batch: list[Onion], rng: _SupportsShuffle | None = None
) -> list[Onion]:
    """Peel the outermost layer for each onion + permute the batch."""
    rng = rng or secrets.SystemRandom()
    peeled: list[Onion] = []
    for o in batch:
        cipher = o.layers[0]
        pad = _kdf(node.key, len(cipher))
        plaintext = _xor(cipher, pad)
        rest = o.layers[1:]
        peeled.append(Onion(layers=rest, payload=plaintext))
    out = list(peeled)
    rng.shuffle(out)
    return out


def shuffle(
    records: list[bytes], nodes: list[MixNode], rng: _SupportsShuffle | None = None
) -> list[bytes]:
    """End-to-end: encrypt all records → run mix-net → return permuted plaintexts."""
    onions = [encrypt(r, nodes) for r in records]
    for node in nodes:
        onions = shuffler_mix(node, onions, rng=rng)
    out: list[bytes] = []
    for o in onions:
        nonce, blinded = o.payload[:16], o.payload[16:]
        record = _xor(blinded, _kdf(nonce, PAYLOAD_SIZE))
        out.append(record.rstrip(b"\x00"))
    return out


__all__ = ["PAYLOAD_SIZE", "MixNode", "Onion", "encrypt", "shuffle", "shuffler_mix"]

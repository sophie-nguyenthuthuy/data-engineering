"""Mixed-workload generator: yields (op, key, value) triples."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def _key(i: int) -> bytes:
    return f"k{i:08d}".encode()


def mixed_workload(
    n_ops: int,
    n_keys: int,
    write_fraction: float = 0.5,
    seed: int = 0,
) -> Iterator[tuple[str, bytes, int | None]]:
    rng = random.Random(seed)
    for _ in range(n_ops):
        if rng.random() < write_fraction:
            k = _key(rng.randint(0, n_keys - 1))
            v = rng.randint(0, 1_000_000)
            yield ("put", k, v)
        else:
            k = _key(rng.randint(0, n_keys - 1))
            yield ("get", k, None)


def write_heavy(n_ops: int, n_keys: int, seed: int = 0):
    yield from mixed_workload(n_ops, n_keys, write_fraction=0.9, seed=seed)


def read_heavy(n_ops: int, n_keys: int, seed: int = 0):
    yield from mixed_workload(n_ops, n_keys, write_fraction=0.1, seed=seed)

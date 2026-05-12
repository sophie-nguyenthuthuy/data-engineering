import numpy as np
import pytest


@pytest.fixture
def small_uniform_keys():
    rng = np.random.default_rng(0)
    return np.sort(rng.choice(1_000_000, size=10_000, replace=False)).astype(np.float64)


@pytest.fixture
def small_zipfian_keys():
    rng = np.random.default_rng(1)
    ranks = rng.zipf(1.3, size=15_000)
    keys = np.clip(ranks, 1, 500_000).astype(np.float64)
    return np.sort(np.unique(keys))


@pytest.fixture
def tiny_keys():
    return np.arange(1, 101, dtype=np.float64)

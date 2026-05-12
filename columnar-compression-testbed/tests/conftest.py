import numpy as np
import pytest


@pytest.fixture
def rng():
    return np.random.default_rng(42)


@pytest.fixture
def string_column():
    words = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"]
    rng = np.random.default_rng(42)
    return np.array([words[i] + "_" + str(rng.integers(0, 100)) for i in rng.integers(0, len(words), 512)], dtype=object)


@pytest.fixture
def float_column():
    rng = np.random.default_rng(42)
    # Decimal-friendly floats (common in finance/sensor data)
    return np.round(rng.uniform(10.0, 999.0, 1024) * 100) / 100


@pytest.fixture
def timestamp_column():
    base = 1_700_000_000_000  # ms since epoch
    rng = np.random.default_rng(42)
    # Near-constant ~1s intervals with small jitter so DODs are tiny (fit in 12-bit range)
    deltas = rng.integers(990, 1_010, 1024)  # ±10ms jitter around 1000ms
    return np.cumsum(np.concatenate([[base], deltas])).astype(np.int64)[:1024]

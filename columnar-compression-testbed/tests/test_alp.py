import numpy as np
import pytest

from cctest.codecs.alp import ALPCodec, _find_best_exponent


def test_roundtrip_basic(float_column):
    codec = ALPCodec()
    encoded = codec.encode(float_column)
    decoded = codec.decode(encoded)
    np.testing.assert_allclose(decoded, float_column, rtol=0, atol=1e-9)


def test_roundtrip_empty():
    data = np.array([], dtype=np.float64)
    codec = ALPCodec()
    decoded = codec.decode(codec.encode(data))
    assert len(decoded) == 0


def test_roundtrip_single():
    data = np.array([3.14159], dtype=np.float64)
    codec = ALPCodec()
    decoded = codec.decode(codec.encode(data))
    np.testing.assert_allclose(decoded, data, atol=1e-9)


def test_best_exponent_decimal():
    # Prices with 2 decimal places → best exponent should be 2
    rng = np.random.default_rng(0)
    prices = np.round(rng.uniform(1.0, 999.0, 2048) * 100) / 100
    e = _find_best_exponent(prices)
    assert e == 2


def test_compression_ratio(float_column):
    codec = ALPCodec()
    encoded = codec.encode(float_column)
    original_bytes = float_column.nbytes
    assert encoded.total_bytes() < original_bytes


def test_exceptions_tracked():
    # Mix of encodable and non-encodable floats
    data = np.array([1.23, float("nan"), 4.56, float("inf"), 7.89], dtype=np.float64)
    codec = ALPCodec()
    encoded = codec.encode(data)
    # nan and inf are exceptions
    assert encoded.metadata["exceptions"] >= 2


def test_preserves_exceptions(float_column):
    rng = np.random.default_rng(7)
    col = float_column.copy()
    col[rng.integers(0, len(col), 10)] = float("nan")
    codec = ALPCodec()
    decoded = codec.decode(codec.encode(col))
    nan_orig = np.isnan(col)
    nan_dec = np.isnan(decoded)
    np.testing.assert_array_equal(nan_orig, nan_dec)


def test_supports_dtype():
    codec = ALPCodec()
    assert codec.supports_dtype(np.dtype("float32"))
    assert codec.supports_dtype(np.dtype("float64"))
    assert not codec.supports_dtype(np.dtype("int64"))
    assert not codec.supports_dtype(np.dtype("O"))

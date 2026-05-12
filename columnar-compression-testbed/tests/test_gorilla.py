import numpy as np
import pytest

from cctest.codecs.gorilla import GorillaDeltaCodec, GorillaFloatCodec


# ---------------------------------------------------------------------------
# GorillaFloat
# ---------------------------------------------------------------------------

def test_float_roundtrip(float_column):
    codec = GorillaFloatCodec()
    decoded = codec.decode(codec.encode(float_column))
    np.testing.assert_array_equal(decoded, float_column)


def test_float_roundtrip_constant():
    data = np.full(256, 42.0, dtype=np.float64)
    codec = GorillaFloatCodec()
    decoded = codec.decode(codec.encode(data))
    np.testing.assert_array_equal(decoded, data)


def test_float_roundtrip_random():
    rng = np.random.default_rng(99)
    data = rng.standard_normal(512)
    codec = GorillaFloatCodec()
    decoded = codec.decode(codec.encode(data))
    np.testing.assert_array_equal(decoded, data)


def test_float_roundtrip_empty():
    data = np.array([], dtype=np.float64)
    codec = GorillaFloatCodec()
    decoded = codec.decode(codec.encode(data))
    assert len(decoded) == 0


def test_float_roundtrip_single():
    data = np.array([1.5], dtype=np.float64)
    codec = GorillaFloatCodec()
    decoded = codec.decode(codec.encode(data))
    np.testing.assert_array_equal(decoded, data)


def test_float_compression_constant():
    data = np.full(1024, 3.14, dtype=np.float64)
    codec = GorillaFloatCodec()
    encoded = codec.encode(data)
    # Constant column: every value after first is '0' bit → very compact
    assert encoded.total_bytes() < data.nbytes * 0.15


def test_float_supports_dtype():
    codec = GorillaFloatCodec()
    assert codec.supports_dtype(np.dtype("float64"))
    assert codec.supports_dtype(np.dtype("float32"))
    assert not codec.supports_dtype(np.dtype("int64"))


# ---------------------------------------------------------------------------
# GorillaDelta
# ---------------------------------------------------------------------------

def test_delta_roundtrip(timestamp_column):
    codec = GorillaDeltaCodec()
    decoded = codec.decode(codec.encode(timestamp_column))
    np.testing.assert_array_equal(decoded, timestamp_column)


def test_delta_roundtrip_monotone():
    data = np.arange(0, 10_000, 100, dtype=np.int64)
    codec = GorillaDeltaCodec()
    decoded = codec.decode(codec.encode(data))
    np.testing.assert_array_equal(decoded, data)


def test_delta_roundtrip_empty():
    data = np.array([], dtype=np.int64)
    codec = GorillaDeltaCodec()
    decoded = codec.decode(codec.encode(data))
    assert len(decoded) == 0


def test_delta_roundtrip_single():
    data = np.array([12345], dtype=np.int64)
    codec = GorillaDeltaCodec()
    decoded = codec.decode(codec.encode(data))
    np.testing.assert_array_equal(decoded, data)


def test_delta_roundtrip_two():
    data = np.array([1000, 2000], dtype=np.int64)
    codec = GorillaDeltaCodec()
    decoded = codec.decode(codec.encode(data))
    np.testing.assert_array_equal(decoded, data)


def test_delta_compression_regular(timestamp_column):
    codec = GorillaDeltaCodec()
    encoded = codec.encode(timestamp_column)
    # Regular timestamps (nearly constant delta) compress much better than raw
    assert encoded.total_bytes() < timestamp_column.nbytes * 0.5


def test_delta_supports_dtype():
    codec = GorillaDeltaCodec()
    assert codec.supports_dtype(np.dtype("int64"))
    assert codec.supports_dtype(np.dtype("int32"))
    assert not codec.supports_dtype(np.dtype("float64"))

import numpy as np
import pytest

from cctest.codecs.fsst import FSSTCodec


def test_roundtrip_basic(string_column):
    codec = FSSTCodec()
    encoded = codec.encode(string_column)
    decoded = codec.decode(encoded)
    assert list(decoded) == list(string_column)


def test_roundtrip_single():
    data = np.array(["hello world"], dtype=object)
    codec = FSSTCodec()
    assert list(codec.decode(codec.encode(data))) == list(data)


def test_roundtrip_empty():
    data = np.array([], dtype=object)
    codec = FSSTCodec()
    decoded = codec.decode(codec.encode(data))
    assert len(decoded) == 0


def test_compression_ratio(string_column):
    codec = FSSTCodec()
    encoded = codec.encode(string_column)
    original = sum(len(s.encode()) for s in string_column)
    assert encoded.total_bytes() < original, "FSST should compress repetitive strings"


def test_high_repetition():
    # Highly repetitive data should compress well
    data = np.array(["user_profile_image_thumbnail"] * 1024, dtype=object)
    codec = FSSTCodec()
    encoded = codec.encode(data)
    decoded = codec.decode(encoded)
    assert list(decoded) == list(data)
    original = sum(len(s.encode()) for s in data)
    assert encoded.total_bytes() < original * 0.5


def test_supports_dtype():
    codec = FSSTCodec()
    assert codec.supports_dtype(np.dtype("O"))
    assert codec.supports_dtype(np.dtype("U10"))
    assert not codec.supports_dtype(np.dtype("float64"))
    assert not codec.supports_dtype(np.dtype("int64"))


def test_benchmark(string_column):
    codec = FSSTCodec()
    result = codec.benchmark(string_column)
    assert result.lossless
    assert result.ratio > 0

import numpy as np
import pytest

from cctest.codecs import ALPCodec, FSSTCodec, GorillaDeltaCodec, GorillaFloatCodec
from cctest.schema import diff_schemas, ChangeKind
from cctest.selector import EncodingSelector, SelectorConfig


@pytest.fixture
def selector():
    cfg = SelectorConfig(sample_size=256, benchmark_rounds=2)
    return EncodingSelector(config=cfg)


def test_selects_alp_for_floats(selector, float_column):
    codec = selector.select("price", float_column)
    assert isinstance(codec, ALPCodec)


def test_selects_fsst_for_strings(selector, string_column):
    codec = selector.select("label", string_column)
    assert isinstance(codec, FSSTCodec)


def test_selects_gorilla_delta_for_timestamps(selector, timestamp_column):
    codec = selector.select("ts", timestamp_column)
    assert isinstance(codec, GorillaDeltaCodec)


def test_cache_hit(selector, float_column):
    codec1 = selector.select("x", float_column)
    codec2 = selector.select("x", float_column)
    assert codec1 is codec2


def test_force_reevaluate(selector, float_column):
    codec1 = selector.select("x", float_column)
    codec2 = selector.force_reevaluate("x", float_column)
    assert type(codec1) == type(codec2)


def test_schema_change_evicts_cache(selector, float_column):
    selector.select("price", float_column)
    old_schema = {"price": "float64"}
    new_schema = {"price": "float32"}
    affected = selector.schema_changed(old_schema, new_schema)
    assert "price" in affected
    # Cache should be gone; next call re-evaluates
    assert ("price", "float64") not in selector._cache


def test_diff_schemas_added():
    diff = diff_schemas({}, {"new_col": "int64"})
    assert diff.has_changes
    assert any(c.kind == ChangeKind.ADDED for c in diff.changes)


def test_diff_schemas_removed():
    diff = diff_schemas({"old": "float64"}, {})
    assert diff.has_changes
    assert any(c.kind == ChangeKind.REMOVED for c in diff.changes)


def test_diff_schemas_type_changed():
    diff = diff_schemas({"col": "int32"}, {"col": "int64"})
    assert diff.has_changes
    assert diff.changes[0].kind == ChangeKind.TYPE_CHANGED


def test_diff_schemas_unchanged():
    diff = diff_schemas({"col": "float64"}, {"col": "float64"})
    assert not diff.has_changes


def test_no_applicable_codec_raises(selector):
    # Use a dtype with no registered codec
    data = np.array([True, False, True], dtype=bool)
    with pytest.raises((ValueError, RuntimeError)):
        selector.select("flags", data)

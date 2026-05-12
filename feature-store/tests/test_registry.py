"""Feature registry tests."""
from __future__ import annotations

import pytest

from feature_store.registry.feature_registry import (
    DType,
    FeatureDef,
    FeatureGroup,
    FeatureRegistry,
)


@pytest.fixture()
def registry():
    r = FeatureRegistry()
    r.register_group(
        FeatureGroup(
            name="users",
            features=[
                FeatureDef("score", DType.FLOAT32, default_value=0.0),
                FeatureDef("count", DType.INT64),
                FeatureDef("label", DType.STRING),
            ],
            ttl_seconds=3600,
        )
    )
    return r


class TestRegistration:
    def test_register_and_retrieve(self, registry):
        g = registry.get_group("users")
        assert g.name == "users"
        assert len(g.features) == 3

    def test_unknown_group_raises(self, registry):
        with pytest.raises(KeyError):
            registry.get_group("nonexistent")

    def test_list_groups(self, registry):
        assert "users" in registry.list_groups()


class TestValidation:
    def test_valid_features_pass_through(self, registry):
        result = registry.validate_features("users", {"score": 0.9, "count": 5})
        assert abs(result["score"] - 0.9) < 1e-5
        assert result["count"] == 5

    def test_unknown_features_stripped(self, registry):
        result = registry.validate_features("users", {"score": 0.5, "unknown_col": "x"})
        assert "unknown_col" not in result

    def test_default_applied_when_missing(self, registry):
        result = registry.validate_features("users", {"count": 3})
        assert result["score"] == 0.0

    def test_coercion(self, registry):
        result = registry.validate_features("users", {"score": "0.75", "count": "10"})
        assert isinstance(result["score"], float)
        assert isinstance(result["count"], int)

    def test_coercion_bad_value_returns_none(self, registry):
        result = registry.validate_features("users", {"count": "not_a_number"})
        assert result.get("count") is None


class TestSerialization:
    def test_to_json(self, registry):
        import json
        data = json.loads(registry.to_json())
        assert "users" in data
        assert data["users"]["ttl_seconds"] == 3600

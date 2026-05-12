"""
Feature registry: single source of truth for feature definitions.
Both batch and streaming processors reference the same registry.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class FeatureType(str, Enum):
    CONTINUOUS = "continuous"
    CATEGORICAL = "categorical"


@dataclass
class FeatureDefinition:
    name: str
    feature_type: FeatureType
    description: str
    # Pure function: (raw_record: dict, context: dict) -> feature_value
    compute_fn: Callable[[dict, dict], Any]
    # Optional default when inputs are missing
    default_value: Any = None
    tags: list[str] = field(default_factory=list)

    def compute(self, record: dict, context: dict | None = None) -> Any:
        try:
            return self.compute_fn(record, context or {})
        except (KeyError, TypeError, ZeroDivisionError, ValueError):
            return self.default_value


class FeatureRegistry:
    def __init__(self) -> None:
        self._features: dict[str, FeatureDefinition] = {}

    def register(self, feature: FeatureDefinition) -> FeatureDefinition:
        self._features[feature.name] = feature
        return feature

    def get(self, name: str) -> FeatureDefinition:
        return self._features[name]

    def all_features(self) -> list[FeatureDefinition]:
        return list(self._features.values())

    def feature_names(self) -> list[str]:
        return list(self._features.keys())

    def to_json(self) -> str:
        return json.dumps(
            [
                {
                    "name": f.name,
                    "feature_type": f.feature_type.value,
                    "description": f.description,
                    "tags": f.tags,
                }
                for f in self._features.values()
            ],
            indent=2,
        )

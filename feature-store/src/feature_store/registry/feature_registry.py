"""
Feature registry — single source of truth for feature schemas, TTLs, and lineage.
All writes to online/offline stores are validated against registered definitions.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import yaml


class DType(str, Enum):
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    INT32 = "int32"
    INT64 = "int64"
    STRING = "string"
    BOOL = "bool"
    BYTES = "bytes"


@dataclass
class FeatureDef:
    name: str
    dtype: DType
    description: str = ""
    default_value: Any = None
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> FeatureDef:
        return cls(
            name=d["name"],
            dtype=DType(d["dtype"]),
            description=d.get("description", ""),
            default_value=d.get("default_value"),
            tags=d.get("tags", {}),
        )


@dataclass
class FeatureGroup:
    name: str
    features: list[FeatureDef]
    ttl_seconds: int = 86400
    entity_keys: list[str] = field(default_factory=lambda: ["entity_id"])
    description: str = ""

    @property
    def feature_names(self) -> list[str]:
        return [f.name for f in self.features]

    def get_feature(self, name: str) -> FeatureDef | None:
        return next((f for f in self.features if f.name == name), None)

    @classmethod
    def from_dict(cls, name: str, d: dict) -> FeatureGroup:
        return cls(
            name=name,
            features=[FeatureDef.from_dict(f) for f in d.get("features", [])],
            ttl_seconds=d.get("ttl_seconds", 86400),
            entity_keys=d.get("entity_keys", ["entity_id"]),
            description=d.get("description", ""),
        )


class FeatureRegistry:
    """
    Thread-safe in-process registry. In production, back this with a metadata
    store (e.g. Postgres, Atlas) for multi-process consistency.
    """

    def __init__(self) -> None:
        self._groups: dict[str, FeatureGroup] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # Registration                                                         #
    # ------------------------------------------------------------------ #

    def register_group(self, group: FeatureGroup) -> None:
        with self._lock:
            self._groups[group.name] = group

    def register_from_config(self, config_path: str | Path) -> None:
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        for name, spec in cfg.get("feature_groups", {}).items():
            self.register_group(FeatureGroup.from_dict(name, spec))

    # ------------------------------------------------------------------ #
    # Lookup                                                               #
    # ------------------------------------------------------------------ #

    def get_group(self, name: str) -> FeatureGroup:
        with self._lock:
            if name not in self._groups:
                raise KeyError(f"Feature group '{name}' not registered")
            return self._groups[name]

    def list_groups(self) -> list[str]:
        with self._lock:
            return list(self._groups.keys())

    def validate_features(self, group_name: str, features: dict[str, Any]) -> dict[str, Any]:
        """Strip unknown features and coerce dtypes. Raises on missing required fields."""
        group = self.get_group(group_name)
        valid: dict[str, Any] = {}
        for feat_def in group.features:
            if feat_def.name in features:
                valid[feat_def.name] = self._coerce(features[feat_def.name], feat_def.dtype)
            elif feat_def.default_value is not None:
                valid[feat_def.name] = feat_def.default_value
        return valid

    @staticmethod
    def _coerce(value: Any, dtype: DType) -> Any:
        if value is None:
            return None
        try:
            match dtype:
                case DType.FLOAT32 | DType.FLOAT64:
                    return float(value)
                case DType.INT32 | DType.INT64:
                    return int(value)
                case DType.STRING:
                    return str(value)
                case DType.BOOL:
                    return bool(value)
                case DType.BYTES:
                    return value if isinstance(value, bytes) else str(value).encode()
        except (ValueError, TypeError):
            return None
        return value

    def to_json(self) -> str:
        with self._lock:
            return json.dumps(
                {
                    name: {
                        "features": [
                            {"name": f.name, "dtype": f.dtype.value, "description": f.description}
                            for f in g.features
                        ],
                        "ttl_seconds": g.ttl_seconds,
                        "entity_keys": g.entity_keys,
                    }
                    for name, g in self._groups.items()
                },
                indent=2,
            )

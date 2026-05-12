from __future__ import annotations
import yaml
import structlog
from pathlib import Path
from typing import Iterator

from ..config import settings
from ..models import FeatureDefinition, FeatureType

log = structlog.get_logger(__name__)


class FeatureRegistry:
    """
    In-process feature registry backed by a YAML definition file.

    Provides versioned feature schemas, type lookups, and validation
    helpers used by the transform pipeline and drift detector.
    """

    def __init__(self, definitions_path: str | None = None) -> None:
        self._path = Path(definitions_path or settings.feature_definitions_path)
        self._registry: dict[str, FeatureDefinition] = {}
        self._load()

    # ------------------------------------------------------------------
    # Load / refresh
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not self._path.exists():
            log.warning("feature_definitions_not_found", path=str(self._path))
            return
        with self._path.open() as f:
            raw = yaml.safe_load(f) or {}
        count = 0
        for entry in raw.get("features", []):
            fd = FeatureDefinition(**entry)
            self._registry[fd.name] = fd
            count += 1
        log.info("feature_registry_loaded", count=count, path=str(self._path))

    def reload(self) -> None:
        self._registry.clear()
        self._load()

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get(self, name: str) -> FeatureDefinition | None:
        return self._registry.get(name)

    def get_or_raise(self, name: str) -> FeatureDefinition:
        fd = self._registry.get(name)
        if fd is None:
            raise KeyError(f"Feature {name!r} not found in registry")
        return fd

    def all_features(self) -> list[FeatureDefinition]:
        return list(self._registry.values())

    def by_type(self, feature_type: FeatureType) -> list[FeatureDefinition]:
        return [f for f in self._registry.values() if f.feature_type == feature_type]

    def numerical_features(self) -> list[str]:
        return [f.name for f in self.by_type(FeatureType.NUMERICAL)]

    def categorical_features(self) -> list[str]:
        return [f.name for f in self.by_type(FeatureType.CATEGORICAL)]

    def __iter__(self) -> Iterator[FeatureDefinition]:
        return iter(self._registry.values())

    def __len__(self) -> int:
        return len(self._registry)

    # ------------------------------------------------------------------
    # Registration (runtime, not persisted — use YAML for permanence)
    # ------------------------------------------------------------------

    def register(self, definition: FeatureDefinition) -> None:
        self._registry[definition.name] = definition
        log.info("feature_registered", name=definition.name, type=definition.feature_type)

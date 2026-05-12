"""Contract loading, parsing, and version management."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class FieldSchema:
    name: str
    type: str  # string | integer | number | boolean | array | object | date | timestamp
    nullable: bool = False
    description: str = ""
    constraints: dict[str, Any] = field(default_factory=dict)


@dataclass
class SLARule:
    """Service-level agreement rule attached to a contract."""
    name: str
    rule_type: str  # freshness | completeness | row_count | latency
    threshold: float
    unit: str = ""
    description: str = ""


@dataclass
class SemanticRule:
    """Custom predicate evaluated against the dataset."""
    name: str
    expression: str         # Python expression; 'df' is the DataFrame
    severity: str = "error" # error | warning
    description: str = ""


@dataclass
class DataContract:
    id: str
    version: str            # semver
    producer: str
    consumers: list[str]
    description: str
    fields: list[FieldSchema]
    sla_rules: list[SLARule]
    semantic_rules: list[SemanticRule]
    tags: list[str] = field(default_factory=list)
    owner: str = ""
    source_path: Path | None = None

    # ------------------------------------------------------------------ #
    @property
    def semver(self) -> tuple[int, int, int]:
        parts = re.match(r"(\d+)\.(\d+)\.(\d+)", self.version)
        if not parts:
            raise ValueError(f"Invalid semver: {self.version}")
        return int(parts[1]), int(parts[2]), int(parts[3])

    def is_breaking_change_from(self, other: "DataContract") -> list[str]:
        """Return list of breaking-change messages compared to *other* (older)."""
        changes: list[str] = []
        old_fields = {f.name: f for f in other.fields}
        new_fields = {f.name: f for f in self.fields}

        for name, old_f in old_fields.items():
            if name not in new_fields:
                changes.append(f"REMOVED field '{name}' (was {old_f.type})")
                continue
            new_f = new_fields[name]
            if old_f.type != new_f.type:
                changes.append(
                    f"TYPE CHANGE on '{name}': {old_f.type} → {new_f.type}"
                )
            if old_f.nullable and not new_f.nullable:
                changes.append(f"NULLABILITY TIGHTENED on '{name}'")

        return changes


# ------------------------------------------------------------------ #
# Loading helpers
# ------------------------------------------------------------------ #

def _parse_field(raw: dict) -> FieldSchema:
    return FieldSchema(
        name=raw["name"],
        type=raw["type"],
        nullable=raw.get("nullable", False),
        description=raw.get("description", ""),
        constraints=raw.get("constraints", {}),
    )


def _parse_sla(raw: dict) -> SLARule:
    return SLARule(
        name=raw["name"],
        rule_type=raw["rule_type"],
        threshold=float(raw["threshold"]),
        unit=raw.get("unit", ""),
        description=raw.get("description", ""),
    )


def _parse_semantic(raw: dict) -> SemanticRule:
    return SemanticRule(
        name=raw["name"],
        expression=raw["expression"],
        severity=raw.get("severity", "error"),
        description=raw.get("description", ""),
    )


def load_contract(path: Path | str) -> DataContract:
    """Load a contract from a YAML file."""
    path = Path(path)
    with path.open() as fh:
        raw = yaml.safe_load(fh)

    return DataContract(
        id=raw["id"],
        version=raw["version"],
        producer=raw["producer"],
        consumers=raw.get("consumers", []),
        description=raw.get("description", ""),
        fields=[_parse_field(f) for f in raw.get("fields", [])],
        sla_rules=[_parse_sla(r) for r in raw.get("sla_rules", [])],
        semantic_rules=[_parse_semantic(r) for r in raw.get("semantic_rules", [])],
        tags=raw.get("tags", []),
        owner=raw.get("owner", ""),
        source_path=path,
    )


def load_contracts_dir(contracts_dir: Path | str) -> dict[str, list[DataContract]]:
    """
    Walk *contracts_dir* and return a mapping of
    contract_id → sorted list of DataContract (oldest first).
    """
    contracts_dir = Path(contracts_dir)
    result: dict[str, list[DataContract]] = {}
    for yaml_file in sorted(contracts_dir.rglob("*.yaml")):
        try:
            c = load_contract(yaml_file)
        except Exception as exc:
            raise ValueError(f"Failed to load {yaml_file}: {exc}") from exc
        result.setdefault(c.id, []).append(c)

    for versions in result.values():
        versions.sort(key=lambda c: c.semver)

    return result

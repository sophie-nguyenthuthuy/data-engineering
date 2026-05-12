"""
Declarative Transformation DSL
================================
YAML/JSON format example:

    version: 1
    description: "Migrate user events v1 → v2"
    steps:
      - op: rename_field
        path: "$.name"
        params:
          to: "full_name"

      - op: split_field
        path: "$.full_name"
        params:
          into: ["first_name", "last_name"]
          separator: " "

      - op: add_field
        path: "$.metadata"
        params:
          default: {}

      - op: remove_field
        path: "$.legacy_id"

      - op: cast_field
        path: "$.age"
        params:
          to_type: integer

      - op: map_value
        path: "$.status"
        params:
          mapping:
            "0": "inactive"
            "1": "active"

      - op: merge_fields
        path: "$.address"
        params:
          sources: ["street", "city", "zip"]
          template: "{street}, {city} {zip}"

      - op: compute_field
        path: "$.full_address"
        params:
          expression: "street + ', ' + city"
"""
from __future__ import annotations

import json
from typing import Any

import yaml

from src.registry.models import MigrationScript, MigrationStep

SUPPORTED_OPS = {
    "rename_field",
    "split_field",
    "add_field",
    "remove_field",
    "cast_field",
    "map_value",
    "merge_fields",
    "compute_field",
    "copy_field",
    "default_field",
    "flatten_field",
    "wrap_field",
}


class DSLParseError(Exception):
    pass


class TransformationDSL:
    """Parse and validate a DSL document (YAML or JSON string)."""

    def parse(self, source: str) -> list[MigrationStep]:
        """Parse DSL source → list of MigrationStep."""
        try:
            doc = yaml.safe_load(source)
        except yaml.YAMLError as e:
            raise DSLParseError(f"Invalid YAML: {e}") from e

        if not isinstance(doc, dict):
            raise DSLParseError("DSL root must be a mapping.")

        raw_steps = doc.get("steps", [])
        if not isinstance(raw_steps, list):
            raise DSLParseError("'steps' must be a list.")

        steps: list[MigrationStep] = []
        for i, raw in enumerate(raw_steps):
            if not isinstance(raw, dict):
                raise DSLParseError(f"Step {i} must be a mapping.")
            op = raw.get("op")
            if not op:
                raise DSLParseError(f"Step {i} missing 'op'.")
            if op not in SUPPORTED_OPS:
                raise DSLParseError(f"Step {i}: unknown op '{op}'. Supported: {sorted(SUPPORTED_OPS)}")
            path = raw.get("path", "$")
            params = raw.get("params", {})
            steps.append(MigrationStep(op=op, path=path, params=params or {}))

        return steps

    def to_yaml(self, steps: list[MigrationStep], description: str = "") -> str:
        doc: dict[str, Any] = {"version": 1, "description": description, "steps": []}
        for s in steps:
            entry: dict[str, Any] = {"op": s.op, "path": s.path}
            if s.params:
                entry["params"] = s.params
            doc["steps"].append(entry)
        return yaml.dump(doc, sort_keys=False, allow_unicode=True)

    def to_json(self, steps: list[MigrationStep], description: str = "") -> str:
        doc: dict[str, Any] = {"version": 1, "description": description, "steps": []}
        for s in steps:
            entry: dict[str, Any] = {"op": s.op, "path": s.path}
            if s.params:
                entry["params"] = s.params
            doc["steps"].append(entry)
        return json.dumps(doc, indent=2)

"""
Auto-Migration Generator
========================
Diffs two JSON schemas and produces a best-effort MigrationScript.
"""
from __future__ import annotations

from typing import Any

from src.registry.models import MigrationScript, MigrationStep
from .dsl import TransformationDSL


class MigrationGenerator:
    """
    Compares old_schema and new_schema (JSON Schema dicts) and emits
    a MigrationScript that transforms old-shaped data into new-shaped data.
    """

    def generate(
        self,
        subject: str,
        from_version: int,
        to_version: int,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any],
    ) -> MigrationScript:
        steps: list[MigrationStep] = []
        breaking: list[str] = []

        old_props = old_schema.get("properties", {})
        new_props = new_schema.get("properties", {})
        old_req = set(old_schema.get("required", []))
        new_req = set(new_schema.get("required", []))

        old_keys = set(old_props)
        new_keys = set(new_props)

        # Removed fields
        for field in old_keys - new_keys:
            steps.append(MigrationStep(op="remove_field", path=f"$.{field}"))
            if field in old_req:
                breaking.append(f"Required field '{field}' removed")

        # Added fields
        for field in new_keys - old_keys:
            default = new_props[field].get("default")
            steps.append(
                MigrationStep(
                    op="add_field",
                    path=f"$.{field}",
                    params={"default": default},
                )
            )
            if field in new_req and default is None:
                breaking.append(f"New required field '{field}' has no default")

        # Changed fields
        for field in old_keys & new_keys:
            old_f = old_props[field]
            new_f = new_props[field]
            old_type = old_f.get("type")
            new_type = new_f.get("type")

            if old_type != new_type and old_type is not None and new_type is not None:
                steps.append(
                    MigrationStep(
                        op="cast_field",
                        path=f"$.{field}",
                        params={"from_type": old_type, "to_type": new_type},
                    )
                )
                # Check if safe widening
                if not _is_safe_cast(old_type, new_type):
                    breaking.append(f"Field '{field}' type changed {old_type!r} → {new_type!r}")

            # Enum changes
            old_enum = set(old_f.get("enum", []))
            new_enum = set(new_f.get("enum", []))
            if old_enum != new_enum and new_enum:
                removed_vals = old_enum - new_enum
                if removed_vals:
                    breaking.append(f"Enum values removed from '{field}': {removed_vals}")
                    steps.append(
                        MigrationStep(
                            op="map_value",
                            path=f"$.{field}",
                            params={
                                "mapping": {v: None for v in removed_vals},
                                "note": "Map removed enum values (set to null or a default)",
                            },
                        )
                    )

        # Required changes (field still exists but requiredness changed)
        for field in new_req - old_req:
            if field in old_keys:
                steps.append(
                    MigrationStep(
                        op="default_field",
                        path=f"$.{field}",
                        params={
                            "default": new_props[field].get("default"),
                            "reason": "Field became required",
                        },
                    )
                )

        dsl = TransformationDSL()
        dsl_source = dsl.to_yaml(
            steps,
            description=f"Auto-generated migration: {subject} v{from_version} → v{to_version}",
        )

        return MigrationScript(
            subject=subject,
            from_version=from_version,
            to_version=to_version,
            steps=steps,
            dsl_source=dsl_source,
            auto_generated=True,
            breaking_changes=breaking,
        )


def _is_safe_cast(from_type: str | list, to_type: str | list) -> bool:
    """integer → number is safe; string → integer is not."""
    safe = {("integer", "number"), ("integer", "string"), ("number", "string")}
    if isinstance(from_type, str) and isinstance(to_type, str):
        return (from_type, to_type) in safe
    return False

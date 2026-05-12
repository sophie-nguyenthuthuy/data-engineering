from __future__ import annotations

from typing import Any

from .models import CompatibilityError, CompatibilityMode, CompatibilityResult, SchemaVersion

# JSON Schema type hierarchy for safe widening
_TYPE_ORDER = ["null", "boolean", "integer", "number", "string", "array", "object"]

_WIDENING = {
    "integer": {"integer", "number"},
    "number": {"number"},
    "string": {"string"},
    "boolean": {"boolean"},
    "null": {"null"},
    "array": {"array"},
    "object": {"object"},
}


def _normalize_type(t: Any) -> set[str]:
    if isinstance(t, str):
        return {t}
    if isinstance(t, list):
        return set(t)
    return {"any"}


def _get_required(schema: dict) -> set[str]:
    return set(schema.get("required", []))


def _get_properties(schema: dict) -> dict[str, Any]:
    return schema.get("properties", {})


def _is_type_compatible(old_type: Any, new_type: Any) -> bool:
    """New type must be a superset of old type (for backward) or subset (for forward)."""
    old_types = _normalize_type(old_type)
    new_types = _normalize_type(new_type)
    return old_types.issubset(new_types)


class CompatibilityChecker:
    """
    Checks JSON Schema compatibility between two schema versions.

    Backward  – new schema can read data written with OLD schema
                (consumers using new schema can still read old messages)
    Forward   – old schema can read data written with NEW schema
                (consumers using old schema can still read new messages)
    Full      – both directions
    *_Transitive – checked against ALL previous versions, not just the last
    """

    def check(
        self,
        new_schema: dict,
        old_schema: dict,
        mode: CompatibilityMode,
    ) -> CompatibilityResult:
        errors: list[CompatibilityError] = []
        warnings: list[str] = []

        if mode == CompatibilityMode.NONE:
            return CompatibilityResult(compatible=True, mode=mode)

        if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.BACKWARD_TRANSITIVE):
            errors.extend(self._check_backward(new_schema, old_schema))

        elif mode in (CompatibilityMode.FORWARD, CompatibilityMode.FORWARD_TRANSITIVE):
            errors.extend(self._check_forward(new_schema, old_schema))

        elif mode in (CompatibilityMode.FULL, CompatibilityMode.FULL_TRANSITIVE):
            errors.extend(self._check_backward(new_schema, old_schema))
            errors.extend(self._check_forward(new_schema, old_schema))

        return CompatibilityResult(
            compatible=not any(e.breaking for e in errors),
            mode=mode,
            errors=errors,
            warnings=warnings,
        )

    # ── Backward: new reads old data ──────────────────────────────────────

    def _check_backward(self, new: dict, old: dict) -> list[CompatibilityError]:
        """
        Rules for BACKWARD compatibility:
        - May add new OPTIONAL fields (with default)
        - May remove fields (consumers simply won't see them)
        - May NOT add new REQUIRED fields
        - May NOT change field type to an incompatible one
        - May NOT remove a field that was required (readers would break on missing key)
          → Actually removing required fields IS backward compatible because new schema
            readers can handle the absence; old data just won't have the field.
        """
        errors: list[CompatibilityError] = []
        old_props = _get_properties(old)
        new_props = _get_properties(new)
        old_req = _get_required(old)
        new_req = _get_required(new)

        # New required fields that don't exist in old schema → old data won't have them
        for field in new_req - old_req:
            if field not in old_props:
                errors.append(
                    CompatibilityError(
                        type="NEW_REQUIRED_FIELD",
                        path=f"$.properties.{field}",
                        message=f"New required field '{field}' added; old data will not have it.",
                    )
                )

        # Type changes on existing fields
        for field, new_field_schema in new_props.items():
            if field in old_props:
                old_field_schema = old_props[field]
                errs = self._check_type_compatible(field, old_field_schema, new_field_schema, direction="backward")
                errors.extend(errs)

        return errors

    # ── Forward: old reads new data ───────────────────────────────────────

    def _check_forward(self, new: dict, old: dict) -> list[CompatibilityError]:
        """
        Rules for FORWARD compatibility:
        - May remove optional fields (old consumers just won't see them in new data)
        - May NOT remove required fields (new data won't have them → old readers break)
        - May NOT add fields without defaults that old readers rely on being absent
        - May NOT change field type to an incompatible one
        """
        errors: list[CompatibilityError] = []
        old_props = _get_properties(old)
        new_props = _get_properties(new)
        old_req = _get_required(old)
        new_req = _get_required(new)

        # Fields removed from new schema that were required in old
        for field in old_req - set(new_props.keys()):
            errors.append(
                CompatibilityError(
                    type="REMOVED_REQUIRED_FIELD",
                    path=f"$.properties.{field}",
                    message=f"Required field '{field}' removed; old consumers expect it.",
                )
            )

        # Type changes
        for field, old_field_schema in old_props.items():
            if field in new_props:
                new_field_schema = new_props[field]
                errs = self._check_type_compatible(field, old_field_schema, new_field_schema, direction="forward")
                errors.extend(errs)

        return errors

    # ── Helpers ───────────────────────────────────────────────────────────

    def _check_type_compatible(
        self,
        field: str,
        old_schema: dict,
        new_schema: dict,
        direction: str,
    ) -> list[CompatibilityError]:
        errors: list[CompatibilityError] = []
        old_type = old_schema.get("type")
        new_type = new_schema.get("type")

        if old_type is None or new_type is None:
            return errors  # no type constraint → skip

        old_types = _normalize_type(old_type)
        new_types = _normalize_type(new_type)

        if direction == "backward":
            # new schema must accept at least all old types
            if not old_types.issubset(new_types):
                errors.append(
                    CompatibilityError(
                        type="INCOMPATIBLE_TYPE",
                        path=f"$.properties.{field}.type",
                        message=(
                            f"Field '{field}' type changed from {old_type!r} to {new_type!r}; "
                            f"new schema must accept all old types."
                        ),
                    )
                )
        else:
            # forward: old schema must accept at least all new types
            if not new_types.issubset(old_types):
                errors.append(
                    CompatibilityError(
                        type="INCOMPATIBLE_TYPE",
                        path=f"$.properties.{field}.type",
                        message=(
                            f"Field '{field}' type changed from {old_type!r} to {new_type!r}; "
                            f"old schema cannot read new data with this type."
                        ),
                    )
                )

        # Enum narrowing
        old_enum = set(old_schema.get("enum", []))
        new_enum = set(new_schema.get("enum", []))
        if old_enum and new_enum:
            if direction == "backward" and not old_enum.issubset(new_enum):
                removed = old_enum - new_enum
                errors.append(
                    CompatibilityError(
                        type="ENUM_VALUE_REMOVED",
                        path=f"$.properties.{field}.enum",
                        message=f"Enum values removed from '{field}': {removed}",
                    )
                )
            if direction == "forward" and not new_enum.issubset(old_enum):
                added = new_enum - old_enum
                errors.append(
                    CompatibilityError(
                        type="ENUM_VALUE_ADDED",
                        path=f"$.properties.{field}.enum",
                        message=f"New enum values in '{field}' unknown to old schema: {added}",
                    )
                )

        # Nested object recursion
        if "properties" in old_schema and "properties" in new_schema:
            if direction == "backward":
                errors.extend(self._check_backward(new_schema, old_schema))
            else:
                errors.extend(self._check_forward(new_schema, old_schema))

        return errors


def check_compatibility(
    new_schema: dict,
    existing_versions: list[SchemaVersion],
    mode: CompatibilityMode,
) -> CompatibilityResult:
    """Entry-point: check new_schema against existing_versions under the given mode."""
    checker = CompatibilityChecker()

    if not existing_versions or mode == CompatibilityMode.NONE:
        return CompatibilityResult(compatible=True, mode=mode)

    transitive = mode in (
        CompatibilityMode.BACKWARD_TRANSITIVE,
        CompatibilityMode.FORWARD_TRANSITIVE,
        CompatibilityMode.FULL_TRANSITIVE,
    )

    versions_to_check = existing_versions if transitive else [existing_versions[-1]]

    all_errors: list[CompatibilityError] = []
    all_warnings: list[str] = []

    for sv in versions_to_check:
        result = checker.check(new_schema, sv.schema_definition, mode)
        all_errors.extend(result.errors)
        all_warnings.extend(result.warnings)

    return CompatibilityResult(
        compatible=not any(e.breaking for e in all_errors),
        mode=mode,
        errors=all_errors,
        warnings=all_warnings,
    )

"""
Migration Executor
==================
Applies a MigrationScript (list of steps) to a single event payload dict.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from src.registry.models import MigrationScript, MigrationStep


class MigrationError(Exception):
    pass


class MigrationExecutor:
    """Execute migration steps against a payload dict."""

    def apply(self, payload: dict[str, Any], script: MigrationScript) -> dict[str, Any]:
        result = deepcopy(payload)
        for step in script.steps:
            result = self._apply_step(result, step)
        return result

    def apply_chain(
        self, payload: dict[str, Any], scripts: list[MigrationScript]
    ) -> dict[str, Any]:
        result = deepcopy(payload)
        for script in scripts:
            result = self.apply(result, script)
        return result

    def _apply_step(self, data: dict[str, Any], step: MigrationStep) -> dict[str, Any]:
        field = _path_to_key(step.path)
        op = step.op
        p = step.params

        if op == "rename_field":
            to = p.get("to")
            if not to:
                raise MigrationError(f"rename_field requires 'to' param at {step.path}")
            if field in data:
                data[to] = data.pop(field)

        elif op == "remove_field":
            data.pop(field, None)

        elif op == "add_field":
            if field not in data:
                data[field] = p.get("default")

        elif op == "default_field":
            if field not in data or data[field] is None:
                data[field] = p.get("default")

        elif op == "cast_field":
            if field in data and data[field] is not None:
                data[field] = _cast(data[field], p.get("to_type", "string"))

        elif op == "map_value":
            mapping = p.get("mapping", {})
            if field in data:
                val = str(data[field])
                if val in mapping:
                    data[field] = mapping[val]

        elif op == "split_field":
            into = p.get("into", [])
            sep = p.get("separator", " ")
            if field in data and isinstance(data[field], str) and len(into) >= 2:
                parts = data[field].split(sep, len(into) - 1)
                for i, key in enumerate(into):
                    data[key] = parts[i] if i < len(parts) else ""
                data.pop(field, None)

        elif op == "merge_fields":
            sources = p.get("sources", [])
            template = p.get("template", " ".join(f"{{{s}}}" for s in sources))
            try:
                values = {s: str(data.get(s, "")) for s in sources}
                data[field] = template.format(**values)
            except KeyError:
                pass

        elif op == "copy_field":
            to = p.get("to")
            if to and field in data:
                data[to] = deepcopy(data[field])

        elif op == "compute_field":
            expression = p.get("expression", "")
            try:
                safe_globals: dict[str, Any] = {"__builtins__": {}}
                result = eval(expression, safe_globals, dict(data))  # noqa: S307
                data[field] = result
            except Exception as exc:
                raise MigrationError(f"compute_field expression failed: {exc}") from exc

        elif op == "flatten_field":
            if field in data and isinstance(data[field], dict):
                nested = data.pop(field)
                prefix = p.get("prefix", f"{field}_")
                for k, v in nested.items():
                    data[f"{prefix}{k}"] = v

        elif op == "wrap_field":
            if field in data:
                wrapper_key = p.get("key", "value")
                data[field] = {wrapper_key: data[field]}

        return data


def _path_to_key(path: str) -> str:
    """Convert '$' or '$.field_name' to 'field_name'."""
    path = path.strip()
    if path.startswith("$."):
        return path[2:]
    if path == "$":
        return "$"
    return path


def _cast(value: Any, to_type: str) -> Any:
    try:
        if to_type in ("int", "integer"):
            return int(float(value))
        if to_type in ("float", "number"):
            return float(value)
        if to_type == "string":
            return str(value)
        if to_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return bool(value)
    except (ValueError, TypeError):
        pass
    return value

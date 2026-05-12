from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Map pandas dtypes to canonical type buckets for drift comparison
_DTYPE_BUCKET: dict[str, str] = {
    "int8": "integer", "int16": "integer", "int32": "integer", "int64": "integer",
    "uint8": "integer", "uint16": "integer", "uint32": "integer", "uint64": "integer",
    "float16": "float", "float32": "float", "float64": "float",
    "bool": "boolean",
    "object": "string",
    "string": "string",
    "datetime64[ns]": "datetime", "datetime64[us]": "datetime",
    "timedelta64[ns]": "timedelta",
    "category": "string",
}


def canonical_dtype(dtype: Any) -> str:
    name = str(dtype)
    return _DTYPE_BUCKET.get(name, name)


def schema_from_df(df: pd.DataFrame) -> dict[str, str]:
    """Extract a canonical schema dict from a DataFrame."""
    return {col: canonical_dtype(df[col].dtype) for col in df.columns}


@dataclass
class DriftReport:
    source_name: str
    expected_version: int
    expected_schema: dict[str, str]
    observed_schema: dict[str, str]
    added_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    type_changes: dict[str, tuple[str, str]] = field(default_factory=dict)  # col -> (old, new)

    @property
    def has_drift(self) -> bool:
        return bool(self.added_columns or self.removed_columns or self.type_changes)

    @property
    def drift_types(self) -> list[str]:
        kinds = []
        if self.added_columns:
            kinds.append("added_columns")
        if self.removed_columns:
            kinds.append("removed_columns")
        if self.type_changes:
            kinds.append("type_changed")
        return kinds

    def root_cause_hints(self) -> list[str]:
        hints = []
        if self.added_columns:
            hints.append(
                f"New columns detected ({', '.join(self.added_columns)}). "
                "Likely a upstream producer schema migration. "
                "Check source changelog or producer deployment history."
            )
        if self.removed_columns:
            hints.append(
                f"Expected columns missing ({', '.join(self.removed_columns)}). "
                "Source may have dropped/renamed columns or query projection changed. "
                "Verify source DDL and any recent ALTER TABLE statements."
            )
        for col, (old, new) in self.type_changes.items():
            hints.append(
                f"Column '{col}' changed type {old!r} -> {new!r}. "
                "Common causes: upstream cast change, CSV text promotion, or ORM mapping update."
            )
        return hints

    def to_details_json(self) -> str:
        return json.dumps({
            "added_columns": self.added_columns,
            "removed_columns": self.removed_columns,
            "type_changes": {k: list(v) for k, v in self.type_changes.items()},
            "root_cause_hints": self.root_cause_hints(),
        }, indent=2)

    def summary(self) -> str:
        parts = []
        if self.added_columns:
            parts.append(f"added={self.added_columns}")
        if self.removed_columns:
            parts.append(f"removed={self.removed_columns}")
        if self.type_changes:
            tc = {k: f"{v[0]}->{v[1]}" for k, v in self.type_changes.items()}
            parts.append(f"type_changes={tc}")
        return "; ".join(parts) if parts else "no drift"


class DriftDetector:
    """Compares an observed DataFrame schema against the registered expected schema."""

    def detect(
        self,
        source_name: str,
        df: pd.DataFrame,
        expected_version: int,
        expected_schema: dict[str, str],
    ) -> DriftReport:
        observed = schema_from_df(df)
        exp_cols = set(expected_schema.keys())
        obs_cols = set(observed.keys())

        added = sorted(obs_cols - exp_cols)
        removed = sorted(exp_cols - obs_cols)
        type_changes: dict[str, tuple[str, str]] = {}
        for col in exp_cols & obs_cols:
            if expected_schema[col] != observed[col]:
                type_changes[col] = (expected_schema[col], observed[col])

        report = DriftReport(
            source_name=source_name,
            expected_version=expected_version,
            expected_schema=expected_schema,
            observed_schema=observed,
            added_columns=added,
            removed_columns=removed,
            type_changes=type_changes,
        )

        if report.has_drift:
            logger.warning(
                "Schema drift detected for '%s' (v%d): %s",
                source_name, expected_version, report.summary(),
            )
        return report

"""Validate pipeline output data against a DataContract."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from .contract import DataContract, FieldSchema, SLARule, SemanticRule

# ------------------------------------------------------------------ #
# Result types
# ------------------------------------------------------------------ #

@dataclass
class ValidationIssue:
    rule: str
    severity: str  # error | warning
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    contract_id: str
    contract_version: str
    producer: str
    validated_at: str
    passed: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    def to_dict(self) -> dict:
        return {
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "producer": self.producer,
            "validated_at": self.validated_at,
            "passed": self.passed,
            "error_count": len(self.errors()),
            "warning_count": len(self.warnings()),
            "issues": [
                {
                    "rule": i.rule,
                    "severity": i.severity,
                    "message": i.message,
                    "details": i.details,
                }
                for i in self.issues
            ],
            "stats": self.stats,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


# ------------------------------------------------------------------ #
# Type mapping
# ------------------------------------------------------------------ #

_TYPE_MAP: dict[str, type | tuple] = {
    "string": str,
    "integer": (int,),
    "number": (int, float),
    "boolean": bool,
    "date": str,
    "timestamp": str,
    "array": list,
    "object": dict,
}

_PANDAS_DTYPE_MAP: dict[str, list[str]] = {
    "string":    ["object", "string", "StringDtype"],
    "integer":   ["int8", "int16", "int32", "int64", "Int8", "Int16", "Int32", "Int64"],
    "number":    ["float16", "float32", "float64", "int8", "int16", "int32", "int64",
                  "Int8", "Int16", "Int32", "Int64", "Float32", "Float64"],
    "boolean":   ["bool", "boolean"],
    "date":      ["object", "string", "datetime64[ns]", "datetime64[us]"],
    "timestamp": ["object", "string", "datetime64[ns]", "datetime64[us]"],
    "array":     ["object"],
    "object":    ["object"],
}


# ------------------------------------------------------------------ #
# Validator
# ------------------------------------------------------------------ #

class ContractValidator:
    def __init__(self, contract: DataContract):
        self.contract = contract

    def validate(
        self,
        df: pd.DataFrame,
        *,
        freshness_seconds: float | None = None,
        latency_seconds: float | None = None,
    ) -> ValidationResult:
        issues: list[ValidationIssue] = []
        stats: dict[str, Any] = {
            "row_count": len(df),
            "column_count": len(df.columns),
        }

        self._check_schema(df, issues)
        self._check_sla(df, issues, stats,
                        freshness_seconds=freshness_seconds,
                        latency_seconds=latency_seconds)
        self._check_semantic(df, issues)

        has_errors = any(i.severity == "error" for i in issues)
        return ValidationResult(
            contract_id=self.contract.id,
            contract_version=self.contract.version,
            producer=self.contract.producer,
            validated_at=datetime.now(timezone.utc).isoformat(),
            passed=not has_errors,
            issues=issues,
            stats=stats,
        )

    # ---------------------------------------------------------------- #

    def _check_schema(self, df: pd.DataFrame, issues: list[ValidationIssue]) -> None:
        field_map = {f.name: f for f in self.contract.fields}

        # Required fields present
        for fname, fschema in field_map.items():
            if fname not in df.columns:
                issues.append(ValidationIssue(
                    rule="schema.required_field",
                    severity="error",
                    message=f"Required field '{fname}' missing from dataset",
                ))
                continue

            series = df[fname]
            dtype_str = str(series.dtype)
            allowed = _PANDAS_DTYPE_MAP.get(fschema.type, [])

            type_ok = any(dtype_str.startswith(a) or dtype_str == a for a in allowed)
            if not type_ok:
                issues.append(ValidationIssue(
                    rule="schema.type_mismatch",
                    severity="error",
                    message=(
                        f"Field '{fname}' expected type '{fschema.type}' "
                        f"but got pandas dtype '{dtype_str}'"
                    ),
                    details={"field": fname, "expected": fschema.type, "actual": dtype_str},
                ))

            if not fschema.nullable:
                null_count = int(series.isna().sum())
                if null_count > 0:
                    issues.append(ValidationIssue(
                        rule="schema.null_violation",
                        severity="error",
                        message=f"Non-nullable field '{fname}' has {null_count} null value(s)",
                        details={"field": fname, "null_count": null_count},
                    ))

            self._check_field_constraints(fname, series, fschema, issues)

        # Unexpected fields (warning only)
        for col in df.columns:
            if col not in field_map:
                issues.append(ValidationIssue(
                    rule="schema.unexpected_field",
                    severity="warning",
                    message=f"Field '{col}' not declared in contract",
                    details={"field": col},
                ))

    def _check_field_constraints(
        self,
        fname: str,
        series: pd.Series,
        fschema: FieldSchema,
        issues: list[ValidationIssue],
    ) -> None:
        c = fschema.constraints
        if not c:
            return

        non_null = series.dropna()

        if "min" in c and len(non_null) > 0:
            bad = int((non_null < c["min"]).sum())
            if bad:
                issues.append(ValidationIssue(
                    rule="schema.constraint.min",
                    severity="error",
                    message=f"Field '{fname}': {bad} value(s) below min={c['min']}",
                    details={"field": fname, "min": c["min"], "violations": bad},
                ))

        if "max" in c and len(non_null) > 0:
            bad = int((non_null > c["max"]).sum())
            if bad:
                issues.append(ValidationIssue(
                    rule="schema.constraint.max",
                    severity="error",
                    message=f"Field '{fname}': {bad} value(s) above max={c['max']}",
                    details={"field": fname, "max": c["max"], "violations": bad},
                ))

        if "unique" in c and c["unique"]:
            dup_count = int(series.duplicated().sum())
            if dup_count:
                issues.append(ValidationIssue(
                    rule="schema.constraint.unique",
                    severity="error",
                    message=f"Field '{fname}': {dup_count} duplicate value(s)",
                    details={"field": fname, "duplicate_count": dup_count},
                ))

        if "allowed_values" in c:
            allowed_set = set(c["allowed_values"])
            bad_vals = set(non_null.unique()) - allowed_set
            if bad_vals:
                issues.append(ValidationIssue(
                    rule="schema.constraint.allowed_values",
                    severity="error",
                    message=f"Field '{fname}': unexpected value(s) {sorted(str(v) for v in bad_vals)}",
                    details={"field": fname, "unexpected": sorted(str(v) for v in bad_vals)},
                ))

    # ---------------------------------------------------------------- #

    def _check_sla(
        self,
        df: pd.DataFrame,
        issues: list[ValidationIssue],
        stats: dict[str, Any],
        freshness_seconds: float | None,
        latency_seconds: float | None,
    ) -> None:
        for rule in self.contract.sla_rules:
            if rule.rule_type == "row_count":
                row_count = len(df)
                stats["row_count"] = row_count
                if row_count < rule.threshold:
                    issues.append(ValidationIssue(
                        rule=f"sla.{rule.name}",
                        severity="error",
                        message=(
                            f"SLA '{rule.name}': row count {row_count} "
                            f"below threshold {rule.threshold}"
                        ),
                        details={"actual": row_count, "threshold": rule.threshold},
                    ))

            elif rule.rule_type == "completeness":
                # threshold is minimum non-null fraction (0–1)
                # only measured over non-nullable fields declared in the contract
                if len(df) == 0:
                    continue
                required_cols = [
                    f.name for f in self.contract.fields
                    if not f.nullable and f.name in df.columns
                ]
                subset = df[required_cols] if required_cols else df
                completeness = float(subset.notna().mean().mean())
                stats["completeness"] = round(completeness, 4)
                if completeness < rule.threshold:
                    issues.append(ValidationIssue(
                        rule=f"sla.{rule.name}",
                        severity="error",
                        message=(
                            f"SLA '{rule.name}': completeness {completeness:.2%} "
                            f"below threshold {rule.threshold:.2%}"
                        ),
                        details={"actual": completeness, "threshold": rule.threshold},
                    ))

            elif rule.rule_type == "freshness" and freshness_seconds is not None:
                stats["freshness_seconds"] = freshness_seconds
                if freshness_seconds > rule.threshold:
                    issues.append(ValidationIssue(
                        rule=f"sla.{rule.name}",
                        severity="error",
                        message=(
                            f"SLA '{rule.name}': data is {freshness_seconds}s old, "
                            f"max allowed {rule.threshold}{rule.unit}"
                        ),
                        details={"actual_seconds": freshness_seconds, "threshold": rule.threshold},
                    ))

            elif rule.rule_type == "latency" and latency_seconds is not None:
                stats["latency_seconds"] = latency_seconds
                if latency_seconds > rule.threshold:
                    issues.append(ValidationIssue(
                        rule=f"sla.{rule.name}",
                        severity="error",
                        message=(
                            f"SLA '{rule.name}': pipeline latency {latency_seconds}s "
                            f"exceeded threshold {rule.threshold}{rule.unit}"
                        ),
                        details={"actual_seconds": latency_seconds, "threshold": rule.threshold},
                    ))

    # ---------------------------------------------------------------- #

    def _check_semantic(self, df: pd.DataFrame, issues: list[ValidationIssue]) -> None:
        for rule in self.contract.semantic_rules:
            try:
                result = eval(rule.expression, {"df": df, "pd": pd})  # noqa: S307
                if result is False or (hasattr(result, "all") and not result.all()):
                    issues.append(ValidationIssue(
                        rule=f"semantic.{rule.name}",
                        severity=rule.severity,
                        message=f"Semantic rule '{rule.name}' failed: {rule.description}",
                        details={"expression": rule.expression},
                    ))
            except Exception as exc:
                issues.append(ValidationIssue(
                    rule=f"semantic.{rule.name}",
                    severity="error",
                    message=f"Semantic rule '{rule.name}' raised an exception: {exc}",
                    details={"expression": rule.expression, "error": str(exc)},
                ))

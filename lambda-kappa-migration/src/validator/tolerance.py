"""Tolerance checker: defines rules for comparing Lambda and Kappa outputs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class MatchResult(str, Enum):
    """Outcome of a single field comparison."""

    EXACT_MATCH = "exact_match"
    WITHIN_TOLERANCE = "within_tolerance"
    MISMATCH = "mismatch"
    MISSING_LEFT = "missing_left"
    MISSING_RIGHT = "missing_right"


@dataclass
class FieldComparison:
    """Result of comparing a single (key, field) pair between Lambda and Kappa."""

    key: str
    field: str
    lambda_value: Any
    kappa_value: Any
    result: MatchResult
    delta_pct: float | None  # None for non-numeric comparisons

    @property
    def passed(self) -> bool:
        """True if the comparison is acceptable (match or within tolerance)."""
        return self.result in (MatchResult.EXACT_MATCH, MatchResult.WITHIN_TOLERANCE)


class ToleranceChecker:
    """Applies configured tolerance rules to numeric and count fields.

    Counts are compared exactly; amounts/averages use a relative tolerance.
    """

    COUNT_FIELDS = {"event_count", "count"}
    AMOUNT_FIELDS = {"total_amount", "avg_amount"}

    def __init__(
        self,
        amount_rel_tolerance: float = 0.0001,
        missing_key_is_mismatch: bool = True,
    ) -> None:
        self.amount_rel_tolerance = amount_rel_tolerance
        self.missing_key_is_mismatch = missing_key_is_mismatch

    def compare_dicts(
        self,
        key: str,
        lambda_dict: dict[str, Any],
        kappa_dict: dict[str, Any],
    ) -> list[FieldComparison]:
        """Compare two flat dicts (one per entity) and return per-field results."""
        comparisons: list[FieldComparison] = []
        all_fields = set(lambda_dict) | set(kappa_dict)

        for field in sorted(all_fields):
            if field not in lambda_dict:
                if self.missing_key_is_mismatch:
                    comparisons.append(
                        FieldComparison(
                            key=key,
                            field=field,
                            lambda_value=None,
                            kappa_value=kappa_dict[field],
                            result=MatchResult.MISSING_LEFT,
                            delta_pct=None,
                        )
                    )
                continue
            if field not in kappa_dict:
                if self.missing_key_is_mismatch:
                    comparisons.append(
                        FieldComparison(
                            key=key,
                            field=field,
                            lambda_value=lambda_dict[field],
                            kappa_value=None,
                            result=MatchResult.MISSING_RIGHT,
                            delta_pct=None,
                        )
                    )
                continue

            lv = lambda_dict[field]
            kv = kappa_dict[field]
            comparisons.append(self._compare_field(key, field, lv, kv))

        return comparisons

    def _compare_field(
        self,
        key: str,
        field: str,
        lv: Any,
        kv: Any,
    ) -> FieldComparison:
        if field in self.COUNT_FIELDS:
            return self._compare_exact(key, field, lv, kv)
        if field in self.AMOUNT_FIELDS:
            return self._compare_relative(key, field, lv, kv)
        # Default: try relative if numeric, else exact
        if isinstance(lv, (int, float)) and isinstance(kv, (int, float)):
            return self._compare_relative(key, field, lv, kv)
        return self._compare_exact(key, field, lv, kv)

    def _compare_exact(self, key: str, field: str, lv: Any, kv: Any) -> FieldComparison:
        if lv == kv:
            return FieldComparison(key=key, field=field, lambda_value=lv, kappa_value=kv,
                                   result=MatchResult.EXACT_MATCH, delta_pct=0.0)
        return FieldComparison(key=key, field=field, lambda_value=lv, kappa_value=kv,
                               result=MatchResult.MISMATCH, delta_pct=None)

    def _compare_relative(self, key: str, field: str, lv: float, kv: float) -> FieldComparison:
        lv_f = float(lv)
        kv_f = float(kv)
        if lv_f == kv_f:
            return FieldComparison(key=key, field=field, lambda_value=lv, kappa_value=kv,
                                   result=MatchResult.EXACT_MATCH, delta_pct=0.0)
        if lv_f == 0.0:
            delta_pct = abs(kv_f) * 100
        else:
            delta_pct = abs(kv_f - lv_f) / abs(lv_f) * 100

        if delta_pct / 100 <= self.amount_rel_tolerance:
            return FieldComparison(key=key, field=field, lambda_value=lv, kappa_value=kv,
                                   result=MatchResult.WITHIN_TOLERANCE, delta_pct=delta_pct)
        return FieldComparison(key=key, field=field, lambda_value=lv, kappa_value=kv,
                               result=MatchResult.MISMATCH, delta_pct=delta_pct)

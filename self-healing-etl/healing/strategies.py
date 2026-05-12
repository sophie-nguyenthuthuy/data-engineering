from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from schema.drift_detector import DriftReport

logger = logging.getLogger(__name__)


@dataclass
class HealingResult:
    success: bool
    healed_df: pd.DataFrame
    actions_taken: list[str] = field(default_factory=list)
    rows_coerced: int = 0
    rows_quarantined: int = 0
    failed_records: list[dict[str, Any]] = field(default_factory=list)
    failure_reason: str = ""

    def summary(self) -> str:
        return "; ".join(self.actions_taken) or "no healing applied"


class HealingEngine:
    """
    Applies a sequence of healing strategies to a DataFrame given a DriftReport.

    Strategy order matters:
      1. Backfill removed/missing columns with typed nulls
      2. Drop unexpected added columns (or keep if schema evolution is enabled)
      3. Coerce type-changed columns toward the expected type
    """

    def __init__(
        self,
        enable_type_coercion: bool = True,
        enable_column_backfill: bool = True,
        enable_schema_evolution: bool = True,
        max_coercion_loss_pct: float = 5.0,
    ):
        self.enable_type_coercion = enable_type_coercion
        self.enable_column_backfill = enable_column_backfill
        self.enable_schema_evolution = enable_schema_evolution
        self.max_coercion_loss_pct = max_coercion_loss_pct

    def heal(self, df: pd.DataFrame, report: DriftReport) -> HealingResult:
        actions: list[str] = []
        df = df.copy()

        # --- 1. Backfill removed columns ---
        if self.enable_column_backfill and report.removed_columns:
            for col in report.removed_columns:
                expected_type = report.expected_schema[col]
                null_val = _null_for_type(expected_type)
                df[col] = null_val
                actions.append(f"backfilled missing column '{col}' ({expected_type}) with null")
                logger.info("Healing: backfilled column '%s'", col)

        # --- 2. Handle added columns ---
        if report.added_columns:
            if self.enable_schema_evolution:
                actions.append(
                    f"kept new columns for schema evolution: {report.added_columns}"
                )
                logger.info("Healing: retaining new columns %s for schema evolution", report.added_columns)
            else:
                df = df.drop(columns=report.added_columns, errors="ignore")
                actions.append(f"dropped unexpected columns: {report.added_columns}")
                logger.info("Healing: dropped unexpected columns %s", report.added_columns)

        # --- 3. Type coercion ---
        rows_coerced = 0
        failed_records: list[dict[str, Any]] = []

        if self.enable_type_coercion and report.type_changes:
            for col, (expected, observed) in report.type_changes.items():
                before_nulls = df[col].isna().sum()
                df, n_failed, failed = _coerce_column(df, col, expected)
                after_nulls = df[col].isna().sum()
                n_coerced = int(after_nulls - before_nulls)
                rows_coerced += n_coerced
                failed_records.extend(failed)

                if n_coerced > 0:
                    actions.append(
                        f"coerced column '{col}' from {observed!r} to {expected!r} "
                        f"({n_coerced} nulled)"
                    )

        # --- 4. Coercion loss gate ---
        total_rows = len(df)
        loss_pct = (rows_coerced / total_rows * 100) if total_rows > 0 else 0

        if loss_pct > self.max_coercion_loss_pct:
            msg = (
                f"Coercion loss {loss_pct:.1f}% exceeds threshold "
                f"{self.max_coercion_loss_pct}% — batch marked for quarantine"
            )
            logger.error("Healing failed: %s", msg)
            return HealingResult(
                success=False,
                healed_df=df,
                actions_taken=actions,
                rows_coerced=rows_coerced,
                rows_quarantined=total_rows,
                failed_records=df.to_dict(orient="records"),
                failure_reason=msg,
            )

        return HealingResult(
            success=True,
            healed_df=df,
            actions_taken=actions,
            rows_coerced=rows_coerced,
            rows_quarantined=len(failed_records),
            failed_records=failed_records,
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _null_for_type(dtype_bucket: str) -> Any:
    mapping = {
        "integer": pd.NA,
        "float": float("nan"),
        "boolean": pd.NA,
        "string": None,
        "datetime": pd.NaT,
    }
    return mapping.get(dtype_bucket, None)


def _coerce_column(
    df: pd.DataFrame, col: str, target_type: str
) -> tuple[pd.DataFrame, int, list[dict[str, Any]]]:
    """
    Attempt to cast column to the target type bucket.
    Returns (modified_df, n_failed_rows, failed_record_dicts).
    """
    failed: list[dict[str, Any]] = []
    original = df[col].copy()

    try:
        if target_type == "integer":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif target_type == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif target_type == "boolean":
            df[col] = df[col].map(
                lambda v: _parse_bool(v) if pd.notna(v) else pd.NA
            ).astype("boolean")
        elif target_type == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        elif target_type == "string":
            df[col] = df[col].astype(str).where(original.notna(), other=None)
    except Exception as exc:
        logger.warning("Type coercion for '%s' -> '%s' raised: %s", col, target_type, exc)

    # Rows that went from non-null to null are coercion failures
    newly_null = original.notna() & df[col].isna()
    if newly_null.any():
        failed_rows = df[newly_null].copy()
        failed_rows[col] = original[newly_null]  # restore original value for context
        failed.extend(failed_rows.to_dict(orient="records"))

    return df, int(newly_null.sum()), failed


def _parse_bool(v: Any) -> bool | type(pd.NA):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        low = v.strip().lower()
        if low in ("true", "1", "yes", "y", "t"):
            return True
        if low in ("false", "0", "no", "n", "f"):
            return False
    if isinstance(v, (int, float)):
        return bool(v)
    return pd.NA

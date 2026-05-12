"""Input validation helpers for SBV pipeline."""
from __future__ import annotations

import re
from typing import Any

import pandas as pd

REQUIRED_COLUMNS = {
    "transaction_id", "transaction_date", "transaction_time",
    "transaction_type", "debit_account", "credit_account",
    "currency", "amount", "vnd_equivalent", "branch_code", "status",
}

VALID_CURRENCIES = {"VND", "USD", "EUR", "JPY", "GBP", "AUD", "CNY", "SGD", "HKD", "CHF"}
VALID_STATUSES = {"SUCCESS", "FAILED", "PENDING", "REVERSED"}


class ValidationError(Exception):
    pass


def validate_dataframe(df: pd.DataFrame) -> list[str]:
    """Return a list of validation warning strings; empty = clean."""
    warnings: list[str] = []

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")

    if df["transaction_id"].duplicated().any():
        n = df["transaction_id"].duplicated().sum()
        warnings.append(f"Duplicate transaction_id found: {n} rows")

    if (df["amount"] <= 0).any():
        n = (df["amount"] <= 0).sum()
        warnings.append(f"Non-positive amount on {n} rows")

    bad_currency = ~df["currency"].isin(VALID_CURRENCIES)
    if bad_currency.any():
        warnings.append(f"Unknown currency codes: {df.loc[bad_currency, 'currency'].unique().tolist()}")

    bad_status = ~df["status"].isin(VALID_STATUSES)
    if bad_status.any():
        warnings.append(f"Unknown status values: {df.loc[bad_status, 'status'].unique().tolist()}")

    null_critical = df[list(REQUIRED_COLUMNS)].isnull().any()
    if null_critical.any():
        cols = null_critical[null_critical].index.tolist()
        warnings.append(f"Null values in critical columns: {cols}")

    return warnings


def validate_vnd_equivalent(df: pd.DataFrame, tolerance: float = 0.01) -> list[str]:
    """Check that VND equivalents are non-negative and internally consistent."""
    warnings: list[str] = []
    if (df["vnd_equivalent"] < 0).any():
        warnings.append("Negative vnd_equivalent values detected")
    vnd_rows = df[df["currency"] == "VND"]
    diff = (vnd_rows["amount"] - vnd_rows["vnd_equivalent"]).abs()
    if (diff > tolerance).any():
        n = (diff > tolerance).sum()
        warnings.append(f"VND amount/vnd_equivalent mismatch on {n} rows")
    return warnings

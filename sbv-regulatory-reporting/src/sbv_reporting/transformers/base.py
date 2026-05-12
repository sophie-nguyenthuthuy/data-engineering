"""Base transformer — loads raw CSV and normalises to canonical DataFrame."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from sbv_reporting.utils.validators import validate_dataframe, validate_vnd_equivalent
from sbv_reporting.utils.config import get_config


class RawTransactionLoader:
    """Load and normalise raw transaction CSV into a clean DataFrame."""

    def __init__(self):
        self.cfg = get_config()

    def load(self, path: str | Path) -> tuple[pd.DataFrame, list[str]]:
        """Return (df, warnings).  Raises ValidationError on fatal issues."""
        df = pd.read_csv(path, dtype={"debit_account": str, "credit_account": str,
                                       "counterparty_account": str})

        df["transaction_date"] = pd.to_datetime(df["transaction_date"], format="%Y-%m-%d")
        df["transaction_datetime"] = pd.to_datetime(
            df["transaction_date"].dt.strftime("%Y-%m-%d") + " " + df["transaction_time"]
        )

        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["vnd_equivalent"] = pd.to_numeric(df["vnd_equivalent"], errors="coerce").fillna(0.0)

        df["currency"] = df["currency"].str.upper().str.strip()
        df["status"] = df["status"].str.upper().str.strip()
        df["transaction_type"] = df["transaction_type"].str.upper().str.strip()
        df["branch_code"] = df["branch_code"].str.upper().str.strip()

        warnings = validate_dataframe(df)
        warnings += validate_vnd_equivalent(df, self.cfg["thresholds"]["reconciliation_tolerance"])

        return df, warnings

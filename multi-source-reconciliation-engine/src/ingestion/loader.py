"""Ingest and normalize financial data from all four sources."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


@dataclass
class NormalizedTransaction:
    """Common schema shared across all sources after normalization."""
    source: str
    source_id: str
    amount: float
    currency: str
    value_date: date
    description: str
    reference: str
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def amount_key(self) -> str:
        return f"{self.amount:.2f}"

    @property
    def date_key(self) -> str:
        return self.value_date.isoformat()


class SourceLoader:
    """Loads and normalizes transactions from any of the four source types."""

    SOURCE_NAMES = ("core_banking", "reporting_system", "third_party_aggregator", "manual_entries")

    def __init__(self, config: dict):
        self.config = config["sources"]

    def load_all(self, source_paths: dict[str, Path]) -> dict[str, list[NormalizedTransaction]]:
        result: dict[str, list[NormalizedTransaction]] = {}
        for source_name, path in source_paths.items():
            if source_name not in self.SOURCE_NAMES:
                raise ValueError(f"Unknown source: {source_name}")
            result[source_name] = self._load_source(source_name, Path(path))
        return result

    def _load_source(self, source_name: str, path: Path) -> list[NormalizedTransaction]:
        ext = path.suffix.lower()
        if ext == ".csv":
            df = pd.read_csv(path, dtype=str)
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(path, dtype=str)
        elif ext == ".json":
            df = pd.read_json(path, dtype=str)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        cfg = self.config[source_name]
        transactions = []

        for _, row in df.iterrows():
            try:
                txn = NormalizedTransaction(
                    source=source_name,
                    source_id=self._get(row, cfg["id_field"]),
                    amount=self._parse_amount(self._get(row, cfg["amount_field"])),
                    currency=str(row.get("currency", "USD")).upper().strip() or "USD",
                    value_date=self._parse_date(self._get(row, cfg["date_field"])),
                    description=self._clean_text(self._get(row, cfg["description_field"])),
                    reference=self._clean_text(self._get(row, cfg["reference_field"])),
                    raw=row.to_dict(),
                )
                transactions.append(txn)
            except Exception as exc:
                print(f"[{source_name}] Skipping row {_}: {exc}")

        return transactions

    @staticmethod
    def _get(row: pd.Series, field: str) -> str:
        val = row.get(field, "")
        return "" if pd.isna(val) else str(val).strip()

    @staticmethod
    def _parse_amount(raw: str) -> float:
        cleaned = re.sub(r"[^\d.\-]", "", raw)
        return round(float(cleaned), 4)

    @staticmethod
    def _parse_date(raw: str) -> date:
        from dateutil import parser as dp
        return dp.parse(raw).date()

    @staticmethod
    def _clean_text(raw: str) -> str:
        return re.sub(r"\s+", " ", raw).upper().strip()

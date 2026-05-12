"""Classify discrepancies within matched groups."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ..matching.engine import MatchGroup


class DiscrepancyType(str, Enum):
    NONE = "none"
    TIMING = "timing"
    ROUNDING = "rounding"
    AMOUNT_MISMATCH = "amount_mismatch"
    MISSING = "missing"
    DESCRIPTION_MISMATCH = "description_mismatch"
    MULTI = "multi"  # more than one type present


@dataclass
class Discrepancy:
    group_id: str
    types: list[DiscrepancyType]
    primary_type: DiscrepancyType
    details: dict
    severity: str  # LOW / MEDIUM / HIGH / CRITICAL


class DiscrepancyClassifier:
    def __init__(self, config: dict):
        rec = config["reconciliation"]
        self.rounding_threshold = rec["rounding_threshold"]
        self.timing_window = rec["timing_day_window"]
        self.all_sources = list(config["sources"].keys())

    def classify(self, groups: list[MatchGroup]) -> list[Discrepancy]:
        return [d for g in groups if (d := self._classify_group(g)) is not None]

    def _classify_group(self, group: MatchGroup) -> Optional[Discrepancy]:
        members = group.transactions
        present_sources = set(members.keys())
        missing_sources = set(self.all_sources) - present_sources

        types: list[DiscrepancyType] = []
        details: dict = {}

        # ── Missing source ────────────────────────────────────────────────────
        if missing_sources:
            types.append(DiscrepancyType.MISSING)
            details["missing_sources"] = sorted(missing_sources)

        txns = list(members.values())
        if len(txns) < 2:
            if types:
                return self._build(group, types, details)
            return None

        amounts = [t.amount for t in txns]
        dates = [t.value_date for t in txns]

        # ── Amount analysis ────────────────────────────────────────────────────
        amt_range = max(amounts) - min(amounts)
        if amt_range > 0:
            max_base = max(abs(a) for a in amounts) or 1
            pct = amt_range / max_base
            if amt_range <= self.rounding_threshold:
                types.append(DiscrepancyType.ROUNDING)
                details["amount_range"] = round(amt_range, 4)
                details["amounts"] = {s: round(t.amount, 4) for s, t in members.items()}
            else:
                types.append(DiscrepancyType.AMOUNT_MISMATCH)
                details["amount_range"] = round(amt_range, 4)
                details["amount_pct_diff"] = round(pct * 100, 2)
                details["amounts"] = {s: round(t.amount, 4) for s, t in members.items()}

        # ── Timing analysis ────────────────────────────────────────────────────
        date_range = (max(dates) - min(dates)).days
        if 0 < date_range <= self.timing_window:
            types.append(DiscrepancyType.TIMING)
            details["date_range_days"] = date_range
            details["dates"] = {s: t.value_date.isoformat() for s, t in members.items()}
        elif date_range > self.timing_window:
            # Large date gap = treat as amount mismatch category but flag separately
            if DiscrepancyType.AMOUNT_MISMATCH not in types:
                types.append(DiscrepancyType.AMOUNT_MISMATCH)
            details["date_range_days"] = date_range
            details["dates"] = {s: t.value_date.isoformat() for s, t in members.items()}

        if not types:
            return None

        primary = types[0] if len(types) == 1 else DiscrepancyType.MULTI
        return self._build(group, types, details, primary)

    def _build(
        self,
        group: MatchGroup,
        types: list[DiscrepancyType],
        details: dict,
        primary: Optional[DiscrepancyType] = None,
    ) -> Discrepancy:
        if primary is None:
            primary = types[0] if types else DiscrepancyType.NONE

        severity = self._severity(types, details)
        return Discrepancy(
            group_id=group.group_id,
            types=types,
            primary_type=primary,
            details=details,
            severity=severity,
        )

    @staticmethod
    def _severity(types: list[DiscrepancyType], details: dict) -> str:
        if DiscrepancyType.AMOUNT_MISMATCH in types:
            pct = details.get("amount_pct_diff", 0)
            if pct > 10:
                return "CRITICAL"
            if pct > 2:
                return "HIGH"
            return "MEDIUM"
        if DiscrepancyType.MISSING in types:
            missing_count = len(details.get("missing_sources", []))
            return "HIGH" if missing_count >= 2 else "MEDIUM"
        if DiscrepancyType.TIMING in types:
            days = details.get("date_range_days", 0)
            return "MEDIUM" if days > 1 else "LOW"
        if DiscrepancyType.ROUNDING in types:
            return "LOW"
        return "LOW"

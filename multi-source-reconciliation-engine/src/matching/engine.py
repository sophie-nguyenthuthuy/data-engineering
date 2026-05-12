"""Multi-key fuzzy matching engine — groups transactions across sources."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

from rapidfuzz import fuzz

from ..ingestion.loader import NormalizedTransaction


@dataclass
class MatchGroup:
    group_id: str
    transactions: dict[str, NormalizedTransaction]  # source -> txn
    confidence: float
    match_scores: dict[str, float] = field(default_factory=dict)


class MatchingEngine:
    def __init__(self, config: dict):
        cfg = config["matching"]
        self.weights = cfg["weights"]
        self.fuzzy_threshold = config["reconciliation"]["fuzzy_match_threshold"]
        self.timing_window = config["reconciliation"]["timing_day_window"]
        self.rounding_threshold = config["reconciliation"]["rounding_threshold"]

    def match(
        self,
        sources: dict[str, list[NormalizedTransaction]],
    ) -> list[MatchGroup]:
        all_txns: list[NormalizedTransaction] = [t for txns in sources.values() for t in txns]
        used: set[int] = set()
        groups: list[MatchGroup] = []
        group_seq = 0

        for i, anchor in enumerate(all_txns):
            if i in used:
                continue
            used.add(i)
            group_members: dict[str, NormalizedTransaction] = {anchor.source: anchor}
            scores: dict[str, float] = {}

            for j, candidate in enumerate(all_txns):
                if j in used:
                    continue
                if candidate.source == anchor.source:
                    continue
                score = self._score_pair(anchor, candidate)
                if score >= self.fuzzy_threshold:
                    # Only one transaction per source per group (take best)
                    existing = group_members.get(candidate.source)
                    if existing is None:
                        group_members[candidate.source] = candidate
                        scores[candidate.source] = score
                        used.add(j)
                    else:
                        existing_score = scores.get(candidate.source, 0)
                        if score > existing_score:
                            # un-use the old index and replace
                            old_j = all_txns.index(existing)
                            used.discard(old_j)
                            group_members[candidate.source] = candidate
                            scores[candidate.source] = score
                            used.add(j)

            group_seq += 1
            confidence = self._group_confidence(group_members, scores, list(sources.keys()))
            groups.append(
                MatchGroup(
                    group_id=f"GRP{group_seq:05d}",
                    transactions=group_members,
                    confidence=confidence,
                    match_scores=scores,
                )
            )

        return groups

    def _score_pair(self, a: NormalizedTransaction, b: NormalizedTransaction) -> float:
        """Weighted composite score (0-100)."""
        ref_score = self._reference_score(a.reference, b.reference)
        amt_score = self._amount_score(a.amount, b.amount)
        desc_score = fuzz.token_sort_ratio(a.description, b.description)
        date_score = self._date_score(a.value_date, b.value_date)

        composite = (
            self.weights["reference"] * ref_score
            + self.weights["amount"] * amt_score
            + self.weights["description"] * desc_score
            + self.weights["date"] * date_score
        )
        return round(composite, 2)

    def _reference_score(self, ref_a: str, ref_b: str) -> float:
        if not ref_a or not ref_b:
            return 0.0
        if ref_a == ref_b:
            return 100.0
        return fuzz.token_set_ratio(ref_a, ref_b)

    def _amount_score(self, a: float, b: float) -> float:
        diff = abs(a - b)
        if diff == 0:
            return 100.0
        if diff <= self.rounding_threshold:
            return 90.0
        base = max(abs(a), abs(b), 1e-9)
        pct = diff / base
        if pct <= 0.001:
            return 85.0
        if pct <= 0.01:
            return 60.0
        if pct <= 0.05:
            return 30.0
        return 0.0

    def _date_score(self, d1, d2) -> float:
        delta = abs((d1 - d2).days)
        if delta == 0:
            return 100.0
        if delta <= self.timing_window:
            return max(0.0, 100.0 - delta * 20)
        return 0.0

    def _group_confidence(
        self,
        members: dict[str, NormalizedTransaction],
        scores: dict[str, float],
        all_sources: list[str],
    ) -> float:
        coverage = len(members) / len(all_sources)
        avg_score = (sum(scores.values()) / len(scores) / 100.0) if scores else 1.0
        return round(coverage * 0.5 + avg_score * 0.5, 4)

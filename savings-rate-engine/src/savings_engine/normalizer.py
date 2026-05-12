"""
Rate normalizer: converts raw RateEntry objects into NormalizedRate with canonical term_days.

Term mapping strategy
─────────────────────
All terms are reduced to a canonical integer number of days so cross-bank comparisons
are simple integer equality.  Canonical set: 0, 7, 14, 30, 60, 90, 120, 180, 270,
365, 540, 730, 1095.

A term_label is matched by:
  1. Explicit keyword lookup (Vietnamese + English)
  2. Regex extraction of (N, unit) then conversion
  3. Fallback: None (record is dropped with a warning)
"""
import logging
import re
from typing import Optional

from savings_engine.models.schemas import RateEntry, NormalizedRate

logger = logging.getLogger(__name__)

# Canonical term days in ascending order
CANONICAL_TERMS: list[int] = [0, 7, 14, 30, 60, 90, 120, 180, 270, 365, 540, 730, 1095]

# Keyword → days (checked before regex)
_KEYWORD_MAP: dict[str, int] = {
    "không kỳ hạn": 0,
    "khong ky han": 0,
    "demand": 0,
    "on-call": 0,
    "overnight": 0,
    "1 tuần": 7,
    "2 tuần": 14,
    "1 tháng": 30,
    "2 tháng": 60,
    "3 tháng": 90,
    "4 tháng": 120,
    "6 tháng": 180,
    "9 tháng": 270,
    "12 tháng": 365,
    "1 năm": 365,
    "18 tháng": 540,
    "24 tháng": 730,
    "2 năm": 730,
    "36 tháng": 1095,
    "3 năm": 1095,
    # English variants
    "1 month": 30, "1m": 30,
    "2 months": 60, "2m": 60,
    "3 months": 90, "3m": 90,
    "6 months": 180, "6m": 180,
    "9 months": 270, "9m": 270,
    "12 months": 365, "12m": 365,
    "1 year": 365,
    "18 months": 540, "18m": 540,
    "24 months": 730,
    "2 years": 730,
    "36 months": 1095,
    "3 years": 1095,
}

_UNIT_DAYS: dict[str, int] = {
    "ngày": 1, "day": 1, "days": 1,
    "tuần": 7, "week": 7, "weeks": 7,
    "tháng": 30, "month": 30, "months": 30, "thang": 30,
    "năm": 365, "year": 365, "years": 365, "nam": 365,
}

_TERM_REGEX = re.compile(
    r"(\d+)\s*(ngày|day|days|tuần|week|weeks|tháng|month|months|thang|năm|year|years|nam)",
    re.IGNORECASE,
)


def _snap_to_canonical(days: int) -> int:
    """Snap an inferred day count to the nearest canonical term."""
    return min(CANONICAL_TERMS, key=lambda c: abs(c - days))


def parse_term_days(label: str) -> Optional[int]:
    clean = label.strip().lower()

    # Exact keyword match
    if clean in _KEYWORD_MAP:
        return _KEYWORD_MAP[clean]

    # Partial keyword match (handles extra whitespace, punctuation)
    for kw, days in _KEYWORD_MAP.items():
        if kw in clean:
            return days

    # Regex: "N unit"
    m = _TERM_REGEX.search(clean)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        multiplier = _UNIT_DAYS.get(unit, 1)
        return _snap_to_canonical(n * multiplier)

    # Pure digit — assume months if ≤ 36, else days
    if clean.isdigit():
        n = int(clean)
        return _snap_to_canonical(n * 30 if n <= 36 else n)

    return None


def normalize(entries: list[RateEntry]) -> list[NormalizedRate]:
    """Convert a list of raw RateEntry to NormalizedRate, dropping unparseable terms."""
    results: list[NormalizedRate] = []
    for e in entries:
        term_days = parse_term_days(e.term_label)
        if term_days is None:
            logger.warning("Could not parse term '%s' for %s — skipping", e.term_label, e.bank_code)
            continue
        results.append(NormalizedRate(
            bank_code=e.bank_code,
            term_days=term_days,
            term_label=e.term_label,
            rate_pa=round(e.rate_pa, 4),
            rate_type=e.rate_type,
            min_amount_vnd=e.min_amount_vnd,
            currency=e.currency,
            scraped_at=e.scraped_at,
        ))
    return results

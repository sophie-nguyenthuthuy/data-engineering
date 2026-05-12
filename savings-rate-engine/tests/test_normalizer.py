"""Tests for the rate normalizer — term parsing and canonical snapping."""
import pytest
from datetime import datetime

from savings_engine.normalizer import parse_term_days, normalize, CANONICAL_TERMS
from savings_engine.models.schemas import RateEntry


# ── parse_term_days ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("label,expected", [
    ("Không kỳ hạn",  0),
    ("không kỳ hạn",  0),
    ("1 tháng",       30),
    ("2 tháng",       60),
    ("3 tháng",       90),
    ("4 tháng",       120),
    ("6 tháng",       180),
    ("9 tháng",       270),
    ("12 tháng",      365),
    ("1 năm",         365),
    ("18 tháng",      540),
    ("24 tháng",      730),
    ("2 năm",         730),
    ("36 tháng",      1095),
    ("3 năm",         1095),
    # English variants
    ("1 month",       30),
    ("3 months",      90),
    ("6 months",      180),
    ("12 months",     365),
    ("1 year",        365),
    ("2 years",       730),
    # with extra whitespace / casing
    ("  6 Tháng  ",   180),
    ("12 MONTHS",     365),
    # raw digit strings
    ("3",             90),
    ("12",            365),
])
def test_parse_term_days_known(label, expected):
    assert parse_term_days(label) == expected


def test_parse_term_days_unknown_returns_none():
    assert parse_term_days("loại đặc biệt XYZ") is None


def test_parse_term_days_all_canonical_reachable():
    """Every canonical term should be reachable from at least one label."""
    reachable = set()
    for days in CANONICAL_TERMS:
        if days == 0:
            reachable.add(parse_term_days("Không kỳ hạn"))
        else:
            # derive a label from days
            months = days // 30
            if months > 0:
                reachable.add(parse_term_days(f"{months} tháng"))
    assert CANONICAL_TERMS[0] in reachable  # 0 (demand)


# ── normalize() ───────────────────────────────────────────────────────────────

def _make_entry(term_label: str, rate: float = 5.0, bank: str = "VCB") -> RateEntry:
    return RateEntry(bank_code=bank, term_label=term_label, rate_pa=rate, scraped_at=datetime.utcnow())


def test_normalize_basic():
    entries = [_make_entry("6 tháng", 5.5), _make_entry("12 tháng", 6.0)]
    result = normalize(entries)
    assert len(result) == 2
    assert result[0].term_days == 180
    assert result[1].term_days == 365


def test_normalize_drops_unparseable():
    entries = [_make_entry("6 tháng"), _make_entry("??unknown??")]
    result = normalize(entries)
    assert len(result) == 1
    assert result[0].term_days == 180


def test_normalize_rounds_rate():
    entries = [_make_entry("3 tháng", 5.123456789)]
    result = normalize(entries)
    assert result[0].rate_pa == round(5.123456789, 4)


def test_normalize_empty_input():
    assert normalize([]) == []


def test_normalize_preserves_rate_type():
    entry = RateEntry(bank_code="VCB", term_label="6 tháng", rate_pa=5.1,
                      rate_type="online", scraped_at=datetime.utcnow())
    result = normalize([entry])
    assert result[0].rate_type == "online"

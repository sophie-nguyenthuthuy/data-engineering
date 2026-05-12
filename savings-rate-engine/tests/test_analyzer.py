"""Tests for trend computation and bank comparison logic."""
import pytest
from datetime import datetime, timedelta

from savings_engine.analyzer.trends import compute_trend, TrendSummary
from savings_engine.analyzer.comparisons import compare_banks
from savings_engine.models.schemas import NormalizedRate
from savings_engine.storage.repository import RateRepository


# ── Helpers ───────────────────────────────────────────────────────────────────

def _history(rates_with_offsets: list[tuple[int, float]]) -> list[tuple[datetime, float]]:
    """Build a history list from (days_ago, rate_pa) pairs."""
    now = datetime.utcnow()
    return [(now - timedelta(days=d), r) for d, r in rates_with_offsets]


def _seed_rates(repo: RateRepository, bank_code: str, term_days: int, rate: float) -> None:
    rates = [NormalizedRate(
        bank_code=bank_code,
        term_days=term_days,
        term_label=f"{term_days}d",
        rate_pa=rate,
        rate_type="standard",
    )]
    repo.save_snapshot(bank_code, rates)


# ── compute_trend ─────────────────────────────────────────────────────────────

def test_compute_trend_none_on_empty():
    assert compute_trend([], "VCB", 365) is None


def test_compute_trend_single_point():
    h = _history([(0, 5.5)])
    summary = compute_trend(h, "VCB", 365)
    assert summary is not None
    assert summary.current_rate == 5.5
    assert summary.change_7d is None   # only 1 point
    assert summary.direction == "stable"


def test_compute_trend_rising():
    h = _history([(60, 5.0), (30, 5.2), (0, 5.5)])
    summary = compute_trend(h, "VCB", 365)
    assert summary.direction == "up"
    assert summary.change_30d == pytest.approx(0.3, abs=0.01)


def test_compute_trend_falling():
    h = _history([(60, 6.0), (30, 5.8), (0, 5.5)])
    summary = compute_trend(h, "VCB", 365)
    assert summary.direction == "down"
    assert summary.change_30d == pytest.approx(-0.3, abs=0.01)


def test_compute_trend_stable():
    h = _history([(30, 5.5), (15, 5.5), (0, 5.5)])
    summary = compute_trend(h, "VCB", 365)
    assert summary.direction == "stable"
    assert summary.change_30d == pytest.approx(0.0)


def test_compute_trend_min_max_avg():
    h = _history([(40, 5.0), (20, 6.0), (0, 5.5)])
    summary = compute_trend(h, "VCB", 365)
    assert summary.min_rate == 5.0
    assert summary.max_rate == 6.0
    assert summary.avg_rate == pytest.approx(5.5, abs=0.01)


def test_compute_trend_delta_from_prev():
    h = _history([(10, 5.0), (5, 5.3), (0, 5.1)])
    summary = compute_trend(h, "VCB", 365)
    deltas = [p.delta_from_prev for p in summary.points]
    assert deltas[0] is None
    assert deltas[1] == pytest.approx(0.3, abs=0.001)
    assert deltas[2] == pytest.approx(-0.2, abs=0.001)


# ── compare_banks ─────────────────────────────────────────────────────────────

def test_compare_banks_ordered_by_rate(repo):
    _seed_rates(repo, "VCB",  180, 5.0)
    _seed_rates(repo, "BIDV", 180, 5.3)
    _seed_rates(repo, "TCB",  180, 5.8)

    comparisons = compare_banks(repo, term_days=180, top_n=10)
    assert len(comparisons) >= 3
    rates = [c.rate_pa for c in comparisons]
    assert rates == sorted(rates, reverse=True), "Results must be sorted highest-first"


def test_compare_banks_rank_starts_at_1(repo):
    _seed_rates(repo, "MBB", 90, 5.1)
    comparisons = compare_banks(repo, term_days=90, top_n=5)
    if comparisons:
        assert comparisons[0].rank == 1


def test_compare_banks_respects_top_n(repo):
    for code in ("VCB", "BIDV", "TCB", "MBB", "ACB", "VPB"):
        _seed_rates(repo, code, 365, 5.0 + hash(code) % 10 * 0.1)

    comparisons = compare_banks(repo, term_days=365, top_n=3)
    assert len(comparisons) <= 3


def test_compare_banks_empty_term_returns_empty(repo):
    comparisons = compare_banks(repo, term_days=9999)
    assert comparisons == []

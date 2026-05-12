"""Tests for the storage repository layer."""
import pytest
from datetime import datetime, timedelta

from savings_engine.models.schemas import NormalizedRate
from savings_engine.storage.repository import RateRepository


def _rate(bank_code: str, term_days: int, rate_pa: float, rate_type: str = "standard") -> NormalizedRate:
    return NormalizedRate(
        bank_code=bank_code,
        term_days=term_days,
        term_label=f"{term_days}d",
        rate_pa=rate_pa,
        rate_type=rate_type,
    )


def test_save_and_retrieve_snapshot(repo):
    rates = [_rate("VCB", 180, 5.0), _rate("VCB", 365, 5.6)]
    snapshot = repo.save_snapshot("VCB", rates)

    assert snapshot.id is not None
    assert snapshot.bank_code == "VCB"
    assert snapshot.scrape_success is True
    assert len(snapshot.records) == 2


def test_save_error_snapshot(repo):
    snapshot = repo.save_snapshot("BIDV", [], error="Connection timeout")
    assert snapshot.scrape_success is False
    assert snapshot.error_message == "Connection timeout"
    assert snapshot.records == []


def test_get_latest_rates_returns_most_recent(repo):
    # Save two snapshots for the same bank
    repo.save_snapshot("TCB", [_rate("TCB", 180, 5.0)])
    repo.save_snapshot("TCB", [_rate("TCB", 180, 5.5)])  # newer

    records = repo.get_latest_rates("TCB")
    assert len(records) == 1
    assert records[0].rate_pa == 5.5  # should be the newer snapshot


def test_get_latest_rates_all_banks(repo):
    repo.save_snapshot("MBB", [_rate("MBB", 90, 5.1)])
    repo.save_snapshot("ACB", [_rate("ACB", 90, 5.3)])

    records = repo.get_latest_rates()
    bank_codes = {r.bank_code for r in records}
    assert "MBB" in bank_codes
    assert "ACB" in bank_codes


def test_get_best_rates_sorted(repo):
    repo.save_snapshot("VCB",  [_rate("VCB",  365, 5.6)])
    repo.save_snapshot("BIDV", [_rate("BIDV", 365, 5.8)])
    repo.save_snapshot("VPB",  [_rate("VPB",  365, 6.5)])

    best = repo.get_best_rates(term_days=365, top_n=10)
    rates = [r.rate_pa for r in best]
    assert rates == sorted(rates, reverse=True)
    assert rates[0] == 6.5


def test_get_rate_history(repo):
    now = datetime.utcnow()
    repo.save_snapshot("CTG", [_rate("CTG", 180, 5.0)])
    repo.save_snapshot("CTG", [_rate("CTG", 180, 5.2)])

    history = repo.get_rate_history("CTG", 180, since=now - timedelta(minutes=5))
    assert len(history) == 2
    dates, rates = zip(*history)
    assert list(rates) == sorted(rates)  # ascending by time


def test_get_available_terms(repo):
    repo.save_snapshot("VCB", [_rate("VCB", 30, 4.7), _rate("VCB", 90, 4.8)])
    terms = repo.get_available_terms()
    assert 30 in terms
    assert 90 in terms
    assert terms == sorted(terms)


def test_get_snapshot_count(repo):
    before = repo.get_snapshot_count("VCB")
    repo.save_snapshot("VCB", [_rate("VCB", 365, 5.6)])
    repo.save_snapshot("VCB", [_rate("VCB", 365, 5.7)])
    after = repo.get_snapshot_count("VCB")
    assert after == before + 2


def test_get_bank_returns_none_for_unknown(repo):
    assert repo.get_bank("UNKNOWN_BANK_XYZ") is None


def test_error_snapshots_excluded_from_latest(repo):
    repo.save_snapshot("VPB", [_rate("VPB", 180, 5.8)])
    repo.save_snapshot("VPB", [], error="Scrape failed")  # error — should be ignored

    records = repo.get_latest_rates("VPB")
    # Should still return the last *successful* snapshot
    vpb_180 = [r for r in records if r.term_days == 180]
    assert any(r.rate_pa == 5.8 for r in vpb_180)

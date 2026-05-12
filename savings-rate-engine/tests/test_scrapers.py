"""Tests for all scraper mock_rates() — verifies each scraper produces valid output."""
import pytest
from datetime import datetime

from savings_engine.scrapers.registry import SCRAPER_REGISTRY
from savings_engine.models.schemas import RateEntry
from savings_engine.normalizer import normalize


@pytest.fixture(autouse=True, scope="module")
def force_mock(monkeypatch_session):
    """Ensure USE_MOCK_DATA is true for all scraper tests."""
    import savings_engine.config as cfg
    monkeypatch_session.setattr(cfg.settings, "use_mock_data", True)


@pytest.fixture(scope="module")
def monkeypatch_session():
    """Module-scoped monkeypatch."""
    import _pytest.monkeypatch
    mp = _pytest.monkeypatch.MonkeyPatch()
    yield mp
    mp.undo()


@pytest.mark.parametrize("bank_code", list(SCRAPER_REGISTRY.keys()))
def test_mock_rates_returns_entries(bank_code):
    scraper = SCRAPER_REGISTRY[bank_code]()
    entries = scraper._mock_rates()
    assert isinstance(entries, list), f"{bank_code}: _mock_rates() must return a list"
    assert len(entries) > 0, f"{bank_code}: mock data is empty"


@pytest.mark.parametrize("bank_code", list(SCRAPER_REGISTRY.keys()))
def test_mock_rates_valid_fields(bank_code):
    scraper = SCRAPER_REGISTRY[bank_code]()
    for entry in scraper._mock_rates():
        assert isinstance(entry, RateEntry)
        assert entry.bank_code == bank_code
        assert isinstance(entry.rate_pa, float)
        assert entry.rate_pa >= 0
        assert entry.term_label and isinstance(entry.term_label, str)
        assert entry.rate_type in ("standard", "online", "promotional")
        assert isinstance(entry.scraped_at, datetime)


@pytest.mark.parametrize("bank_code", list(SCRAPER_REGISTRY.keys()))
def test_mock_rates_normalizable(bank_code):
    """Every mock rate should survive normalization (no dropped rows)."""
    scraper = SCRAPER_REGISTRY[bank_code]()
    raw = scraper._mock_rates()
    normalized = normalize(raw)
    assert len(normalized) == len(raw), (
        f"{bank_code}: {len(raw) - len(normalized)} mock entries failed normalization. "
        f"Bad labels: {[e.term_label for e in raw if e.term_label not in [n.term_label for n in normalized]]}"
    )


@pytest.mark.parametrize("bank_code", list(SCRAPER_REGISTRY.keys()))
def test_scrape_uses_mock_when_flag_set(bank_code, monkeypatch):
    import savings_engine.config as cfg
    monkeypatch.setattr(cfg.settings, "use_mock_data", True)
    scraper = SCRAPER_REGISTRY[bank_code]()
    entries = scraper.scrape()
    assert len(entries) > 0


def test_registry_has_expected_banks():
    expected = {"VCB", "BIDV", "CTG", "TCB", "MBB", "ACB", "VPB"}
    assert expected.issubset(set(SCRAPER_REGISTRY.keys()))

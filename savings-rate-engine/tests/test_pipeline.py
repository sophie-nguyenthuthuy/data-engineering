"""Integration tests for the pipeline orchestrator."""
import os
import pytest
from unittest.mock import patch, MagicMock

os.environ["USE_MOCK_DATA"] = "true"
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from savings_engine.pipeline import PipelineRun, BankResult, _run_bank
from savings_engine.scrapers.registry import SCRAPER_REGISTRY


# ── Helpers ───────────────────────────────────────────────────────────────────

def _noop_persist(*args, **kwargs):
    """Replace DB writes so pipeline tests stay in-memory."""


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_pipeline_run_all_banks(monkeypatch):
    """Full pipeline run with mock data should succeed for all registered banks."""
    monkeypatch.setattr("savings_engine.pipeline._persist_rates", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline._persist_error", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline.init_db", lambda: None)

    import savings_engine.config as cfg
    monkeypatch.setattr(cfg.settings, "use_mock_data", True)

    from savings_engine.pipeline import run_pipeline
    run = run_pipeline()

    assert run.total_banks == len(SCRAPER_REGISTRY)
    assert run.successful_banks == run.total_banks
    assert run.total_rates > 0
    assert run.finished_at is not None


def test_pipeline_run_subset(monkeypatch):
    """Pipeline should only run scrapers for the requested bank codes."""
    monkeypatch.setattr("savings_engine.pipeline._persist_rates", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline._persist_error", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline.init_db", lambda: None)

    import savings_engine.config as cfg
    monkeypatch.setattr(cfg.settings, "use_mock_data", True)

    from savings_engine.pipeline import run_pipeline
    run = run_pipeline(bank_codes=["VCB", "BIDV"])

    assert run.total_banks == 2
    assert {r.bank_code for r in run.results} == {"VCB", "BIDV"}
    assert run.successful_banks == 2


def test_pipeline_bank_result_fields(monkeypatch):
    """BankResult fields are populated correctly on success."""
    monkeypatch.setattr("savings_engine.pipeline._persist_rates", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline._persist_error", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline.init_db", lambda: None)

    import savings_engine.config as cfg
    monkeypatch.setattr(cfg.settings, "use_mock_data", True)

    from savings_engine.pipeline import run_pipeline
    run = run_pipeline(bank_codes=["VCB"])

    result = run.results[0]
    assert result.bank_code == "VCB"
    assert result.success is True
    assert result.rates_saved > 0
    assert result.duration_s >= 0
    assert result.error is None


def test_pipeline_records_failure_on_scraper_error(monkeypatch):
    """A ScraperError should produce a failed BankResult, not raise."""
    monkeypatch.setattr("savings_engine.pipeline._persist_rates", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline._persist_error", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline.init_db", lambda: None)

    from savings_engine.scrapers.base import ScraperError

    broken_scraper = MagicMock()
    broken_scraper.scrape.side_effect = ScraperError("timeout")

    with patch("savings_engine.scrapers.get_scraper", return_value=broken_scraper), \
         patch("savings_engine.scrapers.registry.get_scraper", return_value=broken_scraper):
        # _run_bank does a local `from savings_engine.scrapers import get_scraper`
        import savings_engine.scrapers as _sc_mod
        original = _sc_mod.get_scraper
        _sc_mod.get_scraper = lambda code: broken_scraper
        try:
            result = _run_bank("VCB")
        finally:
            _sc_mod.get_scraper = original

    assert result.success is False
    assert result.rates_saved == 0
    assert "timeout" in (result.error or "")


def test_pipeline_duration_is_positive(monkeypatch):
    monkeypatch.setattr("savings_engine.pipeline._persist_rates", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline._persist_error", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline.init_db", lambda: None)

    import savings_engine.config as cfg
    monkeypatch.setattr(cfg.settings, "use_mock_data", True)

    from savings_engine.pipeline import run_pipeline
    run = run_pipeline(bank_codes=["TCB"])
    assert run.duration_s > 0


def test_pipeline_run_summary_totals(monkeypatch):
    """PipelineRun aggregate properties are consistent with results list."""
    monkeypatch.setattr("savings_engine.pipeline._persist_rates", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline._persist_error", _noop_persist)
    monkeypatch.setattr("savings_engine.pipeline.init_db", lambda: None)

    import savings_engine.config as cfg
    monkeypatch.setattr(cfg.settings, "use_mock_data", True)

    from savings_engine.pipeline import run_pipeline
    run = run_pipeline()

    assert run.total_banks == len(run.results)
    assert run.total_rates == sum(r.rates_saved for r in run.results)
    assert run.successful_banks == sum(1 for r in run.results if r.success)

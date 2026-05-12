"""
Pipeline orchestrator.

Runs every registered scraper, normalizes results, persists snapshots, and
logs a structured run-summary.  Designed to be called by the scheduler or CLI.
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from savings_engine.models.schemas import NormalizedRate
from savings_engine.normalizer import normalize
from savings_engine.scrapers import SCRAPER_REGISTRY, ScraperError
from savings_engine.storage.database import SessionLocal, init_db
from savings_engine.storage.repository import RateRepository

logger = logging.getLogger(__name__)


@dataclass
class BankResult:
    bank_code: str
    success: bool
    rates_saved: int
    duration_s: float
    error: Optional[str] = None


@dataclass
class PipelineRun:
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    results: list[BankResult] = field(default_factory=list)

    @property
    def total_banks(self) -> int:
        return len(self.results)

    @property
    def successful_banks(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def total_rates(self) -> int:
        return sum(r.rates_saved for r in self.results)

    @property
    def duration_s(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0


def run_pipeline(bank_codes: Optional[list[str]] = None) -> PipelineRun:
    """
    Execute the full scrape → normalize → persist pipeline.

    Args:
        bank_codes: If provided, only scrape these banks.  Defaults to all registered.
    """
    init_db()
    run = PipelineRun()
    codes = bank_codes or list(SCRAPER_REGISTRY.keys())

    logger.info("Pipeline starting — %d bank(s) queued", len(codes))

    for code in codes:
        result = _run_bank(code)
        run.results.append(result)

    run.finished_at = datetime.utcnow()
    _log_summary(run)
    return run


def _run_bank(bank_code: str) -> BankResult:
    from savings_engine.scrapers import get_scraper

    t0 = time.perf_counter()
    scraper = get_scraper(bank_code)

    try:
        raw = scraper.scrape()
    except ScraperError as exc:
        elapsed = time.perf_counter() - t0
        logger.error("%-6s  FAILED  %.2fs  %s", bank_code, elapsed, exc)
        _persist_error(bank_code, str(exc))
        return BankResult(bank_code=bank_code, success=False, rates_saved=0,
                          duration_s=elapsed, error=str(exc))

    normalized: list[NormalizedRate] = normalize(raw)
    elapsed = time.perf_counter() - t0

    if not normalized:
        msg = "Normalizer produced 0 rates"
        logger.warning("%-6s  EMPTY   %.2fs", bank_code, elapsed)
        _persist_error(bank_code, msg)
        return BankResult(bank_code=bank_code, success=False, rates_saved=0,
                          duration_s=elapsed, error=msg)

    _persist_rates(bank_code, normalized)
    logger.info("%-6s  OK      %.2fs  %d rates", bank_code, elapsed, len(normalized))
    return BankResult(bank_code=bank_code, success=True,
                      rates_saved=len(normalized), duration_s=elapsed)


def _persist_rates(bank_code: str, rates: list[NormalizedRate]) -> None:
    with SessionLocal() as db:
        repo = RateRepository(db)
        repo.save_snapshot(bank_code, rates)


def _persist_error(bank_code: str, error: str) -> None:
    with SessionLocal() as db:
        repo = RateRepository(db)
        repo.save_snapshot(bank_code, [], error=error)


def _log_summary(run: PipelineRun) -> None:
    logger.info(
        "Pipeline done — %d/%d banks OK, %d rates saved, %.1fs total",
        run.successful_banks, run.total_banks, run.total_rates, run.duration_s,
    )
    for r in run.results:
        status = "✓" if r.success else "✗"
        logger.info("  %s %-6s  %d rates  %.2fs%s",
                    status, r.bank_code, r.rates_saved, r.duration_s,
                    f"  [{r.error}]" if r.error else "")

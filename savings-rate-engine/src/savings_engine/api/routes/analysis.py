from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from savings_engine.analyzer.trends import compute_trend, TrendSummary
from savings_engine.analyzer.comparisons import compare_banks, best_rates_table
from savings_engine.storage.database import get_db
from savings_engine.storage.repository import RateRepository

router = APIRouter()


# ── Pydantic response models ───────────────────────────────────────────────────

class TrendPointOut(BaseModel):
    scraped_at: datetime
    rate_pa: float
    delta_from_prev: Optional[float]


class TrendOut(BaseModel):
    bank_code: str
    term_days: int
    rate_type: str
    current_rate: float
    change_7d: Optional[float]
    change_30d: Optional[float]
    change_90d: Optional[float]
    min_rate: float
    max_rate: float
    avg_rate: float
    direction: str
    points: list[TrendPointOut]


class ComparisonOut(BaseModel):
    term_days: int
    bank_code: str
    bank_name_vi: str
    rate_pa: float
    rate_type: str
    rank: int
    scraped_at: Optional[datetime]


class BestRatesTableOut(BaseModel):
    term_days: int
    rankings: list[ComparisonOut]


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("/trends/{bank_code}", response_model=TrendOut)
def rate_trend(
    bank_code: str,
    term_days: int = Query(..., description="Canonical term in days, e.g. 365"),
    rate_type: str = Query("standard"),
    days_back: int = Query(90, ge=7, le=365, description="How many days of history to include"),
    db: Session = Depends(get_db),
):
    """
    Time-series trend for a specific bank + term.
    Returns change deltas (7d / 30d / 90d) and directional signal.
    """
    repo = RateRepository(db)
    if not repo.get_bank(bank_code.upper()):
        raise HTTPException(404, f"Bank '{bank_code}' not found")

    since = datetime.utcnow() - timedelta(days=days_back)
    history = repo.get_rate_history(bank_code.upper(), term_days, rate_type, since=since)

    summary: Optional[TrendSummary] = compute_trend(history, bank_code.upper(), term_days, rate_type)
    if summary is None:
        raise HTTPException(404, f"No history found for {bank_code} / {term_days}d / {rate_type}")

    return TrendOut(
        bank_code=summary.bank_code,
        term_days=summary.term_days,
        rate_type=summary.rate_type,
        current_rate=summary.current_rate,
        change_7d=summary.change_7d,
        change_30d=summary.change_30d,
        change_90d=summary.change_90d,
        min_rate=summary.min_rate,
        max_rate=summary.max_rate,
        avg_rate=summary.avg_rate,
        direction=summary.direction,
        points=[
            TrendPointOut(
                scraped_at=p.scraped_at,
                rate_pa=p.rate_pa,
                delta_from_prev=p.delta_from_prev,
            )
            for p in summary.points
        ],
    )


@router.get("/compare", response_model=list[ComparisonOut])
def compare(
    term_days: int = Query(..., description="Canonical term in days"),
    rate_type: str = Query("standard"),
    top_n: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Rank all banks by rate for a given term."""
    repo = RateRepository(db)
    comparisons = compare_banks(repo, term_days, rate_type=rate_type, top_n=top_n)
    return [
        ComparisonOut(
            term_days=c.term_days,
            bank_code=c.bank_code,
            bank_name_vi=c.bank_name_vi,
            rate_pa=c.rate_pa,
            rate_type=c.rate_type,
            rank=c.rank,
            scraped_at=c.scraped_at,
        )
        for c in comparisons
    ]


@router.get("/best-table", response_model=list[BestRatesTableOut])
def best_table(
    terms: str = Query(
        "30,90,180,365,730",
        description="Comma-separated list of term_days, e.g. 30,90,180,365",
    ),
    db: Session = Depends(get_db),
):
    """Best rates across multiple terms in one call — ideal for a dashboard overview."""
    try:
        term_list = [int(t.strip()) for t in terms.split(",")]
    except ValueError:
        raise HTTPException(422, "terms must be comma-separated integers")

    repo = RateRepository(db)
    table = best_rates_table(repo, term_list)

    return [
        BestRatesTableOut(
            term_days=term,
            rankings=[
                ComparisonOut(
                    term_days=c.term_days,
                    bank_code=c.bank_code,
                    bank_name_vi=c.bank_name_vi,
                    rate_pa=c.rate_pa,
                    rate_type=c.rate_type,
                    rank=c.rank,
                    scraped_at=c.scraped_at,
                )
                for c in rankings
            ],
        )
        for term, rankings in table.items()
    ]

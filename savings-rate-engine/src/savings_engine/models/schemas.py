from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class RateEntry:
    """Raw rate as returned by a scraper — before normalization."""
    bank_code: str
    term_label: str        # raw string from bank, e.g. "3 tháng", "6M", "180 ngày"
    rate_pa: float         # % per annum
    rate_type: str = "standard"
    min_amount_vnd: Optional[int] = None
    currency: str = "VND"
    scraped_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class NormalizedRate:
    """Rate after normalization — term_days is canonical."""
    bank_code: str
    term_days: int
    term_label: str
    rate_pa: float
    rate_type: str = "standard"
    min_amount_vnd: Optional[int] = None
    currency: str = "VND"
    scraped_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TrendPoint:
    """A single point in a rate's time series."""
    scraped_at: datetime
    rate_pa: float
    delta_from_prev: Optional[float] = None  # pp change from previous point


@dataclass
class BankComparison:
    """Best-rate comparison across banks for a given term."""
    term_days: int
    bank_code: str
    bank_name_vi: str
    rate_pa: float
    rate_type: str
    rank: int
    scraped_at: datetime

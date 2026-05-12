from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from savings_engine.models.schemas import TrendPoint


@dataclass
class TrendSummary:
    bank_code: str
    term_days: int
    rate_type: str
    current_rate: float
    points: list[TrendPoint]
    change_7d: Optional[float]   # pp change over last 7 days
    change_30d: Optional[float]  # pp change over last 30 days
    change_90d: Optional[float]  # pp change over last 90 days
    min_rate: float
    max_rate: float
    avg_rate: float
    direction: str               # "up" | "down" | "stable"


def compute_trend(
    history: list[tuple[datetime, float]],
    bank_code: str,
    term_days: int,
    rate_type: str = "standard",
) -> Optional[TrendSummary]:
    if not history:
        return None

    sorted_h = sorted(history, key=lambda x: x[0])
    points: list[TrendPoint] = []
    for i, (ts, rate) in enumerate(sorted_h):
        delta = None
        if i > 0:
            delta = round(rate - sorted_h[i - 1][1], 4)
        points.append(TrendPoint(scraped_at=ts, rate_pa=rate, delta_from_prev=delta))

    rates = [p.rate_pa for p in points]
    current = rates[-1]
    now = sorted_h[-1][0]

    def _change_over_days(days: int) -> Optional[float]:
        from datetime import timedelta
        cutoff = now - timedelta(days=days)
        older = [(ts, r) for ts, r in sorted_h if ts <= cutoff]
        if not older:
            return None
        return round(current - older[-1][1], 4)

    change_7d = _change_over_days(7)
    change_30d = _change_over_days(30)
    change_90d = _change_over_days(90)

    # Direction based on last 30-day change
    ref = change_30d if change_30d is not None else change_7d
    if ref is None or abs(ref) < 0.01:
        direction = "stable"
    elif ref > 0:
        direction = "up"
    else:
        direction = "down"

    return TrendSummary(
        bank_code=bank_code,
        term_days=term_days,
        rate_type=rate_type,
        current_rate=current,
        points=points,
        change_7d=change_7d,
        change_30d=change_30d,
        change_90d=change_90d,
        min_rate=min(rates),
        max_rate=max(rates),
        avg_rate=round(sum(rates) / len(rates), 4),
        direction=direction,
    )

"""Fetch official US CPI data from BLS for comparison against crawled prices.

Series IDs: https://www.bls.gov/cpi/tables/relative-importance/home.htm
Default CUUR0000SA0 = All Items, US city average, not seasonally adjusted.
"""

from __future__ import annotations

import httpx

from .config import settings
from .logging import get_logger

log = get_logger(__name__)


BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def fetch_cpi_series(
    series_id: str = "CUUR0000SA0",
    start_year: int = 2018,
    end_year: int = 2024,
) -> list[tuple[str, float]]:
    payload: dict[str, object] = {
        "seriesid": [series_id],
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if settings.bls_api_key:
        payload["registrationkey"] = settings.bls_api_key

    resp = httpx.post(BLS_URL, json=payload, timeout=30.0)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API error: {body.get('message')}")

    series = body["Results"]["series"][0]["data"]
    out: list[tuple[str, float]] = []
    for row in series:
        period = row["period"]  # M01..M12
        if not period.startswith("M"):
            continue
        month = int(period[1:])
        if month > 12:
            continue
        out.append((f"{row['year']}-{month:02d}", float(row["value"])))
    log.info("cpi.fetched", series_id=series_id, points=len(out))
    return out

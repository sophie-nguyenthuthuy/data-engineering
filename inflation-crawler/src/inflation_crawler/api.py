"""FastAPI server serving inflation stats + a static dashboard."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from . import analyze
from .store import connect

app = FastAPI(title="Inflation Crawler", version="0.1.0")

STATIC = Path(__file__).parent / "dashboard" / "static"
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/categories")
def categories() -> list[str]:
    con = connect()
    rows = con.execute(
        "SELECT DISTINCT category FROM products WHERE category IS NOT NULL ORDER BY 1"
    ).fetchall()
    return [r[0] for r in rows]


@app.get("/api/inflation")
def inflation(
    category: str | None = Query(None),
    year: int | None = Query(None),
) -> dict:
    ts = analyze.inflation_timeseries(category)
    if ts.is_empty():
        return {"category": category, "series": [], "annualized_pct": None}
    data = ts.to_dicts()
    rate = analyze.annualized_inflation(category, year)
    return {"category": category, "series": data, "annualized_pct": rate}


@app.get("/api/cpi")
def cpi(series_id: str = "CUUR0000SA0") -> list[dict]:
    con = connect()
    rows = con.execute(
        "SELECT period, value FROM cpi WHERE series_id = ? ORDER BY period",
        [series_id],
    ).fetchall()
    if not rows:
        raise HTTPException(404, f"no CPI data for {series_id} — run `ic cpi` first")
    # Convert to month-over-month % for apples-to-apples with crawled inflation.
    out = []
    prev: float | None = None
    for period, value in rows:
        pct = None if prev is None else (value / prev - 1) * 100
        out.append({"period": period, "value": value, "monthly_pct": pct})
        prev = value
    return out


@app.get("/api/products")
def products(category: str | None = None, limit: int = 50) -> list[dict]:
    con = connect()
    where = "WHERE category = ?" if category else ""
    params = [category, limit] if category else [limit]
    rows = con.execute(
        f"""
        SELECT product_id, title, brand, category, currency, price, fetch_time, source
        FROM products
        {where}
        ORDER BY fetch_time DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    cols = ["product_id", "title", "brand", "category", "currency",
            "price", "fetch_time", "source"]
    return [dict(zip(cols, r, strict=True)) for r in rows]

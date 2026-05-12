from datetime import datetime

import pytest

from inflation_crawler import analyze
from inflation_crawler.config import settings
from inflation_crawler.extract import Product
from inflation_crawler.store import connect, upsert_products


@pytest.fixture(autouse=True)
def _tmp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "data_dir", tmp_path)
    monkeypatch.setattr(settings, "db_path", tmp_path / "test.duckdb")
    yield


def _p(pid: str, month: int, price: float, category: str = "laptops") -> Product:
    return Product(
        product_id=pid,
        url=f"https://x/{pid}",
        title=pid,
        brand=None,
        price=price,
        currency="USD",
        category=category,
        fetch_time=datetime(2024, month, 15),
        source="jsonld",
    )


def test_monthly_inflation_uptrend():
    # Two products, both rising ~5%/month for three months.
    upsert_products([
        _p("a", 1, 100.0), _p("a", 2, 105.0), _p("a", 3, 110.25),
        _p("b", 1, 200.0), _p("b", 2, 210.0), _p("b", 3, 220.50),
    ])
    ts = analyze.inflation_timeseries("laptops").to_dicts()
    # Feb and Mar should show ~+5% MoM; Jan has no prior month.
    feb = next(r for r in ts if r["period"] == "2024-02")
    mar = next(r for r in ts if r["period"] == "2024-03")
    assert feb["monthly_inflation_pct"] == pytest.approx(5.0, abs=0.2)
    assert mar["monthly_inflation_pct"] == pytest.approx(5.0, abs=0.2)


def test_annualized_inflation():
    # 1% per month, 12 observations -> 11 month-over-month returns -> 1.01**11 - 1.
    upsert_products([_p("a", m, 100 * (1.01 ** (m - 1))) for m in range(1, 13)])
    rate = analyze.annualized_inflation("laptops", year=2024)
    assert rate is not None
    assert rate == pytest.approx((1.01**11 - 1) * 100, abs=0.1)


def test_empty_category_returns_empty():
    assert analyze.inflation_timeseries("nonexistent").is_empty()
    assert analyze.annualized_inflation("nonexistent") is None

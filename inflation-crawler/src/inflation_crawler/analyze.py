"""Aggregate per-product price time series into inflation metrics.

Methodology:
  - Per product: median price per month (robust to one-day discounts).
  - Per category: unweighted mean of per-product month-over-month log returns.
    Log returns aggregate cleanly and reduce skew from long-tail items.
  - Annualize by summing 12 consecutive monthly log returns and exp()-ing back.

Compared to the original project's arithmetic-mean approach, this is less
sensitive to stockouts/new-item introductions that create phantom price spikes.
"""

from __future__ import annotations

import math

import polars as pl

from .store import connect


def _products_df(category: str | None = None) -> pl.DataFrame:
    con = connect()
    where = "WHERE category = ?" if category else ""
    params = [category] if category else []
    return con.execute(
        f"""
        SELECT product_id, title, brand, category, currency, price, fetch_time
        FROM products
        {where}
        """,
        params,
    ).pl()


def monthly_median_prices(category: str | None = None) -> pl.DataFrame:
    df = _products_df(category)
    if df.is_empty():
        return df
    return (
        df.with_columns(
            pl.col("fetch_time").dt.strftime("%Y-%m").alias("period"),
        )
        .group_by(["product_id", "period", "category", "currency"])
        .agg(pl.col("price").median().alias("median_price"))
        .sort(["product_id", "period"])
    )


def inflation_timeseries(category: str | None = None) -> pl.DataFrame:
    """Return a DataFrame with (period, n_products, monthly_inflation_pct)."""
    monthly = monthly_median_prices(category)
    if monthly.is_empty():
        return monthly

    # Per-product log return vs. prior observed month.
    monthly = monthly.with_columns(
        pl.col("median_price").log().over("product_id").alias("log_price"),
    )
    monthly = monthly.with_columns(
        (pl.col("log_price") - pl.col("log_price").shift(1).over("product_id")).alias("log_return"),
    )

    agg = (
        monthly.drop_nulls("log_return")
        .group_by("period")
        .agg(
            pl.col("log_return").mean().alias("avg_log_return"),
            pl.col("product_id").n_unique().alias("n_products"),
        )
        .sort("period")
    )
    agg = agg.with_columns(
        ((pl.col("avg_log_return").exp() - 1) * 100).alias("monthly_inflation_pct"),
    )
    return agg.select(["period", "n_products", "monthly_inflation_pct"])


def annualized_inflation(category: str | None = None, year: int | None = None) -> float | None:
    ts = inflation_timeseries(category)
    if ts.is_empty():
        return None
    if year is not None:
        ts = ts.filter(pl.col("period").str.starts_with(str(year)))
    if ts.is_empty():
        return None
    log_sum = sum((math.log1p(r / 100) for r in ts["monthly_inflation_pct"]))
    return (math.exp(log_sum) - 1) * 100

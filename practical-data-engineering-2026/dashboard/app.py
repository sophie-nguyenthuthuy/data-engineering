"""Streamlit dashboard reading Iceberg tables via DuckDB."""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from re_pipeline.iceberg_utils import get_catalog
from re_pipeline.config import NAMESPACE

st.set_page_config(page_title="Real Estate Lakehouse", layout="wide")
st.title("🏠 Swiss Real Estate — Lakehouse Dashboard")
st.caption(
    "Iceberg on MinIO • ingested with dlt • transformed with DuckDB • "
    "orchestrated by Dagster"
)


@st.cache_data(ttl=60)
def load(table: str) -> pd.DataFrame:
    cat = get_catalog()
    return cat.load_table(f"{NAMESPACE}.{table}").scan().to_pandas()


try:
    silver = load("silver_listings")
    city_stats = load("gold_city_stats")
    bands = load("gold_price_bands")
except Exception as e:
    st.warning(
        f"Tables not found yet. Run the Dagster `daily_refresh` job first.\n\n`{e}`"
    )
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Listings", f"{len(silver):,}")
col2.metric("Median price (buy)",
            f"CHF {silver[silver.listing_type=='buy'].price.median():,.0f}")
col3.metric("Median CHF/m²", f"{silver.price_per_m2.median():,.0f}")

st.subheader("City leaderboard")
st.dataframe(city_stats, use_container_width=True)

st.subheader("Price-band distribution (buy only)")
chart = (
    alt.Chart(bands)
    .mark_bar()
    .encode(
        x=alt.X("price_band:N", sort=["<500k", "500k-1M", "1M-2M", "2M-5M", "5M+"]),
        y="n:Q",
        color="city:N",
        column="city:N",
    )
    .properties(height=200)
)
st.altair_chart(chart, use_container_width=False)

st.subheader("Filter listings")
city = st.selectbox("City", ["(all)"] + sorted(silver.city.unique()))
ltype = st.selectbox("Listing type", ["(all)", "buy", "rent"])
ptype = st.multiselect("Property type", sorted(silver.property_type.unique()))

f = silver.copy()
if city != "(all)":
    f = f[f.city == city]
if ltype != "(all)":
    f = f[f.listing_type == ltype]
if ptype:
    f = f[f.property_type.isin(ptype)]

st.dataframe(
    f[["id", "city", "property_type", "rooms", "size_m2",
       "price", "price_per_m2", "listing_type", "scraped_at"]]
    .sort_values("price", ascending=False),
    use_container_width=True,
)

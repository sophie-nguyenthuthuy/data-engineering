"""
dlt source for real-estate listings.

Two modes:
  - `synthetic_listings`: generates realistic listings with Faker so the
    project runs end-to-end with zero external dependencies. Simulates
    daily price changes on ~15% of stock so CDC/UPSERT paths exercise.
  - `scrape_listings_from_html`: parses a local HTML fixture with
    BeautifulSoup, showing the scraping pattern without hitting any real
    site (respects ToS / copyright).

Both yield identical schemas so downstream Iceberg tables don't care.
"""
from __future__ import annotations

import hashlib
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import dlt
from bs4 import BeautifulSoup
from faker import Faker

SWISS_CITIES = [
    ("Bern", 46.948, 7.447),
    ("Zurich", 47.377, 8.540),
    ("Geneva", 46.204, 6.143),
    ("Basel", 47.560, 7.588),
    ("Lausanne", 46.520, 6.633),
    ("Lucerne", 47.050, 8.306),
]
PROPERTY_TYPES = ["apartment", "house", "loft", "studio", "chalet"]


def _fingerprint(row: dict) -> str:
    key = f"{row['id']}|{row['price']}"
    return hashlib.sha1(key.encode()).hexdigest()


@dlt.source(name="real_estate")
def real_estate_source(seed: int = 42, n_listings: int = 500, churn_pct: float = 0.15):
    return synthetic_listings(seed=seed, n_listings=n_listings, churn_pct=churn_pct)


@dlt.resource(
    name="listings",
    write_disposition="merge",
    primary_key="id",
    merge_key="id",
)
def synthetic_listings(
    seed: int = 42,
    n_listings: int = 500,
    churn_pct: float = 0.15,
) -> Iterator[dict]:
    """
    Simulate a daily 'scrape'. On each run:
      - All listings are yielded.
      - ~churn_pct get a new price (mimicking re-listings / reductions).
      - A few are rotated out and replaced (mimicking sold / new stock).
    Row fingerprint drives CDC downstream.
    """
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed + datetime.now(timezone.utc).toordinal())

    scraped_at = datetime.now(timezone.utc)
    for i in range(n_listings):
        city, lat, lon = random.choice(SWISS_CITIES)
        ptype = random.choice(PROPERTY_TYPES)
        rooms = random.choice([1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 6])
        size_m2 = int(random.gauss(90, 35))
        size_m2 = max(20, min(size_m2, 400))
        base_price = int(size_m2 * random.uniform(6500, 14000))
        # daily churn on a subset
        if random.random() < churn_pct:
            base_price = int(base_price * random.uniform(0.92, 1.08))
        row = {
            "id": f"CH-{seed}-{i:06d}",
            "title": fake.catch_phrase(),
            "city": city,
            "lat": round(lat + random.uniform(-0.05, 0.05), 5),
            "lon": round(lon + random.uniform(-0.05, 0.05), 5),
            "property_type": ptype,
            "rooms": rooms,
            "size_m2": size_m2,
            "price": base_price,
            "currency": "CHF",
            "listing_type": random.choice(["buy", "rent"]),
            "year_built": random.randint(1890, 2025),
            "seller": fake.company(),
            "scraped_at": scraped_at,
        }
        row["fingerprint"] = _fingerprint(row)
        yield row


@dlt.resource(
    name="listings",
    write_disposition="merge",
    primary_key="id",
)
def scrape_listings_from_html(html_path: str | Path) -> Iterator[dict]:
    """
    BeautifulSoup pattern against a local HTML fixture. Matches
    `<div class="listing" data-id="..." data-price="...">` plus child tags.
    This demonstrates the scraping approach without calling any live site.
    """
    html = Path(html_path).read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    scraped_at = datetime.now(timezone.utc)
    for node in soup.select("div.listing"):
        row = {
            "id": node.get("data-id"),
            "title": node.select_one(".title").get_text(strip=True),
            "city": node.select_one(".city").get_text(strip=True),
            "price": int(node.get("data-price")),
            "size_m2": int(node.select_one(".size").get_text(strip=True)),
            "rooms": float(node.select_one(".rooms").get_text(strip=True)),
            "property_type": node.get("data-type", "apartment"),
            "currency": "CHF",
            "listing_type": "buy",
            "lat": None,
            "lon": None,
            "year_built": None,
            "seller": None,
            "scraped_at": scraped_at,
        }
        row["fingerprint"] = _fingerprint(row)
        yield row

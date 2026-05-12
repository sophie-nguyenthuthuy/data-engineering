"""One-shot seed outside Dagster. `python -m re_pipeline.seed`."""
from __future__ import annotations

from .assets.bronze import bronze_listings
from .assets.gold import gold_city_stats, gold_price_bands
from .assets.silver import silver_listings, silver_price_history
from dagster import materialize

if __name__ == "__main__":
    result = materialize(
        [
            bronze_listings,
            silver_listings,
            silver_price_history,
            gold_city_stats,
            gold_price_bands,
        ]
    )
    print("OK" if result.success else "FAILED")

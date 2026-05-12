#!/usr/bin/env bash
# End-to-end demo using synthetic prices — no Common Crawl access required.
# Seeds 12 months of extracted products, fetches CPI, then analyzes + serves.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export IC_DB_PATH="${IC_DB_PATH:-$ROOT/data/inflation.duckdb}"

python3 -c "
from datetime import datetime
from inflation_crawler.extract import Product
from inflation_crawler.store import upsert_products

# Three products in two categories, rising prices with noise.
import random
random.seed(7)

def spec(pid, base, drift, category):
    rows = []
    for m in range(1, 13):
        price = base * ((1 + drift) ** (m - 1)) * (1 + random.uniform(-0.01, 0.01))
        rows.append(Product(
            product_id=pid, url=f'https://demo/{pid}', title=pid.title(),
            brand=pid.split('-')[0].title(), price=round(price, 2),
            currency='USD', category=category,
            fetch_time=datetime(2024, m, 15), source='jsonld',
        ))
    return rows

products = []
products += spec('acme-ultrabook', 750.0, 0.004, 'laptops')
products += spec('daybreak-coffee', 15.0,  0.008, 'grocery')
products += spec('brightview-tv',   400.0, 0.002, 'electronics')
upsert_products(products)
print(f'seeded {len(products)} product observations')
"

echo
echo ">>> analyzing laptops"
python3 -m inflation_crawler.cli analyze --category laptops --year 2024

echo
echo ">>> analyzing all"
python3 -m inflation_crawler.cli analyze --year 2024

echo
echo ">>> fetching CPI (skip on network failure)"
python3 -m inflation_crawler.cli cpi --start-year 2024 --end-year 2024 || echo "(CPI fetch failed — dashboard will still work without it)"

echo
echo ">>> starting dashboard at http://127.0.0.1:8000"
exec python3 -m inflation_crawler.cli serve --port 8000

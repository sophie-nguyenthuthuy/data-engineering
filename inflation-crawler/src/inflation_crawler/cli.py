"""Command-line interface for the inflation-crawler pipeline."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import click
import duckdb
from rich.console import Console
from rich.table import Table

from . import analyze, cpi, fetch, ingest, store
from .config import settings
from .extract import extract_product
from .logging import configure_logging, get_logger

console = Console()
log = get_logger(__name__)


@click.group()
@click.option("--log-level", default="INFO", show_default=True)
def main(log_level: str) -> None:
    """Inflation crawler pipeline."""
    configure_logging(log_level)
    settings.ensure_dirs()


@main.command("ingest")
@click.option("--crawl", required=True, help="Crawl id, e.g. CC-MAIN-2024-10")
@click.option("--host", required=True, help="SQL LIKE pattern for url_host_name, e.g. %walmart.com")
@click.option("--url-pattern", default=None, help="Optional SQL LIKE pattern for url")
@click.option("--limit", default=5000, show_default=True)
@click.option("--out", type=click.Path(path_type=Path), default=None)
def cli_ingest(crawl: str, host: str, url_pattern: str | None, limit: int, out: Path | None) -> None:
    """Query the Common Crawl columnar index and save matching WARC offsets."""
    out = out or (settings.extracted_dir / f"index_{crawl}.parquet")
    rows = ingest.query_index(
        crawl=crawl, host_pattern=host, url_pattern=url_pattern, limit=limit
    )
    ingest.save_index_rows(rows, out)
    console.print(f"[green]✓[/] {len(rows)} rows -> {out}")


@main.command("fetch")
@click.option("--index", type=click.Path(path_type=Path, exists=True), required=True)
def cli_fetch(index: Path) -> None:
    """Fetch + extract products for every row in an index parquet."""
    con = duckdb.connect(":memory:")
    rows = con.execute(f"SELECT * FROM read_parquet('{index}')").fetchall()
    index_rows = [ingest.IndexRow(*r) for r in rows]

    async def _run() -> int:
        extracted = 0
        batch: list = []
        async for rec in fetch.fetch_records(index_rows):
            product = extract_product(rec.html, rec.url, rec.fetch_time)
            if product:
                batch.append(product)
                extracted += 1
            if len(batch) >= 500:
                store.upsert_products(batch)
                batch.clear()
        if batch:
            store.upsert_products(batch)
        return extracted

    n = asyncio.run(_run())
    console.print(f"[green]✓[/] extracted {n} products of {len(index_rows)} fetched")


@main.command("extract-file")
@click.argument("html_path", type=click.Path(path_type=Path, exists=True))
@click.option("--url", required=True)
@click.option("--fetch-time", default="2024-01-15T00:00:00Z")
def cli_extract_file(html_path: Path, url: str, fetch_time: str) -> None:
    """Extract a product from a local HTML file (useful for testing/fixtures)."""
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    product = extract_product(html, url, fetch_time)
    if not product:
        console.print("[red]no product extracted[/]")
        raise SystemExit(1)
    store.upsert_products([product])
    console.print(json.dumps(
        {**product.__dict__, "fetch_time": product.fetch_time.isoformat()},
        indent=2, default=str,
    ))


@main.command("cpi")
@click.option("--series", default="CUUR0000SA0", show_default=True)
@click.option("--start-year", default=2018, show_default=True, type=int)
@click.option("--end-year", default=2024, show_default=True, type=int)
def cli_cpi(series: str, start_year: int, end_year: int) -> None:
    """Fetch BLS CPI data into the local DB."""
    rows = cpi.fetch_cpi_series(series, start_year, end_year)
    store.upsert_cpi(series, rows)
    console.print(f"[green]✓[/] CPI {series}: {len(rows)} months")


@main.command("analyze")
@click.option("--category", default=None)
@click.option("--year", type=int, default=None)
def cli_analyze(category: str | None, year: int | None) -> None:
    """Print inflation timeseries + annualized rate."""
    ts = analyze.inflation_timeseries(category)
    if ts.is_empty():
        console.print("[yellow]no data — run ingest/fetch or extract-file first[/]")
        return

    table = Table(title=f"Monthly inflation — {category or 'all categories'}")
    table.add_column("Period")
    table.add_column("N products", justify="right")
    table.add_column("Monthly %", justify="right")
    for row in ts.iter_rows(named=True):
        table.add_row(row["period"], str(row["n_products"]),
                      f"{row['monthly_inflation_pct']:+.2f}")
    console.print(table)

    rate = analyze.annualized_inflation(category, year)
    if rate is not None:
        label = f"{year} annualized" if year else "period annualized"
        console.print(f"[bold cyan]{label} inflation:[/] {rate:+.2f}%")


@main.command("serve")
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8000, show_default=True, type=int)
def cli_serve(host: str, port: int) -> None:
    """Serve the FastAPI dashboard."""
    import uvicorn

    uvicorn.run("inflation_crawler.api:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()

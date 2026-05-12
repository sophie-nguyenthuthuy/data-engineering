"""Query Common Crawl's columnar index with DuckDB to find WARC records of interest.

Replaces the original project's AWS Athena step. Common Crawl publishes a columnar
(Parquet) index at s3://commoncrawl/cc-index/table/cc-main/warc/. DuckDB's httpfs
extension lets us query it directly without any AWS account or Athena cluster.

A row in the index gives (url, warc_filename, warc_record_offset, warc_record_length),
which is everything we need to issue a ranged GET against data.commoncrawl.org to pull
a single record out of a ~1GB WARC file.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import urllib.request
import gzip

import duckdb

from .config import settings
from .logging import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class IndexRow:
    url: str
    url_host_name: str
    warc_filename: str
    warc_record_offset: int
    warc_record_length: int
    fetch_time: str
    content_mime_type: str | None


def _get_parquet_urls(crawl: str) -> list[str]:
    """Fetch exact Parquet file URLs from Common Crawl's metadata to bypass S3 bucket listing issues."""
    url = f"https://data.commoncrawl.org/crawl-data/{crawl}/cc-index-table.paths.gz"
    req = urllib.request.Request(url, headers={"User-Agent": "inflation-crawler/1.0"})
    try:
        response = urllib.request.urlopen(req)
        data = gzip.decompress(response.read()).decode("utf-8")
    except Exception as e:
        log.error("Failed to fetch CC paths metadata", error=str(e), url=url)
        raise

    paths = []
    for line in data.splitlines():
        if f"crawl={crawl}/subset=warc/" in line and line.endswith(".parquet"):
            paths.append("https://data.commoncrawl.org/" + line)
    return paths


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=str(settings.db_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET s3_endpoint='s3.amazonaws.com';")
    con.execute("SET s3_use_ssl=true;")
    # Common Crawl is a public requester-pays-free bucket; anonymous access works.
    con.execute("SET s3_url_style='path';")
    return con


def query_index(
    *,
    crawl: str,
    host_pattern: str,
    url_pattern: str | None = None,
    limit: int = 10_000,
) -> list[IndexRow]:
    """Return index rows matching ``host_pattern`` (SQL LIKE) in the given crawl.

    ``crawl`` is a Common Crawl crawl id like ``CC-MAIN-2024-10``.
    ``host_pattern`` is applied to ``url_host_name`` with SQL LIKE, e.g. ``%walmart.com``.
    ``url_pattern`` is optional and applied to ``url`` with LIKE.
    """
    settings.ensure_dirs()
    con = _connect()
    log.info("index.fetching_metadata", crawl=crawl)
    source_urls = _get_parquet_urls(crawl)
    if not source_urls:
        raise RuntimeError(f"Could not find index Parquet files for crawl {crawl}")
    
    source_sql_list = ", ".join(f"'{u}'" for u in source_urls)

    filters = ["url_host_name LIKE ?", "subset = 'warc'"]
    params: list[object] = [host_pattern]
    if url_pattern:
        filters.append("url LIKE ?")
        params.append(url_pattern)
    params.append(limit)

    sql = f"""
        SELECT url, url_host_name, warc_filename, warc_record_offset,
               warc_record_length, fetch_time, content_mime_type
        FROM read_parquet([{source_sql_list}], hive_partitioning=1)
        WHERE {' AND '.join(filters)}
          AND content_mime_type LIKE 'text/html%'
          AND fetch_status = 200
        LIMIT ?
    """
    log.info("index.query", crawl=crawl, host_pattern=host_pattern, limit=limit)
    rows = con.execute(sql, params).fetchall()
    return [IndexRow(*r) for r in rows]


def save_index_rows(rows: list[IndexRow], out_path: Path) -> Path:
    """Persist query results to Parquet for downstream fetch step."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE rows(url VARCHAR, url_host_name VARCHAR, warc_filename VARCHAR,
                          warc_record_offset BIGINT, warc_record_length BIGINT,
                          fetch_time VARCHAR, content_mime_type VARCHAR)
        """
    )
    con.executemany(
        "INSERT INTO rows VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(r.url, r.url_host_name, r.warc_filename, r.warc_record_offset,
          r.warc_record_length, r.fetch_time, r.content_mime_type) for r in rows],
    )
    con.execute(f"COPY rows TO '{out_path}' (FORMAT PARQUET)")
    log.info("index.saved", rows=len(rows), path=str(out_path))
    return out_path

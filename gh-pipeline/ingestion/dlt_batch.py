"""
GH Archive batch ingestion.

Downloads hourly GH Archive dumps (https://data.gharchive.org/YYYY-MM-DD-H.json.gz),
streams them through dlt, and lands them in:
  - GCS bucket as parquet (data lake)
  - BigQuery `gh_raw.events` table, partitioned by event_hour (warehouse raw)

Usage:
  python dlt_batch.py --date 2024-01-15            # one day (24 hourly files)
  python dlt_batch.py --date 2024-01-15 --hour 10  # single hour
  python dlt_batch.py --start 2024-01-01 --end 2024-01-07  # date range
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Iterator

import dlt
import requests
from dlt.sources.helpers.requests import client as dlt_requests

log = logging.getLogger("gh_archive")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")

GH_ARCHIVE_URL = "https://data.gharchive.org/{date}-{hour}.json.gz"


def _hour_url(d: date, hour: int) -> str:
    return GH_ARCHIVE_URL.format(date=d.isoformat(), hour=hour)


def _iter_events(d: date, hour: int) -> Iterator[dict]:
    """Stream one hourly gzip, yielding parsed events with an event_hour partition key."""
    url = _hour_url(d, hour)
    log.info("fetching %s", url)
    with dlt_requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for line in gz:
                try:
                    evt = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # Partition column — hour-level, lets BigQuery prune scans.
                evt["event_hour"] = datetime(d.year, d.month, d.day, hour).isoformat()
                yield evt


@dlt.resource(
    name="events",
    write_disposition="append",
    primary_key="id",
    columns={
        "id": {"data_type": "text"},
        "type": {"data_type": "text"},
        "event_hour": {"data_type": "timestamp", "partition": True},
    },
)
def gh_events(dates_hours: list[tuple[date, int]]):
    for d, h in dates_hours:
        yield from _iter_events(d, h)


def _daterange(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD, shorthand for --start/--end same-day")
    p.add_argument("--start", help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD")
    p.add_argument("--hour", type=int, help="0-23, restrict to one hour")
    args = p.parse_args()

    if args.date:
        start = end = date.fromisoformat(args.date)
    elif args.start and args.end:
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    else:
        p.error("provide --date or --start/--end")

    hours = [args.hour] if args.hour is not None else range(24)
    jobs = [(d, h) for d in _daterange(start, end) for h in hours]
    log.info("ingesting %d hourly files", len(jobs))

    pipeline = dlt.pipeline(
        pipeline_name="gh_archive",
        destination="bigquery",
        dataset_name=os.environ.get("BQ_DATASET_RAW", "gh_raw"),
        progress="log",
    )
    load_info = pipeline.run(gh_events(jobs), loader_file_format="parquet")
    log.info("load complete: %s", load_info)


if __name__ == "__main__":
    main()

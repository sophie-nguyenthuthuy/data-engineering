from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import task

logger = logging.getLogger(__name__)


@task(name="load", retries=2, retry_delay_seconds=15, log_prints=True)
def load_task(
    batches: list[pd.DataFrame],
    destination_type: str,
    destination_path: str | None = None,
    db_engine=None,
    table_name: str | None = None,
    if_exists: str = "append",
) -> int:
    """
    Write clean batches to the configured destination.

    Supported destinations:
      csv    — append rows to a CSV file
      jsonl  — append rows to a JSONL file
      db     — write to a SQLAlchemy-compatible database table
      memory — no-op, return count (useful in tests)
    """
    total_loaded = 0

    if not batches:
        logger.warning("No batches to load")
        return 0

    if destination_type == "csv":
        total_loaded = _load_csv(batches, destination_path)
    elif destination_type == "jsonl":
        total_loaded = _load_jsonl(batches, destination_path)
    elif destination_type == "db":
        total_loaded = _load_db(batches, db_engine, table_name, if_exists)
    elif destination_type == "memory":
        total_loaded = sum(len(b) for b in batches)
        logger.info("Memory sink: %d rows (no-op)", total_loaded)
    else:
        raise ValueError(f"Unknown destination_type: {destination_type!r}")

    logger.info("Loaded %d rows to '%s'", total_loaded, destination_type)
    return total_loaded


def _load_csv(batches: list[pd.DataFrame], path: str) -> int:
    dest = Path(path)
    write_header = not dest.exists()
    total = 0
    with open(dest, "a", newline="") as fh:
        for batch in batches:
            batch.to_csv(fh, header=write_header, index=False)
            write_header = False
            total += len(batch)
    return total


def _load_jsonl(batches: list[pd.DataFrame], path: str) -> int:
    dest = Path(path)
    total = 0
    with open(dest, "a") as fh:
        for batch in batches:
            for record in batch.to_dict(orient="records"):
                fh.write(json.dumps(record, default=str) + "\n")
            total += len(batch)
    return total


def _load_db(
    batches: list[pd.DataFrame],
    engine,
    table_name: str,
    if_exists: str,
) -> int:
    total = 0
    first = True
    for batch in batches:
        mode = if_exists if first else "append"
        batch.to_sql(table_name, engine, if_exists=mode, index=False)
        first = False
        total += len(batch)
    return total

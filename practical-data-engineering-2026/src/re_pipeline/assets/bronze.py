"""Bronze: raw listings from dlt → Iceberg."""
from __future__ import annotations

import pyarrow as pa
from dagster import MaterializeResult, MetadataValue, asset

from ..iceberg_utils import upsert_table
from ..sources.real_estate import synthetic_listings


@asset(
    group_name="bronze",
    compute_kind="dlt",
    description="Raw real-estate listings ingested via dlt → Iceberg. "
                "Fingerprint-based CDC: same-key rows only change when price shifts.",
)
def bronze_listings(context) -> MaterializeResult:
    rows = list(synthetic_listings())
    table = pa.Table.from_pylist(rows)
    full = upsert_table("bronze_listings", table)
    return MaterializeResult(
        metadata={
            "rows": len(rows),
            "iceberg_table": full,
            "preview": MetadataValue.md(
                "| id | city | price | rooms |\n|---|---|---|---|\n" +
                "\n".join(
                    f"| {r['id']} | {r['city']} | {r['price']} | {r['rooms']} |"
                    for r in rows[:5]
                )
            ),
        }
    )

"""Ingest asset: extract one hourly window of `user_interactions` to Parquet on MinIO."""

import io
from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
from dagster import AssetExecutionContext, HourlyPartitionsDefinition, MetadataValue, asset

from orchestrator.obs import track_asset
from orchestrator.resources import ClickHouseResource, S3Resource

# Start date is a project epoch; partitions before the first event return empty.
hourly_partitions = HourlyPartitionsDefinition(start_date="2026-04-20-00:00")


def _parse_partition(partition_key: str) -> datetime:
    # Dagster partition keys for an hourly def are `YYYY-MM-DD-HH:MM`.
    return datetime.strptime(partition_key, "%Y-%m-%d-%H:%M").replace(tzinfo=UTC)


@asset(
    partitions_def=hourly_partitions,
    group_name="ingest",
    description=(
        "Extract the hour of user_interactions rows that fall in [partition, "
        "partition+1h) and write them as Parquet to MinIO."
    ),
)
def raw_events_parquet(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    s3: S3Resource,
) -> dict[str, object]:
    with track_asset(context):
        return _materialize(context, clickhouse, s3)


def _materialize(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    s3: S3Resource,
) -> dict[str, object]:
    start = _parse_partition(context.partition_key)
    end = start + timedelta(hours=1)

    ch = clickhouse.get_client()
    # FINAL forces the ReplacingMergeTree merge before reading so duplicates
    # from at-least-once replay are collapsed. Expensive on a large table; for
    # hourly slices it is fine.
    sql = """
        SELECT
            event_id, occurred_at, user_id, session_id, event_type,
            status, error_code, latency_ms, country, device, ingested_at
        FROM user_interactions FINAL
        WHERE occurred_at >= %(start)s AND occurred_at < %(end)s
    """
    table = ch.query_arrow(sql, parameters={"start": start, "end": end})
    n_rows = table.num_rows
    context.log.info("extracted %d rows for %s", n_rows, context.partition_key)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    body = buf.getvalue()

    key = (
        f"raw/user_interactions/dt={start:%Y-%m-%d}/hour={start:%H}/data.parquet"
    )
    s3c = s3.get_client()
    s3c.put_object(Bucket=s3.bucket, Key=key, Body=body)

    context.add_output_metadata(
        {
            "rows": n_rows,
            "bytes": len(body),
            "s3_uri": MetadataValue.text(f"s3a://{s3.bucket}/{key}"),
            "window_start": MetadataValue.text(start.isoformat()),
        }
    )
    return {"rows": n_rows, "bucket": s3.bucket, "key": key}

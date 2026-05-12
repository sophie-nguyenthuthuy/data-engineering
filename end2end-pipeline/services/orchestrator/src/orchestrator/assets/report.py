"""Report asset: write aggregated cohorts back to ClickHouse `analysis_hourly`."""

from dagster import AssetExecutionContext, asset

from orchestrator.assets.raw_events import _parse_partition, hourly_partitions
from orchestrator.obs import track_asset
from orchestrator.resources import ClickHouseResource


@asset(
    partitions_def=hourly_partitions,
    group_name="analyze",
    description=(
        "Persist the Spark analysis back into ClickHouse "
        "(`events.analysis_hourly`). The engine is ReplacingMergeTree so re-runs "
        "of the same partition collapse on merge — safe to retry."
    ),
)
def analysis_report(
    context: AssetExecutionContext,
    event_analysis: list[dict],
    clickhouse: ClickHouseResource,
) -> None:
    with track_asset(context):
        _write(context, event_analysis, clickhouse)


def _write(
    context: AssetExecutionContext,
    event_analysis: list[dict],
    clickhouse: ClickHouseResource,
) -> None:
    if not event_analysis:
        context.log.info("no cohorts to write for %s", context.partition_key)
        return

    window_start = _parse_partition(context.partition_key)
    rows = [
        (
            window_start,
            r["event_type"],
            r["status"],
            r["country"],
            r["device"],
            int(r["events"]),
            int(r["errors"]),
            float(r["p95_latency_ms"] or 0.0),
            float(r["avg_latency_ms"] or 0.0),
        )
        for r in event_analysis
    ]

    ch = clickhouse.get_client()
    ch.insert(
        "analysis_hourly",
        rows,
        column_names=[
            "window_start",
            "event_type",
            "status",
            "country",
            "device",
            "events",
            "errors",
            "p95_latency_ms",
            "avg_latency_ms",
        ],
    )
    context.log.info("wrote %d rows to analysis_hourly for %s", len(rows), context.partition_key)
    context.add_output_metadata({"rows_written": len(rows)})

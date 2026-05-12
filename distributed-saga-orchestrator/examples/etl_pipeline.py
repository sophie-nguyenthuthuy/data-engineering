"""
ETL Data Pipeline — 8-step Saga example
=========================================
Demonstrates the Saga pattern for a batch data-ingestion workflow:

  Step 1  – ExtractFromSource        compensate: close/release source cursor
  Step 2  – ValidateSchema           compensate: (no-op, read-only)
  Step 3  – TransformAndEnrich       compensate: delete temp transform artefacts
  Step 4  – WriteToStagingTable      compensate: truncate staging table
  Step 5  – RunDataQualityChecks     compensate: (no-op, read-only)
  Step 6  – SwapStagingToProduction  compensate: swap back to previous snapshot
  Step 7  – UpdateMetadataCatalog    compensate: revert catalog entry
  Step 8  – TriggerDownstreamJobs    compensate: send cancellation signal

Run it:
    python -m examples.etl_pipeline
    python -m examples.etl_pipeline --fail-at 6    # rollback from production swap
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

from saga import RetryPolicy, SagaOrchestrator, SagaStep, SagaStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("etl_pipeline")


def _log(step: str, msg: str) -> None:
    logger.info("    [%s] %s", step, msg)


# ---------------------------------------------------------------------------
# Step 1 – Extract from Source
# ---------------------------------------------------------------------------
class ExtractFromSource(SagaStep):
    retry_policy = RetryPolicy(max_attempts=3, backoff_base_seconds=0.2)

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        source = ctx["source_table"]
        _log(self.name, f"Extracting rows from {source!r}")
        row_count = 142_857   # simulated
        extract_path = f"/tmp/extract_{ctx['batch_id']}.parquet"
        _log(self.name, f"Wrote {row_count:,} rows → {extract_path}")
        return {"extract_path": extract_path, "raw_row_count": row_count}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        path = ctx.get("extract_path", "unknown")
        _log(self.name, f"Releasing source cursor and removing {path}")


# ---------------------------------------------------------------------------
# Step 2 – Validate Schema (read-only)
# ---------------------------------------------------------------------------
class ValidateSchema(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        _log(self.name, f"Validating schema of {ctx['extract_path']}")
        expected_cols = {"id", "amount", "currency", "timestamp", "user_id"}
        return {"schema_valid": True, "expected_columns": list(expected_cols)}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        _log(self.name, "No-op — schema validation is read-only")


# ---------------------------------------------------------------------------
# Step 3 – Transform and Enrich
# ---------------------------------------------------------------------------
class TransformAndEnrich(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        in_rows = ctx["raw_row_count"]
        _log(self.name, f"Applying transforms to {in_rows:,} rows")
        dropped = int(in_rows * 0.002)   # 0.2 % invalid rows dropped
        out_rows = in_rows - dropped
        transform_path = f"/tmp/transform_{ctx['batch_id']}.parquet"
        _log(self.name, f"Dropped {dropped} invalid rows → {out_rows:,} clean rows → {transform_path}")
        return {
            "transform_path": transform_path,
            "clean_row_count": out_rows,
            "dropped_rows": dropped,
        }

    async def compensate(self, ctx: dict[str, Any]) -> None:
        path = ctx.get("transform_path", "unknown")
        _log(self.name, f"Deleting transform artefact {path}")


# ---------------------------------------------------------------------------
# Step 4 – Write to Staging Table
# ---------------------------------------------------------------------------
class WriteToStagingTable(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        staging = f"stg_{ctx['target_table']}_{ctx['batch_id']}"
        rows = ctx["clean_row_count"]
        _log(self.name, f"Bulk-inserting {rows:,} rows into {staging!r}")
        return {"staging_table": staging, "staging_row_count": rows}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        tbl = ctx.get("staging_table", "unknown")
        _log(self.name, f"Truncating and dropping staging table {tbl!r}")


# ---------------------------------------------------------------------------
# Step 5 – Run Data Quality Checks (read-only)
# ---------------------------------------------------------------------------
class RunDataQualityChecks(SagaStep):
    THRESHOLD = 0.995   # ≥ 99.5 % rows must pass

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        rows = ctx["staging_row_count"]
        passed = int(rows * 0.9991)
        ratio = passed / rows
        _log(self.name, f"DQ check: {passed:,}/{rows:,} rows passed ({ratio:.3%})")
        if ratio < self.THRESHOLD:
            raise ValueError(
                f"Data quality below threshold: {ratio:.3%} < {self.THRESHOLD:.3%}"
            )
        return {"dq_passed_rows": passed, "dq_ratio": ratio, "dq_passed": True}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        _log(self.name, "No-op — DQ checks are read-only")


# ---------------------------------------------------------------------------
# Step 6 – Swap Staging → Production
# ---------------------------------------------------------------------------
class SwapStagingToProduction(SagaStep):
    def __init__(self, fail: bool = False) -> None:
        self._fail = fail

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        if self._fail:
            raise RuntimeError(
                "Database lock timeout during atomic swap — partition 3 unavailable"
            )
        staging = ctx["staging_table"]
        target = ctx["target_table"]
        snapshot = f"{target}_snap_{ctx['batch_id']}"
        _log(self.name, f"Renaming {target!r} → {snapshot!r} (backup snapshot)")
        _log(self.name, f"Renaming {staging!r} → {target!r} (atomic swap)")
        return {"previous_snapshot": snapshot, "production_table": target}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        snap = ctx.get("previous_snapshot", "unknown")
        target = ctx.get("production_table", ctx["target_table"])
        _log(self.name, f"Swapping {target!r} back to snapshot {snap!r}")


# ---------------------------------------------------------------------------
# Step 7 – Update Metadata Catalog
# ---------------------------------------------------------------------------
class UpdateMetadataCatalog(SagaStep):
    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        _log(self.name, f"Updating catalog entry for {ctx['target_table']!r}: "
                        f"row_count={ctx['clean_row_count']:,}, batch={ctx['batch_id']!r}")
        return {"catalog_updated": True, "catalog_version": 2}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        _log(self.name, f"Reverting catalog entry for {ctx['target_table']!r} to previous version")


# ---------------------------------------------------------------------------
# Step 8 – Trigger Downstream Jobs
# ---------------------------------------------------------------------------
class TriggerDownstreamJobs(SagaStep):
    retry_policy = RetryPolicy(max_attempts=2, backoff_base_seconds=0.3)

    async def execute(self, ctx: dict[str, Any]) -> dict[str, Any]:
        jobs = ["reporting_refresh", "ml_feature_store_sync", "dashboard_cache_bust"]
        _log(self.name, f"Triggering downstream jobs: {jobs}")
        return {"triggered_jobs": jobs}

    async def compensate(self, ctx: dict[str, Any]) -> None:
        jobs = ctx.get("triggered_jobs", [])
        if jobs:
            _log(self.name, f"Sending cancellation signal to: {jobs}")


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def build_steps(fail_at: int | None) -> list[SagaStep]:
    return [
        ExtractFromSource(),                             # 1
        ValidateSchema(),                                # 2
        TransformAndEnrich(),                            # 3
        WriteToStagingTable(),                           # 4
        RunDataQualityChecks(),                          # 5
        SwapStagingToProduction(fail=fail_at == 6),      # 6
        UpdateMetadataCatalog(),                         # 7
        TriggerDownstreamJobs(),                         # 8
    ]


async def run_etl(
    batch_id: str,
    source_table: str,
    target_table: str,
    fail_at: int | None = None,
    db_path: str = "sagas_etl.db",
) -> None:
    store = SagaStore(db_path)
    orchestrator = SagaOrchestrator(store)

    initial_context = {
        "batch_id": batch_id,
        "source_table": source_table,
        "target_table": target_table,
    }

    logger.info("=" * 60)
    logger.info("Starting ETL pipeline saga")
    logger.info("  Batch  : %s", batch_id)
    logger.info("  Source : %s", source_table)
    logger.info("  Target : %s", target_table)
    if fail_at:
        logger.info("  [TEST] : Forcing failure at step %d", fail_at)
    logger.info("=" * 60)

    result = await orchestrator.run(
        steps=build_steps(fail_at),
        initial_context=initial_context,
        saga_type="etl_pipeline",
        saga_id=batch_id,
    )

    logger.info("=" * 60)
    if result.succeeded:
        logger.info("✅  ETL batch %s COMPLETED", batch_id)
        logger.info("    Clean rows  : %s", result.context.get("clean_row_count"))
        logger.info("    Dropped     : %s", result.context.get("dropped_rows"))
        logger.info("    DQ ratio    : %.4f", result.context.get("dq_ratio", 0))
    else:
        logger.warning("❌  ETL batch %s FAILED (status=%s)", batch_id, result.status.value)
        logger.warning("    Failed at step : %s", result.failure_step)
        logger.warning("    Reason         : %s", result.failure_reason)
        if result.compensation_errors:
            for ce in result.compensation_errors:
                logger.error("    ⚠ Compensation error  %s: %s", ce["step"], ce["error"])
        else:
            logger.info("    ↩ All completed steps rolled back successfully")
    logger.info("=" * 60)

    print("\nStep summary:")
    print(f"  {'#':<4} {'Step':<35} {'Status':<22}")
    print("  " + "-" * 62)
    for i, rec in enumerate(result.step_records, 1):
        print(f"  {i:<4} {rec.name:<35} {rec.status.value:<22}")

    store.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ETL Pipeline Saga demo")
    parser.add_argument("--fail-at", type=int, default=None,
                        help="Force failure at step N (1–8) to demo rollback")
    args = parser.parse_args()

    asyncio.run(
        run_etl(
            batch_id="BATCH-20260504-001",
            source_table="raw_transactions",
            target_table="transactions",
            fail_at=args.fail_at,
        )
    )

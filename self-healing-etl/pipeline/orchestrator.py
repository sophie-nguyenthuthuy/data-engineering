from __future__ import annotations

import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
from prefect import flow, get_run_logger
from prefect.context import get_run_context
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import ETLConfig
from models import PipelineRun, init_db
from pipeline.extractor import extract_task
from pipeline.transformer import transform_task, TransformFn
from pipeline.loader import load_task
from alerts.alerter import Alerter
from quarantine.store import QuarantineStore

logger = logging.getLogger(__name__)


@flow(
    name="self-healing-etl",
    description="ETL pipeline with schema drift detection, auto-healing, and quarantine",
    retries=0,          # flow-level retries disabled; task-level retries handle transients
    log_prints=True,
)
def etl_flow(
    source_name: str,
    source_type: str,                          # csv | jsonl | dataframe
    destination_type: str,                     # csv | jsonl | db | memory
    config: ETLConfig | None = None,
    source_path: str | None = None,
    source_df: pd.DataFrame | None = None,
    destination_path: str | None = None,
    destination_table: str | None = None,
    custom_transform: TransformFn | None = None,
) -> dict:
    """
    Top-level ETL flow.

    Returns a run summary dict with counts and status.
    """
    cfg = config or ETLConfig()
    run_id = _run_id()
    plog = get_run_logger()
    plog.info("Starting ETL run %s for source '%s'", run_id, source_name)

    # Track run in the quarantine DB (reuse same SQLite for simplicity)
    run_engine = init_db(cfg.quarantine.db_url)
    alerter = Alerter(
        slack_webhook_url=cfg.alerts.slack_webhook_url,
        min_severity=cfg.alerts.min_severity,
    )

    pipeline_run = PipelineRun(
        run_id=run_id,
        pipeline_name=cfg.pipeline_name,
        source_name=source_name,
        status="RUNNING",
    )
    with Session(run_engine) as session:
        session.add(pipeline_run)
        session.commit()

    summary = {
        "run_id": run_id,
        "source": source_name,
        "status": "UNKNOWN",
        "rows_extracted": 0,
        "rows_loaded": 0,
        "rows_quarantined": 0,
        "schema_evolved": False,
        "drift_detected": False,
    }

    try:
        # ── Extract ──────────────────────────────────────────────────
        batches = extract_task(
            source_type=source_type,
            source_path=source_path,
            source_df=source_df,
            batch_size=cfg.batch_size,
        )
        summary["rows_extracted"] = sum(len(b) for b in batches)

        # ── Transform + Heal ─────────────────────────────────────────
        t_result = transform_task(
            batches=batches,
            source_name=source_name,
            run_id=run_id,
            config=cfg,
            custom_transform=custom_transform,
        )
        summary["rows_quarantined"] = t_result.rows_quarantined
        summary["drift_detected"] = any(r.has_drift for r in t_result.drift_reports)
        summary["schema_evolved"] = t_result.schema_evolved

        # ── Load ─────────────────────────────────────────────────────
        dest_engine = None
        if destination_type == "db" and destination_path:
            dest_engine = create_engine(destination_path)

        rows_loaded = load_task(
            batches=t_result.clean_batches,
            destination_type=destination_type,
            destination_path=destination_path,
            db_engine=dest_engine,
            table_name=destination_table,
        )
        summary["rows_loaded"] = rows_loaded
        summary["status"] = "SUCCESS"

        plog.info(
            "ETL run %s complete: extracted=%d loaded=%d quarantined=%d drift=%s evolved=%s",
            run_id,
            summary["rows_extracted"],
            summary["rows_loaded"],
            summary["rows_quarantined"],
            summary["drift_detected"],
            summary["schema_evolved"],
        )

    except Exception as exc:
        summary["status"] = "FAILED"
        summary["error"] = str(exc)
        plog.error("ETL run %s FAILED: %s", run_id, exc)
        alerter.pipeline_failure_alert(
            pipeline_name=cfg.pipeline_name,
            source_name=source_name,
            run_id=run_id,
            error_message=str(exc),
            root_cause_hints=[
                "Check Prefect task logs for the full traceback.",
                "Verify source file/connection is accessible.",
                f"Source: {source_type}  Destination: {destination_type}",
            ],
        )
        raise

    finally:
        # Persist final run state
        with Session(run_engine) as session:
            from sqlalchemy import select, update
            session.execute(
                update(PipelineRun)
                .where(PipelineRun.run_id == run_id)
                .values(
                    status=summary["status"],
                    rows_extracted=summary["rows_extracted"],
                    rows_loaded=summary.get("rows_loaded", 0),
                    rows_quarantined=summary["rows_quarantined"],
                    drift_detected=summary["drift_detected"],
                    finished_at=datetime.now(timezone.utc),
                    error_message=summary.get("error"),
                )
            )
            session.commit()

    return summary


def _run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"run-{ts}-{uuid.uuid4().hex[:6]}"

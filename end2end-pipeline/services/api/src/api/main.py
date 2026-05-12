from __future__ import annotations

import logging
import sys
from typing import Annotated, Any

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .clickhouse import get_client, rows_to_dicts
from .config import Config
from .obs import instrument_app


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
    )


_configure_logging()
log = structlog.get_logger()


def _cfg() -> Config:
    return Config.from_env()


app = FastAPI(title="Pipeline Analytics API", version="0.1.0")

# CORS for the static dashboard when served on a different port.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# OTel instrumentation (no-op when OTEL_EXPORTER_OTLP_ENDPOINT is unset).
instrument_app(app)


class HealthResponse(BaseModel):
    status: str
    clickhouse: str


class MinuteBucket(BaseModel):
    minute: str
    event_type: str
    status: str
    events: int
    avg_latency_ms: float
    p95_latency_ms: float


@app.get("/healthz", response_model=HealthResponse)
def healthz(cfg: Annotated[Config, Depends(_cfg)]) -> HealthResponse:
    try:
        client = get_client(cfg)
        client.query("SELECT 1")
        ch = "ok"
    except Exception as exc:
        log.error("healthz.clickhouse_error", error=str(exc))
        ch = "error"
    return HealthResponse(status="ok" if ch == "ok" else "degraded", clickhouse=ch)


@app.get("/api/v1/analytics/minute", response_model=list[MinuteBucket])
def minute_buckets(
    cfg: Annotated[Config, Depends(_cfg)],
    minutes: int = Query(15, ge=1, le=360),
    event_type: str | None = Query(None, max_length=32),
) -> list[MinuteBucket]:
    client = get_client(cfg)
    params: dict[str, Any] = {"minutes": minutes}
    where = ["minute >= now() - INTERVAL {minutes:UInt32} MINUTE"]
    if event_type:
        where.append("event_type = {event_type:String}")
        params["event_type"] = event_type

    sql = f"""
        SELECT
            formatDateTime(minute, '%Y-%m-%dT%H:%M:%SZ') AS minute,
            event_type,
            status,
            countMerge(events) AS events,
            sumMerge(latency_sum) / greatest(countMerge(events), 1) AS avg_latency_ms,
            quantileTDigestMerge(0.95)(latency_p95_state) AS p95_latency_ms
        FROM user_interactions_1m
        WHERE {" AND ".join(where)}
        GROUP BY minute, event_type, status
        ORDER BY minute DESC, event_type, status
        LIMIT 5000
    """
    try:
        result = client.query(sql, parameters=params)
    except Exception as exc:
        log.error("minute_buckets.query_failed", error=str(exc))
        raise HTTPException(status_code=502, detail="clickhouse query failed") from exc
    return [MinuteBucket(**row) for row in rows_to_dicts(result)]


class SummaryResponse(BaseModel):
    window_minutes: int
    total_events: int
    error_rate: float
    by_event_type: list[dict[str, Any]]


@app.get("/api/v1/analytics/summary", response_model=SummaryResponse)
def summary(
    cfg: Annotated[Config, Depends(_cfg)],
    minutes: int = Query(5, ge=1, le=360),
) -> SummaryResponse:
    client = get_client(cfg)
    try:
        result = client.query(
            """
            SELECT
                event_type,
                status,
                countMerge(events) AS events
            FROM user_interactions_1m
            WHERE minute >= now() - INTERVAL {minutes:UInt32} MINUTE
            GROUP BY event_type, status
            """,
            parameters={"minutes": minutes},
        )
    except Exception as exc:
        log.error("summary.query_failed", error=str(exc))
        raise HTTPException(status_code=502, detail="clickhouse query failed") from exc

    by_type: dict[str, dict[str, int]] = {}
    total = 0
    errors = 0
    for row in rows_to_dicts(result):
        t = row["event_type"]
        by_type.setdefault(t, {"events": 0, "errors": 0})
        by_type[t]["events"] += int(row["events"])
        total += int(row["events"])
        if row["status"] == "error":
            by_type[t]["errors"] += int(row["events"])
            errors += int(row["events"])

    per_type_out = [
        {
            "event_type": t,
            "events": v["events"],
            "errors": v["errors"],
            "error_rate": (v["errors"] / v["events"]) if v["events"] else 0.0,
        }
        for t, v in sorted(by_type.items())
    ]
    return SummaryResponse(
        window_minutes=minutes,
        total_events=total,
        error_rate=(errors / total) if total else 0.0,
        by_event_type=per_type_out,
    )


class TopErrorRow(BaseModel):
    event_type: str
    country: str
    device: str
    events: int
    errors: int
    error_rate: float


@app.get("/api/v1/analytics/top-errors", response_model=list[TopErrorRow])
def top_errors(
    cfg: Annotated[Config, Depends(_cfg)],
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(10, ge=1, le=100),
) -> list[TopErrorRow]:
    """Cohorts with the most errors, sourced from the Dagster-populated
    `analysis_hourly` table. Empty if Phase 4 has not run yet."""
    client = get_client(cfg)
    try:
        result = client.query(
            """
            SELECT
                event_type,
                country,
                device,
                sum(events) AS events,
                sum(errors) AS errors,
                round(sum(errors) / nullIf(sum(events), 0), 4) AS error_rate
            FROM analysis_hourly FINAL
            WHERE window_start >= now() - INTERVAL {hours:UInt32} HOUR
            GROUP BY event_type, country, device
            HAVING errors > 0
            ORDER BY errors DESC
            LIMIT {limit:UInt32}
            """,
            parameters={"hours": hours, "limit": limit},
        )
    except Exception as exc:
        log.error("top_errors.query_failed", error=str(exc))
        raise HTTPException(status_code=502, detail="clickhouse query failed") from exc
    return [TopErrorRow(**row) for row in rows_to_dicts(result)]

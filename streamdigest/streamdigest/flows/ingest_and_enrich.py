"""Prefect 3 flow: ingest GitHub notifications → enrich with Ollama → store.

Run ad-hoc:
    python -m streamdigest.flows.ingest_and_enrich
or via CLI:
    streamdigest run
or schedule:
    prefect deploy -n streamdigest
"""

from __future__ import annotations

from typing import Any

import dlt
from prefect import flow, get_run_logger, task
from prefect.tasks import exponential_backoff

from streamdigest.ai import OllamaClient, enrich_one
from streamdigest.ai.ollama_client import OllamaError
from streamdigest.config import settings
from streamdigest.sources.github import github_source
from streamdigest.storage import get_store


@task(name="ingest-github", retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=5))
def ingest_github() -> dict[str, Any]:
    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name="streamdigest_github",
        destination=dlt.destinations.duckdb(str(settings.duckdb_path)),
        dataset_name="github",
    )
    info = pipeline.run(github_source())
    logger.info("dlt load info: %s", info)
    return {"pipeline": pipeline.pipeline_name, "dataset": "github"}


@task(name="prepare-storage")
def prepare_storage() -> None:
    store = get_store()
    store.init_schema()
    store.ensure_digest_view()


@task(
    name="enrich-batch",
    retries=1,
    retry_delay_seconds=10,
    tags=["ollama"],
)
def enrich_batch(batch_size: int = 50) -> dict[str, int]:
    logger = get_run_logger()
    store = get_store()

    with OllamaClient() as client:
        if not client.ping():
            logger.warning(
                "Ollama unreachable at %s — skipping enrichment. "
                "Start it with `ollama serve` and pull %s.",
                client.host,
                client.model,
            )
            return {"enriched": 0, "skipped": 0, "failed": 0}

        rows = store.unenriched_notifications(limit=batch_size)
        logger.info("Enriching %d notification(s) with %s", len(rows), client.model)

        ok = failed = 0
        for row in rows:
            try:
                e = enrich_one(row, client)
                store.upsert_enrichment(
                    id=row["id"],
                    summary=e.summary,
                    actions=e.actions,
                    priority=e.priority,
                    priority_reason=e.priority_reason,
                    sentiment=e.sentiment,
                    model=e.model,
                    input_hash=e.input_hash,
                )
                ok += 1
            except OllamaError as exc:
                logger.error("Enrichment failed for %s: %s", row["id"], exc)
                failed += 1

        return {"enriched": ok, "skipped": 0, "failed": failed}


@flow(name="streamdigest", log_prints=True)
def ingest_and_enrich(batch_size: int = 50) -> dict[str, Any]:
    """Top-level flow: ingest → prepare → enrich."""
    ingest_info = ingest_github()
    prepare_storage(wait_for=[ingest_info])
    stats = enrich_batch(batch_size=batch_size, wait_for=[ingest_info])
    return {"ingest": ingest_info, "enrichment": stats}


if __name__ == "__main__":
    ingest_and_enrich()

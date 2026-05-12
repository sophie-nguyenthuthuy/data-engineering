"""
Orchestrator — wires together the generator, graph DB, risk algorithms,
alert engine, and API server.

Architecture
------------
  TransactionGenerator ──► MemgraphClient (live graph)
                                │
                        [every N seconds]
                                ▼
                        RiskAnalyzer.run()
                          ├─ detect_cycles()
                          ├─ compute_node_metrics()
                          ├─ compute_concentration()
                          └─ worst_case_cascade()
                                │
                        AlertEngine.evaluate_*()
                                │
                        WebSocket broadcast ──► dashboard
"""

import asyncio
import logging
import sys
import time
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from src.config import settings
from src.generator.transaction_generator import InstitutionRegistry, TransactionGenerator
from src.graph.memgraph_client import MemgraphClient
from src.algorithms.cycle_detection import detect_cycles
from src.algorithms.centrality import compute_node_metrics, compute_concentration
from src.algorithms.contagion import worst_case_cascade
from src.alerts.alert_engine import AlertEngine, Alert, Severity
import src.api.server as api_server

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    handlers=[RichHandler(rich_tracebacks=True, markup=True)],
    format="%(message)s",
    datefmt="[%X]",
)
log = logging.getLogger(__name__)
console = Console()

RISK_ANALYSIS_INTERVAL = 5.0   # seconds between full graph analyses
PRUNE_INTERVAL = 300.0         # seconds between TRANSFERS edge pruning


async def ingest_loop(
    generator: TransactionGenerator,
    mg: MemgraphClient,
    state: dict,
) -> None:
    """Continuously pull transactions from the generator and write to Memgraph."""
    tx_count = 0
    async for tx in generator.stream():
        await mg.ingest_transaction(tx)
        tx_count += 1
        state["tx_count"] = tx_count
        if tx_count % 100 == 0:
            log.info("Ingested %d transactions", tx_count)


async def analysis_loop(
    mg: MemgraphClient,
    alert_engine: AlertEngine,
    state: dict,
) -> None:
    """Periodically run graph algorithms and emit alerts."""
    while True:
        await asyncio.sleep(RISK_ANALYSIS_INTERVAL)
        try:
            edges = await mg.get_all_edges()
            if not edges:
                continue

            # --- Cycle detection ---
            cycles = detect_cycles(edges)

            # --- Centrality ---
            node_metrics = compute_node_metrics(edges)

            # --- Concentration ---
            conc = compute_concentration(edges)

            # --- Worst-case contagion ---
            cascade = worst_case_cascade(edges)

            # --- Fire alerts ---
            alert_engine.evaluate_cycles(cycles)
            alert_engine.evaluate_concentration(conc)
            alert_engine.evaluate_systemic_nodes(node_metrics)
            alert_engine.evaluate_contagion(cascade)

            # --- Build metrics snapshot ---
            metrics = {
                "timestamp": time.time(),
                "tx_count": state.get("tx_count", 0),
                "node_count": len(await mg.get_all_nodes()),
                "edge_count": len(edges),
                "cycles": [
                    {
                        "nodes": c.nodes,
                        "total_exposure": c.total_exposure,
                        "risk_score": c.risk_score,
                    }
                    for c in cycles[:5]
                ],
                "concentration": {
                    "hhi": conc.hhi,
                    "gini": conc.gini,
                    "is_concentrated": conc.is_concentrated,
                    "top_nodes": conc.top_nodes,
                },
                "top_systemic_nodes": [
                    {
                        "id": nm.node_id,
                        "betweenness": nm.betweenness,
                        "pagerank": nm.pagerank,
                        "net_exposure": nm.net_exposure,
                        "is_systemic": nm.is_systemic,
                    }
                    for nm in node_metrics[:10]
                ],
                "worst_cascade": {
                    "seed": cascade.seed_node,
                    "fraction_failed": cascade.fraction_failed,
                    "cascade_depth": cascade.cascade_depth,
                    "exposure_lost": cascade.total_exposure_lost,
                },
                "recent_alerts": alert_engine.recent(10),
            }
            state["latest_metrics"] = metrics

            # Broadcast to WebSocket clients
            await api_server.push_update({"type": "metrics", "data": metrics})

            _log_summary(metrics, cycles)

        except Exception as exc:
            log.exception("Analysis loop error: %s", exc)


async def prune_loop(mg: MemgraphClient) -> None:
    """Periodically remove old raw TRANSFERS edges."""
    while True:
        await asyncio.sleep(PRUNE_INTERVAL)
        try:
            await mg.prune_old_transfers(max_age_seconds=3600)
        except Exception as exc:
            log.warning("Prune error: %s", exc)


def _log_summary(metrics: dict, cycles: list) -> None:
    n_cycles = len(cycles)
    conc = metrics["concentration"]
    cascade = metrics["worst_cascade"]
    worst = metrics["recent_alerts"]

    severity_counts: dict[str, int] = {}
    for a in metrics["recent_alerts"]:
        sev = a.get("severity", "INFO")
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    log.info(
        "[bold]Risk snapshot[/bold] | txs=%d  nodes=%d  edges=%d  "
        "cycles=%d  HHI=%.3f  cascade=%.0f%%  alerts=%s",
        metrics["tx_count"],
        metrics["node_count"],
        metrics["edge_count"],
        n_cycles,
        conc["hhi"],
        cascade["fraction_failed"] * 100,
        severity_counts,
    )


async def main() -> None:
    log.info("Starting Systemic Risk Monitor")

    # Shared state dict passed to API server
    state: dict[str, Any] = {"tx_count": 0, "latest_metrics": {}}

    # ---- Graph DB ----
    mg = MemgraphClient()
    await mg.verify_connectivity()
    await mg.setup_schema()
    state["memgraph"] = mg

    # ---- Institutions ----
    registry = InstitutionRegistry(n=settings.num_institutions)
    state["registry"] = registry
    log.info("Seeding %d institutions", len(registry.institutions))
    for inst in registry.institutions.values():
        await mg.upsert_institution(inst)

    # ---- Alert engine ----
    alert_engine = AlertEngine()
    state["alert_engine"] = alert_engine

    def _console_alert(alert: Alert):
        color = {
            Severity.CRITICAL: "bold red",
            Severity.HIGH: "red",
            Severity.MEDIUM: "yellow",
            Severity.INFO: "blue",
        }.get(alert.severity, "white")
        console.print(
            f"[{color}][{alert.severity}][/{color}] [{alert.category}] {alert.title}"
        )

    alert_engine.subscribe(_console_alert)

    # ---- Generator ----
    generator = TransactionGenerator(registry)

    # ---- Start background tasks ----
    loop = asyncio.get_event_loop()

    tasks = [
        loop.create_task(ingest_loop(generator, mg, state)),
        loop.create_task(analysis_loop(mg, alert_engine, state)),
        loop.create_task(prune_loop(mg)),
    ]

    # ---- API server in a thread (uvicorn is sync-friendly) ----
    import threading
    api_thread = threading.Thread(
        target=api_server.start,
        args=(state,),
        daemon=True,
    )
    api_thread.start()
    log.info("API server started on http://%s:%d", settings.api_host, settings.api_port)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await mg.close()
        log.info("Shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())

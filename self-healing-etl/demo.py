"""
Self-Healing ETL Demo
=====================
Simulates three consecutive pipeline runs showing:

  Run 1 — Clean baseline data  →  registers initial schema, loads 100 rows
  Run 2 — Schema drift data    →  detects added col, type change, removed col;
                                  auto-heals; evolves schema to v2
  Run 3 — Post-drift data      →  runs clean against evolved schema v2

Run with:
    python demo.py
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path

# Ensure project root is on sys.path so imports resolve
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
for sub in ("schema", "pipeline", "quarantine", "alerts", "healing"):
    sys.path.insert(0, str(ROOT / sub))

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from config import ETLConfig, HealingConfig, AlertConfig, SchemaRegistryConfig, QuarantineConfig
from pipeline.orchestrator import etl_flow
from schema.registry import SchemaRegistry
from quarantine.store import QuarantineStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
# Quiet Prefect's own verbose loggers for demo clarity
logging.getLogger("prefect").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

console = Console()

# ── Config (all SQLite, no external services needed) ─────────────────────────

CFG = ETLConfig(
    pipeline_name="orders_pipeline",
    batch_size=50,
    schema_registry=SchemaRegistryConfig(db_url="sqlite:///demo_registry.db"),
    quarantine=QuarantineConfig(db_url="sqlite:///demo_quarantine.db"),
    healing=HealingConfig(
        enable_type_coercion=True,
        enable_column_backfill=True,
        enable_schema_evolution=True,
        max_coercion_loss_pct=10.0,
    ),
    alerts=AlertConfig(min_severity="INFO"),   # no Slack — prints to console
)


# ── Synthetic datasets ────────────────────────────────────────────────────────

def baseline_data(n: int = 100) -> pd.DataFrame:
    """v1 schema: order_id(int), customer_id(int), amount(float), status(str)"""
    import numpy as np
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "order_id":    range(1, n + 1),
        "customer_id": rng.integers(1000, 9999, size=n),
        "amount":      rng.uniform(10.0, 500.0, size=n).round(2),
        "status":      rng.choice(["pending", "shipped", "delivered"], size=n),
    })


def drifted_data(n: int = 100) -> pd.DataFrame:
    """
    Simulates three simultaneous drift events:
      • 'order_id' stays (no drift)
      • 'amount' changed from float -> string  (type drift)
      • 'status' column REMOVED               (removed column drift)
      • 'region' NEW column added              (added column drift)
    """
    import numpy as np
    rng = np.random.default_rng(99)
    df = pd.DataFrame({
        "order_id":    range(101, n + 101),
        "customer_id": rng.integers(1000, 9999, size=n),
        "amount":      [f"${v:.2f}" for v in rng.uniform(10.0, 500.0, size=n)],  # TYPE DRIFT
        "region":      rng.choice(["APAC", "EMEA", "AMER"], size=n),              # NEW COLUMN
        # 'status' intentionally absent                                            # REMOVED
    })
    return df


def post_drift_data(n: int = 50) -> pd.DataFrame:
    """Matches evolved schema v2 cleanly."""
    import numpy as np
    rng = np.random.default_rng(7)
    return pd.DataFrame({
        "order_id":    range(201, n + 201),
        "customer_id": rng.integers(1000, 9999, size=n),
        "amount":      rng.uniform(10.0, 500.0, size=n).round(2),
        "region":      rng.choice(["APAC", "EMEA", "AMER"], size=n),
        # 'status' not present — matches v2 schema after healing
    })


# ── Display helpers ───────────────────────────────────────────────────────────

def print_run_summary(title: str, summary: dict) -> None:
    t = Table(title=title, show_header=True, header_style="bold cyan")
    t.add_column("Field", style="dim")
    t.add_column("Value")
    for k, v in summary.items():
        t.add_row(str(k), str(v))
    console.print(t)


def print_schema_history(registry: SchemaRegistry, source: str) -> None:
    history = registry.get_history(source)
    t = Table(title=f"Schema History — {source}", show_header=True, header_style="bold green")
    t.add_column("Version")
    t.add_column("Columns (name: type)")
    t.add_column("Registered At")
    for version, schema, ts in history:
        cols = ", ".join(f"{c}: {d}" for c, d in schema.items())
        t.add_row(str(version), cols, str(ts))
    console.print(t)


def print_quarantine_summary(store: QuarantineStore) -> None:
    stats = store.stats(pipeline_name="orders_pipeline")
    t = Table(title="Quarantine Stats", show_header=True, header_style="bold red")
    t.add_column("Metric")
    t.add_column("Value")
    t.add_row("Total quarantined", str(stats["total"]))
    t.add_row("Unresolved", str(stats["unresolved"]))
    t.add_row("Resolved", str(stats["resolved"]))
    for err_type, cnt in stats.get("by_error_type", {}).items():
        t.add_row(f"  [{err_type}]", str(cnt))
    console.print(t)


# ── Main demo ─────────────────────────────────────────────────────────────────

def main() -> None:
    console.rule("[bold blue]Self-Healing ETL Framework — Demo")

    registry = SchemaRegistry(CFG.schema_registry.db_url)
    store = QuarantineStore(CFG.quarantine.db_url)

    # ── Run 1: Baseline ───────────────────────────────────────────────────────
    console.print(Panel("[bold]Run 1 of 3:[/bold] Clean baseline data (100 rows)", style="green"))
    summary1 = etl_flow(
        source_name="orders",
        source_type="dataframe",
        destination_type="memory",
        config=CFG,
        source_df=baseline_data(),
    )
    print_run_summary("Run 1 Summary", summary1)

    # ── Run 2: Drifted ────────────────────────────────────────────────────────
    console.print(Panel(
        "[bold]Run 2 of 3:[/bold] Schema drift — amount(str), +region, -status",
        style="yellow",
    ))
    summary2 = etl_flow(
        source_name="orders",
        source_type="dataframe",
        destination_type="memory",
        config=CFG,
        source_df=drifted_data(),
    )
    print_run_summary("Run 2 Summary", summary2)

    # ── Run 3: Post-drift (evolved schema) ────────────────────────────────────
    console.print(Panel("[bold]Run 3 of 3:[/bold] Post-drift clean data (50 rows)", style="green"))
    summary3 = etl_flow(
        source_name="orders",
        source_type="dataframe",
        destination_type="memory",
        config=CFG,
        source_df=post_drift_data(),
    )
    print_run_summary("Run 3 Summary", summary3)

    # ── Final state report ────────────────────────────────────────────────────
    console.rule("[bold]Final State")
    print_schema_history(registry, "orders")
    print_quarantine_summary(store)

    console.rule("[bold blue]Demo Complete")


if __name__ == "__main__":
    main()

"""
Benchmark VQE (volcano + pipeline modes) against DuckDB on TPC-H–style queries.

Run:
    cd vectorized-query-engine
    pip install -e ".[dev]"
    python -m benchmarks.bench [--sf 0.1] [--runs 3]
"""
from __future__ import annotations

import argparse
import time
from typing import Callable

import duckdb
import pyarrow as pa
from rich.console import Console
from rich.table import Table

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from benchmarks.data_gen import generate_lineitem, generate_orders
from vqe import Engine


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

QUERIES = {
    "Q1 – Aggregate + group by + filter": {
        "vqe": """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity) AS sum_qty,
                   SUM(l_extendedprice) AS sum_base_price,
                   SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                   COUNT(*) AS count_order
            FROM lineitem
            WHERE l_shipdate <= '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """,
        "duckdb": """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity) AS sum_qty,
                   SUM(l_extendedprice) AS sum_base_price,
                   SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                   COUNT(*) AS count_order
            FROM lineitem
            WHERE l_shipdate <= '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """,
    },
    "Q6 – Selective filter + aggregate": {
        "vqe": """
            SELECT SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE l_shipdate >= '1994-01-01'
              AND l_shipdate < '1995-01-01'
              AND l_discount BETWEEN 0.05 AND 0.07
              AND l_quantity < 24
        """,
        "duckdb": """
            SELECT SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE l_shipdate >= '1994-01-01'
              AND l_shipdate < '1995-01-01'
              AND l_discount BETWEEN 0.05 AND 0.07
              AND l_quantity < 24
        """,
    },
    "Scan + filter (no agg)": {
        "vqe": """
            SELECT l_orderkey, l_extendedprice, l_discount
            FROM lineitem
            WHERE l_discount > 0.08 AND l_quantity < 10
        """,
        "duckdb": """
            SELECT l_orderkey, l_extendedprice, l_discount
            FROM lineitem
            WHERE l_discount > 0.08 AND l_quantity < 10
        """,
    },
    "Count with GROUP BY": {
        "vqe": """
            SELECT l_shipmode, COUNT(*) AS cnt
            FROM lineitem
            GROUP BY l_shipmode
            ORDER BY cnt DESC
        """,
        "duckdb": """
            SELECT l_shipmode, COUNT(*) AS cnt
            FROM lineitem
            GROUP BY l_shipmode
            ORDER BY cnt DESC
        """,
    },
}


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def timeit(fn: Callable, runs: int = 3) -> tuple[float, float]:
    """Return (min_ms, avg_ms) over `runs` calls."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return min(times), sum(times) / len(times)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_benchmarks(sf: float = 0.1, runs: int = 3) -> None:
    console = Console()
    console.print(f"\n[bold cyan]Generating TPC-H data (SF={sf})…[/bold cyan]")
    lineitem = generate_lineitem(sf)
    orders = generate_orders(sf)
    console.print(f"  lineitem: {lineitem.num_rows:,} rows")
    console.print(f"  orders:   {orders.num_rows:,} rows\n")

    # --- VQE setup ---
    engine = Engine()
    engine.register("lineitem", lineitem)
    engine.register("orders", orders)

    # --- DuckDB setup ---
    con = duckdb.connect()
    con.register("lineitem", lineitem)
    con.register("orders", orders)
    # Warm up DuckDB (JIT compile)
    for q in QUERIES.values():
        try:
            con.execute(q["duckdb"]).fetchall()
        except Exception:
            pass

    # --- Results table ---
    tbl = Table(title=f"VQE vs DuckDB  (SF={sf}, {lineitem.num_rows:,} lineitem rows, {runs} runs)")
    tbl.add_column("Query", style="bold")
    tbl.add_column("DuckDB min (ms)", justify="right")
    tbl.add_column("VQE volcano min (ms)", justify="right")
    tbl.add_column("VQE pipeline min (ms)", justify="right")
    tbl.add_column("volcano / DuckDB", justify="right")
    tbl.add_column("pipeline / DuckDB", justify="right")

    for qname, qsql in QUERIES.items():
        console.print(f"[yellow]Running:[/yellow] {qname}")

        # DuckDB
        try:
            duck_min, _ = timeit(lambda: con.execute(qsql["duckdb"]).fetchall(), runs)
        except Exception as e:
            console.print(f"  [red]DuckDB error:[/red] {e}")
            duck_min = float("nan")

        # VQE volcano
        try:
            vqe_vol_min, _ = timeit(
                lambda: engine.execute(qsql["vqe"], mode="volcano"), runs
            )
        except Exception as e:
            console.print(f"  [red]VQE volcano error:[/red] {e}")
            vqe_vol_min = float("nan")

        # VQE pipeline
        try:
            vqe_pip_min, _ = timeit(
                lambda: engine.execute(qsql["vqe"], mode="pipeline"), runs
            )
        except Exception as e:
            console.print(f"  [red]VQE pipeline error:[/red] {e}")
            vqe_pip_min = float("nan")

        def ratio(a, b):
            if b == 0 or b != b:
                return "N/A"
            if a != a:
                return "error"
            r = a / b
            color = "green" if r < 5 else "yellow" if r < 20 else "red"
            return f"[{color}]{r:.1f}x[/{color}]"

        tbl.add_row(
            qname,
            f"{duck_min:.1f}",
            f"{vqe_vol_min:.1f}",
            f"{vqe_pip_min:.1f}",
            ratio(vqe_vol_min, duck_min),
            ratio(vqe_pip_min, duck_min),
        )

    console.print()
    console.print(tbl)
    console.print("\n[dim]Ratios: 1x = parity with DuckDB. Lower is better for VQE.[/dim]")
    console.print("[dim]DuckDB uses highly optimized C++ + SIMD; gap is expected.[/dim]")
    console.print("[dim]Both VQE modes use the same Arrow/SIMD kernels for compute.[/dim]\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", type=float, default=0.1, help="TPC-H scale factor")
    parser.add_argument("--runs", type=int, default=3, help="Timing runs per query")
    args = parser.parse_args()
    run_benchmarks(sf=args.sf, runs=args.runs)

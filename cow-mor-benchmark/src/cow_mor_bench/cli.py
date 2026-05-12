"""CLI entry point — `cow-mor-bench` command."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from cow_mor_bench.benchmark.metrics import compare
from cow_mor_bench.benchmark.runner import BenchmarkSuite, run_benchmark
from cow_mor_bench.compaction.model import build_amplification_curve, DEFAULT_CLUSTER
from cow_mor_bench.recommender.engine import recommend, recommend_from_params
from cow_mor_bench.workload.classifier import classify_trace
from cow_mor_bench.workload.patterns import PROFILES

console = Console()


def _header(text: str) -> None:
    console.print(Panel(f"[bold cyan]{text}[/]", expand=False))


def _print_metric_table(result, profile_name: str) -> None:
    rows = compare(result)
    t = Table(title=f"Profile: {profile_name}", box=box.SIMPLE_HEAD, show_lines=False)
    t.add_column("Metric", style="dim")
    t.add_column("CoW", justify="right")
    t.add_column("MoR", justify="right")
    t.add_column("Winner", justify="center")
    for r in rows:
        color = "green" if r.winner == "CoW" else ("yellow" if r.winner == "MoR" else "dim")
        t.add_row(r.metric, r.cow_value, r.mor_value, f"[{color}]{r.winner}[/]")
    console.print(t)


@click.group()
def main() -> None:
    """Copy-on-Write vs Merge-on-Read benchmark engine."""


@main.command()
@click.option("--profile", "-p", multiple=True, default=list(PROFILES.keys()),
              help="Workload profile(s) to benchmark")
@click.option("--schema", "-s", default="orders",
              type=click.Choice(["orders", "events", "inventory"]))
@click.option("--table-size", "-n", default=20_000, show_default=True)
@click.option("--ops", default=60, show_default=True, help="Number of operations per run")
@click.option("--compact-every", default=None, type=int,
              help="Trigger compaction every N operations (MoR)")
@click.option("--output-dir", "-o", default=None, help="Directory to save results JSON")
@click.option("--plot/--no-plot", default=False, help="Generate comparison charts")
def run(profile, schema, table_size, ops, compact_every, output_dir, plot):
    """Run benchmarks across workload profiles."""
    suite = BenchmarkSuite()

    for pname in profile:
        if pname not in PROFILES:
            console.print(f"[red]Unknown profile: {pname}[/]")
            continue
        prof = PROFILES[pname]
        _header(f"Running profile: {pname} | schema: {schema} | rows: {table_size:,}")

        with console.status(f"  CoW + MoR workload ({ops} ops)…"):
            result = run_benchmark(
                prof, schema_name=schema,
                table_size=table_size, n_ops=ops,
                compact_every=compact_every,
            )
        suite.add(result)
        _print_metric_table(result, pname)

        cow_cls = classify_trace(result.cow_trace)
        rec = recommend(result, cow_cls, table_name=f"{schema}_{pname}")
        console.print(Panel(
            rec.summary(),
            title="[bold green]Strategy Recommendation[/]",
            expand=False,
        ))

    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        export_data = []
        for r in suite.results:
            export_data.append({
                "profile": r.profile_name,
                "schema": r.schema_name,
                "cow_total_read_s": r.cow_trace.total_read_s,
                "cow_total_write_s": r.cow_trace.total_write_s,
                "mor_total_read_s": r.mor_trace.total_read_s,
                "mor_total_write_s": r.mor_trace.total_write_s,
                "read_speedup_cow": r.read_speedup_cow,
                "write_speedup_mor": r.write_speedup_mor,
                "space_overhead_mor": r.space_overhead_mor,
            })
        (out / "results.json").write_text(json.dumps(export_data, indent=2))
        console.print(f"\n[dim]Results saved to {out / 'results.json'}[/]")

    if plot:
        try:
            _plot_suite(suite, output_dir)
        except ImportError:
            console.print("[yellow]matplotlib not available — skipping plots[/]")


@main.command()
@click.option("--write-ratio", default=0.3, show_default=True)
@click.option("--update-fraction", default=0.1, show_default=True)
@click.option("--avg-batch-rows", default=1000, show_default=True)
@click.option("--full-scan-ratio", default=0.3, show_default=True)
@click.option("--point-read-ratio", default=0.4, show_default=True)
@click.option("--data-gb", default=10.0, show_default=True)
@click.option("--reads-per-hour", default=100.0, show_default=True)
@click.option("--table-name", default="my_table", show_default=True)
def recommend_cmd(write_ratio, update_fraction, avg_batch_rows, full_scan_ratio,
                  point_read_ratio, data_gb, reads_per_hour, table_name):
    """Get a strategy recommendation from workload parameters (no benchmark run)."""
    rec = recommend_from_params(
        write_ratio=write_ratio,
        update_fraction_of_table=update_fraction,
        avg_batch_rows=avg_batch_rows,
        full_scan_ratio=full_scan_ratio,
        point_read_ratio=point_read_ratio,
        data_gb=data_gb,
        read_ops_per_hour=reads_per_hour,
        table_name=table_name,
    )
    console.print(Panel(rec.summary(), title="[bold green]Strategy Recommendation[/]", expand=False))


@main.command("compaction-model")
@click.option("--data-gb", default=10.0, show_default=True)
@click.option("--bytes-per-delta-mb", default=5.0, show_default=True)
@click.option("--max-delta-files", default=50, show_default=True)
@click.option("--plot/--no-plot", default=False)
def compaction_model_cmd(data_gb, bytes_per_delta_mb, max_delta_files, plot):
    """Show read amplification curve as delta files accumulate (MoR)."""
    data_bytes = int(data_gb * 1024**3)
    bytes_per_delta = int(bytes_per_delta_mb * 1024 * 1024)
    curve = build_amplification_curve(data_bytes, bytes_per_delta, max_delta_files)

    t = Table(title="MoR Read Amplification vs Delta File Count", box=box.SIMPLE_HEAD)
    t.add_column("Delta files", justify="right")
    t.add_column("Amplification", justify="right")
    t.add_column("Extra latency (ms)", justify="right")

    for row in curve[::max(1, max_delta_files // 20)]:
        amp = row["amplification"]
        color = "green" if amp < 1.2 else ("yellow" if amp < 2.0 else "red")
        t.add_row(
            str(row["delta_files"]),
            f"[{color}]{amp:.2f}x[/]",
            f"{row['extra_latency_ms']:.1f}",
        )
    console.print(t)

    if plot:
        try:
            _plot_amplification(curve, max_delta_files)
        except ImportError:
            console.print("[yellow]matplotlib not available — skipping plot[/]")


@main.command("list-profiles")
def list_profiles():
    """List available workload profiles."""
    t = Table(title="Workload Profiles", box=box.SIMPLE_HEAD)
    t.add_column("Name")
    t.add_column("Class")
    t.add_column("Insert %")
    t.add_column("Update %")
    t.add_column("Delete %")
    t.add_column("Scan %")
    t.add_column("Point read %")
    for name, p in PROFILES.items():
        t.add_row(
            name, p.cls.value,
            f"{p.insert_weight:.0%}", f"{p.update_weight:.0%}",
            f"{p.delete_weight:.0%}", f"{p.full_scan_weight:.0%}",
            f"{p.point_read_weight:.0%}",
        )
    console.print(t)


# ------------------------------------------------------------------
# Plot helpers
# ------------------------------------------------------------------

def _plot_suite(suite: "BenchmarkSuite", output_dir: str | None) -> None:
    import matplotlib.pyplot as plt
    import numpy as np

    profiles = [r.profile_name for r in suite.results]
    cow_reads = [r.cow_trace.total_read_s for r in suite.results]
    mor_reads = [r.mor_trace.total_read_s for r in suite.results]
    cow_writes = [r.cow_trace.total_write_s for r in suite.results]
    mor_writes = [r.mor_trace.total_write_s for r in suite.results]

    x = np.arange(len(profiles))
    width = 0.35

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("CoW vs MoR: Total Wall Time by Workload Profile", fontweight="bold")

    ax1.bar(x - width / 2, cow_reads, width, label="CoW", color="#2196F3")
    ax1.bar(x + width / 2, mor_reads, width, label="MoR", color="#FF9800")
    ax1.set_title("Total Read Time (s)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(profiles, rotation=30, ha="right")
    ax1.set_ylabel("seconds")
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    ax2.bar(x - width / 2, cow_writes, width, label="CoW", color="#2196F3")
    ax2.bar(x + width / 2, mor_writes, width, label="MoR", color="#FF9800")
    ax2.set_title("Total Write Time (s)")
    ax2.set_xticks(x)
    ax2.set_xticklabels(profiles, rotation=30, ha="right")
    ax2.set_ylabel("seconds")
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    if output_dir:
        out = Path(output_dir) / "benchmark_comparison.png"
        plt.savefig(out, dpi=150, bbox_inches="tight")
        console.print(f"[dim]Chart saved to {out}[/]")
    else:
        plt.show()


def _plot_amplification(curve: list[dict], max_delta_files: int) -> None:
    import matplotlib.pyplot as plt

    x = [r["delta_files"] for r in curve]
    amp = [r["amplification"] for r in curve]
    latency = [r["extra_latency_ms"] for r in curve]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    fig.suptitle("MoR Read Amplification as Delta Files Accumulate", fontweight="bold")

    ax1.plot(x, amp, color="#FF9800", linewidth=2)
    ax1.axhline(1.0, color="gray", linestyle="--", alpha=0.5, label="ideal (1.0x)")
    ax1.axhline(1.5, color="red", linestyle=":", alpha=0.7, label="compaction trigger (1.5x)")
    ax1.set_xlabel("Delta file count")
    ax1.set_ylabel("Read amplification factor")
    ax1.set_title("Amplification Factor")
    ax1.legend()
    ax1.grid(alpha=0.3)

    ax2.fill_between(x, latency, alpha=0.3, color="#F44336")
    ax2.plot(x, latency, color="#F44336", linewidth=2)
    ax2.set_xlabel("Delta file count")
    ax2.set_ylabel("Extra latency (ms)")
    ax2.set_title("Additional Read Latency")
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    plt.show()

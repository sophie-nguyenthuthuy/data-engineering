"""Test runner: orchestrates cluster, workload clients, nemeses, and verification."""

from __future__ import annotations

import datetime
import os
import time
from dataclasses import dataclass, field
from typing import List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from .history import History
from .checker import check as linearizability_check
from .models import RegisterModel
from ..chaos.nemesis import Nemesis, PeriodicNemesis
from ..chaos.network import NetworkPartitionNemesis, NetworkLatencyNemesis
from ..chaos.clock import ClockSkewNemesis
from ..chaos.process import ProcessCrashNemesis
from ..pipeline.cluster import Cluster
from ..pipeline.client import Client
from ..reporter.html import generate, FaultEvent, ReportMeta

console = Console()


@dataclass
class TestConfig:
    node_count: int = 3
    client_count: int = 5
    test_duration_s: float = 10.0
    keys: List[str] = field(default_factory=lambda: ["x", "y"])
    request_timeout_s: float = 1.5
    enable_network_partitions: bool = True
    enable_clock_skew: bool = True
    enable_process_crashes: bool = True
    max_clock_skew_s: float = 3.0
    output_dir: str = "reports"
    model: str = "register"


@dataclass
class TestResult:
    config: TestConfig
    linearizable: bool
    ops_count: int
    checked_states: int
    check_elapsed_s: float
    test_elapsed_s: float
    report_path: str
    fault_events: List[FaultEvent]


def run(config: TestConfig) -> TestResult:
    console.print(Panel.fit(
        f"[bold cyan]Jepsen Linearizability Test[/]\n"
        f"nodes={config.node_count}  clients={config.client_count}  "
        f"duration={config.test_duration_s}s  model={config.model}",
        border_style="cyan",
    ))

    cluster = Cluster(
        node_count=config.node_count,
        request_timeout=config.request_timeout_s,
    )
    history = History()
    fault_events: List[FaultEvent] = []
    t0 = time.monotonic()

    # ── Start cluster ──────────────────────────────────────────────────
    console.print("[yellow]Starting cluster...[/]")
    cluster.start()
    time.sleep(0.3)  # let nodes initialize

    # ── Build nemeses ──────────────────────────────────────────────────
    nemeses: List[PeriodicNemesis] = []
    nemesis_descriptions: List[str] = []

    if config.enable_network_partitions:
        net_nemesis = NetworkPartitionNemesis(
            cluster.partition_table,
            cluster.node_ids,
        )
        wrapped = PeriodicNemesis(net_nemesis, fault_duration=1.5, heal_duration=2.5)
        nemeses.append(wrapped)
        nemesis_descriptions.append(net_nemesis.describe())

    if config.enable_clock_skew:
        clock_nemesis = ClockSkewNemesis(
            cluster.clock_registry,
            cluster.node_ids,
            max_skew_seconds=config.max_clock_skew_s,
        )
        wrapped = PeriodicNemesis(clock_nemesis, fault_duration=2.0, heal_duration=3.0)
        nemeses.append(wrapped)
        nemesis_descriptions.append(clock_nemesis.describe())

    if config.enable_process_crashes:
        crash_nemesis = ProcessCrashNemesis(
            cluster.process_registry,
            restart_fn=cluster.restart_node,
            restart_delay=1.5,
        )
        wrapped = PeriodicNemesis(crash_nemesis, fault_duration=1.0, heal_duration=4.0)
        nemeses.append(wrapped)
        nemesis_descriptions.append(crash_nemesis.describe())

    # Monkey-patch nemeses to record fault events
    for pn in nemeses:
        _wrap_fault_recording(pn, fault_events, t0)

    # ── Start clients ──────────────────────────────────────────────────
    clients = [
        Client(i, cluster, history, config.keys)
        for i in range(config.client_count)
    ]

    console.print("[yellow]Starting nemeses and clients...[/]")
    for n in nemeses:
        n.start()
    for c in clients:
        c.start()

    # ── Progress display ───────────────────────────────────────────────
    start = time.monotonic()
    while time.monotonic() - start < config.test_duration_s:
        elapsed = time.monotonic() - start
        ops = len(history)
        partitions = cluster.partition_table.active_partitions()
        dead = cluster.process_registry.dead_nodes()
        clock_offsets = cluster.clock_registry.offsets()

        # Sync clock offsets to shared array for nodes
        cluster.apply_clock_offsets()

        status = (
            f"[cyan]{elapsed:.1f}/{config.test_duration_s}s[/]  "
            f"ops=[white]{ops}[/]  "
            f"partitions=[red]{len(partitions)}[/]  "
            f"dead=[red]{dead}[/]  "
            f"skewed=[yellow]{list(clock_offsets.keys())}[/]"
        )
        console.print(f"\r{status}", end="")
        time.sleep(0.5)

    console.print()

    # ── Teardown ───────────────────────────────────────────────────────
    console.print("[yellow]Stopping clients and nemeses...[/]")
    for c in clients:
        c.stop()
    for n in nemeses:
        n.stop()
    cluster.stop()

    test_elapsed = time.monotonic() - t0

    # ── Check linearizability ──────────────────────────────────────────
    console.print("[yellow]Running linearizability check...[/]")
    entries = history.entries()
    model = RegisterModel()

    check_result = linearizability_check(entries, model)

    # ── Report ─────────────────────────────────────────────────────────
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(config.output_dir, f"report_{ts}.html")

    meta = ReportMeta(
        timestamp=datetime.datetime.now().isoformat(timespec="seconds"),
        test_duration_s=test_elapsed,
        node_count=config.node_count,
        client_count=config.client_count,
        nemesis_description=" + ".join(nemesis_descriptions) or "none",
    )

    generate(
        ops=history.ops(),
        entries=entries,
        result=check_result,
        meta=meta,
        faults=fault_events,
        output_path=report_path,
    )

    # ── Print summary ──────────────────────────────────────────────────
    _print_summary(check_result, entries, fault_events, test_elapsed, report_path)

    return TestResult(
        config=config,
        linearizable=check_result.linearizable,
        ops_count=len(entries),
        checked_states=check_result.checked_ops,
        check_elapsed_s=check_result.elapsed_seconds,
        test_elapsed_s=test_elapsed,
        report_path=report_path,
        fault_events=fault_events,
    )


def _wrap_fault_recording(
    periodic: PeriodicNemesis,
    events: List[FaultEvent],
    t0: float,
) -> None:
    orig_loop = periodic._loop

    def patched_loop():
        import random, time, threading
        while periodic._running:
            jitter = random.uniform(0, periodic._heal_duration * 0.5)
            time.sleep(periodic._heal_duration + jitter)
            if not periodic._running:
                break
            periodic._nemesis.start()
            events.append(FaultEvent(
                type=periodic._nemesis.describe().split("(")[0],
                time=time.monotonic() - t0,
                detail=periodic._nemesis.describe(),
            ))
            time.sleep(periodic._fault_duration + random.uniform(0, periodic._fault_duration * 0.5))
            periodic._nemesis.stop()

    periodic._loop = patched_loop
    # Restart the thread with the patched loop
    if periodic._thread and periodic._thread.is_alive():
        periodic._running = False
        periodic._thread.join(timeout=1)
        periodic._running = True
        periodic._thread = __import__("threading").Thread(target=patched_loop, daemon=True)
        periodic._thread.start()


def _print_summary(
    result,
    entries,
    fault_events,
    elapsed: float,
    report_path: str,
) -> None:
    table = Table(box=box.ROUNDED, border_style="cyan")
    table.add_column("Metric", style="cyan")
    table.add_column("Value")

    verdict = "[bold green]✓ LINEARIZABLE[/]" if result.linearizable else "[bold red]✗ NOT LINEARIZABLE[/]"
    table.add_row("Verdict", verdict)
    table.add_row("Operations", str(len(entries)))
    table.add_row("States checked", str(result.checked_ops))
    table.add_row("Check time", f"{result.elapsed_seconds:.3f}s")
    table.add_row("Total test time", f"{elapsed:.2f}s")
    table.add_row("Fault events", str(len(fault_events)))
    table.add_row("Report", f"[link={report_path}]{report_path}[/link]")

    console.print(table)

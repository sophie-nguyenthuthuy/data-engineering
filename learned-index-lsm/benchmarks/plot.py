"""
Generate benchmark result plots from ``benchmark_results/results.json``.

Run with::

    python -m benchmarks.plot
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


_RESULTS_PATH = Path("benchmark_results/results.json")
_DRIFT_PATH = Path("benchmark_results/drift.json")
_PLOTS_DIR = Path("plots")

_COLORS = {
    "RMI": "#2196F3",
    "BTree": "#FF5722",
    "Bloom": "#4CAF50",
    "BinarySearch": "#9C27B0",
}


def load_results() -> list[dict]:
    if not _RESULTS_PATH.exists():
        print(f"Run benchmarks first: python -m benchmarks.runner")
        sys.exit(1)
    with open(_RESULTS_PATH) as f:
        return json.load(f)


def plot_latency_by_workload(results: list[dict]) -> None:
    workloads = sorted(set(r["workload"] for r in results))
    indexes = ["RMI", "BTree", "BinarySearch"]

    fig, axes = plt.subplots(1, len(workloads), figsize=(16, 5), sharey=False)
    if len(workloads) == 1:
        axes = [axes]

    for ax, wl in zip(axes, workloads):
        subset = [r for r in results if r["workload"] == wl and r["index"] in indexes]
        idx_names = [r["index"] for r in subset]
        means = [r["mean_lookup_ns"] for r in subset]
        p99s = [r["p99_lookup_ns"] for r in subset]

        x = np.arange(len(idx_names))
        w = 0.35
        bars_mean = ax.bar(x - w / 2, means, w, label="Mean", color=[_COLORS[i] for i in idx_names], alpha=0.85)
        bars_p99 = ax.bar(x + w / 2, p99s, w, label="P99", color=[_COLORS[i] for i in idx_names], alpha=0.45, hatch="//")

        ax.set_title(wl, fontsize=11)
        ax.set_xticks(x)
        ax.set_xticklabels(idx_names, rotation=15, ha="right", fontsize=9)
        ax.set_ylabel("Latency (ns)" if ax == axes[0] else "")
        ax.grid(axis="y", alpha=0.3)

    handles = [
        plt.Rectangle((0, 0), 1, 1, color=c, alpha=0.85, label=n)
        for n, c in _COLORS.items() if n in indexes
    ]
    fig.legend(handles=handles, loc="upper right", fontsize=9)
    fig.suptitle("Lookup Latency: RMI vs BTree vs Binary Search", fontsize=13, fontweight="bold")
    plt.tight_layout()
    _PLOTS_DIR.mkdir(exist_ok=True)
    out = _PLOTS_DIR / "latency_by_workload.png"
    plt.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close()


def plot_memory_comparison(results: list[dict]) -> None:
    workloads = sorted(set(r["workload"] for r in results))
    indexes = ["RMI", "BTree", "Bloom"]

    mem_data: dict[str, list[float]] = {idx: [] for idx in indexes}
    for wl in workloads:
        for idx in indexes:
            match = next((r for r in results if r["workload"] == wl and r["index"] == idx), None)
            mem_data[idx].append(match["memory_kb"] if match else 0)

    x = np.arange(len(workloads))
    w = 0.25
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, idx in enumerate(indexes):
        ax.bar(x + i * w, mem_data[idx], w, label=idx, color=_COLORS[idx], alpha=0.85)

    ax.set_xticks(x + w)
    ax.set_xticklabels(workloads, rotation=15, ha="right")
    ax.set_ylabel("Memory (KB)")
    ax.set_title("Memory Footprint by Index Type", fontsize=13, fontweight="bold")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    out = _PLOTS_DIR / "memory_comparison.png"
    plt.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close()


def plot_rmi_search_range(results: list[dict]) -> None:
    rmi_results = [r for r in results if r["index"] == "RMI"]
    workloads = [r["workload"] for r in rmi_results]
    ranges = [r["rmi_mean_range"] for r in rmi_results]

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(workloads, ranges, color=_COLORS["RMI"], alpha=0.85)
    for bar, val in zip(bars, ranges):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{val:.1f}", ha="center", va="bottom", fontsize=9)

    ax.set_ylabel("Mean Binary-Search Range (entries)")
    ax.set_title("RMI Mean Search Range by Distribution", fontsize=13, fontweight="bold")
    ax.set_xticklabels(workloads, rotation=15, ha="right")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    out = _PLOTS_DIR / "rmi_search_range.png"
    plt.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close()


def plot_drift_timeline() -> None:
    if not _DRIFT_PATH.exists():
        return
    with open(_DRIFT_PATH) as f:
        drift = json.load(f)

    fig, ax = plt.subplots(figsize=(10, 3))
    p1 = drift["phase1_queries"]
    p2 = drift.get("phase2_queries", 0)
    total = p1 + p2

    ax.axvspan(0, p1, alpha=0.15, color="green", label="Phase 1 (stable)")
    ax.axvspan(p1, total, alpha=0.15, color="red", label="Phase 2 (drift)")
    ax.axvline(p1, color="black", linestyle="--", linewidth=1.2, label="Drift onset")

    for det, col in [("adwin_detected_at", "#2196F3"), ("ks_detected_at", "#FF5722")]:
        v = drift.get(det)
        if v:
            ax.axvline(v, color=col, linestyle="-.", linewidth=1.5, label=f"{det.split('_')[0].upper()} detected")

    ax.set_xlim(0, total)
    ax.set_xlabel("Query number")
    ax.set_title("Drift Detection Timeline", fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.set_yticks([])
    plt.tight_layout()
    out = _PLOTS_DIR / "drift_timeline.png"
    plt.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close()


def main() -> None:
    results = load_results()
    _PLOTS_DIR.mkdir(exist_ok=True)
    plot_latency_by_workload(results)
    plot_memory_comparison(results)
    plot_rmi_search_range(results)
    plot_drift_timeline()
    print(f"\nAll plots written to ./{_PLOTS_DIR}/")


if __name__ == "__main__":
    main()

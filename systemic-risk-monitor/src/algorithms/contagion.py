"""
Contagion cascade simulation.

Given the current NET_EXPOSURE graph, simulate what happens if one institution
fails (or suffers a liquidity shock). Uses a threshold-cascade model:

  An institution "fails" if the fraction of its expected inflow that is lost
  (because counterparties fail) exceeds `cascade_threshold`.

Returns the set of institutions that would fail and the cascade depth.
"""

import logging
from dataclasses import dataclass, field

import networkx as nx

from src.config import settings
from src.algorithms.cycle_detection import build_digraph

log = logging.getLogger(__name__)


@dataclass
class ContagionResult:
    seed_node: str
    failed_nodes: list[str]
    cascade_depth: int
    fraction_failed: float    # failed / total
    total_exposure_lost: float  # $M wiped out


def simulate_cascade(
    edges: list[dict],
    seed_node: str,
    shock_fraction: float = None,
    cascade_threshold: float = None,
) -> ContagionResult:
    """
    BFS cascade from seed_node.

    shock_fraction: fraction of seed_node's outbound exposure that evaporates.
    cascade_threshold: fraction of inbound exposure that must be lost before
                       a node itself fails.
    """
    if shock_fraction is None:
        shock_fraction = settings.liquidity_shock_pct
    if cascade_threshold is None:
        cascade_threshold = settings.contagion_cascade_threshold

    G = build_digraph(edges)
    if seed_node not in G:
        return ContagionResult(
            seed_node=seed_node,
            failed_nodes=[],
            cascade_depth=0,
            fraction_failed=0.0,
            total_exposure_lost=0.0,
        )

    # Pre-compute total inbound exposure per node
    total_in: dict[str, float] = {n: 0.0 for n in G.nodes()}
    for u, v, data in G.edges(data=True):
        total_in[v] += data.get("weight", 0)

    failed = {seed_node}
    frontier = {seed_node}
    depth = 0
    exposure_lost = 0.0

    # Initial shock from seed
    for _, v, data in G.out_edges(seed_node, data=True):
        exposure_lost += data.get("weight", 0) * shock_fraction

    while frontier:
        next_frontier: set[str] = set()
        for failed_node in frontier:
            # Propagate to all nodes that receive flows from this failed node
            for _, receiver, data in G.out_edges(failed_node, data=True):
                if receiver in failed:
                    continue
                lost = data.get("weight", 0) * shock_fraction
                t_in = total_in.get(receiver, 0)
                if t_in > 0 and (lost / t_in) >= cascade_threshold:
                    failed.add(receiver)
                    next_frontier.add(receiver)
                    exposure_lost += sum(
                        d.get("weight", 0) for _, _, d in G.out_edges(receiver, data=True)
                    ) * shock_fraction

        frontier = next_frontier
        if frontier:
            depth += 1

    total_nodes = G.number_of_nodes()
    failed_list = list(failed - {seed_node})  # exclude seed from "collateral damage"

    return ContagionResult(
        seed_node=seed_node,
        failed_nodes=failed_list,
        cascade_depth=depth,
        fraction_failed=round(len(failed) / max(total_nodes, 1), 4),
        total_exposure_lost=round(exposure_lost, 2),
    )


def worst_case_cascade(edges: list[dict]) -> ContagionResult:
    """Run cascade simulation for every node and return the worst outcome."""
    G = build_digraph(edges)
    if not G.nodes():
        return ContagionResult("none", [], 0, 0.0, 0.0)

    worst: ContagionResult | None = None
    for node in G.nodes():
        result = simulate_cascade(edges, node)
        if worst is None or result.fraction_failed > worst.fraction_failed:
            worst = result

    return worst  # type: ignore[return-value]

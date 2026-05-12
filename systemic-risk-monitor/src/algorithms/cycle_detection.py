"""
Cycle detection in the interbank exposure graph.

A cycle A→B→C→A means institution A is ultimately exposed to itself through
intermediaries — a classic circular dependency / daisy-chain lending pattern.

We use Johnson's algorithm (via NetworkX) on the NET_EXPOSURE graph.
Only cycles above a minimum total exposure threshold are surfaced as risks.
"""

import logging
from dataclasses import dataclass

import networkx as nx

from src.config import settings

log = logging.getLogger(__name__)


@dataclass
class Cycle:
    nodes: list[str]
    total_exposure: float   # sum of edge weights around the cycle ($M)
    min_edge: float         # bottleneck — smallest single exposure in cycle
    risk_score: float       # normalized [0, 1]


def build_digraph(edges: list[dict]) -> nx.DiGraph:
    """Build a weighted directed graph from NET_EXPOSURE edge records."""
    G = nx.DiGraph()
    for e in edges:
        G.add_edge(
            e["source"],
            e["target"],
            weight=e["total"],
            count=e["count"],
        )
    return G


def detect_cycles(
    edges: list[dict],
    min_cycle_length: int = None,
    min_total_exposure: float = 100.0,  # $M
) -> list[Cycle]:
    """
    Return all simple cycles in the exposure graph that meet the thresholds.
    Johnson's algorithm — O((V+E)(C+1)) where C = number of cycles.
    """
    if min_cycle_length is None:
        min_cycle_length = settings.cycle_alert_min_length

    G = build_digraph(edges)
    if G.number_of_nodes() == 0:
        return []

    cycles: list[Cycle] = []
    try:
        for raw_cycle in nx.simple_cycles(G):
            if len(raw_cycle) < min_cycle_length:
                continue

            # Compute total exposure around the cycle
            edge_weights = []
            valid = True
            for i, node in enumerate(raw_cycle):
                nxt = raw_cycle[(i + 1) % len(raw_cycle)]
                if G.has_edge(node, nxt):
                    edge_weights.append(G[node][nxt]["weight"])
                else:
                    valid = False
                    break

            if not valid or not edge_weights:
                continue

            total = sum(edge_weights)
            if total < min_total_exposure:
                continue

            min_edge = min(edge_weights)
            # Risk score: log-scaled relative to $10B max exposure cap
            risk_score = min(1.0, total / 10_000)

            cycles.append(
                Cycle(
                    nodes=raw_cycle,
                    total_exposure=round(total, 2),
                    min_edge=round(min_edge, 2),
                    risk_score=round(risk_score, 4),
                )
            )
    except Exception as exc:
        log.error("Cycle detection error: %s", exc)

    # Sort by risk score descending
    cycles.sort(key=lambda c: c.risk_score, reverse=True)
    return cycles

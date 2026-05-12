"""
Centrality and liquidity concentration analysis.

Metrics computed:
- Betweenness centrality: institutions that sit on many shortest paths
  (removing them would disconnect large parts of the network)
- In-degree / out-degree weighted: who owes / is owed the most
- Herfindahl-Hirschman Index (HHI) on total exposures: market concentration
- PageRank on exposure graph: systemic importance weighting
"""

import logging
from dataclasses import dataclass, field

import networkx as nx
import numpy as np

from src.config import settings
from src.algorithms.cycle_detection import build_digraph

log = logging.getLogger(__name__)


@dataclass
class NodeMetrics:
    node_id: str
    betweenness: float       # [0, 1]
    pagerank: float          # [0, 1]
    in_exposure: float       # total $M owed to this node
    out_exposure: float      # total $M this node has lent out
    net_exposure: float      # out - in (positive = net lender)
    degree_in: int
    degree_out: int
    is_systemic: bool        # betweenness > threshold


@dataclass
class ConcentrationMetrics:
    hhi: float                          # [0, 1] — 1 = monopoly
    top_nodes: list[str] = field(default_factory=list)   # nodes holding >10% share
    gini: float = 0.0                   # Gini coefficient of exposure distribution
    is_concentrated: bool = False


def compute_node_metrics(edges: list[dict]) -> list[NodeMetrics]:
    G = build_digraph(edges)
    if G.number_of_nodes() == 0:
        return []

    betweenness = nx.betweenness_centrality(G, weight="weight", normalized=True)
    pagerank = nx.pagerank(G, weight="weight", max_iter=200)

    # Weighted in/out exposure per node
    in_exp: dict[str, float] = {}
    out_exp: dict[str, float] = {}
    for u, v, data in G.edges(data=True):
        out_exp[u] = out_exp.get(u, 0) + data.get("weight", 0)
        in_exp[v] = in_exp.get(v, 0) + data.get("weight", 0)

    threshold = settings.betweenness_threshold
    metrics = []
    for node in G.nodes():
        b = betweenness.get(node, 0)
        metrics.append(
            NodeMetrics(
                node_id=node,
                betweenness=round(b, 6),
                pagerank=round(pagerank.get(node, 0), 6),
                in_exposure=round(in_exp.get(node, 0), 2),
                out_exposure=round(out_exp.get(node, 0), 2),
                net_exposure=round(out_exp.get(node, 0) - in_exp.get(node, 0), 2),
                degree_in=G.in_degree(node),
                degree_out=G.out_degree(node),
                is_systemic=b >= threshold,
            )
        )

    metrics.sort(key=lambda m: m.betweenness, reverse=True)
    return metrics


def compute_concentration(edges: list[dict]) -> ConcentrationMetrics:
    """
    Compute HHI and Gini coefficient across the lending exposure distribution.
    Uses total outbound exposure per institution as market share.
    """
    G = build_digraph(edges)
    if G.number_of_nodes() == 0:
        return ConcentrationMetrics(hhi=0.0)

    out_exp: dict[str, float] = {}
    for u, v, data in G.edges(data=True):
        out_exp[u] = out_exp.get(u, 0) + data.get("weight", 0)

    if not out_exp:
        return ConcentrationMetrics(hhi=0.0)

    total = sum(out_exp.values())
    shares = np.array([v / total for v in out_exp.values()])
    hhi = float(np.sum(shares ** 2))

    # Gini coefficient
    shares_sorted = np.sort(shares)
    n = len(shares_sorted)
    cumsum = np.cumsum(shares_sorted)
    gini = float((2 * np.sum((np.arange(1, n + 1)) * shares_sorted) - (n + 1)) / (n * np.sum(shares_sorted)))

    top_nodes = [k for k, v in out_exp.items() if v / total >= 0.10]

    return ConcentrationMetrics(
        hhi=round(hhi, 6),
        top_nodes=top_nodes,
        gini=round(max(0, gini), 4),
        is_concentrated=hhi > settings.concentration_hhi_threshold,
    )

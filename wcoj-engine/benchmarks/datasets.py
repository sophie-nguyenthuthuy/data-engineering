"""Synthetic graph dataset generators for WCOJ benchmarks.

All generators return edge lists as sorted numpy arrays of shape (m, 2)
where each row is (src, dst) with src < dst (undirected).
"""
from __future__ import annotations

import numpy as np


def erdos_renyi(n: int, p: float, seed: int = 42) -> np.ndarray:
    """Random undirected graph G(n, p) — Erdos–Renyi model."""
    rng = np.random.default_rng(seed)
    edges = []
    for u in range(n):
        for v in range(u + 1, n):
            if rng.random() < p:
                edges.append((u, v))
    if not edges:
        return np.empty((0, 2), dtype=np.int64)
    arr = np.array(edges, dtype=np.int64)
    return arr[np.lexsort((arr[:, 1], arr[:, 0]))]


def barabasi_albert(n: int, m: int = 3, seed: int = 42) -> np.ndarray:
    """Scale-free graph via preferential attachment (Barabási–Albert).

    Starts with a clique of m nodes, then adds n-m nodes each connecting to
    m existing nodes with probability proportional to their degree.
    """
    rng = np.random.default_rng(seed)
    edge_set: set = set()
    # Initial clique.
    for u in range(min(m, n)):
        for v in range(u + 1, min(m, n)):
            edge_set.add((u, v))

    # Degree sequence for preferential attachment.
    degree = np.zeros(n, dtype=np.float64)
    for u, v in edge_set:
        degree[u] += 1
        degree[v] += 1

    for new_node in range(m, n):
        existing = np.arange(new_node)
        d = degree[:new_node]
        if d.sum() == 0:
            probs = np.ones(new_node) / new_node
        else:
            probs = d / d.sum()

        targets = rng.choice(existing, size=min(m, new_node), replace=False, p=probs)
        for t in targets:
            u, v = (int(t), new_node) if t < new_node else (new_node, int(t))
            edge_set.add((min(u, v), max(u, v)))
            degree[u] += 1
            degree[v] += 1

    if not edge_set:
        return np.empty((0, 2), dtype=np.int64)
    arr = np.array(sorted(edge_set), dtype=np.int64)
    return arr[np.lexsort((arr[:, 1], arr[:, 0]))]


def grid_graph(rows: int, cols: int) -> np.ndarray:
    """2-D grid graph with horizontal and vertical edges."""
    edges = []
    for r in range(rows):
        for c in range(cols):
            node = r * cols + c
            if c + 1 < cols:
                edges.append((node, node + 1))
            if r + 1 < rows:
                edges.append((node, node + cols))
    if not edges:
        return np.empty((0, 2), dtype=np.int64)
    arr = np.array(edges, dtype=np.int64)
    return arr[np.lexsort((arr[:, 1], arr[:, 0]))]


def count_triangles_brute(edges: np.ndarray) -> int:
    """Reference triangle count via adjacency-set intersection (O(m * sqrt(m)))."""
    adj: dict = {}
    for u, v in edges:
        adj.setdefault(int(u), set()).add(int(v))
        adj.setdefault(int(v), set()).add(int(u))
    count = 0
    for u in adj:
        for v in adj[u]:
            if v > u:
                count += len(adj[u] & adj[v])
    return count

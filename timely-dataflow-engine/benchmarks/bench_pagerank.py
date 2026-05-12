"""PageRank: timely runtime vs naive pure-Python iteration."""

from __future__ import annotations

import time

from timely.examples.pagerank import pagerank


def naive_pagerank(
    edges: dict[int, list[int]],
    n_nodes: int,
    damping: float = 0.85,
    tol: float = 1e-4,
    max_iter: int = 100,
) -> tuple[list[float], int]:
    ranks = [1.0 / n_nodes] * n_nodes
    for it in range(max_iter):
        new = [(1 - damping) / n_nodes] * n_nodes
        for u, neigh in edges.items():
            if not neigh:
                continue
            share = damping * ranks[u] / len(neigh)
            for v in neigh:
                new[v] += share
        diff = sum(abs(a - b) for a, b in zip(ranks, new, strict=False))
        ranks = new
        if diff < tol:
            return ranks, it + 1
    return ranks, max_iter


def bench(label: str, edges: dict[int, list[int]], n_nodes: int) -> dict:
    # Timely
    t0 = time.perf_counter()
    ranks_t, iters_t = pagerank(edges, n_nodes)
    timely_ms = (time.perf_counter() - t0) * 1000

    # Naive
    t0 = time.perf_counter()
    ranks_n, iters_n = naive_pagerank(edges, n_nodes)
    naive_ms = (time.perf_counter() - t0) * 1000

    return {"label": label, "timely_ms": timely_ms, "naive_ms": naive_ms,
            "timely_iters": iters_t, "naive_iters": iters_n,
            "overhead": timely_ms / naive_ms}


def main() -> None:
    print(f"{'graph':<14} {'timely (ms)':>11} {'naive (ms)':>10} "
          f"{'iters (T)':>9} {'iters (N)':>9} {'overhead':>9}")
    workloads = [
        ("cycle-3",   {0: [1], 1: [2], 2: [0]},                                3),
        ("star-5",    {0: [], 1: [0], 2: [0], 3: [0], 4: [0]},                 5),
        ("clique-4",  {i: [j for j in range(4) if j != i] for i in range(4)},  4),
    ]
    for name, edges, n in workloads:
        r = bench(name, edges, n)
        print(f"{r['label']:<14} {r['timely_ms']:>11.2f} {r['naive_ms']:>10.2f} "
              f"{r['timely_iters']:>9} {r['naive_iters']:>9} {r['overhead']:>9.1f}x")


if __name__ == "__main__":
    main()

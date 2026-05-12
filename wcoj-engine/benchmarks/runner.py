"""Benchmark runner: compare LFTJ vs Hash Join on graph queries.

Usage
-----
    python -m benchmarks.runner          # full benchmark suite
    python -m benchmarks.runner --quick  # small graphs only
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import numpy as np

from wcoj import JoinQuery, Relation, execute
from wcoj.lftj import lftj
from wcoj.generic_join import generic_join
from wcoj.hash_join import hash_join

from .datasets import barabasi_albert, count_triangles_brute, erdos_renyi, grid_graph


# ------------------------------------------------------------------ #
#  Query factories                                                    #
# ------------------------------------------------------------------ #

def triangle_query(edges: np.ndarray) -> JoinQuery:
    """Triangle query: R(x,y) ⋈ S(y,z) ⋈ T(x,z)."""
    R = Relation("R", ["x", "y"], edges)
    S = Relation("S", ["y", "z"], edges)
    T = Relation("T", ["x", "z"], edges)
    return JoinQuery([R, S, T])


def four_cycle_query(edges: np.ndarray) -> JoinQuery:
    """4-cycle query: R(a,b) ⋈ S(b,c) ⋈ T(c,d) ⋈ U(a,d)."""
    R = Relation("R", ["a", "b"], edges)
    S = Relation("S", ["b", "c"], edges)
    T = Relation("T", ["c", "d"], edges)
    U = Relation("U", ["a", "d"], edges)
    return JoinQuery([R, S, T, U])


def path_query(edges: np.ndarray) -> JoinQuery:
    """Length-3 path query (acyclic): R(x,y) ⋈ S(y,z) ⋈ T(z,w)."""
    R = Relation("R", ["x", "y"], edges)
    S = Relation("S", ["y", "z"], edges)
    T = Relation("T", ["z", "w"], edges)
    return JoinQuery([R, S, T])


# ------------------------------------------------------------------ #
#  Timing helper                                                      #
# ------------------------------------------------------------------ #

def timed(fn: Callable, *args, reps: int = 1, **kwargs):
    """Run *fn* *reps* times and return (result, avg_seconds)."""
    best = float("inf")
    result = None
    for _ in range(reps):
        t0 = time.perf_counter()
        result = fn(*args, **kwargs)
        best = min(best, time.perf_counter() - t0)
    return result, best


# ------------------------------------------------------------------ #
#  Single benchmark                                                   #
# ------------------------------------------------------------------ #

@dataclass
class BenchResult:
    query_name: str
    graph_type: str
    n_nodes: int
    n_edges: int
    n_results: int
    lftj_s: float
    gj_s: float
    hj_s: float
    speedup_lftj_vs_hj: float

    def row(self) -> str:
        return (
            f"{self.query_name:<16} {self.graph_type:<12} "
            f"n={self.n_nodes:<5} m={self.n_edges:<6} "
            f"results={self.n_results:<8} "
            f"LFTJ={self.lftj_s*1000:7.1f}ms  "
            f"GJ={self.gj_s*1000:7.1f}ms  "
            f"HJ={self.hj_s*1000:7.1f}ms  "
            f"speedup(LFTJ/HJ)={self.speedup_lftj_vs_hj:6.1f}x"
        )


def run_one(
    query_name: str,
    query_fn: Callable,
    edges: np.ndarray,
    graph_type: str,
    var_order: Optional[List[str]] = None,
    timeout_s: float = 30.0,
) -> BenchResult:
    n_nodes = int(edges.max()) + 1 if len(edges) > 0 else 0
    n_edges = len(edges)
    query = query_fn(edges)

    if var_order is None:
        var_order = query.variable_order()

    # LFTJ
    lftj_res, lftj_t = timed(lftj, query, var_order)
    n_results = len(lftj_res)

    # Generic Join (may be slow — skip if LFTJ already took > 1s)
    if lftj_t < 1.0:
        _, gj_t = timed(generic_join, query, var_order)
    else:
        gj_t = float("nan")

    # Hash Join (with timeout guard — can be huge for cyclic queries)
    if n_edges < 2000:
        _, hj_t = timed(hash_join, query)
    else:
        # Time hash join but don't wait forever.
        try:
            _, hj_t = timed(hash_join, query)
        except MemoryError:
            hj_t = float("nan")

    speedup = hj_t / lftj_t if (lftj_t > 0 and not np.isnan(hj_t)) else float("nan")

    return BenchResult(
        query_name=query_name,
        graph_type=graph_type,
        n_nodes=n_nodes,
        n_edges=n_edges,
        n_results=n_results,
        lftj_s=lftj_t,
        gj_s=gj_t,
        hj_s=hj_t,
        speedup_lftj_vs_hj=speedup,
    )


# ------------------------------------------------------------------ #
#  Full suite                                                         #
# ------------------------------------------------------------------ #

HEADER = (
    f"{'Query':<16} {'Graph':<12} "
    f"{'Nodes':<7} {'Edges':<7} "
    f"{'Results':<9} "
    f"{'LFTJ':>10}  "
    f"{'GenJoin':>10}  "
    f"{'HashJoin':>10}  "
    f"{'Speedup':>14}"
)
DIVIDER = "-" * len(HEADER)


def intermediate_size(edges: np.ndarray) -> int:
    """Count intermediate rows in R(x,y) ⋈ S(y,z) before joining T(x,z)."""
    from collections import defaultdict
    deg: dict = defaultdict(int)
    for u, v in edges:
        deg[int(u)] += 1
        deg[int(v)] += 1
    # For each y, |R_y| * |S_y| intermediate rows.
    counts: dict = defaultdict(lambda: [0, 0])
    for u, v in edges:
        counts[int(v)][0] += 1  # (u, v): v appears as right endpoint
        counts[int(u)][1] += 1  # (u, v): u appears as left endpoint
    # Simpler: just run the intermediate join.
    from collections import defaultdict as dd
    ht = dd(list)
    for u, v in edges:
        ht[int(v)].append(int(u))
    total = 0
    for u, v in edges:
        total += len(ht.get(int(u), []))
    return total


def adversarial_graph(k: int) -> np.ndarray:
    """Three-layer bipartite adversarial graph for the triangle query.

    Structure:
      Layer 0 (left):   nodes  0 ..  k-1
      Layer 1 (middle): nodes  k .. 2k-1
      Layer 2 (right):  nodes 2k .. 3k-1

    Edges: every (left, middle) pair + every (middle, right) pair = 2k² edges.
    NO left-right edges → 0 triangles.

    For the triangle query R(x,y)⋈S(y,z)⋈T(x,z):
      R⋈S joins on y (middle): each middle node contributes k (left) × k (right)
      = k² pairs → k × k² = k³ intermediate rows.
      T has 0 left-right edges → hash join discards all k³ rows at the end.
      LFTJ seeks T at depth 2 and returns immediately (T is empty for x,z).

    Blowup ratio = k³ / 0 (undefined, but HJ materialises k³ rows for nothing).
    """
    edges = []
    for x in range(k):          # left
        for y in range(k, 2*k): # middle
            edges.append((x, y))
    for y in range(k, 2*k):     # middle
        for z in range(2*k, 3*k): # right
            edges.append((y, z))
    arr = np.array(edges, dtype=np.int64)
    return arr[np.lexsort((arr[:, 1], arr[:, 0]))]


def hj_intermediate_count(edges: np.ndarray) -> int:
    """Count rows produced by R(x,y)⋈S(y,z) before the final T(x,z) join."""
    from collections import defaultdict
    # For each node y, count edges where y is the RIGHT endpoint (x,y) = left side of R⋈S
    # and edges where y is the LEFT endpoint (y,z) = right side.
    left_of_y: dict = defaultdict(int)   # y → #edges (x,y)
    right_of_y: dict = defaultdict(int)  # y → #edges (y,z)
    for u, v in edges:
        right_of_y[int(u)] += 1  # u is left endpoint → can be y in (y,z)
        left_of_y[int(v)] += 1   # v is right endpoint → can be y in (x,y)
    total = sum(left_of_y[y] * right_of_y[y] for y in set(left_of_y) | set(right_of_y))
    return total


def run_adversarial() -> None:
    """Demonstrate worst-case intermediate blowup for hash join."""
    print("\n" + "=" * 80)
    print("  Adversarial benchmark: 3-layer bipartite graphs (0 triangles)")
    print("  k layers: left(k) — middle(k) — right(k), no left-right edges")
    print("  Hash join: R⋈S produces k³ intermediates, all discarded by T")
    print("  LFTJ: seeks T(x,z) at depth 2, finds nothing, returns instantly")
    print("=" * 80)
    header = (f"{'k':>4} {'Edges':>7} {'Nodes':>7} "
              f"{'HJ-interm':>12} {'LFTJ':>12} {'HashJoin':>12} {'Speedup':>10}")
    print(header)
    print("-" * len(header))
    for k in [5, 10, 20, 35, 50, 75, 100]:
        edges = adversarial_graph(k)
        q = triangle_query(edges)
        var_order = q.variable_order()

        _, lftj_t = timed(lftj, q, var_order)
        _, hj_t = timed(hash_join, q)
        interm = hj_intermediate_count(edges)
        speedup = hj_t / lftj_t if lftj_t > 0 else float("inf")

        print(
            f"{k:>4} {len(edges):>7,} {3*k:>7} "
            f"{interm:>12,} "
            f"LFTJ={lftj_t*1000:7.3f}ms  "
            f"HJ={hj_t*1000:7.1f}ms  "
            f"{speedup:>8.1f}x"
        )


def run_suite(quick: bool = False) -> List[BenchResult]:
    results = []

    print(HEADER)
    print(DIVIDER)

    # --- Sparse Erdős–Rényi (baseline) ---
    sizes_er_sparse = [100, 300] if not quick else [50, 100]
    for n in sizes_er_sparse:
        p = min(0.08, 8.0 / n)
        edges = erdos_renyi(n, p)
        for qname, qfn in [("triangle", triangle_query)]:
            r = run_one(qname, qfn, edges, f"ER-sparse", timeout_s=20.0)
            print(r.row())
            results.append(r)
        r = run_one("path-3", path_query, edges, "ER-sparse")
        print(r.row())
        results.append(r)

    print(DIVIDER)

    # --- Dense Erdős–Rényi (triggers intermediate blowup in hash join) ---
    sizes_er_dense = [80, 150, 250] if not quick else [60, 100]
    for n in sizes_er_dense:
        p = 0.35
        edges = erdos_renyi(n, p, seed=123)
        interm = intermediate_size(edges)
        for qname, qfn in [("triangle", triangle_query), ("4-cycle", four_cycle_query)]:
            r = run_one(qname, qfn, edges, f"ER-dense", timeout_s=30.0)
            print(r.row() + f"  [HJ-interm~{interm//1000}k]")
            results.append(r)

    print(DIVIDER)

    # --- Barabási–Albert (power-law degree, high-degree hubs cause blowup) ---
    sizes_ba = [200, 500] if not quick else [100, 200]
    for n in sizes_ba:
        edges = barabasi_albert(n, m=5)
        interm = intermediate_size(edges)
        for qname, qfn in [("triangle", triangle_query), ("4-cycle", four_cycle_query)]:
            r = run_one(qname, qfn, edges, "BA", timeout_s=30.0)
            print(r.row() + f"  [HJ-interm~{interm//1000}k]")
            results.append(r)

    print(DIVIDER)
    # Grid — regular, no triangles (control case).
    side = 12 if not quick else 8
    edges = grid_graph(side, side)
    r = run_one("triangle", triangle_query, edges, "grid")
    print(r.row())
    results.append(r)

    return results


def print_summary(results: List[BenchResult]) -> None:
    cyclic = [r for r in results if r.query_name in ("triangle", "4-cycle")]
    valid = [r for r in cyclic if not np.isnan(r.speedup_lftj_vs_hj)]
    if valid:
        avg = sum(r.speedup_lftj_vs_hj for r in valid) / len(valid)
        mx = max(r.speedup_lftj_vs_hj for r in valid)
        print(f"\nCyclic queries — avg LFTJ speedup vs HashJoin: {avg:.1f}x  "
              f"(max: {mx:.1f}x)")


def main() -> None:
    parser = argparse.ArgumentParser(description="WCOJ benchmark suite")
    parser.add_argument("--quick", action="store_true", help="Use small graphs only")
    args = parser.parse_args()

    print("=" * 80)
    print("  Worst-Case Optimal Join Engine — Benchmark Suite")
    print("  Comparing: Leapfrog Triejoin  vs  Generic Join  vs  Hash Join")
    print("=" * 80)
    print()

    results = run_suite(quick=args.quick)
    print_summary(results)
    run_adversarial()
    print()


if __name__ == "__main__":
    main()

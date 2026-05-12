# timely-dataflow-engine

A from-scratch implementation of **Naiad's Timely Dataflow** model: timestamps as `(epoch, iteration)` pairs, cyclic dataflow graphs for iterative computation, **pointstamp-based progress tracking**. Unifies batch and stream computation. Iterative PageRank and belief propagation built on top.

> **Status:** Design / spec phase.

## Why

Most streaming systems can't express iterative computation cleanly. Spark loops by re-submitting jobs; Flink iterations are second-class. Naiad's insight: a richer timestamp algebra (epochs + iterations) lets the **same engine** run batch, streaming, and iterative workloads with one progress-tracking protocol.

The hard part is the **progress tracking protocol** — proving that a given timestamp `(e, i)` is no longer active anywhere in the graph requires distributed coordination, but it must be cheap enough that millisecond-latency streams remain usable.

## Architecture

```
Dataflow graph (possibly cyclic):
  source ──▶ map ──▶ feedback ──▶ join ──▶ sink
                       ▲           │
                       └──── loop ─┘

Timestamps:  (epoch=42, iteration=3)

Each operator:
  - input pointstamps (records pending at timestamp t)
  - output pointstamps (records emitted at timestamp t)
  - progress messages → broadcast to coordinator
  - frontier = min of all active pointstamps

When frontier passes (e, i):
  - operator knows no more records with timestamp ≤ (e, i) will arrive
  - safe to emit final results for that timestamp
```

## Components

| Module | Role |
|---|---|
| `src/timestamp/` | `(epoch, iteration)` algebra: partial order, lattice meets |
| `src/graph/` | Dataflow graph builder, cycle support, scope ingress/egress |
| `src/operators/` | Map, filter, reduce, **iterate**, ingress, egress |
| `src/progress/` | Pointstamp accounting + broadcast protocol |
| `src/progress/frontier.py` | Frontier computation — min over active pointstamps |
| `src/scheduler/` | Worker-thread-per-core, work-stealing |
| `src/examples/pagerank.py` | Iterative PageRank with timestamp-based termination |
| `src/examples/bp.py` | Belief propagation on factor graph |

## The progress tracking protocol

Each worker maintains a local "active pointstamp count" map:
```
{ (op, port, timestamp): count }
```
- On receiving a record with timestamp `t` at op `o`, port `p`: `count[(o,p,t)] += 1`
- On processing: `-1` (and possibly produce records at downstream pointstamps, `+1` each)

Workers broadcast **deltas** (not absolute counts) to a coordinator. Coordinator maintains the global accountancy; broadcasts back the global frontier when it advances.

**Correctness invariant:** sum of all local counts is non-negative. When global count for `(o, p, t)` reaches zero AND no upstream operator can produce timestamp ≤ `t`, the frontier passes `t`.

The bandwidth saver is: only broadcast frontier advances, not every count change.

## Progress proof obligations

1. **No premature advance.** Frontier never passes a pointstamp while records at that pointstamp could still arrive.
2. **No stall.** Frontier eventually advances if no more records are produced.
3. **Bounded message overhead.** Progress messages per operator per second ≤ `O(distinct frontier moves)`, independent of record rate.

Specified in TLA+ (`src/progress/spec.tla`) — verifiable with TLC for small graphs.

## Examples

### PageRank with timestamps

```python
graph = TimelyGraph()
edges = graph.source(edge_iterator)         # (epoch=0, iter=0)
ranks = graph.source(initial_ranks)         # (epoch=0, iter=0)

with graph.iterate() as loop:
    new_ranks = (
        ranks
        .join(edges, key='node')
        .map(redistribute)
        .reduce(sum_contributions)
    )
    loop.connect(ranks, new_ranks)          # feedback edge

ranks.until(convergence_predicate).sink(print)
graph.run()
```

Each iteration increments the iteration counter; outer epoch unchanged.

### Streaming PageRank (epoch-per-batch)

Same graph, but each new batch of edges arrives with a new epoch — graph re-converges incrementally per batch.

## Benchmark vs. GraphX / Spark

Iterative PageRank on Twitter-2010 (1.4B edges):

| System | Time to convergence (10 iters) |
|---|---|
| GraphX (Spark 3.5) | baseline |
| Naiad (paper, 2013) | ~0.3× |
| This | target ≤ 0.5× |

## References

- Murray et al., "Naiad: A Timely Dataflow System" (SOSP 2013)
- Abadi et al., "Differential Dataflow" (CIDR 2013)
- McSherry's `timely-dataflow` (Rust) — implementation reference

## Roadmap

- [ ] `(epoch, iteration)` timestamp algebra with lattice meets
- [ ] Acyclic graph + basic operators (map, filter, reduce)
- [ ] Pointstamp accounting (local + global)
- [ ] Frontier computation + broadcast
- [ ] Iterate scope (cyclic edges)
- [ ] TLA+ progress proof
- [ ] PageRank example
- [ ] BP example
- [ ] GraphX comparison

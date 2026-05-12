# Jepsen-Style Linearizability Tester

A chaos engineering harness that injects **network partitions**, **clock skew**, and **process crashes** into a streaming pipeline, then verifies **linearizability** using a Wing-Gong (Knossos-style) history analysis. Auto-generates correctness reports.

```
╔══════════════════════════════════════════════════╗
║  Clients ──▶ Cluster (3 nodes, last-write-wins)  ║
║               ↑   ↑   ↑                          ║
║          Nemesis: partition / skew / crash        ║
║               ↓                                  ║
║  History ──▶ WGL Checker ──▶ HTML Report         ║
╚══════════════════════════════════════════════════╝
```

## What it tests

The pipeline under test is a **multi-node replicated key-value store** that uses *last-write-wins* (LWW) replication by wall-clock timestamp. This is intentionally fragile:

| Fault | Why it breaks LWW |
|---|---|
| Network partition | Stale reads from isolated nodes |
| Clock skew | Wrong "winner" in LWW conflict resolution |
| Process crash | Mid-write replication loss |

The harness records the exact wall-clock interval of every client operation, then the **Wing-Gong algorithm** checks whether any sequential ordering of those operations satisfies the register model — if not, a linearizability violation is reported.

## Quick start

```bash
pip install -r requirements.txt
python run_tests.py                          # default: 3 nodes, 5 clients, 10s
python run_tests.py --nodes 5 --duration 30 # bigger cluster, longer run
python run_tests.py --no-crashes            # only network + clock faults
open reports/report_*.html                  # view the generated report
```

## Architecture

```
jepsen/
├── core/
│   ├── history.py      # Op / Entry data structures, thread-safe recorder
│   ├── checker.py      # Wing-Gong linearizability checker (WGL 1993)
│   ├── models.py       # Sequential specs: RegisterModel, QueueModel, CASRegisterModel
│   └── runner.py       # Test orchestrator (cluster + nemeses + clients + report)
├── chaos/
│   ├── nemesis.py      # Base Nemesis, PeriodicNemesis, CompositeNemesis
│   ├── network.py      # PartitionTable + NetworkPartitionNemesis / LatencyNemesis
│   ├── clock.py        # ClockRegistry + ClockSkewNemesis
│   └── process.py      # ProcessRegistry + ProcessCrashNemesis
├── pipeline/
│   ├── node.py         # Node worker process (multiprocessing)
│   ├── cluster.py      # Message router + chaos integration
│   └── client.py       # Concurrent workload client threads
└── reporter/
    └── html.py         # Self-contained HTML report with SVG timeline
```

## CLI options

```
Options:
  --nodes INTEGER         Number of cluster nodes  [default: 3]
  --clients INTEGER       Number of concurrent clients  [default: 5]
  --duration FLOAT        Test duration in seconds  [default: 10.0]
  --keys TEXT             Comma-separated key names  [default: x,y,z]
  --timeout FLOAT         Per-request timeout (s)  [default: 1.5]
  --partitions/--no-partitions  Enable network partition nemesis  [default: partitions]
  --clock-skew/--no-clock-skew  Enable clock skew nemesis  [default: clock-skew]
  --crashes/--no-crashes        Enable process crash nemesis  [default: crashes]
  --max-skew FLOAT        Max clock skew in seconds  [default: 3.0]
  --output-dir TEXT       Report output directory  [default: reports]
```

## Linearizability checking

The **Wing-Gong algorithm** (WGL, 1993) works by:

1. Pairing each `invoke`/`ok` event into an `Entry` with `[invoke_time, response_time]`
2. Finding *candidate* operations: those with no other completed op whose response precedes their invoke (i.e., nothing "must come before" them in real time)
3. Recursively trying each candidate as the next element in the linearization, applying it to the sequential model
4. Memoizing on `(remaining_op_set, model_state)` to prune the search space
5. Returning `True` if any complete linearization satisfies the model

Complexity is exponential in the worst case (inherent for linearizability), but the memoization makes it practical for histories of ≤50 concurrent operations.

## Report output

Each run produces a self-contained HTML file in `reports/` containing:

- **Pass/fail badge** with test summary
- **Injected fault timeline** with timestamps
- **SVG operation timeline** — swim lanes per client process, color-coded by operation type and outcome
- **Linearization order** (if valid) or anomaly description
- **Raw history** as collapsible JSON

## Extending

### Add a new sequential model

Implement `initial_state()` and `step(state, entry) -> (new_state, is_valid)` in `jepsen/core/models.py`.

### Add a new nemesis

Subclass `Nemesis` in `jepsen/chaos/nemesis.py` and implement `start()`, `stop()`, `describe()`.

### Change the pipeline

Modify `jepsen/pipeline/node.py` to implement a different data structure (queue, counter, CAS register…).

## Tests

```bash
pytest tests/ -v
```

## References

- Wing, J. M. & Gong, C. (1993). *Testing and Verifying Concurrent Objects*
- Kingsbury, K. (2013–). [Jepsen](https://jepsen.io/) — distributed systems testing
- Herlihy, M. & Wing, J. M. (1990). *Linearizability: A Correctness Condition for Concurrent Objects*

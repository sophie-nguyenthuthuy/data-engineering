# HLC Metadata Store

A multi-region metadata store that replaces wall-clock timestamps with
**Hybrid Logical Clocks (HLC)**, eliminating the causal anomalies that
NTP-synced systems cannot prevent.

## Background

Pure wall-clock timestamps in distributed systems break under two
real-world conditions:

| Condition | Anomaly |
|-----------|---------|
| NTP clock drift between nodes | **Causal inversion** — event B (which happened after A) receives a lower timestamp than A |
| NTP backward correction | **Stale reads** — a replica serves data whose timestamp is lower than an event the client already observed |

HLC (Kulkarni et al., 2014) solves both by pairing each wall-clock reading
with a logical counter. The counter advances whenever the wall clock doesn't,
and resets to zero whenever the wall clock advances — so HLC timestamps are:

- Always ≥ the local wall clock (bounded skew).
- Strictly increasing along every causal path (e → f ⟹ ts(e) < ts(f)).
- Self-healing: after an NTP jump the logical counter absorbs the skew and
  resets to zero as soon as wall time catches up.

## Repository layout

```
src/hlc_store/
  timestamp.py   HLCTimestamp — totally ordered (wall_ms, logical) pair
  clock.py       HybridLogicalClock + WallClock baseline
  store.py       MetadataStore with causal_get for stale-read protection
  region.py      Region node — wraps store with simulated drift & latency
  anomaly.py     Post-hoc causal inversion detection

tests/           pytest suite proving correctness and anomaly elimination
benchmarks/      Inversion rate, recovery latency, and throughput benchmarks
demo/            Interactive anomaly demonstrations
```

## Quick start

```bash
pip install -e ".[dev]"

# Run the test suite
pytest

# Watch anomaly demos
python demo/demo_anomalies.py

# Run benchmarks
python benchmarks/benchmark_drift.py
```

## HLC algorithm (send / receive)

**Tick** (local event or message send):
```
wall = physical_clock()
if wall > l.wall_ms:
    l = (wall, 0)
else:
    l = (l.wall_ms, l.logical + 1)
```

**Update** (message receive, carrying remote timestamp r):
```
wall  = physical_clock()
l'    = max(wall, r.wall_ms, l.wall_ms)
if   l' == l.wall_ms == r.wall_ms : logical = max(l.logical, r.logical) + 1
elif l' == l.wall_ms              : logical = l.logical + 1
elif l' == r.wall_ms              : logical = r.logical + 1
else                              : logical = 0
l = (l', logical)
```

## Anomaly elimination

### Causal inversion

```
us-east  (drift=0ms)    writes db-config at ts=(T, 0)
eu-west  (drift=−300ms) receives replication

  Wall clock: eu-west stamps (T−300, 0) → INVERSION (ts < source ts)
  HLC:        eu-west updates  (T, 1)   → PRESERVED  (ts > source ts)
```

### Stale-read protection

`MetadataStore.causal_get(key, after=T)` blocks until the store's watermark
≥ T, guaranteeing the caller never reads a value that predates something it
already observed.

## Benchmark results

```
BENCHMARK 1 — Causal inversion rate under clock drift
─────────────────────────────────────────────────────────────────────
System & drift scenario                  events   inversions     rate
─────────────────────────────────────────────────────────────────────
WallClock | low drift  (±50ms)              401          266    66.3%
HLC       | low drift  (±50ms)              401            0     0.0%
WallClock | med drift  (±200ms)             401          265    66.1%
HLC       | med drift  (±200ms)             401            0     0.0%
WallClock | high drift (±500ms)             401          266    66.3%
HLC       | high drift (±500ms)             401            0     0.0%

BENCHMARK 2 — HLC recovery after NTP clock jump (backward)
─────────────────────────────────────────────────────────────────────
      Jump magnitude    Events until self-healed
─────────────────────────────────────────────────────────────────────
                50ms                   immediate
               100ms                   immediate
               250ms                   immediate
               500ms                   immediate
              1000ms                   immediate

BENCHMARK 3 — Tick throughput (single thread)
─────────────────────────────────────────────────────────────────────
  HLC  tick rate :  2,400,000 ops/sec
  Wall tick rate :  1,980,000 ops/sec
  HLC overhead   :       ~0%  (often faster — avoids redundant syscalls)
```

Wall-clock systems invert ~66% of causal relationships under any drift level
because replicas ignore source timestamps entirely.  HLC achieves 0 inversions
at all drift levels and recovers immediately after an NTP clock jump — the
logical counter absorbs the backward step and resets to 0 as soon as wall
time catches up.

## Reference

Sandeep Kulkarni, Murat Demirbas, Deepak Madappa, Bharadwaj Avva, Marcelo Leone.
*Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases.*
OPODIS 2014.

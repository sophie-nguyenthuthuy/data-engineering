# causal-feature-store

A feature store with **causal-consistency guarantees**: at serving time, every feature value in a returned vector comes from a single causally consistent snapshot per entity. No mixing of pre-event and post-event values for the same entity, ever — even under concurrent writes and network partitions.

> **Status:** Design / spec phase. Extends [`streaming-feature-store`](../streaming-feature-store/) (batch+stream parity) with formal causal consistency across the hot (Redis) and cold (Parquet) tiers.

## Why this matters

A typical model reads ~50 features. If they're sourced independently from a key-value store:

- Feature `last_click_ts` from row at t=10
- Feature `session_clicks_count` from row at t=20 (after a click landed)
- Feature `is_logged_in` from row at t=5 (before login)

The model sees a state that **never existed**. Even when each individual write is correct, the *combination* is incoherent — and ML models trained on consistent training data behave unpredictably on inconsistent inference data.

This problem is invisible in offline accuracy metrics and silently degrades production.

## Causal consistency, scoped per entity

For each entity (user_id, item_id, …) we maintain a **vector clock** over the components that write features:

```
entity_id = u42
clock = { click_stream: 10, page_view: 7, identity: 3 }
```

Every feature value in the store is tagged with the entity's clock value *at the time of write*.

**Serving guarantee:** when a client requests features for `u42` at "now", the returned vector is consistent with a single clock value `c*` — i.e., for every returned feature `f`, the value reflects the entity's state at clock `c* ≥ clock_at_first_lookup`.

`c*` may be slightly stale (server picks the highest clock for which *all* requested features are available), but never **inconsistent**.

## Architecture

```
                       Producers
            click   page_view   identity
              │         │           │
              ▼         ▼           ▼
        ┌─────────────────────────────────┐
        │ Stream router (per-entity)       │
        │  - bumps vector clock            │
        │  - writes (entity, clock, value) │
        │  - emits to log AND tiers        │
        └────────────────┬─────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                                  ▼
    ┌────────────┐                  ┌─────────────┐
    │ Redis hot  │                  │ Parquet cold│
    │ (last K    │                  │ (history)   │
    │ versions)  │                  └──────┬──────┘
    └─────┬──────┘                         │
          │                                │
          └────────────┬───────────────────┘
                       ▼
            ┌──────────────────────┐
            │  Serving resolver    │
            │  - reads clocks      │
            │  - picks max c*      │
            │  - assembles vector  │
            └──────────────────────┘
```

## Components

| Module | Role |
|---|---|
| `src/clock/vector_clock.py` | Per-entity vector clock |
| `src/stream/router.py` | Bumps clock, writes tagged feature value |
| `src/online/redis_versioned.py` | Stores last K (clock, value) pairs per (entity, feature) |
| `src/offline/parquet_versioned.py` | Append-only versioned Parquet |
| `src/serving/resolver.py` | Picks max consistent clock, assembles vector |
| `src/correctness/partition_test.py` | Network partition + concurrent write tests |
| `src/correctness/jepsen/` | Falsification harness for causal consistency |

## The resolver protocol

```python
def get_features(entity_id, requested_features):
    # 1. Get current clock from each component
    clocks = redis.hgetall(f"clock:{entity_id}")              # {comp: clk}

    # 2. For each requested feature, find max version where ALL components ≤ clocks
    candidates = []
    for f in requested_features:
        versions = redis.zrange(f"{entity_id}:{f}", 0, -1, withscores=True)
        for value, version_clock in versions:
            if dominates(clocks, version_clock):              # ∀c: clocks[c] >= version_clock[c]
                candidates.append((f, value, version_clock))
                break

    # 3. Pick c* = component-wise max over chosen versions, then re-verify
    c_star = pointwise_max(v_clock for _, _, v_clock in candidates)
    re_verified = [(f, v) for f, v, vc in candidates if dominates(c_star, vc)]

    return re_verified, c_star
```

The retry-and-verify loop bounds the wait. In the worst case (heavy partition), some features may be reported as "unavailable at consistent snapshot" — caller decides whether to wait or fall back.

## Correctness tests

Jepsen-style:

1. **Concurrent write test.** 64 writers slam different feature components for the same entity. Reader picks 10⁵ random snapshots. Each snapshot must satisfy: for every (feature_a, feature_b) returned, neither's writer "happens after" the other's per the vector clock.

2. **Partition test.** Partition the redis tier from a subset of writers for 30 s. After healing, no returned vector should mix pre-partition and post-partition writes inconsistently.

3. **Hot/cold mix test.** Force some features to resolve from Parquet (evicted from Redis). Verify the cold values' clocks dominate / are dominated correctly.

## Performance budget

- Online serving p99: ≤ 15 ms (vs. 10 ms for the non-causal baseline; 50 % budget for clock resolution).
- Memory overhead: O(K * components) per entity. With K=3 versions and 5 components, ~120 bytes overhead per entity-feature.

## References

- Lamport, "Time, Clocks, and the Ordering of Events" (1978)
- Cassandra Lightweight Transactions; CockroachDB MVCC for causal-consistency mechanics
- Ahamad et al., "Causal Memory" (DCS 1995)
- Bailis et al., "Bolt-on Causal Consistency" (SIGMOD 2013)

## Roadmap

- [ ] Per-entity vector clock + clock store
- [ ] Versioned online (Redis) writes
- [ ] Versioned offline (Parquet) writes
- [ ] Resolver protocol
- [ ] Single-region correctness tests
- [ ] Partition tests
- [ ] Multi-region replication (open question)

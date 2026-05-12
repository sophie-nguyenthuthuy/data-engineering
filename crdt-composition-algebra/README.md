# CRDT Composition Algebra

A Go implementation of algebraic CRDT composition with formal convergence proofs, delta-state synchronization, Interval Tree Clock anti-entropy, and multi-region partition validation.

## What's implemented

### 1. Algebraic CRDT Composition

CRDTs are built compositionally from a `Ops[S]` algebra:

```go
// Lattice operations for any type S
type Ops[S any] struct {
    Join   func(a, b S) S        // s1 ⊔ s2 — associative, commutative, idempotent
    LessEq func(a, b S) bool     // induced partial order
    Bottom func() S              // identity element
    Equal  func(a, b S) bool
}

// Composition: Lattice(A) ∧ Lattice(B) → Lattice(A×B)
func ProductOps[A, B any](opsA Ops[A], opsB Ops[B]) Ops[Product[A, B]]

// PNCounter is derived purely algebraically:
var PNCounterOps = ProductOps(GCounterOps, GCounterOps)
```

Semilattice laws verified by property-based testing (1000–2000 random inputs each):
- **Idempotency**: `s ⊔ s = s`
- **Commutativity**: `s ⊔ t = t ⊔ s`
- **Associativity**: `(s ⊔ t) ⊔ u = s ⊔ (t ⊔ u)`
- **Monotonicity**: `s ≤ s ⊔ t`
- **Convergence**: any merge order produces the same result

### 2. Concrete CRDTs

| CRDT | Composition | Semantics |
|------|-------------|-----------|
| `GCounter` | `map[NodeID]uint64` under pointwise max | Grow-only counter |
| `PNCounter` | `Product[GCounter, GCounter]` | Increment/decrement counter |
| `LWWRegister[T]` | `(value, timestamp, nodeID)` under timestamp max | Last-write-wins |
| `ORSet[E]` | Dotted version vector map | Add-wins set |
| `MVRegister[V]` | Dot map | Multi-value register (all concurrent writes retained) |

### 3. Delta-State Synchronization (not op-based)

Delta-state CRDTs send the *minimal* update needed rather than full state:

```
Delta(state, recipient_cc) = minimal state ≥ recipient's causal context
```

Causal metadata uses **Dotted Version Vectors** — each `Dot = (nodeID, counter)` uniquely identifies one event. The `CausalContext` compresses contiguous ranges to `max[node]` plus exceptional dots, enabling efficient subset checks.

Key invariant for `ORSet`: use `PeekNext()` (not `Next()`) when building a delta so `state.cc` is not pre-advanced before the `Join`. If state.cc already contains the new dot, the join symmetrically drops it from both sides.

### 4. Interval Tree Clock Anti-Entropy

ITC replaces O(n) vector clocks with O(k) stamps (k = active nodes):

```
Stamp = (ID, Event)
ID    = binary tree partitioning the causal identity space [0,1]
Event = compressed event counter tree
```

Core operations:
- `Fork(stamp) → (s1, s2)` — split identity for new node
- `Join(s1, s2) → stamp` — merge when nodes synchronize
- `RecordEvent(stamp) → stamp` — record a write (fill or grow)
- `Leq(a, b)` — causal ordering check

Anti-entropy protocol exchanges stamps, syncs when concurrent, and achieves bounded metadata:

```
9-node cluster: ITC max_metadata=18   (O(k))
               Vector clock equivalent = 9 entries/node (O(n))
```

### 5. Multi-Region Partition Simulation

Three regions (us-east, eu-west, ap-south), 3 nodes each. Time scale: 1 wall second = 60 simulated seconds.

```
Phase 1: Normal operation         (30s simulated)
Phase 2: us-east partitioned      (5 min simulated = 5s wall)
Phase 3: Partition healed         → convergence in ~2s wall
Phase 4: ITC metadata bound check → O(k) verified
```

Simulation output:
```
Pre-partition:   counter values diverging across regions
Mid-partition:   4 distinct counter values (divergence confirmed)
Post-heal:       counter=-6 members=7 on ALL 9 nodes (converged=true)
ITC bound:       max_metadata=18 ≤ theoretical_bound=17*4 [WITHIN BOUND]
```

## Project structure

```
crdt-composition-algebra/
├── algebra/       # Lattice ops, ProductOps, MapOps, convergence proofs
├── crdt/          # GCounter, PNCounter, LWWRegister, ORSet, MVRegister
├── causal/        # Dot, DotSet, CausalContext (dotted version vectors)
├── delta/         # Delta-state protocol: DeltaBuffer, SyncSession, Message
├── itc/           # Interval Tree Clocks: ID, Event, Stamp, AntiEntropyNode
├── simulation/    # Node, Network, MultiRegionCluster, PartitionScenario
└── cmd/           # Entry point: runs all four demos
```

## Running

```bash
# Full demo (~10 seconds)
go run cmd/main.go

# Quick algebra verification only
go run cmd/main.go --quick

# Tests
go test ./...
```

## Key design decisions

**`PeekNext` vs `Next` in delta construction**: Using `Context.Next()` advances the causal context in place. If called before `Join`, the state's cc already contains the new dot, causing the join to symmetrically drop it from both replica sides. `PeekNext()` computes the next dot without mutation, letting `Join` advance the cc correctly.

**Why delta-state over op-based**: Op-based CRDTs require reliable exactly-once delivery (hard under partitions). Delta-state only requires eventual delivery and is tolerant of duplicates, reordering, and partial delivery — just re-join.

**Why ITC over vector clocks**: Vector clocks need O(n) entries for n nodes ever created. ITC splits the identity space dynamically so metadata is O(k) for k *currently active* nodes. Metadata doesn't grow as nodes join and leave.

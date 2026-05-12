# Flexible Paxos with Dynamic Quorum Reconfiguration

A from-scratch implementation of [Flexible Paxos](https://arxiv.org/abs/1608.06696) (Howard, Malkhi & Spiegelman, 2016), extended with:

- **Dynamic quorum reconfiguration** driven by measured read/write ratios
- **TLA+ formal model** proving safety during concurrent leader elections and membership changes
- **Elle-inspired linearizability verifier** with cycle detection over write-read dependency graphs

---

## Background

Classic Paxos requires both Phase 1 and Phase 2 to use a *majority* quorum. Flexible Paxos relaxes this to a single constraint:

> **Every Phase 1 quorum must intersect every Phase 2 quorum.**

For threshold quorums on `n` nodes this means `Q1 + Q2 > n`. Examples for `n = 5`:

| Q1 | Q2 | Q1+Q2 | Optimises        |
|----|----|-------|------------------|
| 3  | 3  | 6     | Classic majority |
| 4  | 2  | 6     | Write throughput |
| 2  | 4  | 6     | Leader election  |
| 5  | 1  | 6     | Max write speed  |

The metadata store measures the live R/W ratio and adjusts Q1/Q2 accordingly via the `quorum.Manager`.

---

## Project structure

```
flexible-paxos/
├── pkg/
│   ├── paxos/              # Core Paxos types, Acceptor, Proposer, LocalTransport
│   ├── quorum/             # Dynamic quorum manager + metrics
│   ├── store/              # Linearisable KV metadata store
│   ├── reconfig/           # Safe configuration reconfiguration (joint consensus)
│   └── linearizability/    # Elle-inspired verifier (history recorder + cycle detector)
├── tests/
│   ├── paxos_test.go       # Classic + Flexible Paxos correctness + fault tolerance
│   ├── linearizability_test.go  # Verifier unit tests + store end-to-end
│   ├── reconfig_test.go    # Reconfiguration safety tests
│   └── chaos_test.go       # Random partition/heal with linearizability check
├── tla/
│   ├── FlexiblePaxos.tla         # Core Flexible Paxos spec + safety invariants
│   ├── FlexiblePaxosReconfig.tla # Reconfiguration + concurrent elections spec
│   ├── MC.cfg                    # TLC config for FlexiblePaxos
│   └── MC_Reconfig.cfg           # TLC config for reconfiguration spec
└── scripts/
    └── run_tla.sh          # Downloads tla2tools.jar and runs TLC
```

---

## Running

### Tests

```bash
go test ./tests/... -v -race -timeout 60s
```

Individual suites:

```bash
go test ./tests/... -run TestFlexiblePaxos -v
go test ./tests/... -run TestLinearizability -v
go test ./tests/... -run TestReconfig -v
go test ./tests/... -run TestChaos -v -timeout 30s
```

### TLA+ model checking

Requires Java 11+. The script auto-downloads `tla2tools.jar`:

```bash
bash scripts/run_tla.sh
```

This runs TLC on both specs:
- `FlexiblePaxos.tla` — verifies Agreement, Nontriviality, VotesSafe, OneValuePerBallot
- `FlexiblePaxosReconfig.tla` — verifies GlobalAgreement and EpochMonotonicity across epochs

---

## Architecture

### Flexible Paxos core (`pkg/paxos`)

- **`Acceptor`** — stores `(maxBal, maxVBal, maxVal)` durably; handles Prepare and Accept under a mutex.
- **`Proposer`** — runs Phase 1 (Prepare → Promise) with a Q1-quorum and Phase 2 (Accept → Accepted) with a Q2-quorum. Retries on ballot collision.
- **`LocalTransport`** — in-process transport for testing; supports `Partition`/`Heal` for fault injection.
- **`QuorumConfig`** — validates the invariant `Q1 + Q2 > n` before use.

### Dynamic quorum manager (`pkg/quorum`)

The `Manager` samples the `Metrics` counter every `SampleInterval` and applies a heuristic:

```
R/W ratio > ReadHeavyThreshold  →  shrink Q2 (optimise writes)
R/W ratio < WriteHeavyThreshold →  shrink Q1 (optimise leader election)
otherwise                       →  use classic majority
```

Config changes are propagated to all registered `ChangeListener` callbacks.

### Reconfiguration (`pkg/reconfig`)

Uses **joint consensus**: to transition from config C → C', the reconfigurator runs Phase 1 in the *union* of C and C' members. This fences all in-flight proposals in C before C' becomes active, preventing any value committed in C from being overridden by a different value in C'.

Safety argument: see the `pkg/reconfig/reconfig.go` package comment.

### Linearizability verifier (`pkg/linearizability`)

Implements Elle-style dependency graph analysis:

| Edge type | Meaning                                    |
|-----------|--------------------------------------------|
| **wr**    | Writer → Reader (reader observes this write) |
| **ww**    | Earlier write → Later write (version order)  |
| **rw**    | Reader → Writer (anti-dependency: write must follow read) |

Cycle detection uses **Tarjan's SCC algorithm** (O(V+E)). A cycle in {wr,ww,rw} constitutes a G1c or G2 anomaly — the history is not linearisable.

Anomalies:
- `G1c` — cycle in ww+wr only (dirty reads / lost update)
- `G2`  — cycle involving at least one rw anti-dependency (write skew / non-repeatable read)

---

## TLA+ Formal Model

### `FlexiblePaxos.tla`

Models the basic Flexible Paxos protocol with parameterised `Quorum1` and `Quorum2` sets. The key invariant (`QuorumAssumption`) requires every Q1 ∈ Quorum1 to intersect every Q2 ∈ Quorum2.

Verified properties:
- **Agreement** — at most one value is ever chosen
- **Nontriviality** — chosen value was proposed in Phase 2a
- **VotesSafe** — Phase 2b votes are consistent with Phase 2a
- **OneValuePerBallot** — no two different values share a ballot

An informal TLAPS proof sketch of Agreement from the Flexible Paxos intersection property is included as a comment.

### `FlexiblePaxosReconfig.tla`

Extends the base spec with configuration epochs, joint-consensus reconfiguration, and concurrent leader elections. Additional invariants:

- **GlobalAgreement** — no two different values chosen across *any* two epochs
- **EpochMonotonicity** — committed config epoch is non-decreasing
- **ConfigValidity** — committed/pending configs always satisfy Q1+Q2>n

### Model checking parameters

`MC.cfg` uses a read-heavy asymmetric quorum (Q1=4, Q2=2) on 5 acceptors with symmetry reduction, keeping the state space tractable.

---

## References

- Howard, H., Malkhi, D., & Spiegelman, A. (2016). [Flexible Paxos: Quorum Intersection Revisited](https://arxiv.org/abs/1608.06696)
- Kingsbury, K., & Alvaro, P. (2021). [Elle: Inferring Isolation Anomalies from Experimental Observations](https://dl.acm.org/doi/10.14778/3494124.3494152)
- Lamport, L. (1998). [The Part-Time Parliament](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf)
- Lamport, L. (2001). [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)

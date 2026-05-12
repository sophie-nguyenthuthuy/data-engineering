# Raft-Based Distributed Metadata Store

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) backing a distributed metadata store for a data catalog — the same core idea as [etcd](https://etcd.io), purpose-built for catalog metadata.

## What's implemented

| Component | Details |
|-----------|---------|
| **Leader election** | Randomized election timeouts (150–300 ms), majority quorum |
| **Log replication** | AppendEntries RPC with consistency check and conflict resolution |
| **Snapshotting** | Log compaction triggered at configurable threshold; `InstallSnapshot` RPC for lagging followers |
| **Membership changes** | Joint-consensus two-phase config changes (add/remove peers) |
| **KV store** | Versioned, compare-and-swap, prefix scan, watchers |
| **Data catalog API** | Datasets, tables, columns, lineage (upstream/downstream/impact), tag search |
| **Linearizability checker** | Wing-Gong algorithm implementation |
| **Chaos tooling** | `tc netem` + `iptables` partition injection via `scripts/chaos.py` |
| **Jepsen-style tests** | Concurrent workloads + linearizability verification |

## Architecture

```
┌─────────────────────────────────────────────┐
│            Data Catalog REST API             │  /catalog/datasets  /catalog/lineage
├─────────────────────────────────────────────┤
│              KV Store (state machine)        │  versioned get/put/cas/delete/scan
├─────────────────────────────────────────────┤
│           Raft Consensus Layer               │  election · replication · snapshots
├───────────────┬──────────────────────────────┤
│  Persistent   │    HTTP RPC transport         │
│  Log (JSON)   │  /raft/append_entries etc.    │
└───────────────┴──────────────────────────────┘
```

## Quick start

### Local 3-node cluster

```bash
pip install -r requirements.txt
bash scripts/start_cluster.sh
```

### Docker (5-node cluster)

```bash
docker-compose up --build
```

### Run tests

```bash
pip install -r requirements.txt
pytest -v
```

### Jepsen-style end-to-end test (against live cluster)

```bash
python scripts/jepsen_test.py \
  --addrs localhost:8001,localhost:8002,localhost:8003 \
  --clients 20 \
  --ops-per-client 100
```

### Network chaos injection

```bash
# Partition node1 from node2
python scripts/chaos.py partition --src node1 --dst node2

# Add 100ms latency to node3
python scripts/chaos.py delay --node node3 --ms 100

# Drop 20% of packets on node2
python scripts/chaos.py loss --node node2 --pct 20

# Heal everything
python scripts/chaos.py heal
```

## API reference

### KV store

| Method | Endpoint | Body |
|--------|----------|------|
| PUT | `POST /kv/put` | `{key, value, version?}` |
| GET | `GET /kv/get/{key}` | — |
| CAS | `POST /kv/cas` | `{key, expected, new_value}` |
| DELETE | `POST /kv/delete` | `{key}` |
| LIST | `GET /kv/list?prefix=` | — |

### Catalog

| Method | Endpoint |
|--------|----------|
| Create dataset | `POST /catalog/datasets` |
| List datasets | `GET /catalog/datasets` |
| Get dataset | `GET /catalog/datasets/{name}` |
| Create table | `POST /catalog/datasets/{ds}/tables` |
| List tables | `GET /catalog/datasets/{ds}/tables` |
| Add column | `POST /catalog/datasets/{ds}/tables/{t}/columns` |
| Add lineage | `POST /catalog/lineage` |
| Upstream lineage | `GET /catalog/lineage/upstream/{table}` |
| Downstream lineage | `GET /catalog/lineage/downstream/{table}` |
| Impact analysis | `GET /catalog/lineage/impact/{table}?depth=5` |
| Tag search | `GET /catalog/search/tag/{tag}` |

### Cluster management

| Method | Endpoint | Body |
|--------|----------|------|
| Node status | `GET /raft/status` | — |
| Add peer | `POST /cluster/members/add` | `{node_id, addr}` |
| Remove peer | `POST /cluster/members/remove` | `{node_id}` |

## Key schema

```
datasets/{name}                     → Dataset JSON
tables/{dataset}/{table}            → Table JSON
lineage/{source}/{target}/{job}     → DataLineage JSON
tags/by-tag/{tag}/{full_table_name} → "" (index for tag search)
```

## Raft implementation notes

- **Persistent state** (`currentTerm`, `votedFor`, `log[]`) is fsync'd to disk before responding to RPCs, ensuring durability across crashes.
- **Log compaction**: when `last_applied - snapshot_base >= SNAPSHOT_THRESHOLD` (default 1000), the node serializes the state machine and discards old log entries. Lagging followers receive the snapshot via `InstallSnapshot`.
- **No-op entry**: on becoming leader the node immediately appends a no-op entry to establish commit point for prior-term entries (Raft §8).
- **Joint consensus**: membership changes use two committed config entries (C_old,new then C_new) so the cluster never loses quorum during a reconfiguration.
- **Linearizable reads**: reads are served from the leader's local state (which is always at least as up-to-date as any committed write). For strict linearizability in a real deployment, add a read index / lease-based read path.

## Linearizability checker

`tests/checker.py` implements the Wing-Gong algorithm:

1. Record every operation with wall-clock `call_time` and `return_time`.
2. Build a partial order: if `op_i.return_time < op_j.call_time`, then `op_i` must precede `op_j` in any valid sequential ordering.
3. Search (with memoization) for a sequential ordering consistent with both the partial order and the KV sequential specification.

The checker is used both in unit tests and the Jepsen-style script.

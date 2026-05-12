# b-epsilon-tree

[![CI](https://github.com/sophie-nguyenthuthuy/data-engineering/actions/workflows/beps.yml/badge.svg)](https://github.com/sophie-nguyenthuthuy/data-engineering/actions)

A **write-optimized B^ε-tree** implementation: internal nodes reserve a
fraction `ε` of their space for buffered messages, so writes amortise
into batched flushes down the tree — yielding provably lower write I/O
than B+-trees.

A workload-observing **online ε tuner** adjusts the parameter as
read/write ratio shifts; a **write-amplification tracker** measures
exactly how many node rewrites each user-visible operation triggers.

```text
       Root (ε of space = msg buffer | rest = pivots + children)
       ┌────────────────────────────────────────────────────────┐
       │ buffer: [Put(k=z, v=…, seq=42), Del(k=q, seq=41), …]   │
       │ pivots: [m]                                            │
       │ children: [Internal0, Internal1]                       │
       └─────────────┬─────────────────────┬────────────────────┘
                     ▼                     ▼
              Internal0 (buffer + …)  Internal1 (buffer + …)
                     ▼                     ▼
              ...                      ...
                     ▼                     ▼
                   Leaf(pivots, values, seqs)   ← newest-wins by seq
```

55 tests pass; ε tuning + write-amp tracking demonstrated empirically.

## Install

```bash
pip install -e ".[dev]"
```

## Quick start

```python
from beps import BEpsilonTree, WriteAmpStats

stats = WriteAmpStats()
tree = BEpsilonTree(node_size=16, epsilon=0.5, amp_stats=stats)

tree.put(b"hello", 1)
tree.put(b"help",  2)
tree.delete(b"help")
print(tree.get(b"hello"))         # 1
print(tree.get(b"help"))          # None

for k, v in tree.iter_range(b"a", b"z"):
    print(k, v)

print(stats.snapshot())
# {'leaf_applies': 2, 'buffer_inserts': 0, 'flushed_messages': 0, 'splits': 0,
#  'write_amplification': 1.0}
```

Online ε tuner:

```python
from beps import EpsilonTuner
from beps.tuner.observer import Op

t = EpsilonTuner(initial_epsilon=0.5, hysteresis=0.1)
for _ in range(1000):
    t.observe(Op.WRITE)
print(t.recommend())     # ≈ 0.84 (tuner pushed ε up toward eps_max)
```

## CLI

```bash
bepsctl info
bepsctl bench write
bepsctl bench read
```

## Architecture

### Tree (`src/beps/tree/`)

| Module | Role |
|---|---|
| `message.py` | `Op` (PUT/DEL), `Message` with monotone `seq` |
| `node.py`    | `LeafNode` (key/value/seq) + `InternalNode` (pivots/children/buffer) |
| `split.py`   | Leaf + internal-node splits (with buffer partitioning) |
| `tree.py`    | `BEpsilonTree`: put / get / delete / iter / flush_all |

### Tuner (`src/beps/tuner/`)

| Module | Role |
|---|---|
| `observer.py` | `WorkloadObserver`: sliding-window read/write counter |
| `epsilon.py`  | `EpsilonTuner`: linear ε = eps_min + (eps_max - eps_min) × write_fraction, with hysteresis |

### Stats (`src/beps/stats/`)

| Module | Role |
|---|---|
| `amplification.py` | `WriteAmpStats`: counts leaf_applies, buffer_inserts, flushed_messages, splits → write_amplification ratio |

### Workload (`src/beps/workload/`)

`mixed_workload`, `write_heavy`, `read_heavy` generators for benchmarks.

## How the algorithm works

1. **Insert** → place a `Message{op=PUT, key, value, seq}` into the root
   buffer. `seq` is a monotone counter that makes newest-wins
   unambiguous later.

2. **Buffer full** → flush. Group messages by which child they target;
   process **descending** child indices so splits don't shift the indices
   we're about to use; recurse if the child is internal.

3. **Leaf apply** → on reaching a leaf, mutate in-place: only overwrite
   if `msg.seq > leaf.seqs[i]`. This prevents older buffered messages
   from clobbering newer leaf state.

4. **Read** → walk root→leaf. At each internal node, peek the buffer for
   messages targeting the search key; remember the one with highest seq.
   At the leaf: compare against the leaf's `seq` for that key; the higher
   seq wins.

5. **Split** → leaf overflow promotes the new separator to the parent;
   internal-node split partitions the buffer by the new separator key
   (messages with key < sep go left, the rest go right).

## Why ε matters

| ε | space split | bias |
|---|---|---|
| 0.1 | 10% buffer / 90% pivots | B+-tree-like — read-fast, write-slow |
| 0.5 | 50/50 | balanced |
| 0.9 | 90% buffer / 10% pivots | sort-merge-like — write-fast, read-slow |

Asymptotic write amplification:
$$\text{write\_amp} = O\Big(\frac{\log_B N}{B^{1-\epsilon}}\Big)$$

We measure it empirically:

```
$ bepsctl bench write
 epsilon      n         ms        qps depth nodes  write_amp splits
     0.1  10000       40.6    246,313     5  1427       7.13   1422
     0.3  10000       29.0    344,659     5  1497       8.09   1492
     0.5  10000       26.7    373,900     6  1662       9.73   1656
     0.7  10000       28.5    350,566     7  1361      11.40   1354
     0.9  10000     1632.2      6,127   715 255970     713.79 255255
```

The ε=0.9 row is a **degenerate config** with `node_size=16`:
`pivot_capacity = node_size - buffer_capacity = 16 - 14 = 2`, so every
internal node holds at most 1 pivot → the tree degenerates to a near-list.
In production you'd pick `node_size` such that pivot capacity stays
healthy (e.g. `node_size=64, ε=0.5` → buffer=32, pivots=32).

## Reads

```
$ bepsctl bench read
 epsilon  reads         ms        qps depth    buf
     0.1   5000        4.6  1,082,524     5      0
     0.3   5000        5.7    873,839     5      3
     0.5   5000        6.2    801,539     6      7
     0.7   5000        5.9    849,973     7      6
     0.9   5000      961.2      5,202   715      1
```

Lookup cost grows with tree depth; the buffer-scan at each internal node
adds linear-in-buffer overhead. For pivot-heavy configs (low ε) you get
shallow trees → fast lookups.

## Correctness (tests)

- **Tree property tests** (Hypothesis): under any sequence of arbitrary
  put/delete operations, the tree's contents match a reference `dict`
- **Flush regression**: descending child-index iteration prevents the
  "split shifts indices" bug from losing buffered messages
- **Newest-wins**: leaf must NOT be overwritten by an older buffered
  message that arrives via flush after a newer write
- **Concurrent put/get**: tree is correct under 8 writer threads × 200 puts
- **Reader monotonicity**: under concurrent writer, reader observations
  are always valid values (no torn reads)
- **Amp stats are thread-safe** (originally had a deadlock; fixed by
  using `RLock`)

## Development

```bash
make install
make test          # 55 tests
make lint          # ruff
make typecheck     # mypy
make bench         # both benchmarks
docker compose run --rm beps make test
```

## Limitations / roadmap

- [ ] **Crash-consistent flushing** — currently in-memory only; persistent
      backing store needs a WAL or atomic page writes
- [ ] **B^ε-tree-of-B^ε-trees** — leaves themselves use buffers (BetrFS
      approach) for even better write amp
- [ ] **Adaptive node_size** — currently fixed; could grow on hot subtrees
- [ ] **Range query optimization** — current `iter_range` is a full scan
      with filter; should descend to lo and iterate forward

## References

- Bender et al., "An Introduction to B^ε-trees and Write-Optimization"
  (login Usenix Magazine 2015)
- Brodal & Fagerberg, "Lower Bounds for External Memory Dictionaries"
  (SODA 2003)
- Jannen et al., "BetrFS: A Right-Optimized Write-Optimized File System"
  (FAST 2015)

## License

MIT — see `LICENSE`.

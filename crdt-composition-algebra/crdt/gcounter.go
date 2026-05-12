// Package crdt implements concrete CRDTs built from the algebra package.
// Each CRDT provides:
//   - State type
//   - Lattice Ops (for algebraic composition)
//   - Mutators that return (new state, delta) pairs
//   - Value accessor
package crdt

import (
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/algebra"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
)

// GCounter is a grow-only counter. Each node maintains its own counter;
// the total value is the sum of all node counters.
// State = map[NodeID]uint64 under pointwise max join.
type GCounter struct {
	counts map[causal.NodeID]uint64
}

// NewGCounter returns a zero-valued G-Counter.
func NewGCounter() GCounter {
	return GCounter{counts: make(map[causal.NodeID]uint64)}
}

// GCounterOps is the semilattice for GCounter.
// Join = pointwise max; this is associative, commutative, and idempotent.
var GCounterOps = algebra.Ops[GCounter]{
	Join: func(a, b GCounter) GCounter {
		result := GCounter{counts: make(map[causal.NodeID]uint64, len(a.counts))}
		for k, v := range a.counts {
			result.counts[k] = v
		}
		for k, bv := range b.counts {
			if bv > result.counts[k] {
				result.counts[k] = bv
			}
		}
		return result
	},
	LessEq: func(a, b GCounter) bool {
		for k, av := range a.counts {
			if av > b.counts[k] {
				return false
			}
		}
		return true
	},
	Bottom: func() GCounter {
		return NewGCounter()
	},
	Equal: func(a, b GCounter) bool {
		if len(a.counts) != len(b.counts) {
			return false
		}
		for k, av := range a.counts {
			if av != b.counts[k] {
				return false
			}
		}
		return true
	},
}

// Increment returns (new state, delta) — the minimal delta is just the incremented entry.
func GCounterIncrement(state GCounter, node causal.NodeID) (GCounter, GCounter) {
	newVal := state.counts[node] + 1
	delta := GCounter{counts: map[causal.NodeID]uint64{node: newVal}}
	newState := GCounterOps.Join(state, delta)
	return newState, delta
}

// Value returns the total count (sum of all node counters).
func GCounterValue(g GCounter) uint64 {
	var total uint64
	for _, v := range g.counts {
		total += v
	}
	return total
}

// NodeValue returns the counter for a specific node.
func GCounterNodeValue(g GCounter, node causal.NodeID) uint64 {
	return g.counts[node]
}

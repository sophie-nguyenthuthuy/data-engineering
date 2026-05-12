package crdt

import (
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/algebra"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
)

// PNCounter is a positive-negative counter that supports both increment and decrement.
// It is composed algebraically as Product[GCounter, GCounter]:
//   - First  = increments (P counter)
//   - Second = decrements (N counter)
//
// Value = P.Value - N.Value
// This demonstrates algebraic composition: Lattice(PNCounter) derived from Lattice(GCounter)²
type PNCounter = algebra.Product[GCounter, GCounter]

// PNCounterOps is derived directly from GCounterOps via ProductOps.
// No additional convergence proof needed — the composition theorem guarantees it.
var PNCounterOps = algebra.ProductOps(GCounterOps, GCounterOps)

// NewPNCounter returns a zero-valued PN-Counter.
func NewPNCounter() PNCounter {
	return PNCounter{
		First:  NewGCounter(),
		Second: NewGCounter(),
	}
}

// PNCounterIncrement returns (new state, delta) for an increment.
func PNCounterIncrement(state PNCounter, node causal.NodeID) (PNCounter, PNCounter) {
	newP, deltaP := GCounterIncrement(state.First, node)
	delta := PNCounter{First: deltaP, Second: NewGCounter()}
	newState := PNCounterOps.Join(state, delta)
	_ = newP
	return newState, delta
}

// PNCounterDecrement returns (new state, delta) for a decrement.
func PNCounterDecrement(state PNCounter, node causal.NodeID) (PNCounter, PNCounter) {
	newN, deltaN := GCounterIncrement(state.Second, node)
	delta := PNCounter{First: NewGCounter(), Second: deltaN}
	newState := PNCounterOps.Join(state, delta)
	_ = newN
	return newState, delta
}

// PNCounterValue returns the current signed value.
func PNCounterValue(state PNCounter) int64 {
	return int64(GCounterValue(state.First)) - int64(GCounterValue(state.Second))
}

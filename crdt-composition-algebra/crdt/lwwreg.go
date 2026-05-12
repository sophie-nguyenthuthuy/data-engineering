package crdt

import (
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/algebra"
)

// LWWRegister is a Last-Write-Wins Register.
// Ties are broken by node ID (lexicographic) to ensure determinism.
// The lattice order is defined by timestamp; equal timestamps prefer higher nodeID.
type LWWValue[T any] struct {
	Value     T
	Timestamp int64
	NodeID    string
}

// LWWRegisterOps returns lattice ops for LWWRegister over any comparable value type.
// Join selects the value with the highest (timestamp, nodeID) — deterministic under concurrent writes.
func LWWRegisterOps[T any]() algebra.Ops[LWWValue[T]] {
	return algebra.Ops[LWWValue[T]]{
		Join: func(a, b LWWValue[T]) LWWValue[T] {
			if a.Timestamp > b.Timestamp {
				return a
			}
			if b.Timestamp > a.Timestamp {
				return b
			}
			// Tie-break by node ID for determinism
			if a.NodeID >= b.NodeID {
				return a
			}
			return b
		},
		LessEq: func(a, b LWWValue[T]) bool {
			if a.Timestamp < b.Timestamp {
				return true
			}
			if a.Timestamp > b.Timestamp {
				return false
			}
			return a.NodeID <= b.NodeID
		},
		Bottom: func() LWWValue[T] {
			return LWWValue[T]{Timestamp: 0}
		},
		Equal: func(a, b LWWValue[T]) bool {
			return a.Timestamp == b.Timestamp && a.NodeID == b.NodeID
		},
	}
}

// LWWWrite returns (new state, delta) for a write to an LWW register.
func LWWWrite[T any](state LWWValue[T], value T, ts int64, nodeID string) (LWWValue[T], LWWValue[T]) {
	ops := LWWRegisterOps[T]()
	delta := LWWValue[T]{Value: value, Timestamp: ts, NodeID: nodeID}
	newState := ops.Join(state, delta)
	return newState, delta
}

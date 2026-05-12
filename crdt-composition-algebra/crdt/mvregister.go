package crdt

import (
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/algebra"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
)

// MVRegister is a Multi-Value Register.
// Under concurrent writes, all concurrent values are retained.
// Under sequential writes, only the latest survives.
// Implemented as a dot map: dot → value.
type MVRegisterState[V any] struct {
	values map[causal.Dot]V
	cc     causal.Context
}

func NewMVRegister[V any]() MVRegisterState[V] {
	return MVRegisterState[V]{
		values: make(map[causal.Dot]V),
		cc:     causal.NewContext(),
	}
}

// MVRegisterOps returns lattice ops for MVRegister.
func MVRegisterOps[V any]() algebra.Ops[MVRegisterState[V]] {
	return algebra.Ops[MVRegisterState[V]]{
		Join: func(a, b MVRegisterState[V]) MVRegisterState[V] {
			result := MVRegisterState[V]{
				values: make(map[causal.Dot]V),
				cc:     a.cc.Join(b.cc),
			}
			// Keep a's values if b hasn't seen their dots (concurrent)
			for dot, val := range a.values {
				if !b.cc.Contains(dot) {
					result.values[dot] = val
				}
			}
			// Keep b's values if a hasn't seen their dots (concurrent)
			for dot, val := range b.values {
				if !a.cc.Contains(dot) {
					result.values[dot] = val
				}
			}
			// If both have the same dot, keep it (idempotency)
			for dot, val := range a.values {
				if _, ok := b.values[dot]; ok {
					result.values[dot] = val
				}
			}
			return result
		},
		LessEq: func(a, b MVRegisterState[V]) bool {
			return a.cc.LessEq(b.cc)
		},
		Bottom: func() MVRegisterState[V] {
			return NewMVRegister[V]()
		},
		Equal: func(a, b MVRegisterState[V]) bool {
			if len(a.values) != len(b.values) {
				return false
			}
			for dot := range a.values {
				if _, ok := b.values[dot]; !ok {
					return false
				}
			}
			return true
		},
	}
}

// MVRegisterWrite writes a value. Returns (new state, delta).
// The delta carries the new dot and clears all old dots (they're now causally dominated).
// We use PeekNext (not Next) so state.cc is NOT pre-advanced before the Join.
func MVRegisterWrite[V any](state MVRegisterState[V], value V, node causal.NodeID) (MVRegisterState[V], MVRegisterState[V]) {
	dot := state.cc.PeekNext(node)

	deltaCC := causal.NewContext()
	// Include all currently active dots in delta's cc to "dominate" them
	for d := range state.values {
		deltaCC.Add(d)
	}
	deltaCC.Add(dot)

	delta := MVRegisterState[V]{
		values: map[causal.Dot]V{dot: value},
		cc:     deltaCC,
	}

	newState := MVRegisterOps[V]().Join(state, delta)
	return newState, delta
}

// MVRegisterRead returns all current values (may be multiple under concurrent writes).
func MVRegisterRead[V any](state MVRegisterState[V]) []V {
	result := make([]V, 0, len(state.values))
	for _, v := range state.values {
		result = append(result, v)
	}
	return result
}

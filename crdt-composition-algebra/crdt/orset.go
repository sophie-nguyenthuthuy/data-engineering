package crdt

import (
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/algebra"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
)

// ORSet is an Observed-Remove Set.
// It supports add and remove, where "add wins" over concurrent remove.
// Implementation uses a dot map: each element maps to the set of dots that "witness" its presence.
// Remove is causal: it removes all dots currently witnessed, so concurrent adds survive.
//
// State representation: (dotMap, causalContext)
//   - dotMap[e] = set of dots for element e
//   - cc = all dots ever seen (for delta computation)
type ORSetState[E comparable] struct {
	// dots[e] = set of dots that currently witness e's presence
	dots map[E]causal.DotSet
	// cc = causal context: all events this replica has seen
	cc causal.Context
}

func NewORSet[E comparable]() ORSetState[E] {
	return ORSetState[E]{
		dots: make(map[E]causal.DotSet),
		cc:   causal.NewContext(),
	}
}

// ORSetOps returns lattice ops for ORSet.
// The join is: keep elements whose witness dots survive the merge.
func ORSetOps[E comparable]() algebra.Ops[ORSetState[E]] {
	return algebra.Ops[ORSetState[E]]{
		Join: func(a, b ORSetState[E]) ORSetState[E] {
			result := ORSetState[E]{
				dots: make(map[E]causal.DotSet),
				cc:   a.cc.Join(b.cc),
			}

			// For each element, keep dots that appear in both OR that appear
			// in one but weren't known to the other (meaning they weren't removed)
			allElems := make(map[E]struct{})
			for e := range a.dots {
				allElems[e] = struct{}{}
			}
			for e := range b.dots {
				allElems[e] = struct{}{}
			}

			for e := range allElems {
				aDots := a.dots[e]
				bDots := b.dots[e]

				// Keep a's dots if b hasn't seen them (b didn't remove them)
				surviving := make(causal.DotSet)
				for d := range aDots {
					if !b.cc.Contains(d) || bDots.Contains(d) {
						surviving[d] = struct{}{}
					}
				}
				// Keep b's dots if a hasn't seen them (a didn't remove them)
				for d := range bDots {
					if !a.cc.Contains(d) || aDots.Contains(d) {
						surviving[d] = struct{}{}
					}
				}

				if len(surviving) > 0 {
					result.dots[e] = surviving
				}
			}
			return result
		},
		LessEq: func(a, b ORSetState[E]) bool {
			// a ≤ b iff all of a's events are in b's causal context
			return a.cc.LessEq(b.cc)
		},
		Bottom: func() ORSetState[E] {
			return NewORSet[E]()
		},
		Equal: func(a, b ORSetState[E]) bool {
			if len(a.dots) != len(b.dots) {
				return false
			}
			for e, aDots := range a.dots {
				bDots, ok := b.dots[e]
				if !ok || len(aDots) != len(bDots) {
					return false
				}
				for d := range aDots {
					if !bDots.Contains(d) {
						return false
					}
				}
			}
			return true
		},
	}
}

// ORSetAdd adds an element. Returns (new state, delta).
// The delta contains just the new dot for this element.
// We use PeekNext (not Next) so state.cc is NOT pre-advanced — the Join updates it.
// If we used Next, state.cc would already contain the new dot before Join, causing
// the join to treat the new dot as "seen and removed" on both sides.
func ORSetAdd[E comparable](state ORSetState[E], elem E, node causal.NodeID) (ORSetState[E], ORSetState[E]) {
	dot := state.cc.PeekNext(node)

	// Delta: just this element with this dot, plus the dot in cc
	deltaCC := causal.NewContext()
	deltaCC.Add(dot)
	delta := ORSetState[E]{
		dots: map[E]causal.DotSet{elem: causal.NewDotSet(dot)},
		cc:   deltaCC,
	}

	newState := ORSetOps[E]().Join(state, delta)
	return newState, delta
}

// ORSetRemove removes an element. Returns (new state, delta).
// The delta's cc contains all currently witnessing dots (marking them as "seen and removed").
func ORSetRemove[E comparable](state ORSetState[E], elem E, _ causal.NodeID) (ORSetState[E], ORSetState[E]) {
	// Delta: empty element map, but cc contains all dots currently witnessing elem
	deltaCC := causal.NewContext()
	for d := range state.dots[elem] {
		deltaCC.Add(d)
	}
	delta := ORSetState[E]{
		dots: make(map[E]causal.DotSet),
		cc:   deltaCC,
	}

	newState := ORSetOps[E]().Join(state, delta)
	return newState, delta
}

// ORSetContains returns true if elem is in the set.
func ORSetContains[E comparable](state ORSetState[E], elem E) bool {
	return len(state.dots[elem]) > 0
}

// ORSetElements returns all current elements.
func ORSetElements[E comparable](state ORSetState[E]) []E {
	result := make([]E, 0, len(state.dots))
	for e, dots := range state.dots {
		if len(dots) > 0 {
			result = append(result, e)
		}
	}
	return result
}

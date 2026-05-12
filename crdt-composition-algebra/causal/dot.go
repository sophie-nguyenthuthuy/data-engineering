// Package causal implements causal consistency metadata for delta-state CRDTs.
// We use Dotted Version Vectors (DVV) — a compact representation of causal history.
// Each "dot" (nodeID, counter) uniquely identifies one event in the system.
package causal

import "fmt"

// NodeID uniquely identifies a replica in the distributed system.
type NodeID string

// Dot is a (node, counter) pair that uniquely identifies a single event.
// Every write to a CRDT produces exactly one new dot.
type Dot struct {
	Node    NodeID
	Counter uint64
}

func (d Dot) String() string {
	return fmt.Sprintf("%s:%d", d.Node, d.Counter)
}

// DotSet is a set of dots, used to track which events are in a delta or state.
type DotSet map[Dot]struct{}

func NewDotSet(dots ...Dot) DotSet {
	ds := make(DotSet, len(dots))
	for _, d := range dots {
		ds[d] = struct{}{}
	}
	return ds
}

func (ds DotSet) Add(d Dot) DotSet {
	result := make(DotSet, len(ds)+1)
	for k := range ds {
		result[k] = struct{}{}
	}
	result[d] = struct{}{}
	return result
}

func (ds DotSet) Contains(d Dot) bool {
	_, ok := ds[d]
	return ok
}

func (ds DotSet) Union(other DotSet) DotSet {
	result := make(DotSet, len(ds)+len(other))
	for k := range ds {
		result[k] = struct{}{}
	}
	for k := range other {
		result[k] = struct{}{}
	}
	return result
}

func (ds DotSet) Intersection(other DotSet) DotSet {
	result := make(DotSet)
	for k := range ds {
		if other.Contains(k) {
			result[k] = struct{}{}
		}
	}
	return result
}

func (ds DotSet) Difference(other DotSet) DotSet {
	result := make(DotSet)
	for k := range ds {
		if !other.Contains(k) {
			result[k] = struct{}{}
		}
	}
	return result
}

func (ds DotSet) Clone() DotSet {
	result := make(DotSet, len(ds))
	for k := range ds {
		result[k] = struct{}{}
	}
	return result
}

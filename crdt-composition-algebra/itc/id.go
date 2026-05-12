// Package itc implements Interval Tree Clocks (ITC).
//
// ITC is a causality tracking mechanism that solves the scalability problem of
// vector clocks. Instead of O(n) per-node counters (which require knowing all nodes
// in advance), ITC dynamically splits and joins "ID intervals" so each node
// owns a unique portion of the causal identity space.
//
// Key property: metadata size is bounded by the number of active nodes,
// not the total number of nodes ever created. This makes it ideal for
// dynamic distributed systems where nodes join and leave.
//
// Reference: "Interval Tree Clocks: A Logical Clock for Dynamic Systems"
// Almeida, Baquero, Fonte (2008)
package itc

import "fmt"

// ID represents ownership of a portion of the causal identity space [0, 1].
// It's a binary tree where:
//   - Leaf(0) = owns nothing
//   - Leaf(1) = owns the entire interval at this level
//   - Node(L, R) = L owns left half, R owns right half
type ID struct {
	isLeaf bool
	leaf   int  // 0 or 1, only valid if isLeaf
	left   *ID
	right  *ID
}

var (
	IDZero = &ID{isLeaf: true, leaf: 0} // owns nothing
	IDOne  = &ID{isLeaf: true, leaf: 1} // owns everything
)

// leafID creates a leaf ID node.
func leafID(v int) *ID { return &ID{isLeaf: true, leaf: v} }

// nodeID creates an internal ID node.
func nodeID(l, r *ID) *ID { return &ID{left: l, right: r} }

// Clone deep-copies an ID tree.
func (id *ID) Clone() *ID {
	if id == nil {
		return nil
	}
	if id.isLeaf {
		return leafID(id.leaf)
	}
	return nodeID(id.left.Clone(), id.right.Clone())
}

// IsZero returns true if this ID owns no portion of the space.
func (id *ID) IsZero() bool {
	if id.isLeaf {
		return id.leaf == 0
	}
	return id.left.IsZero() && id.right.IsZero()
}

// normalizeID simplifies an ID tree.
// Node(0,0) → 0; Node(1,1) → 1
func normalizeID(id *ID) *ID {
	if id.isLeaf {
		return id
	}
	l := normalizeID(id.left)
	r := normalizeID(id.right)
	if l.isLeaf && r.isLeaf && l.leaf == r.leaf {
		return leafID(l.leaf)
	}
	return nodeID(l, r)
}

// SplitID splits an ID into two disjoint IDs that together cover the same space.
// This is the "fork" operation: creates a new identity for a child node.
//
// After split: original ID is retired, two new IDs each own half the space.
func SplitID(id *ID) (*ID, *ID) {
	if id.isLeaf {
		if id.leaf == 0 {
			return leafID(0), leafID(0)
		}
		// Split 1 into (Node(1,0), Node(0,1))
		return nodeID(leafID(1), leafID(0)), nodeID(leafID(0), leafID(1))
	}

	if id.left.IsZero() {
		l, r := SplitID(id.right)
		return nodeID(leafID(0), l), nodeID(leafID(0), r)
	}
	if id.right.IsZero() {
		l, r := SplitID(id.left)
		return nodeID(l, leafID(0)), nodeID(r, leafID(0))
	}
	return nodeID(id.left.Clone(), leafID(0)), nodeID(leafID(0), id.right.Clone())
}

// JoinID merges two disjoint IDs back into one.
// Used when two nodes merge (one is absorbed by the other).
func JoinID(a, b *ID) *ID {
	if a.isLeaf && b.isLeaf {
		if a.leaf == 0 {
			return b.Clone()
		}
		if b.leaf == 0 {
			return a.Clone()
		}
		return leafID(1)
	}
	if a.isLeaf {
		if a.leaf == 0 {
			return b.Clone()
		}
		return leafID(1)
	}
	if b.isLeaf {
		if b.leaf == 0 {
			return a.Clone()
		}
		return leafID(1)
	}
	return normalizeID(nodeID(JoinID(a.left, b.left), JoinID(a.right, b.right)))
}

func (id *ID) String() string {
	if id.isLeaf {
		return fmt.Sprintf("%d", id.leaf)
	}
	return fmt.Sprintf("(%s,%s)", id.left, id.right)
}

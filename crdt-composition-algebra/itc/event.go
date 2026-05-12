package itc

import "fmt"

// Event records which events have occurred in the causal history.
// It's a binary tree where:
//   - Leaf(n) = all events up to counter n at this level
//   - Node(n, L, R) = base n events, plus L events in left half, R events in right half
//
// The tree is compressed: Node(n, Leaf(0), Leaf(0)) normalizes to Leaf(n).
type Event struct {
	n      int
	isLeaf bool
	left   *Event
	right  *Event
}

// leafEvent creates a leaf event node with counter n.
func leafEvent(n int) *Event { return &Event{n: n, isLeaf: true} }

// nodeEvent creates an internal event node.
func nodeEvent(n int, l, r *Event) *Event { return &Event{n: n, left: l, right: r} }

// Clone deep-copies an Event tree.
func (e *Event) Clone() *Event {
	if e == nil {
		return nil
	}
	if e.isLeaf {
		return leafEvent(e.n)
	}
	return nodeEvent(e.n, e.left.Clone(), e.right.Clone())
}

// Max returns the maximum event counter in this subtree.
func (e *Event) Max() int {
	if e.isLeaf {
		return e.n
	}
	l, r := e.left.Max(), e.right.Max()
	if l > r {
		return e.n + l
	}
	return e.n + r
}

// Min returns the minimum event counter in this subtree.
func (e *Event) Min() int {
	if e.isLeaf {
		return e.n
	}
	l, r := e.left.Min(), e.right.Min()
	if l < r {
		return e.n + l
	}
	return e.n + r
}

// lift adds k to all counters in this event tree.
func lift(e *Event, k int) *Event {
	if e.isLeaf {
		return leafEvent(e.n + k)
	}
	return nodeEvent(e.n+k, e.left.Clone(), e.right.Clone())
}

// sink subtracts k from the base counter (used during normalization).
func sink(e *Event, k int) *Event {
	if e.isLeaf {
		return leafEvent(e.n - k)
	}
	return nodeEvent(e.n-k, e.left.Clone(), e.right.Clone())
}

// normalizeEvent compresses an event tree:
//   - Node(n, Leaf(m), Leaf(m)) → Leaf(n+m)
//   - Node(n, L, R) where min(L,R) > 0 → sink min and add to n
func normalizeEvent(e *Event) *Event {
	if e.isLeaf {
		return e
	}
	l := normalizeEvent(e.left)
	r := normalizeEvent(e.right)

	if l.isLeaf && r.isLeaf && l.n == r.n {
		return leafEvent(e.n + l.n)
	}

	m := l.Min()
	if rm := r.Min(); rm < m {
		m = rm
	}
	if m == 0 {
		return nodeEvent(e.n, l, r)
	}
	return nodeEvent(e.n+m, sink(l, m), sink(r, m))
}

// LeqEvent returns true if event tree a is causally ≤ b.
// Used to determine if one stamp's history is subsumed by another's.
func LeqEvent(a, b *Event) bool {
	if a.isLeaf {
		return a.n <= b.n
	}
	if b.isLeaf {
		an, bn := a.n, b.n
		if an > bn {
			return false
		}
		al := lift(a.left, an)
		ar := lift(a.right, an)
		bl := leafEvent(bn)
		br := leafEvent(bn)
		return LeqEvent(al, bl) && LeqEvent(ar, br)
	}
	// Both are nodes
	an, bn := a.n, b.n
	if an > bn {
		// Adjust b upward
		bl := lift(b.left, bn)
		br := lift(b.right, bn)
		al := lift(a.left, an)
		ar := lift(a.right, an)
		return LeqEvent(al, bl) && LeqEvent(ar, br)
	}
	// an <= bn: adjust a upward
	bl := lift(b.left, bn)
	br := lift(b.right, bn)
	al := lift(a.left, an)
	ar := lift(a.right, an)
	return LeqEvent(al, bl) && LeqEvent(ar, br)
}

// JoinEvent merges two event trees (union of known events).
func JoinEvent(a, b *Event) *Event {
	if a.isLeaf && b.isLeaf {
		if a.n > b.n {
			return leafEvent(a.n)
		}
		return leafEvent(b.n)
	}
	if a.isLeaf {
		// Expand a into a node
		a = nodeEvent(a.n, leafEvent(0), leafEvent(0))
	}
	if b.isLeaf {
		b = nodeEvent(b.n, leafEvent(0), leafEvent(0))
	}

	// Normalize to same base
	an, bn := a.n, b.n
	var base int
	var al, ar, bl, br *Event
	if an >= bn {
		base = bn
		al = lift(a.left, an-bn)
		ar = lift(a.right, an-bn)
		bl = b.left.Clone()
		br = b.right.Clone()
	} else {
		base = an
		al = a.left.Clone()
		ar = a.right.Clone()
		bl = lift(b.left, bn-an)
		br = lift(b.right, bn-an)
	}

	return normalizeEvent(nodeEvent(base, JoinEvent(al, bl), JoinEvent(ar, br)))
}

// FillEvent "fills" an event tree under a given ID.
// This is the "event" operation: record that something happened under the ID's portion.
// Returns the new event tree after recording events in the ID-owned regions.
func FillEvent(id *ID, e *Event) *Event {
	if id.isLeaf {
		if id.leaf == 0 {
			return e.Clone() // owns nothing, no change
		}
		// owns everything: fill up to max
		return leafEvent(e.Max())
	}
	if e.isLeaf {
		// Expand e into a node
		e = nodeEvent(e.n, leafEvent(0), leafEvent(0))
	}

	en := e.n
	el := lift(e.left, en)
	er := lift(e.right, en)
	fl := FillEvent(id.left, el)
	fr := FillEvent(id.right, er)

	return normalizeEvent(nodeEvent(0, fl, fr))
}

// GrowEvent finds the "smallest" inflation of e such that the ID-owned portion increases.
// Returns (new event tree, cost of inflation).
// This minimizes the number of new event nodes created.
func GrowEvent(id *ID, e *Event) (*Event, int) {
	if id.isLeaf {
		if id.leaf == 1 {
			if e.isLeaf {
				return leafEvent(e.n + 1), 0
			}
			return leafEvent(e.Max() + 1), 1000 // prefer simple inflation
		}
		return e.Clone(), 1<<31 - 1 // can't grow with id=0
	}
	if e.isLeaf {
		e = nodeEvent(e.n, leafEvent(0), leafEvent(0))
	}

	en := e.n
	el := lift(e.left, en)
	er := lift(e.right, en)

	gl, cl := GrowEvent(id.left, sink(el, en))
	gr, cr := GrowEvent(id.right, sink(er, en))

	if cl < cr {
		newE := normalizeEvent(nodeEvent(en, gl, sink(er, en)))
		return newE, cl + 1
	}
	newE := normalizeEvent(nodeEvent(en, sink(el, en), gr))
	return newE, cr + 1
}

func (e *Event) String() string {
	if e.isLeaf {
		return fmt.Sprintf("%d", e.n)
	}
	return fmt.Sprintf("(%d,%s,%s)", e.n, e.left, e.right)
}

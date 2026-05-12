package itc

import "fmt"

// Stamp is the core ITC structure: (ID, Event).
//   - ID: which portion of causal space this node owns
//   - Event: which events this node knows about
//
// Invariant: a stamp's event tree must be ≥ all events for IDs it owns.
type Stamp struct {
	ID    *ID
	Event *Event
}

// Seed returns the initial stamp for bootstrapping a new distributed system.
// The seed owns the entire ID space and has seen zero events.
func Seed() Stamp {
	return Stamp{
		ID:    IDOne.Clone(),
		Event: leafEvent(0),
	}
}

// Peek returns an anonymous copy (no ID ownership) useful for observation.
func Peek(s Stamp) Stamp {
	return Stamp{ID: IDZero.Clone(), Event: s.Event.Clone()}
}

// Fork splits this stamp into two stamps, each owning half the ID space.
// The parent stamp's ID is retired after forking.
// Use this when a new replica joins the system.
func Fork(s Stamp) (Stamp, Stamp) {
	id1, id2 := SplitID(s.ID)
	return Stamp{ID: id1, Event: s.Event.Clone()},
		Stamp{ID: id2, Event: s.Event.Clone()}
}

// RecordEvent records that this node performed an event (a write/mutation).
// Returns a new stamp with the event recorded.
// The algorithm fills as much as possible and then grows minimally.
func RecordEvent(s Stamp) Stamp {
	// First try to fill: if ID owns a region, mark all events up to max there
	filled := FillEvent(s.ID, s.Event)
	if !LeqEvent(s.Event, filled) || filled.Max() > s.Event.Max() {
		return Stamp{ID: s.ID.Clone(), Event: normalizeEvent(filled)}
	}
	// Fill didn't help, grow instead
	grown, _ := GrowEvent(s.ID, s.Event)
	return Stamp{ID: s.ID.Clone(), Event: normalizeEvent(grown)}
}

// Join merges two stamps (from different replicas after communication).
// The joined stamp owns the union of both IDs and has seen the union of events.
// Use this when two nodes synchronize.
func Join(a, b Stamp) Stamp {
	return Stamp{
		ID:    JoinID(a.ID, b.ID),
		Event: normalizeEvent(JoinEvent(a.Event, b.Event)),
	}
}

// Leq returns true if stamp a's event history is causally ≤ stamp b's.
// a ≤ b means "b has seen everything a has seen, and possibly more".
func Leq(a, b Stamp) bool {
	return LeqEvent(a.Event, b.Event)
}

// Concurrent returns true if neither a ≤ b nor b ≤ a.
// Concurrent stamps represent events that happened in parallel partitions.
func Concurrent(a, b Stamp) bool {
	return !Leq(a, b) && !Leq(b, a)
}

// MetadataSize returns a measure of the stamp's metadata cost.
// This is bounded by the number of active forks × 2 (one ID node + one Event node per fork level).
func MetadataSize(s Stamp) int {
	return idSize(s.ID) + eventSize(s.Event)
}

func idSize(id *ID) int {
	if id.isLeaf {
		return 1
	}
	return 1 + idSize(id.left) + idSize(id.right)
}

func eventSize(e *Event) int {
	if e.isLeaf {
		return 1
	}
	return 1 + eventSize(e.left) + eventSize(e.right)
}

func (s Stamp) String() string {
	return fmt.Sprintf("(%s, %s)", s.ID, s.Event)
}
